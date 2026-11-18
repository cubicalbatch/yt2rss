"""Thread safety of the UI log buffer and refresh status reads.

Queue, refresh and scheduler threads append UI log lines while request
threads read them, and reserving a refresh clears them. These tests hammer
the buffer from many threads at once with a tiny thread switch interval so
updates genuinely interleave. Every join has a deadline so a deadlock fails
the test instead of hanging the suite. Nothing here touches the network.
"""

import logging
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest
from flask.testing import FlaskClient

from src.web_server import UILogHandler, YouTubePodcastServer
from tests.test_web_server import app_config as app_config
from tests.test_web_server import client as client
from tests.test_web_server import test_server as test_server

WRITERS = 8
MESSAGES_PER_WRITER = 500
READERS = 2
CHURN_LOG_WRITERS = 2
LOG_CAP = 100
REFRESH_CYCLES = 50
JOIN_TIMEOUT_SECONDS = 30.0
SWITCH_INTERVAL_SECONDS = 1e-6
CLEAR_PAUSE_SECONDS = 0.001
ENTRY_FIELDS = {"timestamp", "message"}
STATUS_FIELDS = {"running", "start_time", "duration", "logs", "queue"}


@pytest.fixture
def fast_thread_switching() -> Iterator[None]:
    """Switch threads very often so racing updates genuinely interleave."""
    previous = sys.getswitchinterval()
    sys.setswitchinterval(SWITCH_INTERVAL_SECONDS)
    yield
    sys.setswitchinterval(previous)


@pytest.fixture
def ui_logger(
    test_server: YouTubePodcastServer,
) -> Iterator[tuple[logging.Logger, UILogHandler]]:
    """Provide an isolated DEBUG logger wired to a ``UILogHandler``."""
    handler = UILogHandler(test_server)
    logger = logging.getLogger(f"yt2rss.tests.ui_logs.{id(test_server)}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(handler)
    yield logger, handler
    logger.removeHandler(handler)


def message_for(writer: int, seq: int) -> str:
    """Build the log message a writer emits for one sequence number."""
    return f"writer-{writer}-{seq:04d}"


def parse_message(message: str) -> tuple[int, int]:
    """Split a writer message into its writer index and sequence number."""
    _, writer, seq = message.split("-")
    return int(writer), int(seq)


def logged_messages(server: YouTubePodcastServer) -> list[str]:
    """Return the buffered UI log messages, oldest first."""
    return [entry["message"] for entry in server._log_snapshot()]


def log_writer(server: YouTubePodcastServer, writer: int) -> Callable[[], None]:
    """Build a thread target appending one writer's numbered messages."""

    def write() -> None:
        for seq in range(MESSAGES_PER_WRITER):
            server._add_log_message(message_for(writer, seq))

    return write


def check_entries(entries: Any) -> None:
    """Assert log entries form a capped list, ordered per writer.

    Args:
        entries: A log snapshot or the ``logs`` field of a status response.
    """
    assert isinstance(entries, list)
    assert len(entries) <= LOG_CAP
    last_seq: dict[int, int] = {}
    for entry in entries:
        assert isinstance(entry, dict)
        assert set(entry) == ENTRY_FIELDS
        assert isinstance(entry["timestamp"], str)
        writer, seq = parse_message(entry["message"])
        assert seq > last_seq.get(writer, -1)
        last_seq[writer] = seq


def status_poller(status_client: FlaskClient) -> Callable[[], None]:
    """Build a poller checking ``/api/refresh/status`` responses."""

    def poll() -> None:
        response = status_client.get("/api/refresh/status")
        assert response.status_code == 200
        data = response.get_json()
        assert set(data) == STATUS_FIELDS
        logs = [entry for entry in data["logs"] if entry["message"] != "started"]
        check_entries(logs)

    return poll


def join_all(threads: list[threading.Thread]) -> None:
    """Join threads against one shared deadline, failing on stragglers."""
    deadline = time.monotonic() + JOIN_TIMEOUT_SECONDS
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    stuck = [thread.name for thread in threads if thread.is_alive()]
    assert not stuck, f"threads did not finish: {stuck}"


def race(
    writers: list[Callable[[], None]], pollers: list[Callable[[], None]]
) -> list[BaseException]:
    """Run writers once and pollers repeatedly until every writer finishes.

    All threads start together from a barrier; each poller runs once more
    after the writers finish.

    Args:
        writers: Thread targets run exactly once.
        pollers: Thread targets run in a loop while writers are active.

    Returns:
        The exceptions raised in any thread.
    """
    errors: list[BaseException] = []
    start = threading.Barrier(len(writers) + len(pollers))
    writers_done = threading.Event()

    def run_writer(writer: Callable[[], None]) -> None:
        try:
            start.wait(JOIN_TIMEOUT_SECONDS)
            writer()
        except BaseException as error:
            errors.append(error)

    def run_poller(poller: Callable[[], None]) -> None:
        try:
            start.wait(JOIN_TIMEOUT_SECONDS)
            while not writers_done.is_set():
                poller()
            poller()
        except BaseException as error:
            errors.append(error)

    writer_threads = [
        threading.Thread(target=run_writer, args=(writer,), daemon=True)
        for writer in writers
    ]
    poller_threads = [
        threading.Thread(target=run_poller, args=(poller,), daemon=True)
        for poller in pollers
    ]
    for thread in writer_threads + poller_threads:
        thread.start()
    try:
        join_all(writer_threads)
    finally:
        writers_done.set()
    join_all(poller_threads)
    return errors


def test_concurrent_appends_snapshots_and_clears_stay_consistent(
    test_server: YouTubePodcastServer, fast_thread_switching: None
) -> None:
    """Racing writers, readers and clears never corrupt or overfill the log."""

    def take_snapshot() -> None:
        check_entries(test_server._log_snapshot())

    def clear_now_and_then() -> None:
        test_server._clear_logs()
        time.sleep(CLEAR_PAUSE_SECONDS)

    writers = [log_writer(test_server, writer) for writer in range(WRITERS)]
    pollers = [take_snapshot] * READERS + [clear_now_and_then]

    errors = race(writers, pollers)

    assert not errors, errors[:3]
    check_entries(test_server._log_snapshot())


def test_concurrent_appends_keep_newest_entries_per_writer(
    test_server: YouTubePodcastServer, fast_thread_switching: None
) -> None:
    """Without clears, racing writers lose nothing among the newest entries."""

    def take_snapshot() -> None:
        check_entries(test_server._log_snapshot())

    writers = [log_writer(test_server, writer) for writer in range(WRITERS)]

    errors = race(writers, [take_snapshot] * READERS)

    assert not errors, errors[:3]
    snapshot = test_server._log_snapshot()
    assert len(snapshot) == LOG_CAP
    retained: defaultdict[int, list[int]] = defaultdict(list)
    for entry in snapshot:
        writer, seq = parse_message(entry["message"])
        retained[writer].append(seq)
    for seqs in retained.values():
        newest = list(range(MESSAGES_PER_WRITER - len(seqs), MESSAGES_PER_WRITER))
        assert seqs == newest


def test_status_polling_during_refresh_churn_does_not_deadlock(
    test_server: YouTubePodcastServer, fast_thread_switching: None
) -> None:
    """Refresh reservations, log writes and status requests interleave freely."""

    def churn_refreshes() -> None:
        for _ in range(REFRESH_CYCLES):
            if test_server._begin_refresh():
                test_server._add_log_message("started")
                test_server._finish_refresh()

    writers = [churn_refreshes] + [
        log_writer(test_server, writer) for writer in range(CHURN_LOG_WRITERS)
    ]
    pollers = [status_poller(test_server.app.test_client()) for _ in range(READERS)]

    errors = race(writers, pollers)

    assert not errors, errors[:3]
    assert test_server._refresh_status_snapshot()["running"] is False


def test_sequential_appends_keep_newest_entries(
    test_server: YouTubePodcastServer,
) -> None:
    """The buffer keeps only the newest ``max_logs`` entries, oldest first."""
    total = LOG_CAP * 2 + 5
    for seq in range(total):
        test_server._add_log_message(f"message {seq}")

    snapshot = test_server._log_snapshot()

    assert test_server.max_logs == LOG_CAP
    assert [entry["message"] for entry in snapshot] == [
        f"message {seq}" for seq in range(total - LOG_CAP, total)
    ]
    for entry in snapshot:
        assert set(entry) == ENTRY_FIELDS
        datetime.fromisoformat(entry["timestamp"])


def test_snapshot_is_a_detached_copy(test_server: YouTubePodcastServer) -> None:
    """Changing a snapshot leaves the buffer untouched."""
    test_server._add_log_message("kept")

    snapshot = test_server._log_snapshot()
    snapshot.clear()

    assert logged_messages(test_server) == ["kept"]


def test_clear_logs_empties_buffer(test_server: YouTubePodcastServer) -> None:
    """Clearing drops every entry and later appends start fresh."""
    for seq in range(10):
        test_server._add_log_message(f"message {seq}")

    test_server._clear_logs()

    assert test_server._log_snapshot() == []
    test_server._add_log_message("after clear")
    assert logged_messages(test_server) == ["after clear"]


def test_begin_refresh_clears_logs(test_server: YouTubePodcastServer) -> None:
    """Reserving a refresh clears stale log lines under the state lock."""
    test_server._add_log_message("stale")

    assert test_server._begin_refresh()
    try:
        assert test_server._log_snapshot() == []
        assert test_server._refresh_status_snapshot()["running"] is True
    finally:
        test_server._finish_refresh()

    assert test_server._refresh_status_snapshot()["running"] is False


def test_refresh_status_returns_bounded_log_list(
    client: FlaskClient, test_server: YouTubePodcastServer
) -> None:
    """The status endpoint keeps its shape and caps the log list."""
    total = LOG_CAP + 20
    for seq in range(total):
        test_server._add_log_message(f"message {seq}")

    response = client.get("/api/refresh/status")

    assert response.status_code == 200
    data = response.get_json()
    assert set(data) == STATUS_FIELDS
    assert data["running"] is False
    assert data["duration"] == 0
    assert isinstance(data["queue"], dict)
    assert isinstance(data["logs"], list)
    assert len(data["logs"]) == LOG_CAP
    assert data["logs"][0]["message"] == f"message {total - LOG_CAP}"
    assert data["logs"][-1]["message"] == f"message {total - 1}"


def test_refresh_status_reports_elapsed_time_while_running(
    client: FlaskClient, test_server: YouTubePodcastServer
) -> None:
    """A running refresh reports its elapsed duration from the snapshot."""
    assert test_server._begin_refresh()
    try:
        with test_server._state_lock:
            test_server.refresh_status["start_time"] = time.time() - 5
        data = client.get("/api/refresh/status").get_json()
    finally:
        test_server._finish_refresh()

    assert data["running"] is True
    assert data["duration"] >= 5
    assert data["logs"] == []


def test_ui_log_handler_forwards_info_and_above(
    test_server: YouTubePodcastServer,
    ui_logger: tuple[logging.Logger, UILogHandler],
) -> None:
    """INFO and above reach the UI buffer as bare messages; DEBUG does not."""
    logger, _ = ui_logger

    logger.debug("debug detail")
    logger.info("info %s", "message")
    logger.warning("warning message")
    logger.error("error message")

    assert logged_messages(test_server) == [
        "info message",
        "warning message",
        "error message",
    ]


def test_ui_log_handler_reports_failures_via_handle_error(
    test_server: YouTubePodcastServer,
    ui_logger: tuple[logging.Logger, UILogHandler],
) -> None:
    """A failing buffer write is handed to ``handleError`` instead of raising."""
    logger, handler = ui_logger

    with (
        patch.object(test_server, "_add_log_message", side_effect=RuntimeError("boom")),
        patch.object(handler, "handleError") as handle_error,
    ):
        logger.info("cannot be stored")

    handle_error.assert_called_once()
    record = handle_error.call_args.args[0]
    assert isinstance(record, logging.LogRecord)
    assert record.getMessage() == "cannot be stored"
    assert logged_messages(test_server) == []


def test_ui_log_handler_failure_is_reported_on_stderr(
    test_server: YouTubePodcastServer,
    ui_logger: tuple[logging.Logger, UILogHandler],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The standard ``handleError`` reports the failure without raising."""
    logger, _ = ui_logger

    with patch.object(
        test_server, "_add_log_message", side_effect=RuntimeError("boom")
    ):
        logger.info("cannot be stored")

    assert "RuntimeError: boom" in capsys.readouterr().err


def test_debug_records_skip_the_buffer_entirely(
    test_server: YouTubePodcastServer,
    ui_logger: tuple[logging.Logger, UILogHandler],
) -> None:
    """DEBUG records never call into the log buffer."""
    logger, _ = ui_logger

    with patch.object(test_server, "_add_log_message") as add_log_message:
        logger.debug("debug detail")

    add_log_message.assert_not_called()
