"""Cross-thread and cross-process locking of JSON state files.

The web server and the cron runner may share one config directory, so every
read-modify-write of ``health.json``, ``refresh_timestamps.json``,
``pins.json`` and ``tombstones.json`` holds :func:`state_file_lock`.

Child processes use the ``spawn`` context and wait on a barrier so their
updates genuinely overlap. Their targets are module-level functions, and
every join has a deadline so a regression fails instead of hanging the suite.
Children replace ``os.fsync`` with a short fixed pause: durability is not
under test, and the pause keeps a race window open between each read and
the atomic replace without depending on disk latency. Nothing here touches
the network.
"""

import multiprocessing
import os
import threading
import time
from collections.abc import Callable
from multiprocessing.synchronize import Barrier
from pathlib import Path
from unittest.mock import patch

import pytest

from src import utils
from src.health import (
    HISTORY_LIMIT,
    dispatch_events,
    load_health,
    record_refresh,
)
from src.rss_generator import RSSGenerator
from src.storage import (
    load_pins,
    load_tombstones,
    record_tombstone,
    set_episode_pinned,
)
from src.utils import atomic_write_text, state_file_lock, state_lock_path

try:
    import fcntl
except ImportError:  # pragma: no cover - POSIX test hosts have fcntl.
    fcntl = None

requires_flock = pytest.mark.skipif(fcntl is None, reason="needs fcntl.flock")

PROCESSES = 4
UPDATES_PER_PROCESS = 8
THREADS = 4
INCREMENTS_PER_WORKER = 20
RACE_PAUSE_SECONDS = 0.001
TIMEOUT_SECONDS = 30.0
BLOCK_PROBE_SECONDS = 0.2
# Placeholder only: deliveries are patched out in the worker.
UNUSED_WEBHOOK_URL = "http://127.0.0.1:9/unused"


# ---------- workers (module level so spawned children can import them) ----------


def _increment_counter(path: Path) -> None:
    """Read, pause, then write the counter plus one: a lost-update race."""
    value = int(path.read_text()) if path.exists() else 0
    time.sleep(RACE_PAUSE_SECONDS)
    path.write_text(str(value + 1))


def _locked_increments(path: Path, count: int) -> None:
    """Increment the counter ``count`` times under its state lock."""
    for _ in range(count):
        with state_file_lock(path) as locked:
            assert locked
            _increment_counter(path)


def _counter_writer(target: str, worker: int, count: int) -> None:
    """Increment the counter file ``target``."""
    _locked_increments(Path(target), count)


def _state_writer(target: str, worker: int, count: int) -> None:
    """Interleave every state writer the server and the cron runner use.

    Each worker records refreshes of its own channels plus a shared one,
    notification bookkeeping, refresh times, pins and tombstones in the
    config directory ``target``; channels ``ch0``/``ch1`` are shared.
    """
    generator = RSSGenerator("http://localhost")
    generator.refresh_timestamps_file = Path(target) / "refresh_timestamps.json"
    with patch("src.health._post_webhook"):
        for index in range(count):
            record_refresh(target, f"w{worker}_c{index}", success=True)
            record_refresh(target, "shared", success=False, error="boom")
            event = {"key": f"w{worker}_e{index}", "event": "channel_failure"}
            dispatch_events(target, [event], UNUSED_WEBHOOK_URL)
            generator.update_channel_refresh_time(f"w{worker}_c{index}")
            channel, video = f"ch{worker % 2}", f"w{worker}_v{index}"
            assert set_episode_pinned(target, channel, video, True)
            assert record_tombstone(target, channel, video, "Deleted")


WRITERS: dict[str, Callable[[str, int, int], None]] = {
    "counter": _counter_writer,
    "state": _state_writer,
}


def _race_pause(_fd: int) -> None:
    """Stand in for ``os.fsync`` with a fixed pause inside the critical section."""
    time.sleep(RACE_PAUSE_SECONDS)


def _run_writer(
    name: str, target: str, worker: int, count: int, barrier: Barrier
) -> None:
    """Child entry point: wait until every peer is ready, then write."""
    barrier.wait(timeout=TIMEOUT_SECONDS)
    with patch.object(os, "fsync", _race_pause):
        WRITERS[name](target, worker, count)


# ---------- helpers ----------


def run_in_processes(name: str, target: Path, count: int) -> None:
    """Run one writer in ``PROCESSES`` spawned children and require success.

    Args:
        name: Key of :data:`WRITERS`.
        target: File or config directory handed to the writer.
        count: Updates per child.
    """
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(PROCESSES)
    children = [
        context.Process(
            target=_run_writer,
            args=(name, str(target), worker, count, barrier),
            daemon=True,
        )
        for worker in range(PROCESSES)
    ]
    for child in children:
        child.start()
    deadline = time.monotonic() + TIMEOUT_SECONDS
    try:
        for child in children:
            child.join(max(0.0, deadline - time.monotonic()))
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(5)
    assert [child.exitcode for child in children] == [0] * PROCESSES


def run_threads(target: Callable[[], None]) -> None:
    """Run ``target`` in ``THREADS`` threads and require them all to finish."""
    threads = [threading.Thread(target=target, daemon=True) for _ in range(THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT_SECONDS)
    assert not any(thread.is_alive() for thread in threads)


def start_thread(action: Callable[[], object]) -> threading.Event:
    """Run ``action`` in a daemon thread; the event is set once it returns."""
    done = threading.Event()

    def run() -> None:
        action()
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done


def hold(path: Path) -> None:
    """Take and release the lock for ``path``."""
    with state_file_lock(path) as locked:
        assert locked


def flock_is_free(lock_file: Path) -> bool:
    """Report whether a separate descriptor could flock ``lock_file`` now."""
    assert fcntl is not None
    with lock_file.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return True


def expected_names(prefix: str) -> set[str]:
    """Return every ``w<worker>_<prefix><index>`` the state writers produce."""
    return {
        f"w{worker}_{prefix}{index}"
        for worker in range(PROCESSES)
        for index in range(UPDATES_PER_PROCESS)
    }


# ---------- the primitive ----------


class TestStateFileLock:
    """Behavior of :func:`state_file_lock` itself."""

    def test_serializes_threads(self, tmp_path: Path) -> None:
        counter = tmp_path / "counter.json"
        run_threads(lambda: _locked_increments(counter, INCREMENTS_PER_WORKER))
        assert int(counter.read_text()) == THREADS * INCREMENTS_PER_WORKER

    @requires_flock
    def test_serializes_processes(self, tmp_path: Path) -> None:
        counter = tmp_path / "counter.json"
        run_in_processes("counter", counter, INCREMENTS_PER_WORKER)
        assert int(counter.read_text()) == PROCESSES * INCREMENTS_PER_WORKER

    @requires_flock
    def test_flocks_a_sibling_lock_file_that_is_never_deleted(
        self, tmp_path: Path
    ) -> None:
        state = tmp_path / "state.json"
        lock_file = tmp_path / "state.json.lock"
        assert state_lock_path(state) == lock_file

        with state_file_lock(state) as locked:
            assert locked
            assert not state.exists()  # The data file is never locked.
            assert not flock_is_free(lock_file)
            atomic_write_text(state, "{}")  # Replacing the data keeps the lock.
            assert not flock_is_free(lock_file)

        assert lock_file.is_file()
        assert flock_is_free(lock_file)

    def test_independent_paths_do_not_block_each_other(self, tmp_path: Path) -> None:
        with state_file_lock(tmp_path / "first.json"):
            done = start_thread(lambda: hold(tmp_path / "second.json"))
            assert done.wait(TIMEOUT_SECONDS)

    def test_one_lock_per_resolved_path(self, tmp_path: Path) -> None:
        (tmp_path / "sub").mkdir()
        alias = tmp_path / "sub" / ".." / "state.json"
        with state_file_lock(tmp_path / "state.json"):
            done = start_thread(lambda: hold(alias))
            assert not done.wait(BLOCK_PROBE_SECONDS)
        assert done.wait(TIMEOUT_SECONDS)

    def test_nesting_raises_instead_of_deadlocking(self, tmp_path: Path) -> None:
        state = tmp_path / "state.json"
        (tmp_path / "sub").mkdir()
        with state_file_lock(state):
            with pytest.raises(RuntimeError, match="already held"):
                with state_file_lock(tmp_path / "sub" / ".." / "state.json"):
                    pass  # pragma: no cover
        hold(state)

    def test_creates_missing_directory(self, tmp_path: Path) -> None:
        state = tmp_path / "missing" / "config" / "health.json"
        with state_file_lock(state) as locked:
            assert locked
            atomic_write_text(state, "{}")
        assert state.read_text() == "{}"
        if fcntl is not None:
            assert state_lock_path(state).is_file()

    def test_writers_work_without_config_directory(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "fresh" / "config"
        record_refresh(config_dir, "channel", success=True)
        assert set_episode_pinned(config_dir, "channel", "video", True)
        assert record_tombstone(config_dir, "channel", "gone", "Gone")
        generator = RSSGenerator("http://localhost")
        generator.refresh_timestamps_file = config_dir / "refresh_timestamps.json"
        generator.update_channel_refresh_time("channel")

        assert "channel" in load_health(config_dir)["channels"]
        assert load_pins(config_dir) == {"channel": ["video"]}
        assert set(load_tombstones(config_dir)["channel"]) == {"gone"}
        assert set(generator.load_refresh_timestamps()) == {"channel"}

    @requires_flock
    def test_unusable_directory_skips_writes(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        config_dir.write_text("not a directory")
        with state_file_lock(config_dir / "pins.json") as locked:
            assert not locked

        state = record_refresh(config_dir, "channel", success=True)
        assert len(state["history"]) == 1
        assert not set_episode_pinned(config_dir, "channel", "video", True)
        assert not record_tombstone(config_dir, "channel", "video", "Video")
        generator = RSSGenerator("http://localhost")
        generator.refresh_timestamps_file = config_dir / "refresh_timestamps.json"
        generator.update_channel_refresh_time("channel")
        assert not generator.remove_channel_refresh_time("channel")
        assert config_dir.read_text() == "not a directory"

        # A failed attempt releases the thread lock again.
        config_dir.unlink()
        config_dir.mkdir()
        hold(config_dir / "pins.json")

    def test_thread_only_fallback_without_fcntl(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils, "fcntl", None)
        counter = tmp_path / "counter.json"
        run_threads(lambda: _locked_increments(counter, INCREMENTS_PER_WORKER))
        assert int(counter.read_text()) == THREADS * INCREMENTS_PER_WORKER
        assert not state_lock_path(counter).exists()


# ---------- lost-update regressions across processes ----------


@pytest.fixture(scope="module")
def shared_config(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A config directory written concurrently by every state writer."""
    config_dir = tmp_path_factory.mktemp("shared_config")
    run_in_processes("state", config_dir, UPDATES_PER_PROCESS)
    return config_dir


@requires_flock
class TestCrossProcessUpdates:
    """Concurrent writers in separate processes never lose updates."""

    def test_health_keeps_every_refresh(self, shared_config: Path) -> None:
        channels = load_health(shared_config)["channels"]
        total = PROCESSES * UPDATES_PER_PROCESS
        assert expected_names("c") <= set(channels)
        assert channels["shared"]["consecutive_failures"] == total
        assert len(channels["shared"]["history"]) == min(total, HISTORY_LIMIT)

    def test_health_keeps_every_notification(self, shared_config: Path) -> None:
        notifications = load_health(shared_config)["notifications"]
        assert set(notifications["last_sent"]) == expected_names("e")
        assert set(notifications["last_attempts"]) == expected_names("e")

    def test_refresh_timestamps_keep_every_channel(self, shared_config: Path) -> None:
        generator = RSSGenerator("http://localhost")
        generator.refresh_timestamps_file = shared_config / "refresh_timestamps.json"
        assert set(generator.load_refresh_timestamps()) == expected_names("c")

    def test_pins_keep_every_pin(self, shared_config: Path) -> None:
        pins = load_pins(shared_config)
        assert set(pins) == {"ch0", "ch1"}
        assert set(pins["ch0"]) | set(pins["ch1"]) == expected_names("v")

    def test_tombstones_keep_every_deletion(self, shared_config: Path) -> None:
        tombstones = load_tombstones(shared_config)
        assert set(tombstones) == {"ch0", "ch1"}
        assert set(tombstones["ch0"]) | set(tombstones["ch1"]) == expected_names("v")

    def test_lock_files_sit_next_to_the_state_files(self, shared_config: Path) -> None:
        for name in (
            "health.json",
            "refresh_timestamps.json",
            "pins.json",
            "tombstones.json",
        ):
            assert (shared_config / name).is_file()
            assert state_lock_path(shared_config / name).is_file()


# ---------- refresh timestamp removal ----------


class TestRemoveChannelRefreshTime:
    """:meth:`RSSGenerator.remove_channel_refresh_time`."""

    @pytest.fixture
    def generator(self, tmp_path: Path) -> RSSGenerator:
        generator = RSSGenerator("http://localhost")
        generator.refresh_timestamps_file = tmp_path / "refresh_timestamps.json"
        generator.save_refresh_timestamps({"gone": "2024-01-01", "kept": "2024-01-02"})
        return generator

    def test_removes_only_that_channel(self, generator: RSSGenerator) -> None:
        assert generator.remove_channel_refresh_time("gone")
        assert generator.load_refresh_timestamps() == {"kept": "2024-01-02"}

    def test_unknown_channel_leaves_file_alone(self, generator: RSSGenerator) -> None:
        before = generator.refresh_timestamps_file.read_bytes()
        assert not generator.remove_channel_refresh_time("unknown")
        assert generator.refresh_timestamps_file.read_bytes() == before

    def test_waits_for_the_state_lock(self, generator: RSSGenerator) -> None:
        with state_file_lock(generator.refresh_timestamps_file):
            done = start_thread(lambda: generator.remove_channel_refresh_time("gone"))
            assert not done.wait(BLOCK_PROBE_SECONDS)
            assert "gone" in generator.load_refresh_timestamps()
        assert done.wait(TIMEOUT_SECONDS)
        assert generator.load_refresh_timestamps() == {"kept": "2024-01-02"}
