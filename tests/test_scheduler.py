"""Channel refresh scheduling: anchoring, overdue staggering and busy retries.

The server runs a real APScheduler instance started in paused mode, so job
run times are computed exactly as in production but no job ever fires.
Refresh workers are mocked; nothing here contacts YouTube.
"""

import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_PAUSED

from src.health import record_refresh
from src.web_server import (
    OVERDUE_REFRESH_DELAY_SECONDS,
    OVERDUE_REFRESH_STAGGER_SECONDS,
    REFRESH_JOB_PREFIX,
    REFRESH_RETRY_DELAY_SECONDS,
    RETRY_JOB_PREFIX,
    YouTubePodcastServer,
    latest_history_attempt,
    next_refresh_run,
    overdue_run_time,
    parse_utc_timestamp,
    refresh_due_time,
)

DEFAULT_INTERVAL_HOURS = 6


def channel_entry(name: str, **overrides: Any) -> dict[str, Any]:
    """Return a minimal valid channel configuration entry."""
    entry: dict[str, Any] = {
        "name": name,
        "display_name": name.title(),
        "url": f"https://www.youtube.com/@{name}",
        "max_episodes": 5,
        "download_delay_hours": 0,
        "format": "audio",
        "quality": "best",
        "sponsorblock_categories": [],
    }
    entry.update(overrides)
    return entry


# alpha has its own 24h interval; beta inherits the 6h default.
CHANNELS = [
    channel_entry("alpha", refresh_interval_hours=24),
    channel_entry("beta"),
]


def write_channels(config_dir: Path, channels: list[dict[str, Any]]) -> None:
    """Write ``channels.yaml`` with the default interval used in these tests."""
    config = {
        "channels": channels,
        "default_interval_hours": DEFAULT_INTERVAL_HOURS,
        "download_delay_seconds": 0,
    }
    (config_dir / "channels.yaml").write_text(yaml.safe_dump(config))


def write_timestamps(config_dir: Path, timestamps: dict[str, Any]) -> None:
    """Write ``refresh_timestamps.json`` as the downloader does."""
    (config_dir / "refresh_timestamps.json").write_text(json.dumps(timestamps))


def record_attempt(config_dir: Path, channel: str, at: datetime, success=True):
    """Record a refresh attempt in ``health.json`` at a fixed time."""
    record_refresh(config_dir, channel, success=success, at=at.isoformat())


class PausedScheduler(BackgroundScheduler):
    """Real scheduler that computes run times but never executes jobs."""

    def start(self, *args: Any, **kwargs: Any) -> None:
        """Start paused regardless of the caller's arguments."""
        super().start(paused=True)


@pytest.fixture
def config_dir(temp_directory: Path) -> Path:
    """Config directory holding the two test channels."""
    path = temp_directory / "config"
    path.mkdir()
    write_channels(path, CHANNELS)
    return path


@pytest.fixture
def start_server(
    temp_directory: Path, config_dir: Path, mock_logging: Mock, mock_atexit: Mock
) -> Iterator[Callable[[], YouTubePodcastServer]]:
    """Start servers against ``config_dir`` with a paused real scheduler.

    Write health/timestamp state before calling the factory: jobs are
    scheduled while the server starts.
    """
    servers: list[YouTubePodcastServer] = []

    def start() -> YouTubePodcastServer:
        with patch("src.web_server.BackgroundScheduler", PausedScheduler):
            server = YouTubePodcastServer(
                host="127.0.0.1",
                port=5000,
                videos_dir=str(temp_directory / "podcasts"),
                config_dir=str(config_dir),
            )
        server.app.config["TESTING"] = True
        server.logger = Mock()
        # Mirror the real workers (they always release the reservation)
        # without ever reaching YouTube, including via manual refreshes.
        server._run_refresh = Mock(
            side_effect=lambda *_args, **_kwargs: server._finish_refresh()
        )
        server._run_single_channel_refresh = Mock(
            side_effect=lambda _name: server._finish_refresh()
        )
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.close()


def next_run(server: YouTubePodcastServer, channel: str) -> datetime:
    """Return the channel's regular refresh job run time."""
    job = server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}{channel}")
    assert job is not None
    return job.next_run_time


def retry_jobs(server: YouTubePodcastServer) -> list[Any]:
    """Return every pending busy-retry job."""
    return [
        job
        for job in server.scheduler.get_jobs()
        if job.id.startswith(RETRY_JOB_PREFIX)
    ]


class TestScheduleHelpers:
    """Pure timestamp parsing and run time arithmetic."""

    @pytest.mark.parametrize(
        "raw",
        [
            "2026-01-02T03:04:05+00:00",
            "2026-01-02T03:04:05Z",
            "2026-01-02T03:04:05",
            "2026-01-02T05:04:05+02:00",
        ],
    )
    def test_parse_normalizes_to_utc(self, raw: str) -> None:
        assert parse_utc_timestamp(raw) == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    @pytest.mark.parametrize(
        "raw", [None, "", "yesterday", "2026-13-45T99:00:00", 1700000000, [], {}]
    )
    def test_parse_rejects_malformed_values(self, raw: Any) -> None:
        assert parse_utc_timestamp(raw) is None

    def test_latest_attempt_uses_newest_valid_entry(self) -> None:
        history = [
            {"at": "2026-01-01T00:00:00+00:00", "status": "success"},
            {"at": "2026-01-03T00:00:00+00:00", "status": "failure"},
            {"at": "garbage", "status": "success"},
            "not-an-entry",
            {"status": "success"},
        ]
        assert latest_history_attempt(history) == datetime(2026, 1, 3, tzinfo=UTC)

    @pytest.mark.parametrize("history", [None, {}, "x", [], [{"at": "bad"}]])
    def test_latest_attempt_without_valid_entries(self, history: Any) -> None:
        assert latest_history_attempt(history) is None

    def test_due_time_is_anchored_to_last_attempt(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        last = now - timedelta(hours=1)
        assert refresh_due_time(last, 24, now) == last + timedelta(hours=24)
        assert refresh_due_time(last, 0.5, now) == now - timedelta(minutes=30)

    def test_due_time_without_record_is_one_interval_away(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        assert refresh_due_time(None, 24, now) == now + timedelta(hours=24)

    def test_future_attempt_counts_as_now(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        future = now + timedelta(days=30)
        assert refresh_due_time(future, 24, now) == now + timedelta(hours=24)

    def test_overdue_slots_are_staggered(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        first = overdue_run_time(now, 0)
        second = overdue_run_time(now, 1)
        assert first == now + timedelta(seconds=OVERDUE_REFRESH_DELAY_SECONDS)
        assert second - first == timedelta(seconds=OVERDUE_REFRESH_STAGGER_SECONDS)

    def test_next_run_without_live_job_uses_persisted_data(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        last = now - timedelta(hours=1)
        assert next_refresh_run(last, 24, None, now) == last + timedelta(hours=24)
        assert next_refresh_run(None, 24, None, now) == now + timedelta(hours=24)

    def test_next_run_keeps_live_job_without_record(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        live = now + timedelta(hours=3)
        assert next_refresh_run(None, 24, live, now) == live

    def test_next_run_prefers_later_of_persisted_and_live(self) -> None:
        now = datetime(2026, 1, 2, tzinfo=UTC)
        last = now - timedelta(hours=1)
        anchored = last + timedelta(hours=24)
        earlier_live = anchored - timedelta(minutes=10)
        later_live = anchored + timedelta(minutes=10)
        assert next_refresh_run(last, 24, earlier_live, now) == anchored
        assert next_refresh_run(last, 24, later_live, now) == later_live


class TestAnchoredSchedule:
    """Startup schedules derive from the last recorded refresh attempt."""

    def test_scheduler_never_fires_jobs_in_tests(self, start_server) -> None:
        server = start_server()
        assert server.scheduler.state == STATE_PAUSED

    def test_recent_attempt_anchors_next_run(self, config_dir, start_server) -> None:
        last = datetime.now(UTC) - timedelta(hours=1)
        record_attempt(config_dir, "alpha", last)
        server = start_server()
        assert next_run(server, "alpha") == last + timedelta(hours=24)

    def test_no_record_keeps_full_interval(self, start_server) -> None:
        before = datetime.now(UTC)
        server = start_server()
        after = datetime.now(UTC)
        alpha = next_run(server, "alpha")
        beta = next_run(server, "beta")
        assert before + timedelta(hours=24) <= alpha <= after + timedelta(hours=24)
        assert before + timedelta(hours=6) <= beta <= after + timedelta(hours=6)

    def test_failed_attempt_counts_as_attempt(self, config_dir, start_server) -> None:
        last = datetime.now(UTC) - timedelta(hours=2)
        record_attempt(config_dir, "alpha", last - timedelta(hours=5))
        record_attempt(config_dir, "alpha", last, success=False)
        server = start_server()
        assert next_run(server, "alpha") == last + timedelta(hours=24)

    def test_refresh_timestamp_is_fallback(self, config_dir, start_server) -> None:
        last = datetime.now(UTC) - timedelta(hours=3)
        write_timestamps(config_dir, {"alpha": last.isoformat()})
        server = start_server()
        assert next_run(server, "alpha") == last + timedelta(hours=24)

    def test_health_history_wins_over_timestamp(self, config_dir, start_server) -> None:
        attempt = datetime.now(UTC) - timedelta(hours=4)
        download = datetime.now(UTC) - timedelta(minutes=5)
        record_attempt(config_dir, "alpha", attempt)
        write_timestamps(config_dir, {"alpha": download.isoformat()})
        server = start_server()
        assert next_run(server, "alpha") == attempt + timedelta(hours=24)

    def test_malformed_timestamps_are_ignored(self, config_dir, start_server) -> None:
        fallback = datetime.now(UTC) - timedelta(hours=1)
        health = {
            "channels": {
                "alpha": {"history": [{"at": "not-a-date", "status": "success"}]},
                "beta": {"history": [{"at": 12345, "status": "failure"}]},
            }
        }
        (config_dir / "health.json").write_text(json.dumps(health))
        write_timestamps(config_dir, {"alpha": fallback.isoformat(), "beta": "soon"})
        before = datetime.now(UTC)
        server = start_server()
        after = datetime.now(UTC)
        assert next_run(server, "alpha") == fallback + timedelta(hours=24)
        beta = next_run(server, "beta")
        assert before + timedelta(hours=6) <= beta <= after + timedelta(hours=6)

    def test_non_mapping_timestamp_file_is_ignored(
        self, config_dir, start_server
    ) -> None:
        (config_dir / "refresh_timestamps.json").write_text("[1, 2, 3]")
        server = start_server()
        assert server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}alpha") is not None
        assert server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}beta") is not None


class TestOverdueChannels:
    """Overdue channels run soon after startup, staggered."""

    def test_overdue_channels_run_in_startup_window(
        self, config_dir, start_server
    ) -> None:
        now = datetime.now(UTC)
        record_attempt(config_dir, "alpha", now - timedelta(hours=30))
        record_attempt(config_dir, "beta", now - timedelta(hours=10), success=False)
        before = datetime.now(UTC)
        server = start_server()
        after = datetime.now(UTC)
        delay = timedelta(seconds=OVERDUE_REFRESH_DELAY_SECONDS)
        alpha = next_run(server, "alpha")
        beta = next_run(server, "beta")
        assert before + delay <= alpha <= after + delay
        assert beta - alpha == timedelta(seconds=OVERDUE_REFRESH_STAGGER_SECONDS)

    def test_on_time_channels_do_not_take_stagger_slots(
        self, config_dir, start_server
    ) -> None:
        write_channels(
            config_dir,
            [
                channel_entry("alpha", refresh_interval_hours=24),
                channel_entry("beta"),
                channel_entry("gamma"),
            ],
        )
        now = datetime.now(UTC)
        record_attempt(config_dir, "alpha", now - timedelta(hours=48))
        record_attempt(config_dir, "beta", now - timedelta(hours=1))
        record_attempt(config_dir, "gamma", now - timedelta(hours=7))
        server = start_server()
        assert next_run(server, "beta") > now + timedelta(hours=4)
        gap = next_run(server, "gamma") - next_run(server, "alpha")
        assert gap == timedelta(seconds=OVERDUE_REFRESH_STAGGER_SECONDS)

    def test_overdue_job_keeps_live_run_time_on_reschedule(
        self, config_dir, start_server
    ) -> None:
        """A busy or running refresh is not fired again by a config change."""
        record_attempt(config_dir, "alpha", datetime.now(UTC) - timedelta(hours=30))
        server = start_server()
        # Simulate the job having fired (busy) and advanced one interval.
        fired_next = datetime.now(UTC) + timedelta(hours=23)
        server.scheduler.modify_job(
            f"{REFRESH_JOB_PREFIX}alpha", next_run_time=fired_next
        )
        server._setup_scheduler()
        assert next_run(server, "alpha") == fired_next


class TestChannelChangesKeepSchedules:
    """Add, edit and delete no longer push every channel's refresh out."""

    def test_adding_channel_keeps_other_schedules(
        self, config_dir, start_server
    ) -> None:
        record_attempt(config_dir, "alpha", datetime.now(UTC) - timedelta(hours=1))
        server = start_server()
        alpha = next_run(server, "alpha")
        beta = next_run(server, "beta")
        with patch.object(
            server, "_verify_youtube_channel", return_value=(True, "", {})
        ):
            response = server.app.test_client().post(
                "/api/channels",
                json={"display_name": "Gamma", "url": "https://www.youtube.com/@g"},
            )
        assert response.status_code == 201
        new_id = response.json["channel"]["name"]
        assert server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}{new_id}") is not None
        assert next_run(server, "alpha") == alpha
        assert next_run(server, "beta") == beta

    def test_editing_channel_keeps_other_schedules(
        self, config_dir, start_server
    ) -> None:
        record_attempt(config_dir, "alpha", datetime.now(UTC) - timedelta(hours=1))
        server = start_server()
        alpha = next_run(server, "alpha")
        beta = next_run(server, "beta")
        client = server.app.test_client()
        response = client.put("/api/channels/beta", json={"max_episodes": 7})
        assert response.status_code == 200
        assert next_run(server, "alpha") == alpha
        # beta has no recorded attempt yet; it must not be pushed out either.
        assert next_run(server, "beta") == beta

    def test_interval_change_reanchors_to_last_attempt(
        self, config_dir, start_server
    ) -> None:
        last = datetime.now(UTC) - timedelta(hours=1)
        record_attempt(config_dir, "alpha", last)
        server = start_server()
        client = server.app.test_client()
        response = client.put("/api/channels/alpha", json={"refresh_interval_hours": 2})
        assert response.status_code == 200
        assert next_run(server, "alpha") == last + timedelta(hours=2)

    def test_default_interval_change_reanchors_inheriting_channels(
        self, config_dir, start_server
    ) -> None:
        last = datetime.now(UTC) - timedelta(hours=1)
        record_attempt(config_dir, "alpha", last)
        record_attempt(config_dir, "beta", last)
        server = start_server()
        alpha = next_run(server, "alpha")
        response = server.app.test_client().put(
            "/api/config/refresh-interval", json={"refresh_interval_hours": 12}
        )
        assert response.status_code == 200
        assert next_run(server, "beta") == last + timedelta(hours=12)
        assert next_run(server, "alpha") == alpha

    def test_deleting_channel_removes_its_jobs(self, start_server) -> None:
        server = start_server()
        assert server._begin_refresh()
        try:
            server._scheduled_channel_refresh("beta")
        finally:
            server._finish_refresh()
        assert server.scheduler.get_job(f"{RETRY_JOB_PREFIX}beta") is not None
        response = server.app.test_client().delete("/api/channels/beta")
        assert response.status_code == 200
        assert server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}beta") is None
        assert server.scheduler.get_job(f"{RETRY_JOB_PREFIX}beta") is None
        assert server.scheduler.get_job(f"{REFRESH_JOB_PREFIX}alpha") is not None


class TestBusyRetry:
    """A scheduled refresh that finds the worker busy retries later."""

    @pytest.mark.parametrize("busy_by", ["refresh", "media_lock"])
    def test_busy_refresh_schedules_single_retry(self, start_server, busy_by) -> None:
        server = start_server()
        if busy_by == "refresh":
            assert server._begin_refresh()
        else:  # e.g. the download queue worker holds media.lock
            assert server._media_lock.acquire(blocking=False)
        before = datetime.now(UTC)
        server._scheduled_channel_refresh("alpha")
        server._scheduled_channel_refresh("alpha")
        after = datetime.now(UTC)
        server._run_single_channel_refresh.assert_not_called()
        jobs = retry_jobs(server)
        assert [job.id for job in jobs] == [f"{RETRY_JOB_PREFIX}alpha"]
        delay = timedelta(seconds=REFRESH_RETRY_DELAY_SECONDS)
        assert before + delay <= jobs[0].next_run_time <= after + delay
        assert any(
            "deferred" in str(call.args[0])
            for call in server.logger.info.call_args_list
        )
        if busy_by == "refresh":
            server._finish_refresh()
        else:
            server._media_lock.release()

    def test_retry_runs_refresh_once_worker_is_free(self, start_server) -> None:
        server = start_server()
        assert server._begin_refresh()
        server._scheduled_channel_refresh("alpha")
        server._finish_refresh()
        job = server.scheduler.get_job(f"{RETRY_JOB_PREFIX}alpha")
        assert job is not None
        job.func(*job.args)
        server._run_single_channel_refresh.assert_called_once_with("alpha")
        assert server.refresh_status["running"] is False

    def test_retry_for_deleted_channel_is_noop(self, config_dir, start_server) -> None:
        server = start_server()
        write_channels(config_dir, [CHANNELS[0]])
        server._scheduled_channel_refresh("beta")
        server._run_single_channel_refresh.assert_not_called()
        assert server.refresh_status["running"] is False
        assert server._begin_refresh()
        try:
            server._scheduled_channel_refresh("beta")
        finally:
            server._finish_refresh()
        assert retry_jobs(server) == []

    def test_manual_refresh_still_conflicts_while_reserved(self, start_server) -> None:
        server = start_server()
        client = server.app.test_client()
        assert server._begin_refresh()
        try:
            server._scheduled_channel_refresh("alpha")
            assert client.post("/api/refresh").status_code == 409
            assert client.post("/api/channels/alpha/refresh").status_code == 409
        finally:
            server._finish_refresh()

    def test_refresh_interval_api_lists_only_regular_jobs(self, start_server) -> None:
        server = start_server()
        assert server._begin_refresh()
        try:
            server._scheduled_channel_refresh("alpha")
        finally:
            server._finish_refresh()
        response = server.app.test_client().get("/api/config/refresh-interval")
        assert response.status_code == 200
        data = response.json
        assert set(data) == {"refresh_interval_hours", "next_runs", "next_run"}
        assert sorted(run["channel"] for run in data["next_runs"]) == ["alpha", "beta"]
        assert data["next_run"] == data["next_runs"][0]["next_run"]
