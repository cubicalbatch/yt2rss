"""Tests for the persistent download queue, worker, API, and refresh wiring.

The queue store is exercised directly, the worker with a stub downloader
(and once through the real ``download_video`` with mocked yt-dlp), and the
HTTP surface through the Flask test client.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock, patch

import pytest
import yaml

from src.download_queue import (
    COMPLETED_HISTORY_LIMIT,
    MAX_ATTEMPTS,
    RETRY_BASE_DELAY_SECONDS,
    ChannelMissingError,
    DownloadQueue,
    QueueWorker,
    backoff_delay_seconds,
    execute_queue_job,
)
from tests.test_auth import auth_client as auth_client
from tests.test_auth import auth_server as auth_server
from tests.test_web_server import app_config as app_config
from tests.test_web_server import client as client
from tests.test_web_server import test_server as test_server


def enqueue_job(
    queue: DownloadQueue,
    channel: str = "test_channel",
    video_id: str = "video1",
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Enqueue one job with stable defaults for tests."""
    job = queue.enqueue(
        channel=channel,
        video_id=video_id,
        title=f"Title {video_id}",
        url=f"https://www.youtube.com/watch?v={video_id}",
        snapshot=snapshot
        or {
            "format": "video",
            "quality": "max",
            "max_episodes": 5,
            "download_delay_seconds": 0,
        },
    )
    assert job is not None
    return job


def wait_until(condition, timeout: float = 5.0) -> bool:
    """Poll a condition until it holds or the timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def job_state(queue: DownloadQueue, job_id: str) -> dict[str, Any]:
    """Fetch a job, asserting it still exists."""
    state = queue.get(job_id)
    assert state is not None
    return state


class StubDownloader:
    """Downloader double recording calls and simulating yt-dlp outcomes.

    Set ``behavior`` to ``success``, ``slow``, ``burst``, ``failure`` or
    ``cancel`` before the worker runs a job.
    """

    behavior: ClassVar[str] = "success"
    instances: ClassVar[list["StubDownloader"]] = []

    def __init__(
        self,
        config_path: str = "channels.yaml",
        base_dir: str = ".",
        videos_dir: str | None = None,
    ):
        self.config_path = Path(config_path)
        self.videos_dir = Path(videos_dir or ".")
        self.logger = logging.getLogger("stub-downloader")
        self.rss_generator = Mock()
        self.download_calls: list[dict[str, Any]] = []
        StubDownloader.instances.append(self)

    def load_config(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Read channels.yaml exactly like the real downloader."""
        with open(self.config_path, "r") as handle:
            config = yaml.safe_load(handle) or {}
        channels = config.get("channels", [])
        global_config = {k: v for k, v in config.items() if k != "channels"}
        return channels, global_config

    def video_exists(
        self, channel_name: str, video_id: str, format_type: str = "video"
    ) -> bool:
        """Mirror the real filesystem check."""
        channel_dir = self.videos_dir / channel_name
        extensions = (".m4a", ".mp3") if format_type == "audio" else (".mp4", ".webm")
        return (channel_dir / f"{video_id}.json").is_file() and any(
            (channel_dir / f"{video_id}{ext}").is_file() for ext in extensions
        )

    def download_video(
        self,
        video_info: dict[str, Any],
        channel_name: str,
        sponsorblock_categories: list[str] | None = None,
        format_type: str = "video",
        quality: str = "max",
        progress_hook=None,
    ) -> bool:
        """Simulate a download according to ``behavior``."""
        self.download_calls.append(
            {
                "video_id": video_info["id"],
                "format_type": format_type,
                "quality": quality,
                "sponsorblock": sponsorblock_categories,
            }
        )
        if progress_hook is not None:
            progress_hook(
                {
                    "status": "downloading",
                    "downloaded_bytes": 40,
                    "total_bytes": 100,
                    "speed": 8.0,
                    "eta": 7,
                }
            )
        channel_dir = self.videos_dir / channel_name
        if self.behavior in ("success", "slow", "burst"):
            if self.behavior == "slow":
                time.sleep(0.3)
            if self.behavior == "burst":
                for _ in range(5):
                    if progress_hook is not None:
                        progress_hook(
                            {
                                "status": "downloading",
                                "downloaded_bytes": 41,
                                "total_bytes": 100,
                            }
                        )
            channel_dir.mkdir(parents=True, exist_ok=True)
            (channel_dir / f"{video_info['id']}.mp4").write_bytes(b"media")
            (channel_dir / f"{video_info['id']}.json").write_text(
                json.dumps({"id": video_info["id"]})
            )
            return True
        if self.behavior == "failure":
            channel_dir.mkdir(parents=True, exist_ok=True)
            (channel_dir / f"{video_info['id']}.part").write_bytes(b"partial")
            return False
        if self.behavior == "cancel":
            for _ in range(300):
                if progress_hook is not None:
                    progress_hook(
                        {
                            "status": "downloading",
                            "downloaded_bytes": 1,
                            "total_bytes": 100,
                        }
                    )
                time.sleep(0.05)
            return True
        raise AssertionError(f"Unknown behavior: {self.behavior}")


@pytest.fixture(autouse=True)
def reset_stub_downloader():
    """Give every test a clean stub registry and default behavior."""
    StubDownloader.instances.clear()
    StubDownloader.behavior = "success"
    yield
    StubDownloader.behavior = "success"


@pytest.fixture
def config_dir(tmp_path):
    """Empty configuration directory for direct store tests."""
    directory = tmp_path / "config"
    directory.mkdir()
    return directory


@pytest.fixture
def queue(config_dir):
    """Fresh queue store."""
    return DownloadQueue(config_dir)


@pytest.fixture
def quiet_server(test_server):
    """Server whose bundled worker is stopped for deterministic API tests."""
    test_server.queue_worker.stop()
    return test_server


@pytest.fixture
def quiet_client(quiet_server):
    """Test client for the server with the stopped worker."""
    return quiet_server.app.test_client()


@pytest.fixture
def fast_worker(test_server):
    """Replace the server worker with a fast-polling one."""
    test_server.queue_worker.stop()
    worker = QueueWorker(
        test_server.queue,
        executor=test_server._execute_queue_job,
        media_lock=test_server._media_lock,
        logger=test_server.logger,
        poll_interval=0.05,
    )
    worker.start()
    yield worker
    worker.stop()


class TestQueueStore:
    """Persistent store: add/list/update, dedup, bounds, corruption."""

    def test_enqueue_and_list_roundtrip(self, queue):
        job = enqueue_job(queue)
        stored = queue.jobs()
        assert len(stored) == 1
        assert stored[0]["id"] == job["id"]
        assert stored[0]["channel"] == "test_channel"
        assert stored[0]["video_id"] == "video1"
        assert stored[0]["state"] == "pending"
        assert stored[0]["attempts"] == 0
        assert stored[0]["max_attempts"] == MAX_ATTEMPTS
        assert stored[0]["next_attempt_at"] is None
        assert stored[0]["created_at"] is not None
        assert stored[0]["cancel_requested"] is False

    def test_file_is_written_atomically_after_enqueue(self, queue, config_dir):
        enqueue_job(queue)
        raw = json.loads((config_dir / "download_queue.json").read_text())
        assert len(raw["jobs"]) == 1

    def test_persistence_across_instances(self, queue, config_dir):
        enqueue_job(queue, video_id="keepme")
        reopened = DownloadQueue(config_dir)
        assert [job["video_id"] for job in reopened.jobs()] == ["keepme"]

    def test_enqueue_deduplicates_active_jobs(self, queue):
        first = enqueue_job(queue, video_id="dup")
        assert (
            queue.enqueue(
                channel="test_channel",
                video_id="dup",
                title="Other",
                url="https://www.youtube.com/watch?v=dup",
            )
            is None
        )
        assert len(queue.jobs()) == 1
        assert queue.get(first["id"]) is not None

    def test_terminal_jobs_allow_reenqueue(self, queue):
        first = enqueue_job(queue, video_id="done")
        queue.claim_next()
        queue.mark_completed(first["id"])
        assert enqueue_job(queue, video_id="done") is not None

    def test_completed_history_is_bounded(self, queue):
        for index in range(COMPLETED_HISTORY_LIMIT + 5):
            job = enqueue_job(queue, video_id=f"v{index}")
            queue.claim_next()
            queue.mark_completed(job["id"])
        completed = [job for job in queue.jobs() if job["state"] == "completed"]
        assert len(completed) == COMPLETED_HISTORY_LIMIT
        remaining_ids = {job["video_id"] for job in queue.jobs()}
        assert "v0" not in remaining_ids  # Oldest dropped.
        assert f"v{COMPLETED_HISTORY_LIMIT + 4}" in remaining_ids

    def test_failed_jobs_are_retained(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        for _ in range(MAX_ATTEMPTS - 1):
            queue.mark_failed(job["id"], "boom")
            queue.modify(job["id"], lambda item: item.update(next_attempt_at=0))
            queue.claim_next()
        queue.mark_failed(job["id"], "boom")
        states = {item["video_id"]: item["state"] for item in queue.jobs()}
        assert states["video1"] == "failed"

    def test_missing_file_starts_empty(self, config_dir):
        assert DownloadQueue(config_dir).jobs() == []

    def test_corrupt_file_is_backed_up_and_recovered(self, queue, config_dir):
        enqueue_job(queue)
        queue_file = config_dir / "download_queue.json"
        queue_file.write_text("{not json at all")
        reopened = DownloadQueue(config_dir)
        assert reopened.jobs() == []
        backup = config_dir / "download_queue.json.corrupt"
        assert backup.exists()
        assert "not json" in backup.read_text()

    def test_invalid_entries_are_dropped_not_fatal(self, queue, config_dir):
        good = enqueue_job(queue)
        queue_file = config_dir / "download_queue.json"
        data = json.loads(queue_file.read_text())
        data["jobs"].append({"no_id": True})
        data["jobs"].append("a string")
        queue_file.write_text(json.dumps(data))
        reopened = DownloadQueue(config_dir)
        assert [job["id"] for job in reopened.jobs()] == [good["id"]]

    def test_queue_is_inert_without_config_directory(self, tmp_path):
        ghost = DownloadQueue(tmp_path / "missing")
        assert ghost.enqueue(channel="c", video_id="v", title="t", url="u") is None
        assert ghost.jobs() == []


class TestQueueStateMachine:
    """pending→running→completed, retries, cancels, restart recovery."""

    def test_claim_transitions_pending_to_running(self, queue):
        job = enqueue_job(queue)
        claimed = queue.claim_next()
        assert claimed["id"] == job["id"]
        assert claimed["state"] == "running"
        assert claimed["attempts"] == 1
        assert claimed["started_at"] is not None
        assert queue.get(job["id"])["state"] == "running"

    def test_claim_is_empty_when_queue_is_empty(self, queue):
        assert queue.claim_next() is None

    def test_claim_respects_scheduled_retry_time(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        queue.mark_failed(job["id"], "boom")
        assert queue.peek_due() is None  # Backoff in the future.
        assert queue.claim_next() is None
        queue.modify(job["id"], lambda item: item.update(next_attempt_at=0))
        assert queue.claim_next() is not None

    def test_failure_schedules_deterministic_backoff(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        before = time.time()
        updated = queue.mark_failed(job["id"], "network down")
        assert updated["state"] == "pending"
        expected = before + backoff_delay_seconds(1)
        assert updated["next_attempt_at"] == pytest.approx(expected, abs=1.0)
        assert updated["next_attempt_at"] == pytest.approx(
            before + RETRY_BASE_DELAY_SECONDS, abs=1.0
        )
        assert updated["error"] == "network down"
        assert queue.get(job["id"])["attempts"] == 1

    def test_backoff_doubles_per_attempt(self, queue):
        assert backoff_delay_seconds(1) == 60
        assert backoff_delay_seconds(2) == 120
        assert backoff_delay_seconds(3) == 240

    def test_exhausted_attempts_terminate_as_failed(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        queue.modify(job["id"], lambda item: item.update(attempts=MAX_ATTEMPTS))
        updated = queue.mark_failed(job["id"], "final failure")
        assert updated["state"] == "failed"
        assert updated["finished_at"] is not None
        assert updated["error"] == "final failure"
        assert queue.peek_due() is None

    def test_manual_retry_resets_attempts_and_schedule(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        queue.modify(job["id"], lambda item: item.update(attempts=MAX_ATTEMPTS))
        queue.mark_failed(job["id"], "dead")
        retried = queue.retry(job["id"])
        assert retried["state"] == "pending"
        assert retried["attempts"] == 0
        assert retried["next_attempt_at"] is None
        assert retried["error"] is None
        assert queue.peek_due()["id"] == job["id"]

    def test_retry_rejects_active_and_completed(self, queue):
        job = enqueue_job(queue)
        assert queue.retry(job["id"]) is None  # Still pending.
        queue.claim_next()
        queue.mark_completed(job["id"])
        assert queue.retry(job["id"]) is None

    def test_cancel_pending_is_immediate(self, queue):
        job = enqueue_job(queue)
        updated = queue.request_cancel(job["id"])
        assert updated["state"] == "cancelled"
        assert updated["finished_at"] is not None
        assert queue.claim_next() is None

    def test_cancel_running_sets_flag(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        updated = queue.request_cancel(job["id"])
        assert updated["state"] == "running"
        assert updated["cancel_requested"] is True
        assert queue.memory_cancel_requested(job["id"]) is True
        assert queue.disk_cancel_requested(job["id"]) is True

    def test_claim_retires_cancel_flagged_pending(self, queue):
        job = enqueue_job(queue)
        queue.modify(job["id"], lambda item: item.update(cancel_requested=True))
        assert queue.peek_due() is None
        assert queue.claim_next() is None
        assert queue.get(job["id"])["state"] == "cancelled"

    def test_completed_and_cancelled_are_terminal_states(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        queue.request_cancel(job["id"])
        cancelled = queue.mark_cancelled(job["id"], error="Cancelled by user")
        assert cancelled["state"] == "cancelled"
        assert queue.mark_completed(job["id"]) is None  # Already terminal.

    def test_mark_completed_sets_full_progress(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()
        completed = queue.mark_completed(job["id"])
        assert completed is not None
        assert completed["progress"] == {"percent": 100, "speed": None, "eta": None}

    def test_restart_recovers_running_jobs(self, queue, config_dir):
        job = enqueue_job(queue)
        queue.claim_next()
        queue.update_progress(job["id"], {"percent": 40, "speed": None, "eta": 3})
        reopened = DownloadQueue(config_dir)  # Simulates a restart.
        recovered = job_state(reopened, job["id"])
        assert recovered["state"] == "pending"
        assert recovered["attempts"] == 1  # Attempt count preserved.
        assert recovered["progress"] is None
        assert recovered["next_attempt_at"] is None
        due = reopened.peek_due()
        assert due is not None and due["id"] == job["id"]

    def test_dismiss_rules(self, queue):
        job = enqueue_job(queue)
        assert queue.dismiss(job["id"]) is False  # Active: not dismissable.
        queue.claim_next()
        queue.mark_completed(job["id"])
        assert queue.dismiss(job["id"]) is True
        assert queue.get(job["id"]) is None

    def test_clear_completed_only_removes_completed(self, queue):
        done = enqueue_job(queue, video_id="done")
        failed = enqueue_job(queue, video_id="failed")
        enqueue_job(queue, video_id="queued")
        queue.claim_next()
        queue.mark_completed(done["id"])
        queue.claim_next()
        queue.modify(failed["id"], lambda item: item.update(attempts=MAX_ATTEMPTS))
        queue.mark_failed(failed["id"], "boom")
        assert queue.clear_completed() == 1
        remaining = {job["video_id"] for job in queue.jobs()}
        assert remaining == {"failed", "queued"}

    def test_counts_summary(self, queue):
        completed = enqueue_job(queue, video_id="a")
        failed = enqueue_job(queue, video_id="b")
        enqueue_job(queue, video_id="c")  # Stays pending.
        queue.claim_next()
        queue.mark_completed(completed["id"])
        queue.claim_next()
        queue.modify(failed["id"], lambda item: item.update(attempts=MAX_ATTEMPTS))
        queue.mark_failed(failed["id"], "boom")
        pending = enqueue_job(queue, video_id="d")
        queue.claim_next()  # Claims the oldest pending job first.
        assert queue.counts() == {
            "pending": 1,
            "running": 1,
            "completed": 1,
            "failed": 1,
            "cancelled": 0,
        }
        assert queue.get(pending["id"])["state"] == "pending"

    def test_concurrent_claims_have_single_winner(self, queue):
        job = enqueue_job(queue)
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: queue.claim_next(), range(8)))
        winners = [claim for claim in claims if claim is not None]
        assert len(winners) == 1
        assert winners[0]["id"] == job["id"]
        assert queue.get(job["id"])["attempts"] == 1


class TestQueueWorker:
    """Worker behavior with mocked yt-dlp / stub downloaders."""

    def test_success_runs_full_pipeline(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(
                test_server.queue,
                snapshot={
                    "format": "video",
                    "quality": "max",
                    "max_episodes": 5,
                    "sponsorblock_categories": ["sponsor"],
                    "download_delay_seconds": 0,
                },
            )
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        stub = StubDownloader.instances[-1]
        # The channel's current config (audio/best, fixture categories) wins
        # over the snapshot taken at enqueue time.
        assert stub.download_calls == [
            {
                "video_id": "video1",
                "format_type": "audio",
                "quality": "best",
                "sponsorblock": ["sponsor", "intro"],
            }
        ]
        channel_dir = test_server.videos_dir / "test_channel"
        assert (channel_dir / "video1.mp4").read_bytes() == b"media"
        stub.rss_generator.update_channel_refresh_time.assert_called_once_with(
            "test_channel"
        )

    def test_success_through_real_download_video(self, test_server, fast_worker):
        """Real download_video with yt-dlp mocked to produce files."""
        job = enqueue_job(test_server.queue)

        def fake_download(urls):
            channel_dir = test_server.videos_dir / "test_channel"
            channel_dir.mkdir(parents=True, exist_ok=True)
            video_id = job["video_id"]
            metadata = {
                "title": "Title video1",
                "description": "d",
                "upload_date": "20240101",
                "duration": 5,
                "live_status": "was_live",
                "was_live": True,
            }
            (channel_dir / f"{video_id}.info.json").write_text(json.dumps(metadata))
            # The fixture channel is configured for audio extraction.
            (channel_dir / f"{video_id}.m4a").write_bytes(b"media")
            (channel_dir / f"{video_id}.jpg").write_bytes(b"thumb")
            return 0

        mock_ydl_instance = Mock()
        mock_ydl_instance.download.side_effect = fake_download
        with patch("src.downloader.yt_dlp.YoutubeDL") as mock_ytdl:
            mock_ytdl.return_value.__enter__.return_value = mock_ydl_instance
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        channel_dir = test_server.videos_dir / "test_channel"
        assert (channel_dir / "video1.m4a").read_bytes() == b"media"
        simplified = json.loads((channel_dir / "video1.json").read_text())
        assert simplified["id"] == "video1"
        assert simplified["thumbnail"] == "thumbnails/video1.jpg"
        assert simplified["live_status"] == "was_live"
        assert simplified["was_live"] is True
        assert simplified["is_live"] is False
        assert (channel_dir / "thumbnails" / "video1.jpg").exists()
        assert not (channel_dir / "video1.info.json").exists()
        timestamps = json.loads(
            (test_server.config_dir / "refresh_timestamps.json").read_text()
        )
        assert "test_channel" in timestamps

    def test_failure_schedules_retry_and_worker_stays_alive(
        self, test_server, fast_worker
    ):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "failure"
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: (
                    test_server.queue.get(job["id"])["state"] == "pending"
                    and test_server.queue.get(job["id"])["attempts"] == 1
                )
            )
            state = test_server.queue.get(job["id"])
            assert "Download failed" in state["error"]
            assert state["next_attempt_at"] is not None

    def test_failure_through_real_download_cleans_partials(
        self, test_server, fast_worker
    ):
        job = enqueue_job(test_server.queue)
        channel_dir = test_server.videos_dir / "test_channel"

        def failing_download(urls):
            channel_dir.mkdir(parents=True, exist_ok=True)
            (channel_dir / f"{job['video_id']}.mp4.part").write_bytes(b"part")
            (channel_dir / f"{job['video_id']}.webp").write_bytes(b"webp")
            raise RuntimeError("yt-dlp exploded")

        mock_ydl_instance = Mock()
        mock_ydl_instance.download.side_effect = failing_download
        with patch("src.downloader.yt_dlp.YoutubeDL") as mock_ytdl:
            mock_ytdl.return_value.__enter__.return_value = mock_ydl_instance
            assert wait_until(
                lambda: (
                    (state := job_state(test_server.queue, job["id"]))["state"]
                    == "pending"
                    and state["attempts"] == 1
                )
            )
        assert list(channel_dir.glob("video1.*")) == []
        state = test_server.queue.get(job["id"])
        assert state["attempts"] == 1
        assert "Download failed" in state["error"]

    def test_exhausted_attempts_marked_failed_by_worker(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "failure"
            job = enqueue_job(test_server.queue)
            # Pretend four attempts already failed; this run is number five.
            test_server.queue.modify(
                job["id"], lambda item: item.update(attempts=MAX_ATTEMPTS - 1)
            )
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "failed"
            )
            state = test_server.queue.get(job["id"])
            assert state["attempts"] == MAX_ATTEMPTS
            assert "Download failed" in state["error"]

    def test_cancel_aborts_running_job(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "cancel"
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "running"
            )
            updated = test_server.queue.request_cancel(job["id"])
            assert updated["cancel_requested"] is True
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "cancelled"
            )
            assert (
                test_server.videos_dir / "test_channel" / "video1.mp4"
            ).exists() is False
            final = test_server.queue.get(job["id"])
            assert final["error"] == "Cancelled by user"

    def test_cancelled_job_releases_media_lock(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "cancel"
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "running"
            )
            test_server.queue.request_cancel(job["id"])
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "cancelled"
            )
        # The worker must have released the lock; a refresh can start again.
        assert wait_until(lambda: test_server._begin_refresh())
        test_server._finish_refresh()

    def test_progress_is_persisted(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "slow"
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: (
                    (test_server.queue.get(job["id"])["progress"] or {}).get("percent")
                    == 40
                )
            )
            progress = test_server.queue.get(job["id"])["progress"]
            assert progress["speed"] == 8.0
            assert progress["eta"] == 7
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )

    def test_progress_writes_are_throttled(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            StubDownloader.behavior = "burst"
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        stub = StubDownloader.instances[-1]
        assert len(stub.download_calls) == 1

    def test_inter_job_delay_releases_media_lock(self, test_server, fast_worker):
        """A politeness pause must not block manual refreshes or deletes."""
        snapshot = {
            "format": "video",
            "quality": "max",
            "max_episodes": 5,
            "download_delay_seconds": 30,
        }
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(test_server.queue, snapshot=snapshot)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
            lock = test_server._media_lock
            assert wait_until(lambda: lock.acquire(blocking=False))
            lock.release()

    def test_two_workers_never_double_process(self, test_server):
        test_server.queue_worker.stop()
        workers = [
            QueueWorker(
                test_server.queue,
                executor=test_server._execute_queue_job,
                media_lock=test_server._media_lock,
                logger=test_server.logger,
                poll_interval=0.05,
            )
            for _ in range(2)
        ]
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(test_server.queue)
            for worker in workers:
                worker.start()
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
            time.sleep(0.3)  # Give the second worker a chance to misbehave.
        processed = [
            call
            for instance in StubDownloader.instances
            for call in instance.download_calls
        ]
        assert len(processed) == 1
        for worker in workers:
            worker.stop()

    def test_run_pending_drains_queue_synchronously(self, test_server):
        """Cron-style drain: no polling loop, caller holds the media lock."""
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            enqueue_job(test_server.queue, video_id="a")
            enqueue_job(test_server.queue, video_id="b")
            test_server.queue_worker.stop()
            drainer = QueueWorker(
                test_server.queue,
                executor=test_server._execute_queue_job,
                media_lock=None,
                logger=test_server.logger,
            )
            processed = drainer.run_pending()
        assert processed == 2
        assert all(job["state"] == "completed" for job in test_server.queue.jobs())

    def test_worker_stop_requeues_in_flight_job(self, queue):
        job = enqueue_job(queue)
        queue.claim_next()  # Simulate a job that was mid-flight.
        worker = QueueWorker(queue, executor=Mock())
        worker.stop()
        assert queue.get(job["id"])["state"] == "pending"
        assert queue.get(job["id"])["attempts"] == 1

    def test_deleted_channel_cancels_job(self, test_server, fast_worker):
        job = enqueue_job(test_server.queue, channel="ghost_channel")
        assert wait_until(
            lambda: test_server.queue.get(job["id"])["state"] == "cancelled"
        )
        state = test_server.queue.get(job["id"])
        assert "no longer configured" in state["error"]

    def test_already_downloaded_video_completes_without_download(
        self, test_server, fast_worker
    ):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            # Create the artifacts first so video_exists() is true (the
            # fixture channel downloads audio).
            channel_dir = test_server.videos_dir / "test_channel"
            channel_dir.mkdir(parents=True, exist_ok=True)
            (channel_dir / "video1.m4a").write_bytes(b"media")
            (channel_dir / "video1.json").write_text("{}")
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        stub = StubDownloader.instances[-1]
        assert stub.download_calls == []
        stub.rss_generator.update_channel_refresh_time.assert_called_once_with(
            "test_channel"
        )

    def test_current_channel_config_wins_over_snapshot(self, test_server, fast_worker):
        # The fixture config has format audio / quality best for test_channel.
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(
                test_server.queue,
                snapshot={"format": "video", "quality": "max"},
            )
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        stub = StubDownloader.instances[-1]
        assert stub.download_calls[0]["format_type"] == "audio"
        assert stub.download_calls[0]["quality"] == "best"


class TestQueueExecutor:
    """Direct checks for the shared executor helper."""

    def test_channel_missing_error(self, tmp_path, queue):
        config_path = tmp_path / "channels.yaml"
        config_path.write_text(
            yaml.safe_dump({"channels": [{"name": "other", "url": "u"}]})
        )
        job = enqueue_job(queue, channel="test_channel")
        with pytest.raises(ChannelMissingError):
            execute_queue_job(
                job,
                videos_dir=tmp_path / "media",
                config_path=config_path,
                cancel_check=lambda: False,
            )

    def test_snapshot_used_when_channel_has_no_settings(self, tmp_path, queue):
        config_path = tmp_path / "channels.yaml"
        config_path.write_text(
            yaml.safe_dump({"channels": [{"name": "test_channel", "url": "u"}]})
        )
        job = enqueue_job(
            queue,
            snapshot={
                "format": "audio",
                "quality": "max",
                "sponsorblock_categories": ["intro"],
            },
        )
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            execute_queue_job(
                job,
                videos_dir=tmp_path / "media",
                config_path=config_path,
                cancel_check=lambda: False,
            )
        stub = StubDownloader.instances[-1]
        assert stub.download_calls[0]["format_type"] == "audio"
        assert stub.download_calls[0]["sponsorblock"] == ["intro"]


class TestQueueAPI:
    """HTTP surface: auth, validation, transitions, 404/409."""

    def test_queue_endpoints_require_auth(self, auth_client):
        assert auth_client.get("/api/queue").status_code == 401
        assert auth_client.get("/api/queue/somejob").status_code == 401
        assert auth_client.post("/api/queue/somejob/cancel").status_code == 401
        assert auth_client.post("/api/queue/somejob/retry").status_code == 401
        assert auth_client.post("/api/queue/somejob/dismiss").status_code == 401
        assert auth_client.post("/api/queue/clear-completed").status_code == 401

    def test_empty_queue_lists_summary(self, quiet_client):
        response = quiet_client.get("/api/queue")
        assert response.status_code == 200
        data = response.get_json()
        assert data["jobs"] == []
        assert data["summary"] == {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }

    def test_queue_listing_with_summary_and_filter(self, quiet_server, quiet_client):
        enqueue_job(quiet_server.queue, video_id="kept")
        response = quiet_client.get("/api/queue")
        data = response.get_json()
        assert data["summary"]["pending"] == 1
        assert data["jobs"][0]["title"] == "Title kept"

        filtered = quiet_client.get("/api/queue?state=failed")
        assert filtered.get_json()["jobs"] == []

        claimed = quiet_server.queue.claim_next()
        quiet_server.queue.modify(
            claimed["id"], lambda item: item.update(attempts=MAX_ATTEMPTS)
        )
        quiet_server.queue.mark_failed(claimed["id"], "boom")
        failed_only = quiet_client.get("/api/queue?state=failed").get_json()
        assert [job["state"] for job in failed_only["jobs"]] == ["failed"]
        assert failed_only["summary"]["failed"] == 1

    def test_invalid_state_filter_rejected(self, quiet_client):
        assert quiet_client.get("/api/queue?state=bogus").status_code == 400

    def test_job_detail_and_404(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        found = quiet_client.get(f"/api/queue/{job['id']}")
        assert found.status_code == 200
        assert found.get_json()["id"] == job["id"]
        assert quiet_client.get("/api/queue/unknown").status_code == 404

    def test_cancel_pending_via_api(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        response = quiet_client.post(f"/api/queue/{job['id']}/cancel")
        assert response.status_code == 200
        data = response.get_json()
        assert data["job"]["state"] == "cancelled"
        assert quiet_server.queue.get(job["id"])["state"] == "cancelled"

    def test_cancel_running_sets_flag_via_api(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        quiet_server.queue.claim_next()
        response = quiet_client.post(f"/api/queue/{job['id']}/cancel")
        assert response.status_code == 200
        data = response.get_json()
        assert data["job"]["cancel_requested"] is True
        assert quiet_server.queue.disk_cancel_requested(job["id"]) is True

    def test_cancel_terminal_conflict(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        quiet_server.queue.claim_next()
        quiet_server.queue.mark_completed(job["id"])
        response = quiet_client.post(f"/api/queue/{job['id']}/cancel")
        assert response.status_code == 409
        assert "completed" in response.get_json()["error"]

    def test_cancel_unknown_job(self, quiet_client):
        assert quiet_client.post("/api/queue/none/cancel").status_code == 404

    def test_retry_failed_via_api(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        quiet_server.queue.claim_next()
        quiet_server.queue.modify(
            job["id"], lambda item: item.update(attempts=MAX_ATTEMPTS)
        )
        quiet_server.queue.mark_failed(job["id"], "boom")
        response = quiet_client.post(f"/api/queue/{job['id']}/retry")
        assert response.status_code == 200
        retried = response.get_json()["job"]
        assert retried["state"] == "pending"
        assert retried["attempts"] == 0
        assert quiet_server.queue.get(job["id"])["error"] is None

    def test_retry_active_conflict_and_unknown(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        assert quiet_client.post(f"/api/queue/{job['id']}/retry").status_code == 409
        assert quiet_client.post("/api/queue/none/retry").status_code == 404

    def test_dismiss_completed_and_active_conflict(self, quiet_server, quiet_client):
        job = enqueue_job(quiet_server.queue)
        assert quiet_client.post(f"/api/queue/{job['id']}/dismiss").status_code == 409
        quiet_server.queue.claim_next()
        quiet_server.queue.mark_completed(job["id"])
        response = quiet_client.post(f"/api/queue/{job['id']}/dismiss")
        assert response.status_code == 200
        assert quiet_server.queue.get(job["id"]) is None
        assert quiet_client.post("/api/queue/none/dismiss").status_code == 404

    def test_clear_completed_via_api(self, quiet_server, quiet_client):
        first = enqueue_job(quiet_server.queue, video_id="a")
        enqueue_job(quiet_server.queue, video_id="b")
        quiet_server.queue.claim_next()
        quiet_server.queue.mark_completed(first["id"])
        response = quiet_client.post("/api/queue/clear-completed")
        assert response.status_code == 200
        assert response.get_json()["removed"] == 1
        remaining = {job["video_id"] for job in quiet_server.queue.jobs()}
        assert remaining == {"b"}


class TestRefreshIntegration:
    """Discovery enqueues instead of downloading inline."""

    @pytest.fixture
    def discovery_stub(self):
        instances: list[Any] = []

        class DiscoveryStub:
            instances: ClassVar[list[Any]]

            def __init__(self, config_path, videos_dir=None):
                instances.append(self)
                self.videos_dir = Path(videos_dir) if videos_dir else None
                self.logger = logging.getLogger("discovery-stub")
                self.rss_generator = Mock()
                self.video_calls: list[str] = []

            def get_channel_videos(self, url, max_episodes=10):
                self.max_episodes_used = max_episodes
                return [
                    {
                        "id": "existing",
                        "title": "Existing",
                        "url": "https://www.youtube.com/watch?v=existing",
                    },
                    {
                        "id": "toonew",
                        "title": "Too New",
                        "url": "https://www.youtube.com/watch?v=toonew",
                    },
                    {
                        "id": "metafail",
                        "title": "Meta Fail",
                        "url": "https://www.youtube.com/watch?v=metafail",
                    },
                    {
                        "id": "fresh",
                        "title": "Fresh",
                        "url": "https://www.youtube.com/watch?v=fresh",
                    },
                ]

            def get_video_metadata(self, url):
                video_id = url.rsplit("=", 1)[-1]
                self.video_calls.append(video_id)
                if video_id == "metafail":
                    return {}
                return {"id": video_id, "timestamp": time.time() - 100000}

            def video_exists(self, channel_name, video_id, format_type="video"):
                return video_id == "existing"

            def is_video_too_new(self, video_info, download_delay_hours):
                return video_info["id"] == "toonew"

        DiscoveryStub.instances = instances
        return DiscoveryStub

    def test_discovery_enqueues_only_downloadable(self, test_server, discovery_stub):
        with patch("src.downloader.YouTubeDownloader", discovery_stub):
            assert test_server._begin_refresh()
            test_server._run_refresh("test_channel")
        jobs = test_server.queue.jobs()
        assert [job["video_id"] for job in jobs] == ["fresh"]
        job = jobs[0]
        assert job["state"] == "pending"
        assert job["title"] == "Fresh"
        assert job["channel"] == "test_channel"
        assert job["config_snapshot"]["format"] == "audio"
        assert job["config_snapshot"]["max_episodes"] == 5
        assert job["config_snapshot"]["download_delay_seconds"] == 0
        assert test_server.refresh_status["running"] is False

    def test_discovery_respects_max_episodes_argument(
        self, test_server, discovery_stub
    ):
        with patch("src.downloader.YouTubeDownloader", discovery_stub):
            assert test_server._begin_refresh()
            test_server._run_refresh("test_channel")
        assert discovery_stub.instances[0].max_episodes_used == 5

    def test_status_includes_queue_counts(self, test_server, client):
        enqueue_job(test_server.queue, video_id="counted")
        data = client.get("/api/refresh/status").get_json()
        assert data["queue"]["pending"] == 1
        assert data["running"] is False

    def test_full_cycle_discovery_then_worker_completes(
        self, test_server, discovery_stub, fast_worker
    ):
        with patch("src.downloader.YouTubeDownloader", discovery_stub):
            assert test_server._begin_refresh()
            test_server._run_refresh("test_channel")
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            with patch("src.download_queue.cleanup_episodes") as cleanup:
                job = test_server.queue.jobs()[0]
                assert wait_until(
                    lambda: test_server.queue.get(job["id"])["state"] == "completed"
                )
                cleanup.assert_called_once_with(
                    test_server.videos_dir,
                    "test_channel",
                    5,
                    pinned=set(),
                )
