"""Explicit episode redownloads run as ``redownload`` jobs in the queue.

The endpoint only validates and enqueues; the job replaces the stored
episode through a staging directory. ``YouTubeDownloader.download_video`` is
always faked, so nothing here contacts YouTube.
"""

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml

from src.download_queue import MAX_ATTEMPTS, QueueWorker
from src.storage import load_tombstones, record_tombstone
from src.utils import MediaLock
from src.web_server import YouTubePodcastServer
from tests.test_web_server import (
    another_episode_metadata as another_episode_metadata,
)
from tests.test_web_server import (
    app_config as app_config,
)
from tests.test_web_server import (
    client as client,
)
from tests.test_web_server import (
    episode_metadata as episode_metadata,
)
from tests.test_web_server import (
    setup_test_episodes as setup_test_episodes,
)
from tests.test_web_server import (
    test_server as test_server,
)

REDOWNLOAD_URL = "/api/channels/test_channel/episodes/test123abc/redownload"
DOWNLOAD_VIDEO = "src.downloader.YouTubeDownloader.download_video"


def run_queued_job(server: YouTubePodcastServer, job_id: str) -> dict[str, Any]:
    """Claim the due job ``job_id`` and run it through the real worker.

    The server's background worker must already be stopped.

    Returns:
        The job as persisted after the attempt.
    """
    worker = QueueWorker(
        server.queue,
        executor=server._execute_queue_job,
        media_lock=None,
        logger=server.logger,
    )
    claimed = server.queue.claim_next()
    assert claimed is not None and claimed["id"] == job_id
    worker._execute(claimed)
    final = server.queue.get(job_id)
    assert final is not None
    return final


def queue_redownload(
    server: YouTubePodcastServer, episode_id: str = "test123abc"
) -> dict[str, Any]:
    """Stop the background worker and queue a redownload through the API."""
    server.queue_worker.stop()
    response = server.app.test_client().post(
        f"/api/channels/test_channel/episodes/{episode_id}/redownload"
    )
    assert response.status_code == 202, response.get_json()
    return response.get_json()["job"]


def redownload_and_run(
    server: YouTubePodcastServer, episode_id: str = "test123abc"
) -> dict[str, Any]:
    """Queue a redownload through the API and run the job synchronously."""
    return run_queued_job(server, queue_redownload(server, episode_id)["id"])


class StagedDownload:
    """``download_video`` fake (patch with ``autospec=True``).

    Writes into the storage root of the downloader it is called on, which
    for redownloads is the temporary staging directory.

    Attributes:
        outcome: ``success``, ``fail`` (returns False), ``no_metadata``,
            ``empty_media``, ``no_media`` or ``cancel`` (reports progress
            after ``before_progress`` ran, like a cancelled yt-dlp run).
        before_progress: Optional callback run before progress is reported.
        calls: Storage root and keyword arguments of every call.
    """

    def __init__(
        self,
        outcome: str = "success",
        before_progress: Callable[[], Any] | None = None,
    ) -> None:
        self.outcome = outcome
        self.before_progress = before_progress
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        downloader: Any,
        video_info: dict[str, Any],
        channel_name: str,
        **kwargs: Any,
    ) -> bool:
        self.calls.append({"videos_dir": Path(downloader.videos_dir), **kwargs})
        channel_dir = Path(downloader.videos_dir) / channel_name
        (channel_dir / "thumbnails").mkdir(parents=True, exist_ok=True)
        video_id = video_info["id"]
        if self.outcome == "fail":
            (channel_dir / f"{video_id}.mp4.part").write_bytes(b"partial")
            return False
        if self.outcome != "no_metadata":
            (channel_dir / f"{video_id}.json").write_text(
                json.dumps({"id": video_id, "title": "Fresh title"})
            )
        if self.outcome != "no_media":
            media = b"" if self.outcome == "empty_media" else b"replacement"
            (channel_dir / f"{video_id}.mp4").write_bytes(media)
        (channel_dir / "thumbnails" / f"{video_id}.jpg").write_bytes(b"fresh thumb")
        if self.before_progress is not None:
            self.before_progress()
        if self.outcome == "cancel":
            kwargs["progress_hook"](
                {"status": "downloading", "downloaded_bytes": 1, "total_bytes": 10}
            )
        return True


def file_contents(root: Path) -> dict[str, bytes]:
    """Map every file below ``root`` to its bytes."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def staging_dirs(server: YouTubePodcastServer) -> list[Path]:
    """List leftover redownload staging directories."""
    return list(server.videos_dir.glob(".redownload-*"))


def update_config(
    server: YouTubePodcastServer,
    channel: dict[str, Any] | None = None,
    **top_level: Any,
) -> None:
    """Rewrite channels.yaml with test_channel and top-level changes."""
    path = server.config_dir / "channels.yaml"
    config = yaml.safe_load(path.read_text())
    config.update(top_level)
    if channel is not None:
        config["channels"][0].update(channel)
    path.write_text(yaml.safe_dump(config))


class TestRedownloadEndpoint:
    """The endpoint validates, enqueues and answers immediately."""

    def test_queues_job_without_downloading(
        self, client, test_server, setup_test_episodes
    ):
        test_server.queue_worker.stop()
        before = file_contents(test_server.videos_dir)
        with patch(DOWNLOAD_VIDEO) as download:
            started = time.monotonic()
            response = client.post(REDOWNLOAD_URL)
            elapsed = time.monotonic() - started

        assert response.status_code == 202
        data = response.get_json()
        assert "queued" in data["message"]
        job = data["job"]
        assert job["kind"] == "redownload"
        assert job["state"] == "pending"
        assert job["channel"] == "test_channel"
        assert job["video_id"] == "test123abc"
        assert job["title"] == "Test Episode: How to Test"
        assert job["url"] == "https://www.youtube.com/watch?v=test123abc"
        assert job["config_snapshot"]["format"] == "audio"
        assert test_server.queue.jobs() == [job]
        download.assert_not_called()
        assert elapsed < 2
        assert file_contents(test_server.videos_dir) == before
        assert staging_dirs(test_server) == []

    @pytest.mark.parametrize(
        "url,error",
        [
            ("/api/channels/ghost_channel/episodes/test123abc/redownload", "Channel"),
            ("/api/channels/test_channel/episodes/ghost/redownload", "Episode"),
        ],
    )
    def test_unknown_channel_or_episode_is_404(
        self, client, test_server, setup_test_episodes, url, error
    ):
        test_server.queue_worker.stop()
        response = client.post(url)
        assert response.status_code == 404
        assert response.get_json()["error"] == f"{error} not found"
        assert test_server.queue.jobs() == []

    def test_tombstoned_episode_without_files_can_be_queued(
        self, client, test_server, setup_test_episodes
    ):
        test_server.queue_worker.stop()
        record_tombstone(test_server.config_dir, "test_channel", "gone1", "Gone")
        response = client.post("/api/channels/test_channel/episodes/gone1/redownload")
        assert response.status_code == 202
        assert response.get_json()["job"]["title"] == "Gone"

    @pytest.mark.parametrize("existing_kind", ["redownload", "download"])
    def test_active_job_for_the_episode_is_409(
        self, client, test_server, setup_test_episodes, existing_kind
    ):
        test_server.queue_worker.stop()
        existing = test_server.queue.enqueue(
            channel="test_channel",
            video_id="test123abc",
            title="Queued",
            url="https://www.youtube.com/watch?v=test123abc",
            kind=existing_kind,
        )
        assert existing is not None

        response = client.post(REDOWNLOAD_URL)

        assert response.status_code == 409
        data = response.get_json()
        assert "already queued" in data["error"]
        assert data["job"]["id"] == existing["id"]
        assert [job["id"] for job in test_server.queue.jobs()] == [existing["id"]]

    def test_unusable_queue_is_500_not_409(
        self, client, test_server, setup_test_episodes, unreadable_files
    ):
        test_server.queue_worker.stop()
        test_server.queue.enqueue(channel="c", video_id="v", title="t", url="u")
        unreadable_files(test_server.config_dir / "download_queue.json")
        response = client.post(REDOWNLOAD_URL)
        assert response.status_code == 500
        assert response.get_json()["error"] == "Could not queue redownload"

    def test_accepted_while_media_lock_is_held(
        self, client, test_server, setup_test_episodes
    ):
        """Another process (e.g. cron) holding media.lock does not block it."""
        test_server.queue_worker.stop()
        other_process = MediaLock(test_server.config_dir / "media.lock")
        assert other_process.acquire(blocking=False)
        try:
            assert not test_server._media_lock.acquire(blocking=False)
            response = client.post(REDOWNLOAD_URL)
        finally:
            other_process.release()
        assert response.status_code == 202
        assert test_server.queue.get(response.get_json()["job"]["id"]) is not None


class TestRedownloadExecution:
    """Queue execution replaces the episode only with a complete copy."""

    def test_success_replaces_media_metadata_and_thumbnail(
        self, test_server, setup_test_episodes
    ):
        channel_dir = setup_test_episodes
        untouched = {
            name: content
            for name, content in file_contents(channel_dir).items()
            if "test456def" in name
        }
        fake = StagedDownload()
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=fake):
            job = redownload_and_run(test_server)

        assert job["state"] == "completed"
        assert job["error"] is None
        assert (channel_dir / "test123abc.mp4").read_bytes() == b"replacement"
        assert not (channel_dir / "test123abc.m4a").exists()
        metadata = json.loads((channel_dir / "test123abc.json").read_text())
        assert metadata["title"] == "Fresh title"
        thumbnail = channel_dir / "thumbnails" / "test123abc.jpg"
        assert thumbnail.read_bytes() == b"fresh thumb"
        current = file_contents(channel_dir)
        assert {name: current[name] for name in untouched} == untouched
        staged_root = fake.calls[0]["videos_dir"]
        assert staged_root.parent == test_server.videos_dir
        assert staged_root.name.startswith(".redownload-")
        assert staging_dirs(test_server) == []

    @pytest.mark.parametrize(
        "outcome", ["fail", "no_metadata", "empty_media", "no_media"]
    )
    def test_incomplete_download_keeps_original_and_retries(
        self, test_server, setup_test_episodes, outcome
    ):
        before = file_contents(test_server.videos_dir)
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload(outcome)):
            job = redownload_and_run(test_server)

        assert job["state"] == "pending"
        assert job["attempts"] == 1
        assert job["next_attempt_at"] is not None
        assert "original episode preserved" in job["error"]
        assert file_contents(test_server.videos_dir) == before
        assert staging_dirs(test_server) == []

    def test_last_failed_attempt_fails_job_and_keeps_original(
        self, test_server, setup_test_episodes
    ):
        before = file_contents(test_server.videos_dir)
        job = queue_redownload(test_server)
        test_server.queue.modify(
            job["id"], lambda item: item.update(attempts=MAX_ATTEMPTS - 1)
        )
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload("fail")):
            final = run_queued_job(test_server, job["id"])

        assert final["state"] == "failed"
        assert "original episode preserved" in final["error"]
        assert file_contents(test_server.videos_dir) == before

    @pytest.mark.parametrize("outcome", ["cancel", "success"])
    def test_cancellation_keeps_original_and_removes_staging(
        self, test_server, setup_test_episodes, outcome
    ):
        """Cancelled mid-download, or after the last progress report."""
        before = file_contents(test_server.videos_dir)
        job = queue_redownload(test_server)
        fake = StagedDownload(
            outcome, before_progress=lambda: test_server.queue.request_cancel(job["id"])
        )
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=fake):
            final = run_queued_job(test_server, job["id"])

        assert final["state"] == "cancelled"
        assert final["error"] == "Cancelled by user"
        assert file_contents(test_server.videos_dir) == before
        assert staging_dirs(test_server) == []

    def test_success_clears_tombstone(self, test_server, setup_test_episodes):
        record_tombstone(test_server.config_dir, "test_channel", "test123abc", "T")
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload()):
            assert redownload_and_run(test_server)["state"] == "completed"
        assert load_tombstones(test_server.config_dir) == {}

    def test_tombstoned_episode_without_files_is_restored(
        self, test_server, setup_test_episodes
    ):
        record_tombstone(test_server.config_dir, "test_channel", "gone1", "Gone")
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload()):
            job = redownload_and_run(test_server, "gone1")

        assert job["state"] == "completed"
        assert (setup_test_episodes / "gone1.mp4").read_bytes() == b"replacement"
        assert (setup_test_episodes / "gone1.json").is_file()
        assert load_tombstones(test_server.config_dir) == {}

    def test_filters_and_tombstones_do_not_apply(
        self, test_server, setup_test_episodes
    ):
        job = queue_redownload(test_server)
        update_config(
            test_server,
            channel={"title_exclude": ["Test Episode"], "skip_shorts": True},
        )
        record_tombstone(test_server.config_dir, "test_channel", "test123abc", "T")
        metadata = Mock(return_value={"duration": 10})
        with (
            patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload()),
            patch("src.downloader.YouTubeDownloader.get_video_metadata", metadata),
        ):
            final = run_queued_job(test_server, job["id"])

        assert final["state"] == "completed"
        metadata.assert_not_called()
        assert (setup_test_episodes / "test123abc.mp4").exists()

    def test_low_free_space_blocks_terminally(self, test_server, setup_test_episodes):
        update_config(test_server, storage={"min_free_space_gb": 10_000_000})
        before = file_contents(test_server.videos_dir)
        with patch(DOWNLOAD_VIDEO, autospec=True) as download:
            job = redownload_and_run(test_server)

        assert job["state"] == "failed"
        assert "Insufficient free disk space" in job["error"]
        assert job["next_attempt_at"] is None
        download.assert_not_called()
        assert file_contents(test_server.videos_dir) == before

    def test_quotas_are_ignored(self, test_server, setup_test_episodes):
        update_config(
            test_server,
            channel={"quota_gb": 0.000001},
            storage={"global_quota_gb": 0.000001, "min_free_space_gb": 0},
        )
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload()):
            job = redownload_and_run(test_server)
        assert job["state"] == "completed"

    def test_uses_current_channel_settings(self, test_server, setup_test_episodes):
        job = queue_redownload(test_server)
        assert job["config_snapshot"]["quality"] == "best"
        update_config(
            test_server,
            channel={
                "format": "video",
                "quality": "720p",
                "sponsorblock_categories": ["selfpromo"],
            },
        )
        fake = StagedDownload()
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=fake):
            run_queued_job(test_server, job["id"])

        call = fake.calls[0]
        assert call["format_type"] == "video"
        assert call["quality"] == "720p"
        assert call["sponsorblock_categories"] == ["selfpromo"]

    def test_does_not_run_cleanup_or_retention(self, test_server, setup_test_episodes):
        update_config(
            test_server,
            channel={"max_episodes": 1},
            storage={"retention_days": 1, "min_free_space_gb": 0},
        )
        with patch(DOWNLOAD_VIDEO, autospec=True, side_effect=StagedDownload()):
            assert redownload_and_run(test_server)["state"] == "completed"
        assert (setup_test_episodes / "test456def.m4a").exists()
        assert (setup_test_episodes / "test123abc.mp4").exists()

    def test_removed_channel_cancels_job(self, test_server, setup_test_episodes):
        job = queue_redownload(test_server)
        config_path = test_server.config_dir / "channels.yaml"
        config = yaml.safe_load(config_path.read_text())
        config["channels"] = config["channels"][1:]
        config_path.write_text(yaml.safe_dump(config))
        with patch(DOWNLOAD_VIDEO, autospec=True) as download:
            final = run_queued_job(test_server, job["id"])
        assert final["state"] == "cancelled"
        assert "no longer configured" in final["error"]
        download.assert_not_called()

    def test_unreadable_config_defers_job(self, test_server, setup_test_episodes):
        job = queue_redownload(test_server)
        (test_server.config_dir / "channels.yaml").write_text("channels: [broken\n")
        with patch(DOWNLOAD_VIDEO, autospec=True) as download:
            final = run_queued_job(test_server, job["id"])
        assert final["state"] == "pending"
        assert final["attempts"] == 0
        assert "channels.yaml" in final["error"]
        download.assert_not_called()
