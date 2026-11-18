"""Per-channel episode filters and tombstones of deleted episodes.

Covers validation, matching semantics, Shorts/livestream detection,
discovery and queue-execution enforcement, feed/episodes-API visibility,
and the tombstone lifecycle (record, skip, forget, purge, corrupt file).
"""

import json
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock, patch

import pytest
import yaml

from src.downloader import YouTubeDownloader
from src.health import load_health
from src.storage import (
    load_tombstones,
    record_tombstone,
    remove_channel_tombstones,
    remove_episode_tombstone,
    tombstoned_ids,
    tombstones_path,
)
from src.utils import (
    MAX_TITLE_FILTER_LENGTH,
    MAX_TITLE_FILTER_RULES,
    SHORTS_MAX_DURATION_SECONDS,
    StateFileError,
    filter_skip_reason,
    is_livestream_entry,
    load_episode_filters,
    validate_channel_settings,
)
from tests.test_download_queue import (
    StubDownloader,
    enqueue_job,
)
from tests.test_download_queue import (
    fast_worker as fast_worker,
)
from tests.test_download_queue import wait_until as wait_until
from tests.test_redownload import redownload_and_run
from tests.test_web_server import another_episode_metadata as another_episode_metadata
from tests.test_web_server import app_config as app_config
from tests.test_web_server import client as client
from tests.test_web_server import episode_metadata as episode_metadata
from tests.test_web_server import setup_test_episodes as setup_test_episodes
from tests.test_web_server import test_server as test_server


def update_channel_config(test_server, channel_name: str = "test_channel", **fields):
    """Rewrite one channel's entry in the server's ``channels.yaml``."""
    config_file = test_server.config_dir / "channels.yaml"
    config = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    for channel in config["channels"]:
        if channel["name"] == channel_name:
            channel.update(fields)
            break
    config_file.write_text(yaml.safe_dump(config), encoding="utf-8")


def channel_entry(test_server, channel_name: str = "test_channel", **overrides):
    """Return a channel config entry (freshly read, with overrides)."""
    config = yaml.safe_load(
        (test_server.config_dir / "channels.yaml").read_text(encoding="utf-8")
    )
    channel = next(ch for ch in config["channels"] if ch["name"] == channel_name)
    channel.update(overrides)
    return channel


def recent_log_text(test_server) -> str:
    """Join the server's UI log messages for assertions."""
    return "\n".join(entry["message"] for entry in test_server.recent_logs)


def tombstone_backups(config_dir: Path) -> list[Path]:
    """List quarantine backups of ``tombstones.json``."""
    path = tombstones_path(config_dir)
    return sorted(path.parent.glob(f"{path.name}.corrupt*"))


def make_tombstones_unreadable(config_dir: Path, unreadable_files) -> bytes:
    """Record one tombstone, then make the file unreadable; return its bytes."""
    record_tombstone(config_dir, "test_channel", "video1", "Deleted video1")
    path = tombstones_path(config_dir)
    unreadable_files(path)
    return path.read_bytes()


class FakeDiscoveryDownloader:
    """Downloader double serving canned flat entries and metadata.

    Metadata for a flat entry defaults to a detailed-metadata-shaped dict
    built from the entry itself; explicit overrides win.
    """

    def __init__(self, videos: list[dict[str, Any]]):
        self.videos = videos
        self.metadata_by_url: dict[str, dict[str, Any]] = {}
        self.metadata_requests: list[str] = []

    def get_channel_videos(self, url: str, max_episodes: int = 10):
        return deepcopy(self.videos)

    def get_video_metadata(self, url: str):
        self.metadata_requests.append(url)
        if url in self.metadata_by_url:
            return dict(self.metadata_by_url[url])
        video = next((entry for entry in self.videos if entry["url"] == url), None)
        if video is None:
            return {}
        return {
            "id": video["id"],
            "title": video.get("title"),
            "upload_date": video.get("upload_date"),
            "timestamp": None,
            "duration": video.get("duration"),
            "filesize": None,
            "url": url,
        }

    def video_exists(self, channel_name: str, video_id: str, format_type="video"):
        return False

    def is_video_too_new(self, video, download_delay_hours):
        return False


class MetadataStubDownloader(StubDownloader):
    """Stub queue downloader that also serves metadata for filter checks."""

    metadata_to_serve: ClassVar[dict[str, dict[str, Any]]] = {}
    metadata_requests: ClassVar[list[str]] = []

    def get_video_metadata(self, url: str):
        type(self).metadata_requests.append(url)
        return dict(type(self).metadata_to_serve.get(url, {}))


class FakeStagedDownloader(YouTubeDownloader):
    """Real downloader whose ``download_video`` never contacts YouTube.

    Writes a complete episode (media, metadata, thumbnail) into the
    videos directory it was given — a redownload job stages into a
    temporary directory and moves successful results into the channel.
    """

    def download_video(
        self,
        video_info: dict[str, Any],
        channel_name: str,
        sponsorblock_categories=None,
        format_type: str = "video",
        quality: str = "max",
        progress_hook=None,
    ) -> bool:
        channel_dir = self.videos_dir / channel_name
        channel_dir.mkdir(parents=True, exist_ok=True)
        video_id = video_info["id"]
        extension = ".m4a" if format_type == "audio" else ".mp4"
        # The real download_video leaves the simplified metadata file,
        # the media file, and the thumbnail under thumbnails/.
        (channel_dir / f"{video_id}.json").write_text(
            json.dumps(
                {
                    "id": video_id,
                    "title": video_info.get("title", "Staged"),
                    "upload_date": video_info.get("upload_date") or "20241201",
                    "duration": video_info.get("duration") or 1234,
                    "thumbnail": f"thumbnails/{video_id}.jpg",
                }
            ),
            encoding="utf-8",
        )
        (channel_dir / f"{video_id}{extension}").write_bytes(b"fresh-staged-audio")
        thumbnails = channel_dir / "thumbnails"
        thumbnails.mkdir(exist_ok=True)
        (thumbnails / f"{video_id}.jpg").write_bytes(b"thumb")
        return True


@pytest.fixture(autouse=True)
def reset_metadata_stub():
    """Give every test a clean metadata registry for the stub downloader."""
    MetadataStubDownloader.metadata_to_serve = {}
    MetadataStubDownloader.metadata_requests = []
    yield
    MetadataStubDownloader.metadata_to_serve = {}
    MetadataStubDownloader.metadata_requests = []


class TestFilterValidation:
    """validate_channel_settings covers every filter field."""

    def test_all_fields_accepted(self):
        validate_channel_settings(
            {
                "title_include": ["Review", " interview"],
                "title_exclude": ["trailer"],
                "min_duration_seconds": 60,
                "max_duration_seconds": 7200,
                "skip_shorts": True,
                "skip_livestreams": False,
            }
        )

    @pytest.mark.parametrize(
        "payload",
        [
            {"title_include": "review"},
            {"title_include": [1]},
            {"title_include": [True]},
            {"title_include": [""]},
            {"title_include": ["   "]},
            {"title_exclude": "trailer"},
            {"title_exclude": [None]},
            {"min_duration_seconds": "60"},
            {"min_duration_seconds": 1.5},
            {"min_duration_seconds": -1},
            {"min_duration_seconds": True},
            {"max_duration_seconds": -5},
            {"max_duration_seconds": False},
            {"skip_shorts": 1},
            {"skip_shorts": "yes"},
            {"skip_livestreams": 0},
            {"skip_livestreams": None},
        ],
    )
    def test_invalid_values_rejected(self, payload):
        with pytest.raises(ValueError):
            validate_channel_settings(payload)

    def test_min_greater_than_max_rejected(self):
        with pytest.raises(ValueError, match="min_duration_seconds"):
            validate_channel_settings(
                {"min_duration_seconds": 600, "max_duration_seconds": 60}
            )

    def test_min_equal_max_accepted(self):
        validate_channel_settings(
            {"min_duration_seconds": 60, "max_duration_seconds": 60}
        )

    def test_zero_bounds_are_valid(self):
        validate_channel_settings(
            {"min_duration_seconds": 0, "max_duration_seconds": 0}
        )

    def test_unknown_keys_ignored(self):
        validate_channel_settings({"title_filter_hack": "x", "skip_extra": True})

    def test_rule_bounds_enforced(self):
        with pytest.raises(ValueError):
            validate_channel_settings(
                {"title_include": ["x"] * (MAX_TITLE_FILTER_RULES + 1)}
            )
        with pytest.raises(ValueError):
            validate_channel_settings(
                {"title_include": ["x" * (MAX_TITLE_FILTER_LENGTH + 1)]}
            )

    def test_back_compat_configs_pass(self):
        validate_channel_settings({"max_episodes": 5, "format": "audio"})

    def test_load_episode_filters_defaults(self):
        filters = load_episode_filters({"name": "ch", "url": "x"})
        assert filters == {
            "title_include": [],
            "title_exclude": [],
            "min_duration_seconds": 0,
            "max_duration_seconds": 0,
            "skip_shorts": False,
            "skip_livestreams": False,
        }

    def test_load_episode_filters_tolerates_invalid_values(self):
        filters = load_episode_filters(
            {
                "title_include": "nope",
                "title_exclude": ["ok", 5, ""],
                "min_duration_seconds": "many",
                "max_duration_seconds": True,
                "skip_shorts": 1,
                "skip_livestreams": "yes",
            }
        )
        assert filters["title_include"] == []
        assert filters["title_exclude"] == ["ok"]
        assert filters["min_duration_seconds"] == 0
        assert filters["max_duration_seconds"] == 0
        assert filters["skip_shorts"] is False
        assert filters["skip_livestreams"] is False


class TestFilterMatching:
    """Title, duration, Shorts and livestream matching semantics."""

    def filters(self, **overrides):
        return load_episode_filters({"name": "ch", **overrides})

    def test_no_filters_match_everything(self):
        filters = self.filters()
        assert (
            filter_skip_reason({"title": "Anything", "duration": 10}, filters) is None
        )

    def test_include_match_is_case_insensitive(self):
        filters = self.filters(title_include=["REVIEW"])
        assert (
            filter_skip_reason({"title": "My Great review of things"}, filters) is None
        )
        reason = filter_skip_reason({"title": "Unrelated topic"}, filters)
        assert reason is not None and "title matches no include rule" in reason

    def test_include_whitespace_is_ignored(self):
        filters = self.filters(title_include=["  review  "])
        assert filter_skip_reason({"title": "Weekly Review #12"}, filters) is None

    def test_exclude_wins_over_include(self):
        filters = self.filters(title_include=["review"], title_exclude=["short"])
        reason = filter_skip_reason({"title": "Review short clips"}, filters)
        assert reason is not None and "exclude rule 'short'" in reason

    def test_unicode_casefold_matching(self):
        filters = self.filters(title_include=["STRASSE"])
        assert filter_skip_reason({"title": "Die straße von berlin"}, filters) is None
        emoji_filters = self.filters(title_include=["🚀"])
        assert (
            filter_skip_reason({"title": "Rocket launch 🚀 today"}, emoji_filters)
            is None
        )

    def test_empty_lists_behave_like_absent(self):
        filters = self.filters(title_include=[], title_exclude=[])
        assert filter_skip_reason({"title": "Anything at all"}, filters) is None

    def test_missing_title_fails_include_only(self):
        filters = self.filters(title_include=["review"])
        assert filter_skip_reason({"duration": 100}, filters) is not None
        filters = self.filters(title_exclude=["review"])
        assert filter_skip_reason({"duration": 100}, filters) is None

    def test_min_duration_filters_short_video(self):
        filters = self.filters(min_duration_seconds=300)
        reason = filter_skip_reason({"title": "t", "duration": 299}, filters)
        assert reason is not None and "below min_duration_seconds (300s)" in reason
        assert filter_skip_reason({"title": "t", "duration": 300}, filters) is None

    def test_max_duration_filters_long_video(self):
        filters = self.filters(max_duration_seconds=3600)
        reason = filter_skip_reason({"title": "t", "duration": 3601}, filters)
        assert reason is not None and "exceeds max_duration_seconds (3600s)" in reason
        assert filter_skip_reason({"title": "t", "duration": 3600}, filters) is None

    def test_unknown_duration_escapes_bounds_and_shorts(self):
        filters = self.filters(
            min_duration_seconds=300,
            max_duration_seconds=600,
            skip_shorts=True,
        )
        assert filter_skip_reason({"title": "t", "duration": None}, filters) is None
        assert filter_skip_reason({"title": "t"}, filters) is None
        assert filter_skip_reason({"title": "t", "duration": "45"}, filters) is None

    def test_shorts_boundary_is_sixty_seconds(self):
        filters = self.filters(skip_shorts=True)
        reason = filter_skip_reason(
            {"title": "t", "duration": SHORTS_MAX_DURATION_SECONDS}, filters
        )
        assert reason is not None and "YouTube Short" in reason
        assert (
            filter_skip_reason(
                {"title": "t", "duration": SHORTS_MAX_DURATION_SECONDS + 1}, filters
            )
            is None
        )

    def test_zero_duration_is_a_short_when_known(self):
        filters = self.filters(skip_shorts=True)
        assert filter_skip_reason({"title": "t", "duration": 0}, filters) is not None

    @pytest.mark.parametrize(
        "entry",
        [
            {"live_status": "is_live"},
            {"live_status": "is_upcoming"},
            {"is_live": True},
            {"was_live": True},
            {"live_status": "not_live", "was_live": True},
        ],
    )
    def test_livestream_variants_detected(self, entry):
        assert is_livestream_entry(entry)

    @pytest.mark.parametrize(
        "entry",
        [
            {"live_status": "not_live"},
            {"live_status": "was_live"},
            {"live_status": "post_live"},
            {"is_live": False, "was_live": False},
            {},
        ],
    )
    def test_non_livestream_entries_pass(self, entry):
        assert not is_livestream_entry(entry)

    def test_skip_livestreams_reason_names_status(self):
        filters = self.filters(skip_livestreams=True)
        reason = filter_skip_reason({"title": "t", "live_status": "is_live"}, filters)
        assert reason is not None and "live_status=is_live" in reason

    def test_livestream_filter_needs_markers(self):
        filters = self.filters(skip_livestreams=True)
        assert filter_skip_reason({"title": "t", "duration": 1800}, filters) is None


class TestTombstoneStore:
    """tombstones.json persistence and corruption recovery."""

    def test_record_and_load_roundtrip(self, tmp_path):
        assert load_tombstones(tmp_path) == {}
        assert record_tombstone(tmp_path, "ch", "vid1", "My Episode") is True
        data = load_tombstones(tmp_path)
        assert data["ch"]["vid1"]["title"] == "My Episode"
        assert data["ch"]["vid1"]["deleted_at"]
        assert tombstoned_ids(tmp_path, "ch") == {"vid1"}
        assert tombstoned_ids(tmp_path, "other") == set()

    def test_record_keeps_existing_entries(self, tmp_path):
        record_tombstone(tmp_path, "ch", "vid1", "One")
        record_tombstone(tmp_path, "ch", "vid2", "Two")
        assert sorted(tombstoned_ids(tmp_path, "ch")) == ["vid1", "vid2"]

    def test_persists_across_restarts(self, tmp_path):
        record_tombstone(tmp_path, "ch", "vid1", "One")
        # A fresh server process simply re-reads the same file.
        assert load_tombstones(tmp_path) == load_tombstones(tmp_path)
        assert "vid1" in load_tombstones(tmp_path)["ch"]

    def test_remove_episode_tombstone(self, tmp_path):
        record_tombstone(tmp_path, "ch", "vid1", "One")
        assert remove_episode_tombstone(tmp_path, "ch", "vid1") is True
        assert load_tombstones(tmp_path) == {}
        assert remove_episode_tombstone(tmp_path, "ch", "vid1") is False

    def test_remove_channel_tombstones(self, tmp_path):
        record_tombstone(tmp_path, "ch", "vid1", "One")
        record_tombstone(tmp_path, "ch", "vid2", "Two")
        assert remove_channel_tombstones(tmp_path, "ch") == 2
        assert remove_channel_tombstones(tmp_path, "ch") == 0
        assert load_tombstones(tmp_path) == {}

    def test_corrupt_file_backed_up_and_empty(self, tmp_path):
        path = tombstones_path(tmp_path)
        path.write_text("{not json at all", encoding="utf-8")
        assert load_tombstones(tmp_path) == {}
        backup = Path(f"{path}.corrupt")
        assert backup.is_file()
        assert backup.read_text(encoding="utf-8") == "{not json at all"

    def test_non_object_file_backed_up(self, tmp_path):
        tombstones_path(tmp_path).write_text("[1, 2]", encoding="utf-8")
        assert load_tombstones(tmp_path) == {}
        assert Path(f"{tombstones_path(tmp_path)}.corrupt").is_file()

    def test_unreadable_file_raises_and_is_never_quarantined(
        self, tmp_path, unreadable_files
    ):
        original = make_tombstones_unreadable(tmp_path, unreadable_files)
        with pytest.raises(StateFileError, match="Permission denied"):
            load_tombstones(tmp_path)
        with pytest.raises(StateFileError):
            tombstoned_ids(tmp_path, "test_channel")
        assert tombstones_path(tmp_path).read_bytes() == original
        assert tombstone_backups(tmp_path) == []

    def test_writers_never_overwrite_unreadable_file(self, tmp_path, unreadable_files):
        original = make_tombstones_unreadable(tmp_path, unreadable_files)
        assert record_tombstone(tmp_path, "test_channel", "video2", "Two") is False
        assert remove_episode_tombstone(tmp_path, "test_channel", "video1") is False
        assert remove_channel_tombstones(tmp_path, "test_channel") == 0
        assert tombstones_path(tmp_path).read_bytes() == original
        assert tombstone_backups(tmp_path) == []

    def test_lock_free_read_leaves_corrupt_file_for_writers(self, tmp_path):
        path = tombstones_path(tmp_path)
        path.write_text("{not json", encoding="utf-8")
        assert tombstoned_ids(tmp_path, "test_channel") == set()
        assert path.read_text(encoding="utf-8") == "{not json"
        # The next locked write quarantines it and starts fresh.
        assert record_tombstone(tmp_path, "test_channel", "video2", "Two") is True
        assert Path(f"{path}.corrupt").read_text(encoding="utf-8") == "{not json"
        assert tombstoned_ids(tmp_path, "test_channel") == {"video2"}

    def test_invalid_entries_dropped(self, tmp_path):
        tombstones_path(tmp_path).write_text(
            json.dumps(
                {
                    "good": {"vid1": {"deleted_at": "2026-01-01T00:00:00+00:00"}},
                    "bad_channel": ["not", "a", "dict"],
                    "bad_entry": {"vid2": "not-a-dict", 7: {"deleted_at": "x"}},
                }
            ),
            encoding="utf-8",
        )
        data = load_tombstones(tmp_path)
        assert set(data) == {"good", "bad_entry"}
        assert data["bad_entry"] == {"7": {"deleted_at": "x", "title": ""}}


class TestDiscoveryFiltering:
    """Refresh discovery never queues filtered or deleted videos."""

    GLOBAL_CONFIG = {"download_delay_seconds": 0, "check_delay_seconds": 0}

    def flat_videos(self):
        return [
            {
                "id": "vid_normal",
                "title": "Deep Dive Episode",
                "url": "https://www.youtube.com/watch?v=vid_normal",
                "upload_date": "20240101",
                "duration": 1800,
            },
            {
                "id": "vid_trailer",
                "title": "TRAILER: New Season",
                "url": "https://www.youtube.com/watch?v=vid_trailer",
                "upload_date": "20240102",
                "duration": 90,
            },
            {
                "id": "vid_short",
                "title": "Quick tip",
                "url": "https://www.youtube.com/watch?v=vid_short",
                "upload_date": "20240103",
                "duration": 45,
            },
            {
                "id": "vid_live",
                "title": "Live Show",
                "url": "https://www.youtube.com/watch?v=vid_live",
                "upload_date": "20240104",
                "duration": None,
                "live_status": "is_live",
            },
        ]

    def discover(self, test_server, downloader, **channel_overrides):
        channel = channel_entry(test_server, **channel_overrides)
        return test_server._discover_channel_jobs(
            downloader, channel, dict(self.GLOBAL_CONFIG)
        )

    def test_filtered_videos_are_not_enqueued(self, test_server):
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        queued = self.discover(
            test_server,
            downloader,
            title_exclude=["trailer"],
            skip_shorts=True,
            skip_livestreams=True,
        )
        assert queued == 1
        jobs = test_server.queue.jobs()
        assert [job["video_id"] for job in jobs] == ["vid_normal"]

    def test_skip_logs_name_the_matched_rule(self, test_server):
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        self.discover(
            test_server,
            downloader,
            title_exclude=["trailer"],
            skip_shorts=True,
            skip_livestreams=True,
        )
        logs = recent_log_text(test_server)
        assert "title matches exclude rule 'trailer'" in logs
        assert "YouTube Short" in logs
        assert "live_status=is_live" in logs

    def test_duration_bounds_from_metadata_block_enqueue(self, test_server):
        downloader = FakeDiscoveryDownloader(
            [
                {
                    "id": "vid_unknown",
                    "title": "Mystery length",
                    "url": "https://www.youtube.com/watch?v=vid_unknown",
                    "duration": None,
                }
            ]
        )
        downloader.metadata_by_url = {
            "https://www.youtube.com/watch?v=vid_unknown": {"duration": 30}
        }
        queued = self.discover(
            test_server, downloader, min_duration_seconds=600, skip_shorts=True
        )
        assert queued == 0
        assert test_server.queue.jobs() == []
        assert "below min_duration_seconds (600s)" in recent_log_text(test_server)

    def test_tombstoned_videos_are_not_enqueued(self, test_server):
        record_tombstone(
            test_server.config_dir, "test_channel", "vid_trailer", "TRAILER"
        )
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        queued = self.discover(test_server, downloader)
        assert queued == 3
        queued_ids = {job["video_id"] for job in test_server.queue.jobs()}
        assert "vid_trailer" not in queued_ids
        assert "deleted by the user" in recent_log_text(test_server)

    def test_unreadable_tombstones_queue_nothing_and_fail_refresh(
        self, test_server, unreadable_files
    ):
        make_tombstones_unreadable(test_server.config_dir, unreadable_files)
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        setattr(downloader, "logger", logging.getLogger("fake-discovery"))
        setattr(downloader, "last_channel_error", None)

        assert test_server._begin_refresh()
        with patch("src.downloader.YouTubeDownloader", return_value=downloader):
            test_server._run_refresh("test_channel")

        assert test_server.queue.jobs() == []
        state = load_health(test_server.config_dir)["channels"]["test_channel"]
        assert state["consecutive_failures"] == 1
        assert state["history"][-1]["status"] == "failure"
        assert "tombstones.json" in state["last_error"]

    def test_metadata_not_fetched_for_cheap_skips(self, test_server):
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        self.discover(test_server, downloader, title_exclude=["trailer"])
        assert (
            "https://www.youtube.com/watch?v=vid_trailer"
            not in downloader.metadata_requests
        )

    def test_unfiltered_channel_enqueues_everything(self, test_server):
        downloader = FakeDiscoveryDownloader(self.flat_videos())
        queued = self.discover(test_server, downloader)
        assert queued == 4
        assert len(test_server.queue.jobs()) == 4


class TestExecutionFiltering:
    """Queue execution re-checks current config and tombstones."""

    def test_config_change_after_enqueue_cancels_job(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(test_server.queue)
            update_channel_config(test_server, title_exclude=["title"])
            assert wait_until(
                lambda: (
                    (state := test_server.queue.get(job["id"]))
                    and state["state"] == "cancelled"
                )
            )
        state = test_server.queue.get(job["id"])
        assert "title matches exclude rule 'title'" in state["error"]
        assert StubDownloader.instances[-1].download_calls == []
        assert not (test_server.videos_dir / "test_channel" / "video1.mp4").exists()

    def test_tombstoned_job_is_cancelled(self, test_server, fast_worker):
        record_tombstone(
            test_server.config_dir, "test_channel", "video1", "Title video1"
        )
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "cancelled"
            )
        state = test_server.queue.get(job["id"])
        assert "deleted by the user" in state["error"]
        assert StubDownloader.instances[-1].download_calls == []

    def test_unreadable_tombstones_fail_attempt_without_download(
        self, test_server, fast_worker, unreadable_files
    ):
        record_tombstone(test_server.config_dir, "test_channel", "other", "Other")
        unreadable_files(tombstones_path(test_server.config_dir))
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: (
                    (state := test_server.queue.get(job["id"]))
                    and state["attempts"] == 1
                    and state["state"] == "pending"
                    and state["error"]
                )
            )
        state = test_server.queue.get(job["id"])
        # A normal failed attempt: retried with backoff, never downloaded.
        assert "tombstones.json" in state["error"]
        assert state["next_attempt_at"] > time.time()
        assert StubDownloader.instances[-1].download_calls == []
        assert not (test_server.videos_dir / "test_channel" / "video1.mp4").exists()

    def test_duration_filters_fetch_metadata_and_cancel(self, test_server, fast_worker):
        update_channel_config(test_server, min_duration_seconds=600)
        MetadataStubDownloader.metadata_to_serve = {
            "https://www.youtube.com/watch?v=video1": {"duration": 120}
        }
        with patch("src.downloader.YouTubeDownloader", MetadataStubDownloader):
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "cancelled"
            )
        state = test_server.queue.get(job["id"])
        assert "below min_duration_seconds (600s)" in state["error"]
        assert MetadataStubDownloader.metadata_requests == [
            "https://www.youtube.com/watch?v=video1"
        ]
        assert MetadataStubDownloader.instances[-1].download_calls == []

    def test_title_only_filters_skip_metadata_fetch(self, test_server, fast_worker):
        update_channel_config(test_server, title_exclude=["nope-never"])
        with patch("src.downloader.YouTubeDownloader", MetadataStubDownloader):
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        assert MetadataStubDownloader.metadata_requests == []

    def test_unfiltered_job_still_completes(self, test_server, fast_worker):
        with patch("src.downloader.YouTubeDownloader", MetadataStubDownloader):
            job = enqueue_job(test_server.queue)
            assert wait_until(
                lambda: test_server.queue.get(job["id"])["state"] == "completed"
            )
        assert MetadataStubDownloader.metadata_requests == []
        assert (test_server.videos_dir / "test_channel" / "video1.mp4").exists()


class TestFeedAndEpisodesVisibility:
    """Feed, episodes API and index counts hide filtered/tombstoned episodes."""

    def hide_second_episode(self, test_server):
        update_channel_config(test_server, title_exclude=["Another Test Episode"])

    def test_feed_hides_filtered_episode(
        self, client, test_server, setup_test_episodes
    ):
        self.hide_second_episode(test_server)
        response = client.get("/feeds/test_channel")
        assert response.status_code == 200
        assert b"Test Episode: How to Test" in response.data
        assert b"Another Test Episode" not in response.data
        assert response.data.count(b"<item>") == 1

    def test_episodes_api_hides_filtered_episode(
        self, client, test_server, setup_test_episodes
    ):
        self.hide_second_episode(test_server)
        response = client.get("/api/channels/test_channel/episodes")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["total_count"] == 1
        assert [episode["id"] for episode in data["episodes"]] == ["test123abc"]

    def test_feed_and_api_hide_tombstoned_episode(
        self, client, test_server, setup_test_episodes
    ):
        record_tombstone(
            test_server.config_dir, "test_channel", "test456def", "Another Test Episode"
        )
        feed = client.get("/feeds/test_channel")
        assert b"Another Test Episode" not in feed.data
        assert feed.data.count(b"<item>") == 1
        episodes = client.get("/api/channels/test_channel/episodes")
        data = json.loads(episodes.data)
        assert data["total_count"] == 1
        assert [episode["id"] for episode in data["episodes"]] == ["test123abc"]

    def test_unreadable_tombstones_do_not_break_feed_or_api(
        self, client, test_server, setup_test_episodes, unreadable_files
    ):
        make_tombstones_unreadable(test_server.config_dir, unreadable_files)
        feed = client.get("/feeds/test_channel")
        assert feed.status_code == 200
        assert b"Test Episode: How to Test" in feed.data
        episodes = client.get("/api/channels/test_channel/episodes")
        assert episodes.status_code == 200
        assert json.loads(episodes.data)["total_count"] == 2
        assert client.get("/").status_code == 200

    def test_media_of_hidden_episode_is_still_served(
        self, client, test_server, setup_test_episodes
    ):
        self.hide_second_episode(test_server)
        record_tombstone(
            test_server.config_dir, "test_channel", "test456def", "Another Test Episode"
        )
        response = client.get("/podcasts/test_channel/test456def.m4a")
        assert response.status_code == 200
        assert response.data == b"fake audio data 2"

    def test_unfiltered_episodes_keep_order(
        self, client, test_server, setup_test_episodes
    ):
        response = client.get("/api/channels/test_channel/episodes")
        data = json.loads(response.data)
        assert [episode["id"] for episode in data["episodes"]] == [
            "test456def",
            "test123abc",
        ]

    def test_index_episode_count_matches_visible(
        self, client, test_server, setup_test_episodes
    ):
        self.hide_second_episode(test_server)
        response = client.get("/")
        assert response.status_code == 200
        assert b'<span class="stat-value">1</span>' in response.data

    def test_duration_bounds_hide_stored_shorts(
        self, client, test_server, setup_test_episodes
    ):
        update_channel_config(test_server, max_duration_seconds=2000)
        response = client.get("/api/channels/test_channel/episodes")
        data = json.loads(response.data)
        # test456def has duration 2345s and is hidden; test123abc (1234s) stays.
        assert [episode["id"] for episode in data["episodes"]] == ["test123abc"]
        assert client.get("/feeds/test_channel").data.count(b"<item>") == 1

    def test_skip_livestreams_hides_stored_episode(
        self, client, test_server, setup_test_episodes
    ):
        metadata_file = setup_test_episodes / "test456def.json"
        metadata = json.loads(metadata_file.read_text())
        metadata.update({"live_status": "was_live", "was_live": True})
        metadata_file.write_text(json.dumps(metadata))
        update_channel_config(test_server, skip_livestreams=True)

        response = client.get("/api/channels/test_channel/episodes")
        data = json.loads(response.data)
        assert [episode["id"] for episode in data["episodes"]] == ["test123abc"]
        assert client.get("/feeds/test_channel").data.count(b"<item>") == 1


class TestTombstoneAPIs:
    """The tombstone management endpoints."""

    def test_get_tombstones(self, client, test_server):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "One")
        record_tombstone(test_server.config_dir, "test_channel", "vid2", "Two")
        response = client.get("/api/channels/test_channel/tombstones")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["channel"] == "test_channel"
        assert data["total_count"] == 2
        assert data["tombstones"]["vid1"]["title"] == "One"
        assert data["tombstones"]["vid2"]["deleted_at"]

    def test_get_tombstones_unknown_channel(self, client):
        response = client.get("/api/channels/nonexistent/tombstones")
        assert response.status_code == 404
        assert json.loads(response.data)["error"] == "Channel not found"

    def test_clear_all_tombstones(self, client, test_server):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "One")
        response = client.delete("/api/channels/test_channel/tombstones")
        assert response.status_code == 200
        assert json.loads(response.data)["removed"] == 1
        assert load_tombstones(test_server.config_dir) == {}

    def test_clear_tombstones_unknown_channel(self, client):
        response = client.delete("/api/channels/nonexistent/tombstones")
        assert response.status_code == 404

    def test_forget_single_tombstone(self, client, test_server):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "One")
        record_tombstone(test_server.config_dir, "test_channel", "vid2", "Two")
        response = client.delete("/api/channels/test_channel/tombstones/vid1")
        assert response.status_code == 200
        assert tombstoned_ids(test_server.config_dir, "test_channel") == {"vid2"}

    def test_forget_unknown_tombstone(self, client, test_server):
        response = client.delete("/api/channels/test_channel/tombstones/ghost")
        assert response.status_code == 404
        assert json.loads(response.data)["error"] == "Tombstone not found"

    def test_forget_tombstone_unknown_channel(self, client):
        response = client.delete("/api/channels/nonexistent/tombstones/vid1")
        assert response.status_code == 404

    def test_unreadable_tombstones_return_clear_error(
        self, client, test_server, unreadable_files
    ):
        original = make_tombstones_unreadable(test_server.config_dir, unreadable_files)
        base = "/api/channels/test_channel/tombstones"
        for response in (
            client.get(base),
            client.delete(base),
            client.delete(f"{base}/video1"),
        ):
            assert response.status_code == 500
            assert "tombstones.json cannot be read" in response.get_json()["error"]
        assert tombstones_path(test_server.config_dir).read_bytes() == original
        assert tombstone_backups(test_server.config_dir) == []

    def test_empty_tombstones_list(self, client, test_server):
        response = client.get("/api/channels/test_channel/tombstones")
        data = json.loads(response.data)
        assert data["total_count"] == 0
        assert data["tombstones"] == {}


class TestTombstoneLifecycle:
    """Delete → skip on refresh → redownload/purge/channel-delete resets."""

    def test_delete_records_tombstone(self, client, test_server, setup_test_episodes):
        response = client.delete(
            "/api/channels/test_channel/episodes/test123abc/delete"
        )
        assert response.status_code == 200
        data = load_tombstones(test_server.config_dir)
        assert (
            data["test_channel"]["test123abc"]["title"] == "Test Episode: How to Test"
        )
        assert data["test_channel"]["test123abc"]["deleted_at"]
        assert not (setup_test_episodes / "test123abc.m4a").exists()

    def test_delete_refused_while_tombstones_unreadable(
        self, client, test_server, setup_test_episodes, unreadable_files
    ):
        original = make_tombstones_unreadable(test_server.config_dir, unreadable_files)
        response = client.delete(
            "/api/channels/test_channel/episodes/test123abc/delete"
        )
        assert response.status_code == 500
        assert "tombstones.json cannot be read" in response.get_json()["error"]
        # Nothing was deleted, so nothing can come back unremembered.
        assert (setup_test_episodes / "test123abc.m4a").exists()
        assert (setup_test_episodes / "test123abc.json").exists()
        assert tombstones_path(test_server.config_dir).read_bytes() == original

    def test_refresh_skips_deleted_episode(self, test_server):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "Deleted one")
        downloader = FakeDiscoveryDownloader(
            [
                {
                    "id": "vid1",
                    "title": "Deleted one",
                    "url": "https://www.youtube.com/watch?v=vid1",
                    "duration": 600,
                }
            ]
        )
        channel = channel_entry(test_server)
        queued = test_server._discover_channel_jobs(
            downloader, channel, {"download_delay_seconds": 0}
        )
        assert queued == 0
        assert test_server.queue.jobs() == []
        assert "deleted by the user" in recent_log_text(test_server)

    def test_redownload_clears_tombstone(
        self, client, test_server, setup_test_episodes
    ):
        client.delete("/api/channels/test_channel/episodes/test123abc/delete")
        assert "test123abc" in tombstoned_ids(test_server.config_dir, "test_channel")

        with patch("src.downloader.YouTubeDownloader", FakeStagedDownloader):
            job = redownload_and_run(test_server)

        assert job["state"] == "completed"
        assert load_tombstones(test_server.config_dir) == {}
        channel_dir = setup_test_episodes
        assert (channel_dir / "test123abc.m4a").read_bytes() == b"fresh-staged-audio"
        metadata = json.loads((channel_dir / "test123abc.json").read_text())
        assert metadata["id"] == "test123abc"

    def test_redownload_tombstoned_episode_without_files(
        self, client, test_server, setup_test_episodes
    ):
        client.delete("/api/channels/test_channel/episodes/test123abc/delete")
        channel_dir = setup_test_episodes
        for leftover in channel_dir.glob("test123abc.*"):
            leftover.unlink()
        (channel_dir / "thumbnails" / "test123abc.jpg").unlink(missing_ok=True)
        assert not (channel_dir / "test123abc.json").exists()

        with patch("src.downloader.YouTubeDownloader", FakeStagedDownloader):
            job = redownload_and_run(test_server)

        assert job["state"] == "completed"
        assert load_tombstones(test_server.config_dir) == {}
        assert (channel_dir / "test123abc.json").is_file()
        assert (channel_dir / "test123abc.m4a").is_file()

    def test_redownload_unknown_episode_is_404(self, client, test_server):
        response = client.post("/api/channels/test_channel/episodes/ghost/redownload")
        assert response.status_code == 404
        assert test_server.queue.jobs() == []

    def test_purge_clears_tombstones(self, client, test_server, setup_test_episodes):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "One")
        response = client.post("/api/channels/test_channel/purge")
        assert response.status_code == 200
        assert load_tombstones(test_server.config_dir) == {}

    def test_purge_without_directory_still_clears_tombstones(self, client, test_server):
        record_tombstone(test_server.config_dir, "another_channel", "vid1", "One")
        response = client.post("/api/channels/another_channel/purge")
        assert response.status_code == 200
        assert "No episodes found to purge" in json.loads(response.data)["message"]
        assert load_tombstones(test_server.config_dir) == {}

    def test_channel_delete_clears_tombstones(self, client, test_server):
        record_tombstone(test_server.config_dir, "test_channel", "vid1", "One")
        response = client.delete("/api/channels/test_channel")
        assert response.status_code == 200
        assert load_tombstones(test_server.config_dir) == {}


class TestFilterConfigAPI:
    """Filter settings flow through the channel config endpoints."""

    def test_update_channel_accepts_filters(self, client, test_server):
        payload = {
            "title_include": ["Review", "Interview"],
            "title_exclude": ["Trailer"],
            "min_duration_seconds": 60,
            "max_duration_seconds": 7200,
            "skip_shorts": True,
            "skip_livestreams": True,
        }
        response = client.put(
            "/api/channels/test_channel",
            data=json.dumps(payload),
            content_type="application/json",
        )
        assert response.status_code == 200
        channels = json.loads(client.get("/api/channels").data)["channels"]
        channel = next(ch for ch in channels if ch["name"] == "test_channel")
        assert channel["title_include"] == ["Review", "Interview"]
        assert channel["title_exclude"] == ["Trailer"]
        assert channel["min_duration_seconds"] == 60
        assert channel["max_duration_seconds"] == 7200
        assert channel["skip_shorts"] is True
        assert channel["skip_livestreams"] is True

    def test_update_channel_rejects_invalid_filters(self, client):
        for payload in (
            {"title_exclude": "not-a-list"},
            {"min_duration_seconds": True},
            {"max_duration_seconds": -1},
            {"skip_shorts": 1},
            {"skip_livestreams": "maybe"},
            {"min_duration_seconds": 600, "max_duration_seconds": 60},
        ):
            response = client.put(
                "/api/channels/test_channel",
                data=json.dumps(payload),
                content_type="application/json",
            )
            assert response.status_code == 400, payload

    @patch("src.web_server.yt_dlp.YoutubeDL")
    def test_add_channel_with_filters(self, mock_yt_dlp, client):
        mock_ydl = Mock()
        mock_yt_dlp.return_value.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "title": "Filtered Channel",
            "id": "UCtest123",
            "webpage_url": "https://www.youtube.com/@filtered",
        }
        payload = {
            "display_name": "Filtered Channel",
            "url": "https://www.youtube.com/@filtered",
            "title_exclude": ["trailer"],
            "skip_shorts": True,
        }
        response = client.post(
            "/api/channels",
            data=json.dumps(payload),
            content_type="application/json",
        )
        assert response.status_code == 201
        channel = json.loads(response.data)["channel"]
        assert channel["title_exclude"] == ["trailer"]
        assert channel["skip_shorts"] is True
        assert "title_include" not in channel

    @patch("src.web_server.yt_dlp.YoutubeDL")
    def test_add_channel_rejects_invalid_filter(self, mock_yt_dlp, client):
        mock_ydl = Mock()
        mock_yt_dlp.return_value.__enter__.return_value = mock_ydl
        response = client.post(
            "/api/channels",
            data=json.dumps(
                {
                    "display_name": "Broken Filters",
                    "url": "https://www.youtube.com/@broken",
                    "skip_shorts": "yes",
                }
            ),
            content_type="application/json",
        )
        assert response.status_code == 400
        assert "skip_shorts" in json.loads(response.data)["error"]
