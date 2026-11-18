"""
Simplified tests for cron_runner module.

Tests the AutomationRunner class without calling yt-dlp.
Only includes tests that work reliably.
"""

import copy
import fcntl
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from src.cron_runner import AutomationRunner
from src.health import load_health, record_refresh
from src.storage import pins_path, record_tombstone, tombstones_path


@pytest.fixture
def test_config():
    """Create test configuration data."""
    return {
        "channels": [
            {
                "name": "test_channel_1",
                "display_name": "Test Channel 1",
                "url": "https://www.youtube.com/@test1",
                "max_episodes": 5,
                "download_delay_hours": 6,
                "format": "audio",
                "quality": "best",
            },
            {
                "name": "test_channel_2",
                "display_name": "Test Channel 2",
                "url": "https://www.youtube.com/@test2",
                "max_episodes": 10,
                "download_delay_hours": 12,
                "format": "video",
                "quality": "480p",
                "sponsorblock_categories": ["sponsor", "selfpromo"],
            },
        ],
        "default_interval_hours": 24,
    }


@pytest.fixture
def temp_directory():
    """Create temporary directory for testing."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def automation_runner(temp_directory, test_config):
    """Create AutomationRunner instance with temporary directory."""
    # Create the expected config directory structure
    config_dir = temp_directory / "appdata" / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "channels.yaml"
    with open(config_file, "w") as f:
        yaml.dump(test_config, f)

    with (
        patch.object(AutomationRunner, "setup_logging") as mock_logging,
        patch("src.cron_runner.YouTubeDownloader") as mock_downloader,
    ):
        mock_logging.return_value = Mock()
        mock_downloader.return_value = Mock()
        runner = AutomationRunner(str(temp_directory))
        # Add a mock logger since we mocked setup_logging
        runner.logger = Mock()
        return runner


class TestAutomationRunner:
    """Test AutomationRunner class functionality."""

    def test_initialization(self, automation_runner, test_config):
        """Test AutomationRunner initialization."""
        assert automation_runner.config_path is not None
        assert automation_runner.base_dir is not None
        assert automation_runner.videos_dir is not None
        assert automation_runner.lock_file_path is not None
        assert (
            not hasattr(automation_runner, "lock_file")
            or automation_runner.lock_file is None
        )

    @patch("fcntl.flock")
    @patch("builtins.open", new_callable=mock_open)
    def test_acquire_lock_success(self, mock_file, mock_flock, automation_runner):
        """Test successful lock acquisition."""
        result = automation_runner.acquire_lock()
        assert result is True
        mock_flock.assert_called_once()

    def test_validate_channel_config_valid(self, automation_runner):
        """Test validation of a valid channel configuration."""
        valid_config = {
            "name": "test_channel",
            "url": "https://www.youtube.com/@test",
            "max_episodes": 10,
        }
        result = automation_runner.validate_channel_config(valid_config)
        assert result is True

    def test_validate_channel_config_missing_name(self, automation_runner):
        """Test validation with missing name."""
        invalid_config = {
            "url": "https://www.youtube.com/@test",
            "max_episodes": 10,
        }
        result = automation_runner.validate_channel_config(invalid_config)
        assert result is False

    def test_validate_channel_config_missing_url(self, automation_runner):
        """Test validation with missing URL."""
        invalid_config = {
            "name": "test_channel",
            "max_episodes": 10,
        }
        result = automation_runner.validate_channel_config(invalid_config)
        assert result is False

    def test_validate_channel_config_invalid_max_episodes(self, automation_runner):
        """Test validation with invalid max_episodes."""
        invalid_config = {
            "name": "test_channel",
            "url": "https://www.youtube.com/@test",
            "max_episodes": "invalid",
        }
        result = automation_runner.validate_channel_config(invalid_config)
        assert result is False

    def test_cleanup_old_videos_no_cleanup_needed(
        self, automation_runner, temp_directory
    ):
        """Test cleanup when no cleanup is needed."""
        # Create test directory with recent episodes
        channel_dir = temp_directory / "appdata" / "podcasts" / "test_channel"
        channel_dir.mkdir(parents=True, exist_ok=True)

        # Create recent episodes (should not be cleaned up)
        for i in range(3):
            episode_data = {"id": f"recent{i}", "upload_date": "20241201"}
            (channel_dir / f"recent{i}.json").write_text(json.dumps(episode_data))
            (channel_dir / f"recent{i}.m4a").write_text("audio data")

        # Should not clean up any files
        automation_runner.cleanup_old_videos("test_channel", 5)

        # Verify all files still exist
        remaining_count = len(list(channel_dir.glob("*.json")))
        assert remaining_count == 3

    def test_cleanup_old_videos_nonexistent_channel(self, automation_runner):
        """Test cleanup for nonexistent channel."""
        # Should not raise exception
        automation_runner.cleanup_old_videos("nonexistent_channel", 5)

    @patch("src.cron_runner.AutomationRunner.acquire_lock")
    def test_run_lock_failure(self, mock_acquire, automation_runner):
        """Test run when lock acquisition fails."""
        mock_acquire.return_value = False
        result = automation_runner.run()
        assert result == 1


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_cleanup_with_corrupted_metadata(self, automation_runner, temp_directory):
        """Test cleanup with corrupted metadata files."""
        channel_dir = temp_directory / "appdata" / "podcasts" / "test_channel"
        channel_dir.mkdir(parents=True, exist_ok=True)

        # Create corrupted metadata file
        (channel_dir / "corrupted.json").write_text("invalid json content")
        (channel_dir / "corrupted.m4a").write_text("audio data")

        # Should not raise exception
        automation_runner.cleanup_old_videos("test_channel", 1)

    @pytest.mark.parametrize("mode", ["corrupt", "unreadable"])
    def test_cleanup_skipped_while_pins_unusable(
        self, automation_runner, temp_directory, unreadable_files, mode
    ):
        """A broken pins.json pauses cleanup instead of deleting pinned files."""
        channel_dir = temp_directory / "appdata" / "podcasts" / "test_channel"
        channel_dir.mkdir(parents=True, exist_ok=True)
        for index in range(3):
            episode = {"id": f"ep{index}", "upload_date": f"2024110{index + 1}"}
            (channel_dir / f"ep{index}.json").write_text(json.dumps(episode))
            (channel_dir / f"ep{index}.m4a").write_text("audio data")
        pins = pins_path(automation_runner.config_path.parent)
        if mode == "corrupt":
            pins.write_text('{"test_channel": ["ep0"')
        else:
            pins.write_text(json.dumps({"test_channel": ["ep0"]}))
            unreadable_files(pins)
        original = pins.read_bytes()
        automation_runner.logger.error = Mock()

        automation_runner.cleanup_old_videos("test_channel", 1)

        assert len(list(channel_dir.glob("*.m4a"))) == 3
        assert pins.read_bytes() == original
        message = automation_runner.logger.error.call_args.args[0]
        assert "Skipping cleanup for test_channel" in message
        assert "pins.json is repaired or removed" in message

    def test_file_system_errors_during_cleanup(self, automation_runner, temp_directory):
        """Test cleanup with file system errors."""
        channel_dir = temp_directory / "appdata" / "podcasts" / "test_channel"
        channel_dir.mkdir(parents=True, exist_ok=True)

        # Create test files
        episode_data = {"id": "test", "upload_date": "20241101"}
        (channel_dir / "test.json").write_text(json.dumps(episode_data))
        (channel_dir / "test.m4a").write_text("audio data")

        # Mock file removal to raise permission error
        with patch("pathlib.Path.unlink", side_effect=PermissionError("Access denied")):
            # Should not raise exception
            automation_runner.cleanup_old_videos("test_channel", 1)


class TestCronHealth:
    """Cron deployments record health and dispatch notifications."""

    def test_process_channel_records_success(self, automation_runner, test_config):
        channel = test_config["channels"][0]
        automation_runner.downloader.process_channel = Mock(return_value=2)
        automation_runner.cleanup_old_videos = Mock()

        assert automation_runner.process_channel(channel, {}) is True

        state = load_health(automation_runner.config_path.parent)["channels"][
            "test_channel_1"
        ]
        assert state["consecutive_failures"] == 0
        assert state["last_success_at"]
        assert state["history"][-1]["queued"] == 2

    def test_process_channel_records_unavailable(self, automation_runner, test_config):
        channel = test_config["channels"][0]
        automation_runner.downloader.process_channel = Mock(return_value=0)
        automation_runner.downloader.last_channel_error = "channel not found"
        automation_runner.cleanup_old_videos = Mock()

        assert automation_runner.process_channel(channel, {}) is False

        state = load_health(automation_runner.config_path.parent)["channels"][
            "test_channel_1"
        ]
        assert state["unavailable"] is True
        assert "channel not found" in state["last_error"]

    def test_process_channel_records_failure(self, automation_runner, test_config):
        channel = test_config["channels"][0]
        automation_runner.downloader.process_channel = Mock(
            side_effect=RuntimeError("boom")
        )
        automation_runner.cleanup_old_videos = Mock()

        assert automation_runner.process_channel(channel, {}) is False

        state = load_health(automation_runner.config_path.parent)["channels"][
            "test_channel_1"
        ]
        assert state["consecutive_failures"] == 1
        assert state["unavailable"] is False
        assert "boom" in state["last_error"]

    def test_notify_health_dispatches_due_events(self, automation_runner):
        config_dir = automation_runner.config_path.parent
        for _ in range(3):
            record_refresh(config_dir, "test_channel_1", success=False, error="boom")
        global_config = {
            "notifications": {
                "webhook_url": "https://example.test/hook",
                "failure_threshold": 3,
                "cooldown_minutes": 0,
            },
            "storage": {"min_free_space_gb": 0},
        }
        with (
            patch("src.cron_runner.dispatch_events") as dispatch,
            patch("src.cron_runner.free_disk_bytes", return_value=10**12),
        ):
            automation_runner.notify_health(global_config)

        dispatch.assert_called_once()
        events = dispatch.call_args.args[1]
        assert [event["event"] for event in events] == ["channel_failure"]

    def test_notify_health_without_webhook_does_not_dispatch(self, automation_runner):
        config_dir = automation_runner.config_path.parent
        for _ in range(3):
            record_refresh(config_dir, "test_channel_1", success=False, error="boom")
        with patch("src.cron_runner.dispatch_events") as dispatch:
            automation_runner.notify_health({})
        dispatch.assert_not_called()

    def test_unreadable_tombstones_record_failure_without_download(
        self, automation_runner, test_config, temp_directory, unreadable_files
    ):
        """Inline discovery fails closed instead of re-downloading deletions."""
        from src.downloader import YouTubeDownloader

        config_dir = automation_runner.config_path.parent
        downloader = YouTubeDownloader(
            str(automation_runner.config_path),
            videos_dir=str(temp_directory / "appdata" / "podcasts"),
        )
        automation_runner.downloader = downloader
        record_tombstone(config_dir, "test_channel_1", "deleted1", "Deleted")
        unreadable_files(tombstones_path(config_dir))
        video = {
            "id": "deleted1",
            "title": "Deleted",
            "url": "https://www.youtube.com/watch?v=deleted1",
        }
        with (
            patch.object(downloader, "get_channel_videos", return_value=[video]),
            patch.object(downloader, "get_video_metadata") as metadata,
            patch.object(downloader, "download_video") as download,
        ):
            result = automation_runner.process_channel(test_config["channels"][0], {})

        assert result is False
        metadata.assert_not_called()
        download.assert_not_called()
        state = load_health(config_dir)["channels"]["test_channel_1"]
        assert state["consecutive_failures"] == 1
        assert "tombstones.json" in state["last_error"]


class TestCronQueueDrain:
    """The cron drain shares the queue's unavailable-config handling."""

    def test_drain_defers_jobs_while_config_is_broken(self, automation_runner):
        automation_runner.config_path.write_text("channels: [unclosed\n")
        jobs = [
            automation_runner.queue.enqueue(
                channel="test_channel_1",
                video_id=f"v{index}",
                title=f"Video {index}",
                url=f"https://www.youtube.com/watch?v=v{index}",
            )
            for index in range(2)
        ]

        with patch("src.downloader.yt_dlp.YoutubeDL") as ytdl:
            assert automation_runner.drain_queue() == 2

        ytdl.assert_not_called()
        for job in jobs:
            state = automation_runner.queue.get(job["id"])
            assert state["state"] == "pending"
            assert state["attempts"] == 0
            assert "channels.yaml" in state["error"]


def days_ago(days: int) -> str:
    """Return the ``YYYYMMDD`` upload date ``days`` days before today."""
    return (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")


def logged(method: Mock) -> str:
    """Join every message passed to a mocked logger method."""
    return " ".join(str(call.args[0]) for call in method.call_args_list)


class TestCronRetention:
    """Every cron pass applies storage retention while holding media.lock."""

    @staticmethod
    def configure(
        runner: AutomationRunner,
        test_config: dict[str, Any],
        storage: dict[str, Any] | None = None,
        overrides: dict[str, int] | None = None,
    ) -> None:
        """Rewrite channels.yaml with storage settings and channel overrides."""
        config = copy.deepcopy(test_config)
        if storage is not None:
            config["storage"] = storage
        for channel in config["channels"]:
            if overrides and channel["name"] in overrides:
                channel["retention_days"] = overrides[channel["name"]]
        runner.config_path.write_text(yaml.safe_dump(config))

    @staticmethod
    def seed(runner: AutomationRunner, channel_name: str, **ages: int) -> Path:
        """Store episodes (metadata + media) uploaded ``age`` days ago."""
        channel_dir = runner.videos_dir / channel_name
        channel_dir.mkdir(parents=True, exist_ok=True)
        for episode_id, age in ages.items():
            metadata = {"id": episode_id, "title": episode_id}
            metadata["upload_date"] = days_ago(age)
            (channel_dir / f"{episode_id}.json").write_text(json.dumps(metadata))
            (channel_dir / f"{episode_id}.m4a").write_bytes(b"audio")
        return channel_dir

    @staticmethod
    def surviving(channel_dir: Path) -> list[str]:
        return sorted(path.stem for path in channel_dir.glob("*.m4a"))

    @staticmethod
    def stub_discovery(runner: AutomationRunner) -> None:
        """Make inline discovery succeed with nothing new, without YouTube."""
        runner.downloader.process_channel = Mock(return_value=0)
        runner.downloader.last_channel_error = None

    def test_global_retention_keeps_recent_and_pinned(
        self, automation_runner, test_config
    ):
        self.configure(automation_runner, test_config, storage={"retention_days": 30})
        first = self.seed(
            automation_runner, "test_channel_1", ancient=400, keepme=400, fresh=2
        )
        second = self.seed(automation_runner, "test_channel_2", old=40, recent=10)
        pins_path(automation_runner.config_path.parent).write_text(
            json.dumps({"test_channel_1": ["keepme"]})
        )
        self.stub_discovery(automation_runner)

        assert automation_runner.run() == 0

        assert self.surviving(first) == ["fresh", "keepme"]
        assert not (first / "ancient.json").exists()
        assert self.surviving(second) == ["recent"]
        messages = logged(automation_runner.logger.info)
        assert "Retention removed 1 old episode(s) from test_channel_1" in messages
        assert "Retention removed 1 old episode(s) from test_channel_2" in messages

    def test_channel_override_wins_and_zero_inherits_global(
        self, automation_runner, test_config
    ):
        self.configure(
            automation_runner,
            test_config,
            storage={"retention_days": 365},
            overrides={"test_channel_1": 30, "test_channel_2": 0},
        )
        first = self.seed(automation_runner, "test_channel_1", mid=100, fresh=2)
        second = self.seed(automation_runner, "test_channel_2", mid=100, ancient=400)
        self.stub_discovery(automation_runner)

        assert automation_runner.run() == 0

        # The 30-day override deletes what the 365-day global window keeps.
        assert self.surviving(first) == ["fresh"]
        # retention_days: 0 inherits the global 365 days.
        assert self.surviving(second) == ["mid"]

    @pytest.mark.parametrize(
        "storage", [None, {"retention_days": 0}], ids=["absent", "zero"]
    )
    def test_disabled_retention_deletes_nothing(
        self, automation_runner, test_config, storage
    ):
        self.configure(
            automation_runner,
            test_config,
            storage=storage,
            overrides={"test_channel_1": 0},
        )
        channel_dir = self.seed(automation_runner, "test_channel_1", ancient=4000)
        self.stub_discovery(automation_runner)

        assert automation_runner.run() == 0

        assert self.surviving(channel_dir) == ["ancient"]
        assert "Retention removed" not in logged(automation_runner.logger.info)

    def test_disabled_retention_ignores_broken_pins(
        self, automation_runner, test_config
    ):
        """No retention skip is reported for a pass that is disabled."""
        self.configure(automation_runner, test_config)
        pins_path(automation_runner.config_path.parent).write_text("{broken")
        self.stub_discovery(automation_runner)

        automation_runner.run()

        errors = logged(automation_runner.logger.error)
        assert "Skipping cleanup for test_channel_1" in errors
        assert "Skipping retention" not in errors

    @pytest.mark.parametrize("failure", ["unavailable", "crash"])
    def test_retention_runs_when_discovery_fails(
        self, automation_runner, test_config, temp_directory, failure
    ):
        """Age-based retention never depends on YouTube being reachable."""
        from src.downloader import YouTubeDownloader

        self.configure(automation_runner, test_config, storage={"retention_days": 30})
        channel_dir = self.seed(
            automation_runner, "test_channel_1", ancient=400, fresh=2
        )
        if failure == "unavailable":
            # The real discovery path, with yt-dlp failing like a gone channel.
            automation_runner.downloader = YouTubeDownloader(
                str(automation_runner.config_path),
                videos_dir=str(automation_runner.videos_dir),
            )
        else:
            automation_runner.downloader.process_channel = Mock(
                side_effect=RuntimeError("boom")
            )
        automation_runner.cleanup_old_videos = Mock()

        with patch("src.downloader.yt_dlp.YoutubeDL") as ytdl:
            extract = ytdl.return_value.__enter__.return_value.extract_info
            extract.side_effect = Exception("This channel does not exist")
            assert automation_runner.run() == 1

        assert self.surviving(channel_dir) == ["fresh"]
        automation_runner.cleanup_old_videos.assert_not_called()
        state = load_health(automation_runner.config_path.parent)["channels"][
            "test_channel_1"
        ]
        assert state["consecutive_failures"] == 1
        assert state["unavailable"] is (failure == "unavailable")
        expected = "does not exist" if failure == "unavailable" else "boom"
        assert expected in state["last_error"]

    @pytest.mark.parametrize("mode", ["corrupt", "unreadable"])
    def test_broken_pins_skip_retention(
        self, automation_runner, test_config, unreadable_files, mode
    ):
        self.configure(automation_runner, test_config, storage={"retention_days": 30})
        channel_dir = self.seed(
            automation_runner, "test_channel_1", ancient=400, keepme=400
        )
        pins = pins_path(automation_runner.config_path.parent)
        if mode == "corrupt":
            pins.write_text('{"test_channel_1": ["keepme"')
        else:
            pins.write_text(json.dumps({"test_channel_1": ["keepme"]}))
            unreadable_files(pins)
        original = pins.read_bytes()
        self.stub_discovery(automation_runner)

        automation_runner.run()

        assert self.surviving(channel_dir) == ["ancient", "keepme"]
        assert pins.read_bytes() == original
        errors = logged(automation_runner.logger.error)
        assert "Skipping retention for test_channel_1" in errors
        assert "pins.json is repaired or removed" in errors

    def test_retention_error_never_fails_the_run(self, automation_runner, test_config):
        self.configure(automation_runner, test_config, storage={"retention_days": 30})
        self.stub_discovery(automation_runner)

        with patch(
            "src.cron_runner.apply_channel_retention",
            side_effect=OSError("disk hiccup"),
        ):
            assert automation_runner.run() == 0

        errors = logged(automation_runner.logger.error)
        assert "Retention failed for test_channel_1: disk hiccup" in errors
        assert "Retention failed for test_channel_2: disk hiccup" in errors
        health = load_health(automation_runner.config_path.parent)["channels"]
        assert health["test_channel_1"]["consecutive_failures"] == 0
        assert health["test_channel_2"]["consecutive_failures"] == 0

    def test_retention_runs_while_media_lock_is_held(
        self, automation_runner, test_config
    ):
        self.configure(automation_runner, test_config, storage={"retention_days": 30})
        self.stub_discovery(automation_runner)
        locked: list[bool] = []

        def probe_lock(*_args: Any, **_kwargs: Any) -> int:
            with open(automation_runner.lock_file_path, "a") as probe:
                try:
                    fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    locked.append(True)
                else:
                    fcntl.flock(probe.fileno(), fcntl.LOCK_UN)
                    locked.append(False)
            return 0

        with patch("src.cron_runner.apply_channel_retention", side_effect=probe_lock):
            assert automation_runner.run() == 0

        assert locked == [True, True]
