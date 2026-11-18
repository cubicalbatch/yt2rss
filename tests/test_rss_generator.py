"""Tests for the RSS generator module."""

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from src.rss_generator import RSSGenerator


class TestRSSGenerator:
    """Test cases for RSSGenerator class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def rss_generator(self):
        """Create RSSGenerator instance."""
        return RSSGenerator("http://test.example.com")

    @pytest.fixture
    def sample_episodes(self):
        """Sample episode data for testing."""
        return [
            {
                "id": "video1",
                "title": "Test Episode 1",
                "description": "First test episode",
                "upload_date": "20231201",
                "duration": 1800,
                "file_size": 5000000,
                "file_extension": ".mp4",
                "uploader": "Test Channel",
                "thumbnail": "thumbnails/video1.jpg",
            },
            {
                "id": "video2",
                "title": "Test Episode 2 & Special Characters",
                "description": "Second test episode with <HTML> & entities",
                "upload_date": "20231202",
                "duration": 3600,
                "file_size": 8000000,
                "file_extension": ".m4a",
                "uploader": "Test Channel",
                "thumbnail": "thumbnails/video2.jpg",
            },
        ]

    def test_init_default_base_url(self):
        """Test RSSGenerator initialization with default base URL."""
        with patch.dict("os.environ", {"BASE_URL": "http://custom.example.com"}):
            rss_gen = RSSGenerator()
            assert rss_gen.base_url == "http://custom.example.com"

    def test_init_custom_base_url(self):
        """Test RSSGenerator initialization with custom base URL."""
        rss_gen = RSSGenerator("http://custom.example.com/")
        assert rss_gen.base_url == "http://custom.example.com"

    def test_init_default_fallback(self):
        """Test RSSGenerator initialization with default fallback."""
        with patch.dict("os.environ", {}, clear=True):
            rss_gen = RSSGenerator()
            assert rss_gen.base_url == "http://localhost:5000"

    def test_sanitize_text_basic(self, rss_generator):
        """Test basic text sanitization."""
        text = "Hello World"
        result = rss_generator.sanitize_text(text)
        assert result == "Hello World"

    def test_sanitize_text_html_entities(self, rss_generator):
        """Test sanitization of HTML entities."""
        text = "Test & <script>alert('test')</script>"
        result = rss_generator.sanitize_text(text)
        assert result == text

    def test_sanitize_text_control_characters(self, rss_generator):
        """Test removal of control characters."""
        text = "Test\x00\x01\x02 Valid\t\n\r Text"
        result = rss_generator.sanitize_text(text)
        assert result == "Test Valid\t\n\r Text"

    def test_sanitize_text_empty(self, rss_generator):
        """Test sanitization of empty text."""
        assert rss_generator.sanitize_text("") == ""
        assert rss_generator.sanitize_text(None) == ""

    def test_format_duration_seconds(self, rss_generator):
        """Test duration formatting from seconds."""
        assert rss_generator.format_duration(3661) == "01:01:01"
        assert rss_generator.format_duration(90) == "00:01:30"
        assert rss_generator.format_duration(0) == "00:00:00"

    def test_format_duration_string_seconds(self, rss_generator):
        """Test duration formatting from string seconds."""
        assert rss_generator.format_duration("3661") == "01:01:01"
        assert rss_generator.format_duration("90.5") == "00:01:30"

    def test_format_duration_already_formatted(self, rss_generator):
        """Test duration formatting when already in HH:MM:SS format."""
        assert rss_generator.format_duration("01:30:45") == "01:30:45"

    def test_format_duration_invalid(self, rss_generator):
        """Test duration formatting with invalid input."""
        assert rss_generator.format_duration("invalid") == "00:00:00"
        assert rss_generator.format_duration(None) == "00:00:00"

    def test_load_refresh_timestamps_file_exists(self, rss_generator, temp_dir):
        """Test loading refresh timestamps when file exists."""
        timestamps = {"channel1": "2023-12-01T10:00:00Z"}
        timestamps_file = Path(temp_dir) / "refresh_timestamps.json"

        with open(timestamps_file, "w") as f:
            json.dump(timestamps, f)

        rss_generator.refresh_timestamps_file = timestamps_file
        result = rss_generator.load_refresh_timestamps()

        assert result == timestamps

    def test_load_refresh_timestamps_file_not_exists(self, rss_generator, temp_dir):
        """Test loading refresh timestamps when file doesn't exist."""
        rss_generator.refresh_timestamps_file = Path(temp_dir) / "nonexistent.json"
        result = rss_generator.load_refresh_timestamps()

        assert result == {}

    def test_load_refresh_timestamps_invalid_json(self, rss_generator, temp_dir):
        """Test loading refresh timestamps with invalid JSON."""
        timestamps_file = Path(temp_dir) / "refresh_timestamps.json"

        with open(timestamps_file, "w") as f:
            f.write("invalid json content")

        rss_generator.refresh_timestamps_file = timestamps_file
        result = rss_generator.load_refresh_timestamps()

        assert result == {}

    def test_save_refresh_timestamps(self, rss_generator, temp_dir):
        """Test saving refresh timestamps."""
        timestamps = {"channel1": "2023-12-01T10:00:00Z"}
        timestamps_file = Path(temp_dir) / "refresh_timestamps.json"
        rss_generator.refresh_timestamps_file = timestamps_file

        rss_generator.save_refresh_timestamps(timestamps)

        assert timestamps_file.exists()
        with open(timestamps_file) as f:
            saved_data = json.load(f)
        assert saved_data == timestamps

    @patch.object(RSSGenerator, "load_refresh_timestamps")
    @patch.object(RSSGenerator, "save_refresh_timestamps")
    def test_update_channel_refresh_time(
        self, mock_save, mock_load, rss_generator, temp_dir
    ):
        """Test updating channel refresh time."""
        # The update takes a lock file next to the timestamps file.
        rss_generator.refresh_timestamps_file = (
            Path(temp_dir) / "refresh_timestamps.json"
        )
        mock_load.return_value = {"channel1": "2023-12-01T10:00:00Z"}

        with patch("src.rss_generator.datetime") as mock_datetime:
            mock_now = datetime(2023, 12, 2, 15, 30, 0, tzinfo=UTC)
            mock_datetime.now.return_value = mock_now
            mock_datetime.strptime = datetime.strptime

            rss_generator.update_channel_refresh_time("channel2")

            mock_save.assert_called_once()
            saved_timestamps = mock_save.call_args[0][0]
            assert "channel2" in saved_timestamps
            assert (
                saved_timestamps["channel1"] == "2023-12-01T10:00:00Z"
            )  # Existing preserved

    def test_scan_channel_videos_channel_not_exists(self, rss_generator, temp_dir):
        """Test scanning videos when channel directory doesn't exist."""
        channel_path = Path(temp_dir) / "nonexistent"
        result = rss_generator.scan_channel_videos(channel_path)

        assert result == []

    def test_scan_channel_videos_success(self, rss_generator, temp_dir):
        """Test successful video scanning."""
        channel_path = Path(temp_dir) / "test-channel"
        channel_path.mkdir()
        thumbnails_dir = channel_path / "thumbnails"
        thumbnails_dir.mkdir()

        # Create video files and metadata
        metadata1 = {
            "id": "video1",
            "title": "Test Video 1",
            "thumbnail": "thumbnails/video1.jpg",
            "duration": 300,
        }
        metadata2 = {
            "id": "video2",
            "title": "Test Video 2",
            "thumbnail": "thumbnails/video2.jpg",
            "duration": 600,
        }

        # Create metadata files
        with open(channel_path / "video1.json", "w") as f:
            json.dump(metadata1, f)
        with open(channel_path / "video2.json", "w") as f:
            json.dump(metadata2, f)

        # Create video files and thumbnails
        (channel_path / "video1.mp4").touch()
        (channel_path / "video2.m4a").touch()
        (thumbnails_dir / "video1.jpg").touch()
        (thumbnails_dir / "video2.jpg").touch()

        result = rss_generator.scan_channel_videos(channel_path)

        assert len(result) == 2
        # Results are not guaranteed to be in order, so check both exist
        ids = [r["id"] for r in result]
        assert "video1" in ids
        assert "video2" in ids

        # Check file extensions are set correctly
        for episode in result:
            if episode["id"] == "video1":
                assert episode["file_extension"] == ".mp4"
            elif episode["id"] == "video2":
                assert episode["file_extension"] == ".m4a"

    def test_scan_channel_videos_missing_episode_file(self, rss_generator, temp_dir):
        """Test video scanning with missing episode file."""
        channel_path = Path(temp_dir) / "test-channel"
        channel_path.mkdir()

        metadata = {
            "id": "video1",
            "title": "Test Video 1",
            "thumbnail": "thumbnails/video1.jpg",
        }

        with open(channel_path / "video1.json", "w") as f:
            json.dump(metadata, f)

        # Don't create the video file
        result = rss_generator.scan_channel_videos(channel_path)

        assert result == []

    def test_scan_channel_videos_missing_thumbnail(self, rss_generator, temp_dir):
        """Test video scanning with missing thumbnail."""
        channel_path = Path(temp_dir) / "test-channel"
        channel_path.mkdir()

        metadata = {
            "id": "video1",
            "title": "Test Video 1",
            "thumbnail": "thumbnails/video1.jpg",
        }

        with open(channel_path / "video1.json", "w") as f:
            json.dump(metadata, f)

        # Create video file but not thumbnail
        (channel_path / "video1.mp4").touch()

        result = rss_generator.scan_channel_videos(channel_path)

        assert len(result) == 1
        assert result[0]["thumbnail"] is None

    def test_scan_channel_videos_invalid_json(self, rss_generator, temp_dir):
        """Test video scanning with invalid JSON metadata."""
        channel_path = Path(temp_dir) / "test-channel"
        channel_path.mkdir()

        # Create invalid JSON file
        with open(channel_path / "video1.json", "w") as f:
            f.write("invalid json")

        (channel_path / "video1.mp4").touch()

        result = rss_generator.scan_channel_videos(channel_path)

        assert result == []

    def test_generate_rss_feed_basic(self, rss_generator, sample_episodes):
        """Test basic RSS feed generation."""
        rss_content = rss_generator.generate_rss_feed("test-channel", sample_episodes)

        assert "<title>Test-Channel</title>" in rss_content
        assert (
            "<description>YouTube videos from Test-Channel</description>" in rss_content
        )
        assert "Test Episode 1" in rss_content
        assert "Test Episode 2 &amp; Special Characters" in rss_content
        assert "video/mp4" in rss_content
        assert "audio/mp4" in rss_content

    def test_generate_rss_feed_with_display_name(self, rss_generator, sample_episodes):
        """Test RSS feed generation with custom display name."""
        rss_content = rss_generator.generate_rss_feed(
            "test-channel", sample_episodes, "My Custom Channel"
        )

        assert "<title>My Custom Channel</title>" in rss_content
        assert (
            "<description>YouTube videos from My Custom Channel</description>"
            in rss_content
        )

    def test_generate_rss_feed_empty_episodes(self, rss_generator):
        """Test RSS feed generation with no episodes."""
        rss_content = rss_generator.generate_rss_feed("test-channel", [])

        assert "<title>Test-Channel</title>" in rss_content
        assert "<item>" not in rss_content

    def test_generate_rss_feed_sorting(self, rss_generator):
        """Test RSS feed generation sorts episodes by upload date."""
        episodes = [
            {
                "id": "video1",
                "title": "Older Episode",
                "upload_date": "20231201",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".mp4",
                "description": "Older",
            },
            {
                "id": "video2",
                "title": "Newer Episode",
                "upload_date": "20231203",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".mp4",
                "description": "Newer",
            },
        ]

        rss_content = rss_generator.generate_rss_feed("test-channel", episodes)

        # Check that episodes are sorted by date (newer first)
        newer_pos = rss_content.find("Newer Episode")
        older_pos = rss_content.find("Older Episode")
        assert 0 <= newer_pos < older_pos

    def test_generate_rss_feed_mime_types(self, rss_generator):
        """Test RSS feed generation with different file types."""
        episodes = [
            {
                "id": "video1",
                "title": "MP4 Video",
                "upload_date": "20231201",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".mp4",
                "description": "Video",
            },
            {
                "id": "video2",
                "title": "M4A Audio",
                "upload_date": "20231202",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".m4a",
                "description": "Audio",
            },
            {
                "id": "video3",
                "title": "WebM Video",
                "upload_date": "20231203",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".webm",
                "description": "WebM",
            },
        ]

        rss_content = rss_generator.generate_rss_feed("test-channel", episodes)

        assert 'type="video/mp4"' in rss_content
        assert 'type="audio/mp4"' in rss_content
        assert 'type="video/webm"' in rss_content

    @pytest.mark.parametrize(
        "extension, mime_type",
        [
            (".mp3", "audio/mpeg"),
            (".mkv", "video/x-matroska"),
            (".avi", "video/x-msvideo"),
        ],
    )
    def test_generate_rss_feed_enclosure_type(
        self, rss_generator, extension, mime_type
    ):
        """Enclosure types come from the shared media MIME map."""
        episodes = [
            {
                "id": "video1",
                "title": "Episode",
                "upload_date": "20231201",
                "file_size": 10,
                "file_extension": extension,
            }
        ]

        rss_content = rss_generator.generate_rss_feed("test-channel", episodes)

        assert f'type="{mime_type}"' in rss_content

    def test_scan_channel_videos_uses_first_regular_media_file(
        self, rss_generator, temp_dir
    ):
        """Media lookup follows MEDIA_EXTENSIONS order and skips directories."""
        channel_path = Path(temp_dir) / "test-channel"
        channel_path.mkdir()
        (channel_path / "video1.json").write_text(
            json.dumps({"id": "video1", "title": "Test Video 1"})
        )
        (channel_path / "video1.mp4").mkdir()
        (channel_path / "video1.mp3").write_bytes(b"mp3")
        (channel_path / "video1.m4a").write_bytes(b"audio")

        [episode] = rss_generator.scan_channel_videos(channel_path)

        assert episode["file_extension"] == ".m4a"
        assert episode["file_size"] == len(b"audio")

    def test_scan_and_generate_compose_into_feed(
        self, rss_generator, temp_dir, sample_episodes
    ):
        """Scan + generate compose into a feed (the web server's composition)."""
        channel_dir = Path(temp_dir) / "test-channel"
        channel_dir.mkdir()
        for episode in sample_episodes:
            (channel_dir / f"{episode['id']}.json").write_text(
                json.dumps(episode), encoding="utf-8"
            )
            (channel_dir / f"{episode['id']}.mp4").write_bytes(b"media")

        videos = rss_generator.scan_channel_videos(channel_dir)
        rss_content = rss_generator.generate_rss_feed(
            "test-channel", videos, "My Channel"
        )

        assert "<title>My Channel</title>" in rss_content
        assert "Test Episode 1" in rss_content
        assert "Test Episode 2 &amp; Special Characters" in rss_content

    def test_scan_and_generate_empty_channel(self, rss_generator, temp_dir):
        """An empty channel directory yields a feed without items."""
        channel_dir = Path(temp_dir) / "test-channel"
        channel_dir.mkdir()

        videos = rss_generator.scan_channel_videos(channel_dir)
        rss_content = rss_generator.generate_rss_feed("test-channel", videos)

        assert "<title>Test-Channel</title>" in rss_content
        assert "<item>" not in rss_content

    def test_generate_rss_feed_special_characters_in_channel_name(
        self, rss_generator, sample_episodes
    ):
        """Test RSS feed generation with special characters in channel name."""
        rss_content = rss_generator.generate_rss_feed(
            "test channel & co", sample_episodes
        )

        # Channel name should be URL-encoded in links
        assert "test%20channel%20%26%20co" in rss_content
        # But displayed normally in title
        assert "<title>Test Channel &amp; Co</title>" in rss_content

    def test_generate_rss_feed_cdata_descriptions(self, rss_generator):
        """Test RSS feed generation wraps descriptions in CDATA."""
        episodes = [
            {
                "id": "video1",
                "title": "Test Episode",
                "description": "Description with <HTML> tags & entities",
                "upload_date": "20231201",
                "duration": 300,
                "file_size": 1000000,
                "file_extension": ".mp4",
            }
        ]

        rss_content = rss_generator.generate_rss_feed("test-channel", episodes)

        assert "<![CDATA[Description with <HTML> tags & entities]]>" in rss_content

    def test_generate_rss_feed_duration_formatting(self, rss_generator):
        """Test RSS feed generation formats durations correctly."""
        episodes = [
            {
                "id": "video1",
                "title": "Test Episode",
                "description": "Test",
                "upload_date": "20231201",
                "duration": 3661,  # 1:01:01
                "file_size": 1000000,
                "file_extension": ".mp4",
            }
        ]

        rss_content = rss_generator.generate_rss_feed("test-channel", episodes)

        assert "<itunes:duration>01:01:01</itunes:duration>" in rss_content
