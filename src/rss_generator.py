import json
import logging
import math
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

from feedgen.feed import FeedGenerator

try:
    from .utils import MEDIA_EXTENSIONS, contained_path, save_json_file
except ImportError:
    from utils import MEDIA_EXTENSIONS, contained_path, save_json_file

_timestamp_lock = threading.RLock()


class RSSGenerator:
    def __init__(self, base_url: str | None = None, feed_token: str | None = None):
        if base_url is None:
            base_url = os.getenv("BASE_URL", "http://localhost:5000")
        self.base_url = base_url.rstrip("/")
        self.feed_token = feed_token
        self.logger = logging.getLogger(__name__)
        self.refresh_timestamps_file = (
            Path(__file__).parent.parent
            / "appdata"
            / "config"
            / "refresh_timestamps.json"
        )

    def _tokenize_url(self, url: str) -> str:
        """Append the private feed token to a generated URL when one is set."""
        if not self.feed_token:
            return url
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}token={quote(self.feed_token, safe='')}"

    def _apply_feed_token(self, rss_content: str, plain_urls: list[str]) -> str:
        """Rewrite generated feed URLs to carry the access token.

        URLs are handed to feedgen in plain form (its iTunes extension rejects
        image URLs that do not end in the file extension), so the token is
        appended to the serialized XML afterwards.
        """
        if not self.feed_token:
            return rss_content
        for url in dict.fromkeys(plain_urls):
            rss_content = rss_content.replace(url, self._tokenize_url(url))
        return rss_content

    def sanitize_text(self, text: str) -> str:
        """Sanitize text for XML content"""
        if not text:
            return ""
        # XML serializers escape text themselves. Only remove invalid XML 1.0 characters.
        sanitized = str(text)
        # Remove or replace control characters that are invalid in XML
        sanitized = "".join(
            char
            for char in sanitized
            if char in "\t\n\r"
            or 0x20 <= ord(char) <= 0xD7FF
            or 0xE000 <= ord(char) <= 0xFFFD
            or 0x10000 <= ord(char) <= 0x10FFFF
        )
        return sanitized

    def format_duration(self, duration) -> str:
        """Convert duration to HH:MM:SS format"""
        if isinstance(duration, str):
            # If already in HH:MM:SS format, return as is
            if ":" in duration:
                return duration
            # If it's a string number, convert to int
            try:
                duration = int(float(duration))
            except (ValueError, TypeError, OverflowError):
                return "00:00:00"

        if (
            isinstance(duration, (int, float))
            and math.isfinite(duration)
            and duration >= 0
        ):
            # Convert seconds to HH:MM:SS
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        return "00:00:00"

    def load_refresh_timestamps(self) -> dict[str, str]:
        """Load refresh timestamps from file"""
        if not self.refresh_timestamps_file.exists():
            return {}
        try:
            with open(self.refresh_timestamps_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.error(f"Error loading refresh timestamps: {e}")
            return {}

    def save_refresh_timestamps(self, timestamps: dict[str, str]):
        """Save refresh timestamps to file"""
        try:
            save_json_file(timestamps, str(self.refresh_timestamps_file))
        except Exception as e:
            self.logger.error(f"Error saving refresh timestamps: {e}")

    def update_channel_refresh_time(self, channel_name: str):
        """Update the last refresh time for a channel"""
        with _timestamp_lock:
            timestamps = self.load_refresh_timestamps()
            timestamps[channel_name] = datetime.now(UTC).isoformat()
            self.save_refresh_timestamps(timestamps)
        self.logger.info(f"Updated refresh timestamp for {channel_name}")

    def scan_channel_videos(self, channel_path: Path) -> list[dict]:
        """Scan episode directory and extract metadata from JSON files"""
        episodes = []

        if not channel_path.exists():
            self.logger.warning(f"Channel path does not exist: {channel_path}")
            return episodes

        # Support both video and audio file extensions
        supported_extensions = MEDIA_EXTENSIONS

        # Find all JSON metadata files
        for json_file in channel_path.glob("*.json"):
            if json_file.name.endswith(".info.json") or json_file.is_symlink():
                continue
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                if (
                    not isinstance(metadata, dict)
                    or metadata.get("id") != json_file.stem
                ):
                    continue

                # Check for episode file with any supported extension
                episode_file = None
                for ext in supported_extensions:
                    potential_file = contained_path(
                        channel_path, f"{metadata['id']}{ext}"
                    )
                    if potential_file.exists():
                        episode_file = potential_file
                        break

                if not episode_file:
                    self.logger.warning(f"Episode file missing for {metadata['id']}")
                    continue

                # Store the actual file extension found
                metadata["file_extension"] = episode_file.suffix
                metadata["file_size"] = episode_file.stat().st_size

                thumbnail = metadata.get("thumbnail")
                metadata["thumbnail"] = None
                if isinstance(thumbnail, str):
                    try:
                        thumbnail_file = contained_path(
                            channel_path, *thumbnail.split("/")
                        )
                        if thumbnail_file.is_file():
                            metadata["thumbnail"] = thumbnail
                    except ValueError:
                        pass
                metadata["title"] = metadata.get("title") or metadata["id"]
                metadata["description"] = str(metadata.get("description") or "")
                metadata["upload_date"] = str(metadata.get("upload_date") or "")

                episodes.append(metadata)
                self.logger.debug(f"Found episode: {metadata['title']}")

            except (ValueError, TypeError, KeyError, OSError) as e:
                self.logger.error(f"Error reading metadata from {json_file}: {e}")
                continue

        return episodes

    def generate_rss_feed(
        self, channel_name: str, videos: list[dict], display_name: str | None = None
    ) -> str:
        """Generate RSS 2.0 feed with podcast extensions"""
        fg = FeedGenerator()

        # Use display name if provided, otherwise fallback to formatted channel name
        display_title = (
            display_name if display_name else channel_name.replace("_", " ").title()
        )

        # Set feed metadata
        tokenizable_urls = [f"{self.base_url}/feeds/{quote(channel_name)}"]
        feed_url = tokenizable_urls[0]

        fg.id(feed_url)
        fg.title(self.sanitize_text(display_title))
        fg.link(href=f"{self.base_url}/", rel="alternate")
        fg.description(self.sanitize_text(f"YouTube videos from {display_title}"))
        fg.language("en-us")
        # Note: Removed author and generator to match example format

        # Load podcast extension
        fg.load_extension("podcast")

        # Set podcast-specific metadata
        podcast_ext = getattr(fg, "podcast", None)
        if podcast_ext:
            podcast_ext.itunes_explicit("no")
            podcast_ext.itunes_author(self.sanitize_text(display_title))
            podcast_ext.itunes_summary(
                self.sanitize_text(f"YouTube videos from {display_title}")
            )
        # Note: Removed itunes_category and itunes_owner to match example

        # Add atom:link with rel="self"
        fg.link(href=feed_url, rel="self", type="application/rss+xml")

        # Sort videos by upload date (newest first)
        sorted_videos = sorted(
            videos, key=lambda x: str(x.get("upload_date") or ""), reverse=True
        )

        # Add episodes
        for video in sorted_videos:
            try:
                upload_date = datetime.strptime(
                    str(video.get("upload_date") or "19700101"), "%Y%m%d"
                ).replace(tzinfo=UTC)
            except ValueError:
                self.logger.warning(
                    f"Skipping episode with invalid upload date: {video.get('id')}"
                )
                continue
            fe = fg.add_entry(order="append")

            # Get file extension and determine MIME type
            file_ext = video.get("file_extension", ".mp4")
            episode_filename = f"{video['id']}{file_ext}"
            encoded_episode_filename = quote(episode_filename)
            episode_url = f"{self.base_url}/podcasts/{quote(channel_name)}/{encoded_episode_filename}"
            tokenizable_urls.append(episode_url)

            # Determine MIME type based on file extension
            mime_type_map = {
                ".mp4": "video/mp4",
                ".m4a": "audio/mp4",
                ".mp3": "audio/mpeg",
                ".webm": "video/webm",
                ".mkv": "video/x-matroska",
                ".avi": "video/avi",
            }
            mime_type = mime_type_map.get(file_ext.lower(), "video/mp4")

            fe.id(episode_url)
            fe.guid(video["id"], permalink=False)
            fe.title(self.sanitize_text(video.get("title") or video["id"]))
            fe.content(self.sanitize_text(video.get("description") or ""), type="CDATA")
            fe.link(href=episode_url)

            fe.published(upload_date)

            # Add episode as enclosure with correct MIME type
            fe.enclosure(episode_url, str(video.get("file_size") or 0), mime_type)

            # Add thumbnail as episode image (iTunes supports PNG/JPG, WebP for other clients)
            if video.get("thumbnail"):
                thumbnail_path = video["thumbnail"]
                if thumbnail_path.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                    thumbnail_url = f"{self.base_url}/thumbnails/{quote(channel_name)}/{quote(Path(thumbnail_path).name)}"
                    tokenizable_urls.append(thumbnail_url)
                    # Only add iTunes image for supported formats
                    if thumbnail_path.lower().endswith((".png", ".jpg", ".jpeg")):
                        podcast_ext = getattr(fe, "podcast", None)
                        if podcast_ext:
                            podcast_ext.itunes_image(thumbnail_url)

            # Add podcast-specific metadata
            podcast_ext = getattr(fe, "podcast", None)
            if podcast_ext:
                podcast_ext.itunes_duration(self.format_duration(video.get("duration")))
                podcast_ext.itunes_author(
                    self.sanitize_text(video.get("uploader", "Unknown"))
                )

            self.logger.debug(f"Added episode: {video['title']}")

        rss_content = fg.rss_str(pretty=True).decode("utf-8")
        return self._apply_feed_token(rss_content, tokenizable_urls)

    def generate_rss_feed_from_filesystem(
        self, channel_name: str, videos_dir: Path, display_name: str | None = None
    ) -> str:
        """Generate RSS feed content dynamically from filesystem without saving to file"""
        channel_path = videos_dir / channel_name

        # Scan for videos
        videos = self.scan_channel_videos(channel_path)
        if not videos:
            self.logger.info(
                f"No videos found for channel: {channel_name}, generating empty RSS feed"
            )

        # Generate RSS content with display name (works with empty video list)
        rss_content = self.generate_rss_feed(channel_name, videos, display_name)

        display_title = display_name if display_name else channel_name
        self.logger.info(
            f"Generated dynamic RSS feed for {display_title} ({channel_name}) with {len(videos)} episodes"
        )
        return rss_content
