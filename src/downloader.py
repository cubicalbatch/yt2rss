import json
import logging
import os
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import yaml
import yt_dlp
from PIL import Image

# Handle both relative and absolute imports
try:
    from .download_queue import ConfigUnavailableError, DownloadCancelled
    from .rss_generator import RSSGenerator
    from .storage import (
        admission_error,
        free_disk_bytes,
        load_storage_settings,
        measure_media_usage,
        tombstoned_ids,
    )
    from .utils import (
        contained_path,
        filter_skip_reason,
        find_episode_media,
        load_episode_filters,
        media_extensions_for,
        safe_component,
        save_json_file,
        validate_channel_settings,
    )
except ImportError:
    from download_queue import ConfigUnavailableError, DownloadCancelled
    from rss_generator import RSSGenerator
    from storage import (
        admission_error,
        free_disk_bytes,
        load_storage_settings,
        measure_media_usage,
        tombstoned_ids,
    )
    from utils import (
        contained_path,
        filter_skip_reason,
        find_episode_media,
        load_episode_filters,
        media_extensions_for,
        safe_component,
        save_json_file,
        validate_channel_settings,
    )


class YouTubeDownloader:
    def __init__(
        self,
        config_path: str = "appdata/config/channels.yaml",
        base_dir: str = ".",
        videos_dir: str | None = None,
    ):
        self.config_path = config_path
        self.base_dir = Path(base_dir)
        self.videos_dir = (
            Path(videos_dir)
            if videos_dir is not None
            else self.base_dir / "appdata" / "podcasts"
        )

        # Ensure directories exist
        self.videos_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)
        # Set by ``get_channel_videos`` so discovery can distinguish a
        # genuinely empty channel from a fetch failure (health tracking).
        self.last_channel_error: str | None = None

        # Initialize RSS generator for timestamp updates
        self.rss_generator = RSSGenerator()
        self.rss_generator.refresh_timestamps_file = (
            Path(config_path).resolve().parent / "refresh_timestamps.json"
        )

    def _get_proxy_opts(self) -> dict[str, str]:
        """Get proxy configuration from environment variables.

        Returns a dictionary with proxy settings for yt-dlp if YT_DLP_PROXY is set.
        Supports SOCKS5, HTTP, and HTTPS proxies.

        Example formats:
        - socks5://host:port
        - socks5://user:pass@host:port
        - http://host:port
        - https://host:port
        """
        proxy_url = os.getenv("YT_DLP_PROXY")
        if proxy_url:
            self.logger.info(
                f"🔐 Using proxy: {proxy_url.split('@')[-1]}"
            )  # Don't log credentials
            return {"proxy": proxy_url}
        return {}

    def load_config(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Load channel configuration, treating an unusable file as empty.

        Returns:
            The configured channels and the remaining global settings, or
            ``([], {})`` (with the error logged) when the file cannot be
            loaded.
        """
        try:
            return self.load_config_strict()
        except ConfigUnavailableError as e:
            self.logger.error(f"Failed to load config: {e}")
            return [], {}

    def load_config_strict(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Load channel configuration, failing loudly when it is unusable.

        Callers that must not mistake a broken file for "no channels" (the
        download queue) use this instead of :meth:`load_config`.

        Returns:
            The configured channels and the remaining global settings.

        Raises:
            ConfigUnavailableError: When the file is missing, unreadable, not
                valid YAML, or not a mapping with a ``channels`` list of
                channel mappings.
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except (OSError, UnicodeDecodeError) as e:
            raise ConfigUnavailableError(f"cannot read {self.config_path}: {e}") from e
        except yaml.YAMLError as e:
            raise ConfigUnavailableError(
                f"invalid YAML in {self.config_path}: {e}"
            ) from e
        channels = config.get("channels") if isinstance(config, dict) else None
        if not isinstance(channels, list) or not all(
            isinstance(channel, dict) for channel in channels
        ):
            raise ConfigUnavailableError(
                f"{self.config_path} must be a mapping with a 'channels' list"
            )
        global_config = {k: v for k, v in config.items() if k != "channels"}
        return channels, global_config

    def get_channel_videos(
        self, channel_url: str, max_episodes: int = 10
    ) -> list[dict[str, Any]]:
        """Get video information from a YouTube channel."""
        self.last_channel_error = None
        self.logger.info(
            f"📡 Fetching video list from channel (max {max_episodes} episodes)..."
        )

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "playlist_items": f"1:{max_episodes}",
            **self._get_proxy_opts(),
        }

        try:
            with yt_dlp.YoutubeDL(cast(Any, ydl_opts)) as ydl:
                info = ydl.extract_info(channel_url, download=False)

                if not info or "entries" not in info:
                    self.last_channel_error = "No channel data returned"
                    self.logger.warning(
                        f"❌ No videos found for channel: {channel_url}"
                    )
                    return []

                videos = []
                for entry in info["entries"] if info else []:
                    if (
                        entry
                        and isinstance(entry, dict)
                        and safe_component(entry.get("id"))
                    ):
                        videos.append(
                            {
                                "id": entry.get("id"),
                                "title": entry.get("title"),
                                "url": f"https://www.youtube.com/watch?v={entry.get('id')}",
                                "upload_date": entry.get("upload_date"),
                                "duration": entry.get("duration"),
                                # Live markers feed the skip_livestreams filter.
                                "live_status": entry.get("live_status"),
                                "is_live": entry.get("is_live"),
                                "was_live": entry.get("was_live"),
                            }
                        )

                self.logger.info(f"✅ Found {len(videos)} videos in channel")
                return videos

        except Exception as e:
            self.last_channel_error = str(e)
            self.logger.error(f"Failed to get channel videos: {e}")
            return []

    def get_video_metadata(self, video_url: str) -> dict[str, Any]:
        """Get detailed metadata for a single video."""
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            **self._get_proxy_opts(),
        }

        try:
            with yt_dlp.YoutubeDL(cast(Any, ydl_opts)) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info:
                    return {
                        "id": info.get("id"),
                        "title": info.get("title"),
                        "upload_date": info.get("upload_date"),
                        "timestamp": info.get("timestamp"),
                        "duration": info.get("duration"),
                        "filesize": info.get("filesize") or info.get("filesize_approx"),
                        "url": video_url,
                        # Live markers feed the skip_livestreams filter when
                        # queue execution re-checks filters after enqueue.
                        "live_status": info.get("live_status"),
                        "is_live": info.get("is_live"),
                        "was_live": info.get("was_live"),
                    }
                else:
                    return {}
        except Exception as e:
            self.logger.error(f"Failed to get video metadata for {video_url}: {e}")
            return {}

    def video_exists(
        self, channel_name: str, video_id: str, format_type: str = "video"
    ) -> bool:
        """Check if video has already been downloaded."""
        channel_dir = contained_path(self.videos_dir, channel_name)
        if not contained_path(channel_dir, f"{video_id}.json").is_file():
            return False
        extensions = media_extensions_for(format_type)
        return find_episode_media(channel_dir, video_id, extensions) is not None

    def is_video_too_new(
        self, video_info: dict[str, Any], download_delay_hours: int
    ) -> bool:
        """Check if video is too new based on download_delay_hours."""
        if download_delay_hours <= 0:
            return False

        timestamp = video_info.get("timestamp")
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            return time.time() - timestamp < download_delay_hours * 3600

        upload_date_str = video_info.get("upload_date")
        if not upload_date_str:
            return False

        try:
            # Parse upload date (format: YYYYMMDD)
            upload_date = datetime.strptime(upload_date_str, "%Y%m%d")

            # Calculate minimum age required
            min_age = timedelta(hours=download_delay_hours)
            current_time = datetime.now()

            # Check if video is too new
            video_age = current_time - upload_date
            return video_age < min_age

        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to parse upload date {upload_date_str}: {e}")
            return False

    def download_video(
        self,
        video_info: dict[str, Any],
        channel_name: str,
        sponsorblock_categories: list[str] | None = None,
        format_type: str = "video",
        quality: str = "max",
        progress_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> bool:
        """Download a single video with SponsorBlock integration.

        Args:
            video_info: Mapping with at least ``id`` and ``url``.
            channel_name: Sanitized channel identifier.
            sponsorblock_categories: SponsorBlock categories to remove.
            format_type: ``video`` or ``audio``.
            quality: Quality preset (``max`` or e.g. ``720p``).
            progress_hook: Optional yt-dlp progress callback. It may raise
                :class:`DownloadCancelled` to abort cooperatively; partial
                artifacts are removed and the exception propagates.
        """
        video_id = video_info["id"]
        if not safe_component(video_id) or not safe_component(channel_name):
            self.logger.error("Invalid channel or video identifier")
            return False
        video_url = video_info["url"]
        video_title = video_info.get("title", "Unknown Title")

        self.logger.info(f"\n🎬 Starting download: {video_title}")
        self.logger.info(f"   📺 Video ID: {video_id}")
        self.logger.info(f"   🎯 Format: {format_type}")
        self.logger.info(f"   🔧 Quality: {quality}")

        if sponsorblock_categories:
            self.logger.info(
                f"   🚫 SponsorBlock categories: {', '.join(sponsorblock_categories)}"
            )

        # Create channel directory
        channel_dir = contained_path(self.videos_dir, channel_name)
        channel_dir.mkdir(exist_ok=True)

        # Create thumbnails subdirectory
        thumbnails_dir = contained_path(channel_dir, "thumbnails")
        thumbnails_dir.mkdir(exist_ok=True)
        existing_files = set(channel_dir.glob(f"{video_id}.*")) | set(
            thumbnails_dir.glob(f"{video_id}.*")
        )

        if format_type == "audio":
            format_string = "bestaudio[ext=m4a]/bestaudio"
        else:
            limit = (
                f"[height<={quality[:-1]}]"
                if quality in ("480p", "720p", "1080p")
                else ""
            )
            format_string = f"bestvideo{limit}[ext=mp4]+bestaudio[ext=m4a]/best{limit}[ext=mp4]/best{limit}"

        # Configure yt-dlp options
        ydl_opts = {
            "outtmpl": str(channel_dir / f"{video_id}.%(ext)s"),
            "format": format_string,
            "writeinfojson": True,
            "writethumbnail": True,
            "writedescription": False,
            "writesubtitles": False,
            "writeautomaticsub": False,
            "noplaylist": True,
            "socket_timeout": 30,
            "merge_output_format": "mp4",
            **self._get_proxy_opts(),
        }
        if progress_hook is not None:
            ydl_opts["progress_hooks"] = [progress_hook]

        # Add SponsorBlock postprocessor if categories specified
        if sponsorblock_categories:
            ydl_opts["postprocessors"] = [
                {
                    "key": "SponsorBlock",
                    "categories": sponsorblock_categories,
                    "when": "after_move",
                },
                {
                    "key": "ModifyChapters",
                    "remove_sponsor_segments": sponsorblock_categories,
                    "when": "after_move",
                },
            ]

        if format_type == "audio":
            ydl_opts.setdefault("postprocessors", []).append(
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "m4a",
                }
            )

        try:
            self.logger.info("   ⬇️  Downloading video file...")

            with yt_dlp.YoutubeDL(cast(Any, ydl_opts)) as ydl:
                result = ydl.download([video_url])
                if isinstance(result, int) and result != 0:
                    raise RuntimeError("yt-dlp reported a download failure")

            media_file = find_episode_media(
                channel_dir, video_id, media_extensions_for(format_type)
            )
            if media_file is None or media_file.stat().st_size == 0:
                raise RuntimeError("Download produced no usable media file")

            self.logger.info("   ✅ Video download completed")

            # Move thumbnail to thumbnails directory and convert WebP to JPG
            thumbnail_extensions = [".jpg", ".jpeg", ".png", ".webp"]
            final_thumbnail_ext = None
            for ext in thumbnail_extensions:
                thumbnail_path = channel_dir / f"{video_id}{ext}"
                if thumbnail_path.exists():
                    if ext == ".webp":
                        # Convert WebP to JPG for iTunes compatibility
                        jpg_path = thumbnails_dir / f"{video_id}.jpg"
                        try:
                            self.logger.info(
                                "   🖼️  Converting WebP thumbnail to JPG..."
                            )
                            with Image.open(thumbnail_path) as img:
                                # Convert to RGB if necessary (WebP can have transparency)
                                if img.mode in ("RGBA", "LA", "P"):
                                    rgb_img = Image.new(
                                        "RGB", img.size, (255, 255, 255)
                                    )
                                    if img.mode == "P":
                                        img = img.convert("RGBA")
                                    rgb_img.paste(
                                        img,
                                        mask=img.split()[-1]
                                        if img.mode in ("RGBA", "LA")
                                        else None,
                                    )
                                    img = rgb_img
                                img.save(jpg_path, "JPEG", quality=90)
                            thumbnail_path.unlink()  # Remove original WebP
                            final_thumbnail_ext = ".jpg"
                            self.logger.info("   ✅ Thumbnail converted to JPG")
                        except Exception as e:
                            self.logger.error(
                                f"Failed to convert WebP thumbnail for {video_id}: {e}"
                            )
                            # Fallback: move WebP as-is
                            new_thumbnail_path = thumbnails_dir / f"{video_id}{ext}"
                            thumbnail_path.rename(new_thumbnail_path)
                            final_thumbnail_ext = ext
                    else:
                        # Move non-WebP thumbnails as-is
                        new_thumbnail_path = thumbnails_dir / f"{video_id}{ext}"
                        thumbnail_path.rename(new_thumbnail_path)
                        final_thumbnail_ext = ext
                    break

            # Load and enhance metadata
            metadata_file = channel_dir / f"{video_id}.info.json"
            if not metadata_file.is_file():
                raise RuntimeError("Download produced no metadata")
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    metadata = json.load(f)

                # Create simplified metadata file
                simplified_metadata = {
                    "id": video_id,
                    "title": metadata.get("title"),
                    "description": metadata.get("description", ""),
                    "upload_date": metadata.get("upload_date"),
                    "duration": metadata.get("duration"),
                    "thumbnail": f"thumbnails/{video_id}{final_thumbnail_ext}"
                    if final_thumbnail_ext
                    else None,
                    "file_size": media_file.stat().st_size,
                    "file_extension": media_file.suffix,
                    "timestamp": metadata.get("timestamp"),
                    "uploader": metadata.get("uploader"),
                    "view_count": metadata.get("view_count", 0),
                    # Live markers let the feed and episodes API apply
                    # ``skip_livestreams`` to downloaded episodes too.
                    "live_status": metadata.get("live_status"),
                    "is_live": bool(metadata.get("is_live")),
                    "was_live": bool(metadata.get("was_live")),
                }

                # Save simplified metadata
                simple_metadata_file = channel_dir / f"{video_id}.json"
                if not save_json_file(simplified_metadata, str(simple_metadata_file)):
                    raise OSError("Could not save episode metadata")

                # Remove the detailed info.json file
                metadata_file.unlink()

            self.logger.info(f"🎉 Successfully downloaded: {video_title}")
            return True

        except DownloadCancelled:
            self.logger.warning(f"🛑 Download cancelled: {video_title}")
            self._remove_partial_artifacts(
                channel_dir, thumbnails_dir, video_id, existing_files
            )
            raise

        except Exception as e:
            self.logger.error(f"❌ Failed to download: {video_title} - {e}")
            self._remove_partial_artifacts(
                channel_dir, thumbnails_dir, video_id, existing_files
            )
            return False

    def _remove_partial_artifacts(
        self,
        channel_dir: Path,
        thumbnails_dir: Path,
        video_id: str,
        existing_files: set[Path],
    ) -> None:
        """Remove partial artifacts so a failed download doesn't linger as a
        broken episode (stray info.json/webp/jpg/mp4.part files pollute
        the feed and occupy cleanup slots).
        """
        for partial in list(channel_dir.glob(f"{video_id}.*")) + list(
            thumbnails_dir.glob(f"{video_id}.*")
        ):
            if partial in existing_files:
                continue
            try:
                partial.unlink()
                self.logger.info(f"   🧹 Removed partial file: {partial.name}")
            except Exception as cleanup_err:
                self.logger.warning(
                    f"   ⚠️ Could not remove {partial.name}: {cleanup_err}"
                )

    def process_channel(
        self, channel_config: dict[str, Any], global_config: dict[str, Any]
    ) -> int:
        """Process a single channel configuration."""
        validate_channel_settings(channel_config)
        validate_channel_settings(
            {
                key: value
                for key, value in global_config.items()
                if key in ("download_delay_seconds", "check_delay_seconds")
            }
        )
        channel_name = channel_config["name"]
        contained_path(self.videos_dir, channel_name)
        channel_url = channel_config["url"]
        max_episodes = channel_config.get("max_episodes", 10)
        sponsorblock_categories = channel_config.get("sponsorblock_categories", [])
        download_delay_hours = channel_config.get("download_delay_hours", 0)
        download_delay_seconds = global_config.get(
            "download_delay_seconds", 60
        )  # Default 60 seconds
        check_delay_seconds = channel_config.get(
            "check_delay_seconds", global_config.get("check_delay_seconds", 0)
        )  # Channel-specific or global default (0 seconds)
        format_type = channel_config.get("format", "video")  # Default to video
        quality = channel_config.get("quality", "max")  # Default to max quality

        self.logger.info(f"\n📻 Processing channel: {channel_name}")
        self.logger.info(f"   🔗 URL: {channel_url}")
        self.logger.info(f"   📊 Max episodes: {max_episodes}")
        self.logger.info(f"   🎯 Format: {format_type}")
        self.logger.info(f"   🔧 Quality: {quality}")
        self.logger.info(
            f"   ⏰ Download delay: {download_delay_seconds}s between episodes"
        )
        if check_delay_seconds > 0:
            self.logger.info(
                f"   🔍 Check delay: {check_delay_seconds}s between metadata checks"
            )
        if download_delay_hours > 0:
            self.logger.info(
                f"   ⌛ Skip videos newer than: {download_delay_hours} hours"
            )

        # Get video list
        videos = self.get_channel_videos(channel_url, max_episodes)
        if not videos:
            self.logger.warning(f"⚠️  No videos found for channel: {channel_name}")
            return 0

        # Episode filters and tombstones keep filtered and manually deleted
        # videos out of the inline download loop, exactly like discovery
        # keeps them out of the download queue.
        filters = load_episode_filters(channel_config)
        deleted = tombstoned_ids(Path(self.config_path).parent, channel_name)

        # Download videos
        downloaded_count = 0
        self.logger.info(f"\n🔍 Checking {len(videos)} videos for download...")

        # Storage controls: quota and free-space checks reuse the discovery
        # snapshot so large libraries are measured once per channel run.
        storage_settings = load_storage_settings(global_config)
        usage_totals = measure_media_usage(self.videos_dir)
        try:
            free_bytes: int | None = free_disk_bytes(self.videos_dir)
        except OSError as error:
            self.logger.warning(f"Free disk space check failed: {error}")
            free_bytes = None

        def apply_check_delay():
            """Apply check delay if configured and not on last video."""
            if check_delay_seconds > 0 and i < len(videos):
                self.logger.info(
                    f"   ⏸️  Waiting {check_delay_seconds}s before next video check..."
                )
                time.sleep(check_delay_seconds)

        for i, video in enumerate(videos, 1):
            video_id = video["id"]
            video_title = video.get("title", "Unknown Title")

            self.logger.info(f"\n[{i}/{len(videos)}] 🔎 Checking: {video_title}")

            if video_id in deleted:
                self.logger.info(
                    f"   🪦 Skipping episode deleted by the user: {video_title}"
                )
                apply_check_delay()
                continue

            # Cheap filters (title rules, known durations, live markers)
            # run before any per-video metadata request.
            skip_reason = filter_skip_reason(video, filters)
            if skip_reason is not None:
                self.logger.info(f"   🚫 Skipped by episode filter: {skip_reason}")
                apply_check_delay()
                continue

            # Check if already downloaded
            if self.video_exists(channel_name, video_id, format_type):
                self.logger.info("   ⏭️  Already downloaded, skipping")
                apply_check_delay()
                continue

            # Get detailed metadata to check upload date
            self.logger.info("   📊 Fetching detailed metadata...")
            detailed_metadata = self.get_video_metadata(video["url"])
            if not detailed_metadata:
                self.logger.warning("   ❌ Failed to get metadata, skipping")
                apply_check_delay()
                continue

            # Update video info with detailed metadata
            video.update(detailed_metadata)

            # Duration bounds and Shorts detection are re-applied now that
            # the authoritative metadata replaced the flat entry values.
            skip_reason = filter_skip_reason(video, filters)
            if skip_reason is not None:
                self.logger.info(f"   🚫 Skipped by episode filter: {skip_reason}")
                apply_check_delay()
                continue

            # Check if video is too new
            if self.is_video_too_new(video, download_delay_hours):
                self.logger.info(
                    f"   ⌛ Video too new (< {download_delay_hours}h old), skipping for SponsorBlock data"
                )
                apply_check_delay()
                continue

            blocked = admission_error(
                settings=storage_settings,
                channel=channel_config,
                channel_dir=contained_path(self.videos_dir, channel_name),
                videos_dir=self.videos_dir,
                format_type=format_type,
                metadata_bytes=video.get("filesize"),
                usage_totals=usage_totals,
                free_bytes=free_bytes,
            )
            if blocked is not None:
                self.logger.warning(f"   ⛔ Not downloading: {blocked}")
                apply_check_delay()
                continue

            # Download the video
            self.logger.info("   ▶️  Starting download...")
            download_success = self.download_video(
                video, channel_name, sponsorblock_categories, format_type, quality
            )
            if download_success:
                downloaded_count += 1

            # Add appropriate delay based on what happened
            if i < len(videos):  # Not the last video
                if download_success and download_delay_seconds > 0:
                    # Longer delay after successful downloads
                    self.logger.info(
                        f"   ⏸️  Waiting {download_delay_seconds}s before next download..."
                    )
                    time.sleep(download_delay_seconds)
                else:
                    # Apply check delay if no download delay was used
                    apply_check_delay()

        self.logger.info("\n✨ Channel processing complete!")
        self.logger.info(f"   📥 Downloaded: {downloaded_count} new episodes")
        self.logger.info(f"   📻 Channel: {channel_name}")

        # Update refresh timestamp for this channel
        self.rss_generator.update_channel_refresh_time(channel_name)

        return downloaded_count

    def process_all_channels(self) -> int:
        """Process all configured channels."""
        channels, global_config = self.load_config()
        if not channels:
            self.logger.error("No channels configured")
            return 0

        total_downloaded = 0
        for channel_config in channels:
            try:
                downloaded = self.process_channel(channel_config, global_config)
                total_downloaded += downloaded
            except Exception as e:
                self.logger.error(
                    f"Failed to process channel {channel_config.get('name', 'unknown')}: {e}"
                )
                continue

        self.logger.info(f"Total videos downloaded: {total_downloaded}")
        return total_downloaded


if __name__ == "__main__":
    downloader = YouTubeDownloader()
    downloader.process_all_channels()
