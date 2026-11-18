#!/usr/bin/env python3

import argparse
import fcntl
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

# Add the src directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Handle both relative and absolute imports
try:
    from .download_queue import DownloadQueue, QueueWorker, execute_queue_job
    from .downloader import YouTubeDownloader
    from .health import (
        ChannelUnavailableError,
        collect_events,
        dispatch_events,
        load_notification_settings,
        record_refresh,
    )
    from .storage import (
        apply_channel_retention,
        channel_retention_days,
        free_disk_bytes,
        load_storage_settings,
        pins_for_deletion,
    )
    from .utils import cleanup_episodes, safe_component, validate_channel_settings
except ImportError:
    from download_queue import DownloadQueue, QueueWorker, execute_queue_job
    from downloader import YouTubeDownloader
    from health import (
        ChannelUnavailableError,
        collect_events,
        dispatch_events,
        load_notification_settings,
        record_refresh,
    )
    from storage import (
        apply_channel_retention,
        channel_retention_days,
        free_disk_bytes,
        load_storage_settings,
        pins_for_deletion,
    )
    from utils import cleanup_episodes, safe_component, validate_channel_settings


class AutomationRunner:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.config_path = self.base_dir / "appdata" / "config" / "channels.yaml"
        self.lock_file_path = self.config_path.parent / "media.lock"
        self.videos_dir = self.base_dir / "appdata" / "podcasts"

        # Setup logging
        self.setup_logging()

        # Initialize modules
        self.downloader = YouTubeDownloader(str(self.config_path), str(self.base_dir))

        # Drain downloads queued by the web server with the same store and
        # pipeline. The worker holds no extra lock here: run() already owns
        # the exclusive flock on media.lock while draining. getattr keeps
        # construction working when setup_logging is replaced in tests.
        self.queue = DownloadQueue(
            self.config_path.parent, logger=getattr(self, "logger", None)
        )
        self.queue_worker = QueueWorker(
            self.queue,
            executor=self._execute_queue_job,
            media_lock=None,
            logger=getattr(self, "logger", None),
        )

    def _execute_queue_job(
        self,
        job: dict[str, Any],
        cancel_check: Any,
        progress_callback: Any,
    ) -> None:
        """Run one queued download with the runner's paths and logging."""
        execute_queue_job(
            job,
            videos_dir=self.videos_dir,
            config_path=self.config_path,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
            logger=self.logger,
        )

    def drain_queue(self) -> int:
        """Process downloads the web server left queued, then report count."""
        recovered = self.queue.recover_interrupted()
        if recovered:
            self.logger.info(f"♻️  Resumed {recovered} interrupted download job(s)")
        drained = self.queue_worker.run_pending()
        if drained:
            self.logger.info(f"✅ Drained {drained} queued download(s)")
        return drained

    def setup_logging(self):
        """Setup comprehensive logging for the automation runner"""
        # Create logger
        self.logger = logging.getLogger("cron_runner")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicate logs
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Console handler only
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def acquire_lock(self) -> bool:
        """Acquire lock file to prevent concurrent runs"""
        try:
            self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
            self.lock_file = open(self.lock_file_path, "a+")
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_file.seek(0)
            self.lock_file.truncate()
            self.lock_file.write(f"{os.getpid()}\n")
            self.lock_file.flush()
            self.logger.info("Lock acquired successfully")
            return True
        except OSError as e:
            if hasattr(self, "lock_file") and not self.lock_file.closed:
                self.lock_file.close()
            self.logger.warning(f"Could not acquire lock: {e}")
            return False

    def release_lock(self):
        """Release the lock file"""
        try:
            if hasattr(self, "lock_file") and not self.lock_file.closed:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                # Keep the inode: unlinking a lock lets another process lock a
                # different file while an existing waiter holds the old inode.
                self.logger.info("Lock released successfully")
        except Exception as e:
            self.logger.error(f"Error releasing lock: {e}")

    def load_and_validate_config(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Load and validate configuration from channels.yaml"""
        try:
            if not self.config_path.exists():
                self.logger.error(f"Configuration file not found: {self.config_path}")
                return [], {}

            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            channels = config.get("channels", [])
            global_config = {k: v for k, v in config.items() if k != "channels"}

            if not channels:
                self.logger.warning("No channels configured")
                return [], global_config

            # Validate each channel configuration
            valid_channels = []
            for channel in channels:
                if self.validate_channel_config(channel):
                    valid_channels.append(channel)
                else:
                    self.logger.warning(f"Skipping invalid channel config: {channel}")

            self.logger.info(f"Loaded {len(valid_channels)} valid channel(s)")
            return valid_channels, global_config

        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return [], {}

    def validate_channel_config(self, channel: dict[str, Any]) -> bool:
        """Validate individual channel configuration"""
        if not isinstance(channel, dict):
            return False
        required_fields = ["name", "url", "max_episodes"]

        for field in required_fields:
            if field not in channel:
                self.logger.error(f"Missing required field '{field}' in channel config")
                return False

        try:
            validate_channel_settings(channel)
        except (ValueError, OverflowError):
            return False
        if not safe_component(channel["name"]):
            return False
        if not isinstance(channel["max_episodes"], int) or channel["max_episodes"] <= 0:
            self.logger.error("max_episodes must be a positive integer")
            return False

        if not isinstance(channel["url"], str) or not channel["url"].startswith(
            ("https://www.youtube.com/", "https://youtube.com/")
        ):
            self.logger.error(f"Invalid YouTube URL: {channel['url']}")
            return False

        return True

    def cleanup_old_videos(self, channel_name: str, max_episodes: int) -> None:
        """Remove completed media files exceeding the configured limit.

        Skipped (with an actionable error) while ``pins.json`` is unusable,
        so pinned episodes are never deleted by mistake.
        """
        pinned = pins_for_deletion(
            self.config_path.parent, channel_name, self.logger, "cleanup"
        )
        if pinned is None:
            return
        try:
            cleanup_episodes(self.videos_dir, channel_name, max_episodes, pinned=pinned)
        except Exception as error:
            self.logger.error(f"Error cleaning up {channel_name}: {error}")

    def enforce_channel_retention(
        self, channel: dict[str, Any], global_config: dict[str, Any]
    ) -> int:
        """Delete a channel's episodes past its effective retention window.

        Skipped (with an actionable error) while ``pins.json`` is unusable.
        Failures are logged and never fail the channel or the run.

        Args:
            channel: Validated channel configuration entry.
            global_config: Configuration keys outside ``channels``.

        Returns:
            The number of episodes deleted.
        """
        channel_name = str(channel["name"])
        try:
            if channel_retention_days(global_config, channel) <= 0:
                return 0
            pinned = pins_for_deletion(
                self.config_path.parent, channel_name, self.logger, "retention"
            )
            if pinned is None:
                return 0
            removed = apply_channel_retention(
                self.videos_dir,
                channel,
                global_config,
                pinned=pinned,
                logger=self.logger,
            )
        except Exception as error:
            self.logger.error(f"Retention failed for {channel_name}: {error}")
            return 0
        if removed:
            self.logger.info(
                f"🗑️  Retention removed {removed} old episode(s) from {channel_name}"
            )
        return removed

    def enforce_retention(
        self, channels: list[dict[str, Any]], global_config: dict[str, Any]
    ) -> int:
        """Apply retention to every channel, like the web server's maintenance.

        Retention is age-based and needs no YouTube access, so it also runs
        for channels whose discovery failed. The caller must hold the media
        lock.

        Args:
            channels: Validated channel configuration entries.
            global_config: Configuration keys outside ``channels``.

        Returns:
            The total number of episodes deleted.
        """
        self.logger.info("🗓️  Applying retention...")
        total = sum(
            self.enforce_channel_retention(channel, global_config)
            for channel in channels
        )
        if total:
            self.logger.info(f"✅ Retention removed {total} episode(s)")
        return total

    def process_channel(
        self, channel: dict[str, Any], global_config: dict[str, Any]
    ) -> bool:
        """Process a single channel: download videos and generate RSS feed"""
        channel_name = channel["name"]

        self.logger.info(
            f"\n🚀 ========== PROCESSING CHANNEL: {channel_name} =========="
        )

        started = time.monotonic()
        try:
            # Download new videos
            self.logger.info("📥 Starting video download phase...")
            downloaded_count = self.downloader.process_channel(channel, global_config)
            channel_error = getattr(self.downloader, "last_channel_error", None)
            if downloaded_count == 0 and channel_error:
                raise ChannelUnavailableError(
                    f"Could not fetch channel data: {channel_error}"
                )

            if downloaded_count > 0:
                self.logger.info(
                    f"✅ Download phase complete - {downloaded_count} new episodes downloaded"
                )
            else:
                self.logger.info("💭 No new episodes to download")

            # Cleanup old videos
            self.logger.info("🧹 Starting cleanup phase...")
            self.cleanup_old_videos(channel_name, channel["max_episodes"])
            self.logger.info("✅ Cleanup phase complete")

            # RSS feeds are now generated dynamically when requested
            self.logger.info("🎉 Channel processing successful!")

            record_refresh(
                self.config_path.parent,
                str(channel_name),
                success=True,
                queued=downloaded_count,
                duration_seconds=time.monotonic() - started,
            )
            return True

        except ChannelUnavailableError as e:
            self.logger.error(f"❌ Channel unavailable {channel_name}: {e}")
            record_refresh(
                self.config_path.parent,
                str(channel_name),
                success=False,
                error=str(e),
                unavailable=True,
                duration_seconds=time.monotonic() - started,
            )
            return False
        except Exception as e:
            self.logger.error(f"❌ Error processing channel {channel_name}: {e}")
            record_refresh(
                self.config_path.parent,
                str(channel_name),
                success=False,
                error=str(e),
                duration_seconds=time.monotonic() - started,
            )
            return False

    def notify_health(self, global_config: dict[str, Any]) -> None:
        """Deliver due health notifications after a cron pass.

        Cron deployments share ``health.json`` with the web server, so
        repeated failures and low disk space alert through the same webhook
        even when the web UI is not running.
        """
        try:
            settings = load_notification_settings(global_config)
            webhook_url = settings.get("webhook_url") or ""
            storage_settings = load_storage_settings(global_config)
            try:
                free_bytes: int | None = free_disk_bytes(self.videos_dir)
            except OSError as error:
                self.logger.warning(f"Could not check free disk space: {error}")
                free_bytes = None
            events = collect_events(
                self.config_path.parent,
                settings,
                free_bytes=free_bytes,
                min_free_space_gb=storage_settings.get("min_free_space_gb", 0),
            )
            if not events:
                return
            if not webhook_url:
                self.logger.warning(
                    f"{len(events)} health issue(s) detected; no webhook configured"
                )
                return
            self.logger.info(f"Dispatching {len(events)} health notification(s)")
            dispatch_events(self.config_path.parent, events, webhook_url)
        except Exception as error:
            self.logger.error(f"Health notification dispatch failed: {error}")

    def run(self) -> int:
        """Main automation runner method"""
        start_time = time.time()

        self.logger.info("\n🚀 ========== YT2RSS AUTOMATION STARTING ==========\n")
        self.logger.info(
            f"🕰️  Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # Acquire lock
        if not self.acquire_lock():
            self.logger.error("❌ Another instance is already running, exiting")
            return 1

        notify_config: dict[str, Any] | None = None
        try:
            # Load and validate configuration
            self.logger.info("📄 Loading configuration...")
            channels, global_config = self.load_and_validate_config()
            notify_config = global_config
            if not channels:
                self.logger.error("❌ No valid channels to process")
                return 1

            self.logger.info("✅ Configuration loaded successfully")
            self.logger.info(f"📻 Found {len(channels)} channel(s) to process\n")

            # Finish downloads the web server queued before its restart or
            # between cron runs; we hold the media lock so nothing races.
            self.drain_queue()

            # Process each channel
            processed_count = 0
            error_count = 0

            for i, channel in enumerate(channels, 1):
                try:
                    self.logger.info(
                        f"\n[{i}/{len(channels)}] Starting channel processing..."
                    )
                    if self.process_channel(channel, global_config):
                        processed_count += 1
                        self.logger.info(
                            f"[{i}/{len(channels)}] ✅ Channel processed successfully"
                        )
                    else:
                        error_count += 1
                        self.logger.error(
                            f"[{i}/{len(channels)}] ❌ Channel processing failed"
                        )
                except Exception as e:
                    self.logger.error(
                        f"[{i}/{len(channels)}] ❌ Unexpected error processing channel {channel.get('name', 'unknown')}: {e}"
                    )
                    error_count += 1

            # Still under the media lock; independent of discovery results.
            self.enforce_retention(channels, global_config)

            # Summary
            elapsed_time = time.time() - start_time

            self.logger.info("\n\n🏁 ========== AUTOMATION COMPLETE ==========\n")
            self.logger.info(f"🕰️  Total time: {elapsed_time:.2f} seconds")
            self.logger.info(f"✅ Successful: {processed_count} channels")
            self.logger.info(f"❌ Errors: {error_count} channels")

            if error_count == 0:
                self.logger.info("🎉 All channels processed successfully!")
            else:
                self.logger.warning(
                    "⚠️  Some channels had errors - check logs for details"
                )

            return 0 if error_count == 0 else 1

        except Exception as e:
            self.logger.error(f"\n🔥 FATAL ERROR in automation runner: {e}")
            return 1
        finally:
            self.release_lock()
            # Deliver notifications after releasing the media lock so a
            # slow webhook never blocks downloads or refreshes.
            if notify_config is not None:
                self.notify_health(notify_config)


def main():
    """Main entry point for the automation script"""
    parser = argparse.ArgumentParser(description="YouTube to Podcast automation runner")
    parser.add_argument(
        "--base-dir",
        default=".",
        help="Base directory for the project (default: current directory)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run automation
    runner = AutomationRunner(args.base_dir)
    exit_code = runner.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
