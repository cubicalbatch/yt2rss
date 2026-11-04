import atexit
import json
import logging
import mimetypes
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, urlsplit

import yaml
import yt_dlp
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.wrappers import Response as WerkzeugResponse
from yt_dlp.utils import DownloadError

try:
    from .auth import (
        LoginThrottler,
        constant_time_equals,
        generate_csrf_token,
        generate_feed_token,
        is_safe_relative_path,
        load_or_create_session_secret,
    )
    from .download_queue import (
        ACTIVE_STATES,
        JOB_STATES,
        TERMINAL_STATES,
        DownloadQueue,
        QueueWorker,
        execute_queue_job,
    )
    from .rss_generator import RSSGenerator
    from .storage import (
        GB,
        admission_error,
        apply_retention,
        effective_channel_storage,
        estimate_download_bytes,
        free_disk_bytes,
        is_positive_number,
        load_pins,
        load_storage_settings,
        load_tombstones,
        measure_media_usage,
        pinned_ids,
        record_tombstone,
        remove_channel_pins,
        remove_channel_tombstones,
        remove_episode_pin,
        remove_episode_tombstone,
        set_episode_pinned,
        tombstoned_ids,
        validate_storage_settings,
    )
    from .utils import (
        MEDIA_EXTENSIONS,
        MediaLock,
        atomic_write_text,
        cleanup_episodes,
        contained_path,
        ensure_directory,
        filter_skip_reason,
        load_episode_filters,
        safe_component,
        sanitize_channel_name,
        setup_logging,
        validate_channel_settings,
    )
except ImportError:
    from auth import (
        LoginThrottler,
        constant_time_equals,
        generate_csrf_token,
        generate_feed_token,
        is_safe_relative_path,
        load_or_create_session_secret,
    )
    from download_queue import (
        ACTIVE_STATES,
        JOB_STATES,
        TERMINAL_STATES,
        DownloadQueue,
        QueueWorker,
        execute_queue_job,
    )
    from rss_generator import RSSGenerator
    from storage import (
        GB,
        admission_error,
        apply_retention,
        effective_channel_storage,
        estimate_download_bytes,
        free_disk_bytes,
        is_positive_number,
        load_pins,
        load_storage_settings,
        load_tombstones,
        measure_media_usage,
        pinned_ids,
        record_tombstone,
        remove_channel_pins,
        remove_channel_tombstones,
        remove_episode_pin,
        remove_episode_tombstone,
        set_episode_pinned,
        tombstoned_ids,
        validate_storage_settings,
    )
    from utils import (
        MEDIA_EXTENSIONS,
        MediaLock,
        atomic_write_text,
        cleanup_episodes,
        contained_path,
        ensure_directory,
        filter_skip_reason,
        load_episode_filters,
        safe_component,
        sanitize_channel_name,
        setup_logging,
        validate_channel_settings,
    )


def config_transaction(method: Callable[..., Any]) -> Callable[..., Any]:
    """Serialize configuration read-modify-write operations in this server."""

    @wraps(method)
    def wrapped(self: "YouTubePodcastServer", *args: Any, **kwargs: Any) -> Any:
        with self._config_lock:
            return method(self, *args, **kwargs)

    return wrapped


def exclusive_media_operation(method: Callable[..., Any]) -> Callable[..., Any]:
    """Prevent destructive media operations from racing with downloads."""

    @wraps(method)
    def wrapped(self: "YouTubePodcastServer", *args: Any, **kwargs: Any) -> Any:
        if not self._media_lock.acquire(blocking=False):
            return jsonify({"error": "A media operation is already in progress"}), 409
        try:
            return method(self, *args, **kwargs)
        finally:
            self._media_lock.release()

    return wrapped


class UILogHandler(logging.Handler):
    """Custom logging handler to capture downloader logs for UI display."""

    def __init__(self, server_instance):
        super().__init__()
        self.server = server_instance

    def emit(self, record):
        """Emit a log record to the server's log storage."""
        try:
            # Capture INFO level messages and above
            if record.levelno >= logging.INFO:
                # Only add the message part, not the timestamp and level
                message_only = record.getMessage()
                self.server._add_log_message(message_only)
        except Exception:
            pass  # Avoid infinite recursion on logging errors


class YouTubePodcastServer:
    """Flask-based web server for serving RSS feeds and media files."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5000,
        videos_dir: str = "appdata/podcasts",
        config_dir: str = "appdata/config",
    ):
        self.host = host
        self.port = port
        # Use absolute paths to avoid Flask app instance path issues
        self.videos_dir = Path(videos_dir).resolve()
        self.config_dir = Path(config_dir).resolve()
        self.logger = setup_logging()

        self._config_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._media_lock = MediaLock(self.config_dir / "media.lock")

        # Refresh state tracking
        self.refresh_status = {"running": False, "start_time": None, "duration": 0}

        # Log storage for real-time display (keep last 100 messages)
        self.recent_logs = []
        self.max_logs = 100

        # Initialize scheduler with single thread executor
        executors = {
            "default": ThreadPoolExecutor(1)  # Only one thread for sequential execution
        }
        self.scheduler = BackgroundScheduler(executors=executors)
        self.scheduler.start()
        atexit.register(self.close)

        # Durable download queue drained by a single worker thread. Jobs
        # survive restarts; a shared media lock keeps the worker from
        # racing refreshes, destructive actions, and the cron runner.
        self.queue = DownloadQueue(self.config_dir, logger=self.logger)
        self.queue_worker = QueueWorker(
            self.queue,
            executor=self._execute_queue_job,
            media_lock=self._media_lock,
            logger=self.logger,
        )
        self.queue_worker.start()

        # Start automatic refresh schedule
        self._setup_scheduler()

        # Initialize Flask app with templates and static folders
        self.app = Flask(__name__, template_folder="templates", static_folder="static")
        self.app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
        self._setup_auth()
        self._setup_custom_filters()
        self._setup_routes()

        # Enable MIME type for MP4 files
        mimetypes.add_type("video/mp4", ".mp4")

    def close(self) -> None:
        """Stop the scheduler once, including during interpreter shutdown."""
        if self.scheduler.running:
            self.scheduler.shutdown()
        # Non-blocking: an in-flight job is requeued as pending so it
        # resumes after the next start.
        self.queue_worker.stop()

    def _execute_queue_job(
        self,
        job: dict[str, Any],
        cancel_check: Callable[[], bool],
        progress_callback: Callable[[dict[str, Any]], None],
    ) -> None:
        """Run one queued download with UI logging wired in."""
        execute_queue_job(
            job,
            videos_dir=self.videos_dir,
            config_path=self.config_dir / "channels.yaml",
            cancel_check=cancel_check,
            progress_callback=progress_callback,
            log_handler=UILogHandler(self),
            logger=self.logger,
        )

    def _setup_auth(self) -> None:
        """Configure optional password authentication from the environment.

        Authentication is enabled only when ``YT2RSS_PASSWORD`` is set. The
        Flask session cookie is signed with ``YT2RSS_SECRET_KEY`` when provided,
        otherwise a key is generated once and persisted in the config directory.
        """
        self.auth_enabled = bool(os.getenv("YT2RSS_PASSWORD"))
        self.auth_username = os.getenv("YT2RSS_USERNAME", "admin")
        self.auth_password = os.getenv("YT2RSS_PASSWORD")
        self._login_throttler = LoginThrottler()

        base_url = os.getenv("BASE_URL", "")
        self.app.config.update(
            PERMANENT_SESSION_LIFETIME=timedelta(days=7),
            SESSION_COOKIE_HTTPONLY=True,
            SESSION_COOKIE_SAMESITE="Lax",
            SESSION_COOKIE_SECURE=base_url.startswith("https://"),
        )

        if not self.auth_enabled:
            return

        secret_key = os.getenv("YT2RSS_SECRET_KEY")
        if secret_key:
            self.app.secret_key = secret_key
        else:
            ensure_directory(str(self.config_dir))
            self.app.secret_key = load_or_create_session_secret(self.config_dir)
        self.logger.info("🔒 Password authentication enabled")

    def _has_authenticated_session(self) -> bool:
        """Check the request carries a logged-in session."""
        return bool(session.get("authenticated"))

    def _redirect_to_login(self) -> WerkzeugResponse:
        """Redirect an anonymous page request to the login form."""
        target = request.full_path if request.query_string else request.path
        return redirect(url_for("login", next=target))

    def _safe_next_target(self, target: str | None) -> str:
        """Resolve a post-login redirect target, rejecting off-site paths."""
        if is_safe_relative_path(target):
            return target
        return url_for("index")

    def _stored_feed_token(self, channel_name: str) -> str | None:
        """Read a channel's feed token without generating one."""
        config = self._load_channel_config()
        for channel in config.get("channels", []):
            if channel["name"] == channel_name:
                token = channel.get("feed_token")
                return str(token) if token else None
        return None

    def _request_feed_token_valid(self, channel_name: str) -> bool:
        """Check the request token against the channel's stored feed token."""
        provided = request.args.get("token", "")
        if not provided:
            return False
        expected = self._stored_feed_token(channel_name)
        return bool(expected) and constant_time_equals(provided, expected)

    @config_transaction
    def _get_or_create_feed_token(self, channel_name: str) -> str | None:
        """Return a channel's feed token, generating and persisting it on demand.

        Args:
            channel_name: Sanitized channel identifier.

        Returns:
            The channel's stable feed token, or None when the channel is
            unknown or the configuration could not be saved.
        """
        config = self._load_channel_config()
        for channel in config.get("channels", []):
            if channel["name"] == channel_name:
                token = channel.get("feed_token")
                if not token:
                    token = generate_feed_token()
                    channel["feed_token"] = token
                    if not self._save_channel_config(config):
                        return None
                return str(token)
        return None

    @config_transaction
    def _rotate_feed_token(self, channel_name: str) -> str | None:
        """Replace a channel's feed token, invalidating previous URLs.

        Args:
            channel_name: Sanitized channel identifier.

        Returns:
            The new feed token, or None when the channel is unknown or the
            configuration could not be saved.
        """
        config = self._load_channel_config()
        for channel in config.get("channels", []):
            if channel["name"] == channel_name:
                token = generate_feed_token()
                channel["feed_token"] = token
                if not self._save_channel_config(config):
                    return None
                return token
        return None

    def _serve_login_page(self, error: str | None, next_target: str) -> str:
        """Render the login form with an optional error message."""
        return render_template("login.html", error=error, next=next_target or "")

    def _handle_login(self) -> WerkzeugResponse | str | tuple[str, int]:
        """Show the login page and authenticate submitted credentials."""
        if not self.auth_enabled:
            return redirect(url_for("index"))

        next_target = request.values.get("next", "")
        if request.method == "GET":
            if self._has_authenticated_session():
                return redirect(self._safe_next_target(next_target))
            return self._serve_login_page(None, next_target)

        address = request.remote_addr or "unknown"
        if self._login_throttler.is_blocked(address):
            self.logger.warning(f"Login throttled for {address}")
            return self._serve_login_page(
                "Too many failed attempts. Try again later.", next_target
            ), 429

        username = request.form.get("username", "")
        password = request.form.get("password", "")
        username_ok = constant_time_equals(username, self.auth_username)
        password_ok = constant_time_equals(password, self.auth_password or "")
        if not (username_ok and password_ok):
            self._login_throttler.record_failure(address)
            return self._serve_login_page(
                "Invalid username or password.", next_target
            ), 401

        self._login_throttler.record_success(address)
        session.clear()
        session["authenticated"] = True
        session["username"] = self.auth_username
        session["csrf_token"] = generate_csrf_token()
        session.permanent = True
        return redirect(self._safe_next_target(next_target))

    def _handle_logout(self) -> WerkzeugResponse:
        """Clear the login session after validating the CSRF token."""
        if not self.auth_enabled:
            return redirect(url_for("index"))

        token = request.form.get("csrf_token", "")
        expected = session.get("csrf_token")
        if not (isinstance(expected, str) and constant_time_equals(token, expected)):
            abort(400)

        session.clear()
        return redirect(url_for("login"))

    def _get_proxy_opts(self) -> dict:
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

    def _add_log_message(self, message: str):
        """Add a log message to the recent logs list for UI display."""
        timestamp = datetime.now().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "message": message,
        }

        # Add to recent logs
        self.recent_logs.append(log_entry)

        # Keep only the most recent messages
        if len(self.recent_logs) > self.max_logs:
            self.recent_logs = self.recent_logs[-self.max_logs :]

    def _clear_logs(self):
        """Clear the recent logs."""
        self.recent_logs = []

    def _load_refresh_timestamps(self):
        """Load refresh timestamps from file"""
        timestamps_file = self.config_dir / "refresh_timestamps.json"
        if not timestamps_file.exists():
            return {}
        try:
            with open(timestamps_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.error(f"Error loading refresh timestamps: {e}")
            return {}

    def _format_timestamp(self, iso_timestamp: str) -> str:
        """Format ISO timestamp for display - returns raw ISO for client-side formatting"""
        try:
            # Validate the timestamp and return it as-is for client-side processing
            datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            return iso_timestamp
        except (ValueError, AttributeError):
            return ""

    def _channel_exists(self, channel_name: str) -> bool:
        """Check the configured channels contain ``channel_name``."""
        config = self._load_channel_config()
        return any(ch["name"] == channel_name for ch in config.get("channels", []))

    def _visible_episodes(
        self, channel_name: str, channel_config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Episodes shown to users for a channel (feed, API and counts).

        Applies the channel's episode filters and its tombstones to the
        episodes stored on disk; files are never deleted here. Downloaded
        metadata keeps yt-dlp's live markers, so ``skip_livestreams`` also
        applies to stored episodes; episodes downloaded before those
        markers were recorded stay visible.

        Args:
            channel_name: Sanitized channel identifier.
            channel_config: The channel's configuration entry.

        Returns:
            The episode metadata dicts that pass every filter.
        """
        generator = RSSGenerator()
        episodes = generator.scan_channel_videos(self.videos_dir / channel_name)
        filters = load_episode_filters(channel_config)
        deleted = tombstoned_ids(self.config_dir, channel_name)
        visible = []
        for episode in episodes:
            if str(episode.get("id") or "") in deleted:
                continue
            if filter_skip_reason(episode, filters) is not None:
                continue
            visible.append(episode)
        return visible

    def _load_channel_config(self):
        """Load channel configuration from YAML file."""
        config_file = self.config_dir / "channels.yaml"
        if not config_file.exists():
            return {"channels": [], "default_interval_hours": 24}

        try:
            with open(config_file, "r") as f:
                config = yaml.safe_load(f) or {}
                if not isinstance(config, dict) or not isinstance(
                    config.get("channels", []), list
                ):
                    raise ValueError(
                        "Configuration must be a mapping with a channels list"
                    )
                for channel in config.get("channels", []):
                    if not isinstance(channel, dict) or not safe_component(
                        channel.get("name")
                    ):
                        raise ValueError(
                            "Each channel must have a safe filesystem name"
                        )
                    contained_path(self.videos_dir, channel["name"])
                # Handle backwards compatibility: rename refresh_interval_hours to default_interval_hours
                if (
                    "refresh_interval_hours" in config
                    and "default_interval_hours" not in config
                ):
                    config["default_interval_hours"] = config["refresh_interval_hours"]
                    del config["refresh_interval_hours"]
                # Ensure defaults
                if "channels" not in config:
                    config["channels"] = []
                if "default_interval_hours" not in config:
                    config["default_interval_hours"] = 24
                return config
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            raise ValueError(
                "Cannot read channels.yaml; repair it before making changes"
            ) from e

    def _setup_scheduler(self):
        """Setup per-channel automatic refresh scheduler based on config."""
        try:
            config = self._load_channel_config()
            channels = config.get("channels", [])
            default_interval = config.get("default_interval_hours", 24)

            # Remove all existing channel jobs
            for job in self.scheduler.get_jobs():
                if job.id.startswith("channel_refresh_"):
                    self.scheduler.remove_job(job.id)

            # Add per-channel jobs
            for channel in channels:
                channel_name = channel.get("name")
                if not channel_name:
                    continue

                # Get channel-specific interval or use default
                interval_hours = channel.get("refresh_interval_hours", default_interval)

                job_id = f"channel_refresh_{channel_name}"

                # Add job for this channel
                self.scheduler.add_job(
                    func=self._scheduled_channel_refresh,
                    args=[channel_name],
                    trigger=IntervalTrigger(hours=interval_hours),
                    id=job_id,
                    name=f"Auto Refresh: {channel.get('display_name', channel_name)}",
                    replace_existing=True,
                    misfire_grace_time=3600,  # Allow jobs to run up to 1 hour late
                    coalesce=True,  # Collapse multiple missed executions into one
                    max_instances=1,  # Only one instance of each job at a time
                )

                self.logger.info(
                    f"Scheduled refresh for '{channel_name}' every {interval_hours} hours"
                )

            # Storage maintenance applies retention cleanup once at startup
            # and then once per day.
            self.scheduler.add_job(
                func=self._run_storage_maintenance,
                trigger=IntervalTrigger(days=1),
                id="storage_maintenance",
                name="Storage maintenance (retention)",
                replace_existing=True,
                misfire_grace_time=3600,
                coalesce=True,
                max_instances=1,
                next_run_time=datetime.now(),
            )

        except Exception as e:
            self.logger.error(f"Error setting up scheduler: {e}")

    @config_transaction
    def _restart_scheduler(self, new_default_interval_hours: float):
        """Restart scheduler with new default interval."""
        try:
            # Update config with new default interval
            config = self._load_channel_config()
            config["default_interval_hours"] = new_default_interval_hours

            # Save config first
            if not self._save_channel_config(config):
                return False

            # Restart all channel schedules
            self._setup_scheduler()

            self.logger.info(
                f"Rescheduled all channels with new default interval: {new_default_interval_hours} hours"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error restarting scheduler: {e}")
            return False

    def _save_channel_config(self, config):
        """Save channel configuration to YAML file."""
        # Ensure config directory exists
        ensure_directory(str(self.config_dir))

        config_file = self.config_dir / "channels.yaml"
        try:
            atomic_write_text(
                config_file,
                yaml.safe_dump(config, default_flow_style=False, sort_keys=False),
            )
            return True
        except Exception as e:
            self.logger.error(f"Error saving config: {e}")
            return False

    def _setup_custom_filters(self):
        """Setup custom Jinja2 filters."""

        def format_youtube_url(url: str) -> str:
            """Replace YouTube URL display with YouTube logo emoji while keeping full URL."""
            if not url:
                return url

            # Replace common YouTube URL patterns with emoji
            if url.startswith("https://www.youtube.com/"):
                # Extract the part after youtube.com/
                path_part = url.replace("https://www.youtube.com/", "")
                return f"📺/{path_part}"
            elif url.startswith("https://youtube.com/"):
                # Extract the part after youtube.com/
                path_part = url.replace("https://youtube.com/", "")
                return f"📺/{path_part}"

            return url

        # Register the filter with Jinja2
        self.app.jinja_env.filters["format_youtube_url"] = format_youtube_url

    def _setup_routes(self):
        """Setup Flask routes."""

        @self.app.before_request
        def validate_request() -> tuple[Response, int] | None:
            """Reject unsafe paths and browser requests from another origin."""
            # Flask's static handler enforces its own containment rules.
            if request.endpoint == "static":
                return None
            arguments = request.view_args or {}
            try:
                for field in ("channel_name", "episode_id", "filename"):
                    if field in arguments and not safe_component(arguments[field]):
                        abort(404)
                if "channel_name" in arguments:
                    channel_dir = contained_path(
                        self.videos_dir, arguments["channel_name"]
                    )
                    # Check existing descendants as well as the channel directory.
                    if "episode_id" in arguments:
                        episode_id = arguments["episode_id"]
                        for ext in (*MEDIA_EXTENSIONS, ".json"):
                            contained_path(channel_dir, f"{episode_id}{ext}")
                        contained_path(channel_dir, "thumbnails")
                    if "filename" in arguments:
                        parts = [arguments["filename"]]
                        if request.endpoint == "serve_thumbnail":
                            parts.insert(0, "thumbnails")
                        contained_path(channel_dir, *parts)
            except ValueError:
                abort(404)

            if request.path.startswith("/api/") and request.method not in (
                "GET",
                "HEAD",
                "OPTIONS",
            ):
                origin = request.headers.get("Origin")
                allowed_origins = {request.host_url.rstrip("/")}
                configured_url = os.getenv("BASE_URL")
                if configured_url:
                    parsed = urlsplit(configured_url)
                    allowed_origins.add(f"{parsed.scheme}://{parsed.netloc}")
                if (origin and origin not in allowed_origins) or request.headers.get(
                    "Sec-Fetch-Site"
                ) in ("cross-site", "same-site"):
                    return jsonify(
                        {"error": "Cross-origin API changes are not allowed"}
                    ), 403
                if request.endpoint in (
                    "add_channel",
                    "update_channel",
                    "update_refresh_interval",
                    "update_storage_config",
                ):
                    data = request.get_json(silent=True)
                    if not isinstance(data, dict) or not data:
                        return jsonify(
                            {"error": "No data provided; expected a JSON object"}
                        ), 400
                    try:
                        validate_channel_settings(data)
                    except (ValueError, OverflowError) as error:
                        return jsonify({"error": str(error)}), 400

        @self.app.before_request
        def authenticate_request() -> (
            WerkzeugResponse | tuple[WerkzeugResponse | str, int] | None
        ):
            """Enforce optional password auth and per-channel feed tokens."""
            if not self.auth_enabled:
                return None
            if request.endpoint in ("static", "login", "logout") or (
                request.endpoint is None
            ):
                return None

            if self._has_authenticated_session():
                return None

            channel_name = (request.view_args or {}).get("channel_name")
            if request.endpoint == "index":
                return self._redirect_to_login()
            if request.endpoint == "serve_feed":
                # Unknown channels fail the same way as bad tokens (404).
                if not (channel_name and self._request_feed_token_valid(channel_name)):
                    abort(404)
                return None
            if request.endpoint in ("serve_video", "serve_thumbnail"):
                if not (channel_name and self._request_feed_token_valid(channel_name)):
                    return jsonify({"error": "Authentication required"}), 401
                return None

            # Everything else (management API) requires a login session.
            return jsonify({"error": "Authentication required"}), 401

        @self.app.route("/")
        def index():
            """Index page listing available feeds."""
            return self._serve_index()

        @self.app.route("/feeds/<channel_name>")
        def serve_feed(channel_name: str):
            """Serve RSS feed for a channel."""
            return self._serve_rss_feed(channel_name)

        @self.app.route("/podcasts/<channel_name>/<filename>")
        def serve_video(channel_name: str, filename: str):
            """Serve video file with range request support."""
            return self._serve_video_file(channel_name, filename)

        @self.app.route("/thumbnails/<channel_name>/<filename>")
        def serve_thumbnail(channel_name: str, filename: str):
            """Serve thumbnail image."""
            return self._serve_thumbnail_file(channel_name, filename)

        # API Routes for config management
        @self.app.route("/api/channels", methods=["GET"])
        def get_channels():
            """Get all channel configurations."""
            return jsonify(self._load_channel_config())

        @self.app.route("/api/channels", methods=["POST"])
        def add_channel():
            """Add a new channel configuration."""
            return self._handle_add_channel()

        @self.app.route("/api/channels/<channel_name>", methods=["PUT"])
        def update_channel(channel_name: str):
            """Update an existing channel configuration."""
            return self._handle_update_channel(channel_name)

        @self.app.route("/api/channels/<channel_name>", methods=["DELETE"])
        def delete_channel(channel_name: str):
            """Delete a channel configuration."""
            return self._handle_delete_channel(channel_name)

        @self.app.route("/api/refresh", methods=["POST"])
        def trigger_refresh():
            """Trigger the automation script refresh."""
            return self._handle_refresh_trigger()

        @self.app.route("/api/refresh/status", methods=["GET"])
        def refresh_status():
            """Get current refresh status."""
            return self._handle_refresh_status()

        @self.app.route("/api/queue", methods=["GET"])
        def get_queue():
            """List download queue jobs with summary counts."""
            return self._handle_get_queue()

        @self.app.route("/api/queue/<job_id>", methods=["GET"])
        def get_queue_job(job_id: str):
            """Return a single download queue job."""
            return self._handle_get_queue_job(job_id)

        @self.app.route("/api/queue/<job_id>/cancel", methods=["POST"])
        def cancel_queue_job(job_id: str):
            """Cancel a pending job or request cancellation of a running one."""
            return self._handle_queue_cancel(job_id)

        @self.app.route("/api/queue/<job_id>/retry", methods=["POST"])
        def retry_queue_job(job_id: str):
            """Requeue a failed or cancelled download job."""
            return self._handle_queue_retry(job_id)

        @self.app.route("/api/queue/<job_id>/dismiss", methods=["POST"])
        def dismiss_queue_job(job_id: str):
            """Remove a terminal download job from the queue."""
            return self._handle_queue_dismiss(job_id)

        @self.app.route("/api/queue/clear-completed", methods=["POST"])
        def clear_completed_queue_jobs():
            """Remove every completed download job."""
            return self._handle_queue_clear_completed()

        @self.app.route("/api/config/refresh-interval", methods=["GET"])
        def get_refresh_interval():
            """Get current refresh interval."""
            return self._handle_get_refresh_interval()

        @self.app.route("/api/config/refresh-interval", methods=["PUT"])
        def update_refresh_interval():
            """Update refresh interval."""
            return self._handle_update_refresh_interval()

        @self.app.route("/api/storage", methods=["GET"])
        def get_storage():
            """Report media usage, quotas, retention and free disk space."""
            return self._handle_get_storage()

        @self.app.route("/api/config/storage", methods=["GET"])
        def get_storage_config():
            """Get global storage settings."""
            return self._handle_get_storage_config()

        @self.app.route("/api/config/storage", methods=["PUT"])
        def update_storage_config():
            """Update global storage settings."""
            return self._handle_update_storage_config()

        @self.app.route("/api/channels/<channel_name>/purge", methods=["POST"])
        def purge_channel_episodes(channel_name: str):
            """Purge downloaded episodes for a channel and regenerate RSS."""
            return self._handle_purge_episodes(channel_name)

        @self.app.route("/api/channels/<channel_name>/tombstones", methods=["GET"])
        def get_channel_tombstones(channel_name: str):
            """List the episodes remembered as deleted for a channel."""
            return self._handle_get_tombstones(channel_name)

        @self.app.route("/api/channels/<channel_name>/tombstones", methods=["DELETE"])
        def clear_channel_tombstones(channel_name: str):
            """Forget every remembered deletion for a channel."""
            return self._handle_clear_tombstones(channel_name)

        @self.app.route(
            "/api/channels/<channel_name>/tombstones/<episode_id>", methods=["DELETE"]
        )
        def forget_channel_tombstone(channel_name: str, episode_id: str):
            """Forget one remembered deletion."""
            return self._handle_forget_tombstone(channel_name, episode_id)

        @self.app.route("/api/channels/<channel_name>/episodes", methods=["GET"])
        def get_channel_episodes(channel_name: str):
            """Get list of downloaded episodes for a channel."""
            return self._handle_get_episodes(channel_name)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/download-mp3",
            methods=["POST"],
        )
        def download_episode_as_mp3(channel_name: str, episode_id: str):
            """Convert and download episode as MP3."""
            return self._handle_mp3_download(channel_name, episode_id)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/download-original",
            methods=["POST"],
        )
        def download_episode_original(channel_name: str, episode_id: str):
            """Download episode in original format with clean filename."""
            return self._handle_original_download(channel_name, episode_id)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/delete",
            methods=["DELETE"],
        )
        def delete_single_episode(channel_name: str, episode_id: str):
            """Delete a single episode."""
            return self._handle_delete_episode(channel_name, episode_id)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/redownload",
            methods=["POST"],
        )
        def redownload_single_episode(channel_name: str, episode_id: str):
            """Redownload a single episode."""
            return self._handle_redownload_episode(channel_name, episode_id)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/pin",
            methods=["POST"],
        )
        def pin_episode(channel_name: str, episode_id: str):
            """Pin a single episode against automatic cleanup."""
            return self._handle_pin_episode(channel_name, episode_id, True)

        @self.app.route(
            "/api/channels/<channel_name>/episodes/<episode_id>/pin",
            methods=["DELETE"],
        )
        def unpin_episode(channel_name: str, episode_id: str):
            """Remove an episode's pin."""
            return self._handle_pin_episode(channel_name, episode_id, False)

        @self.app.route("/api/channels/<channel_name>/refresh", methods=["POST"])
        def refresh_single_channel(channel_name: str):
            """Trigger refresh for a single channel."""
            return self._handle_single_channel_refresh(channel_name)

        @self.app.route("/api/channels/<channel_name>/rotate-token", methods=["POST"])
        def rotate_feed_token(channel_name: str):
            """Rotate a channel's private feed token."""
            return self._handle_rotate_feed_token(channel_name)

        @self.app.route("/login", methods=["GET", "POST"])
        def login():
            """Show the login form and process password logins."""
            return self._handle_login()

        @self.app.route("/logout", methods=["POST"])
        def logout():
            """Clear the login session (CSRF-protected POST)."""
            return self._handle_logout()

        @self.app.after_request
        def after_request(response):
            """Allow public feed playback while keeping management same-origin."""
            if not request.path.startswith("/api/"):
                response.headers["Access-Control-Allow-Origin"] = "*"
                response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
                response.headers["Access-Control-Allow-Headers"] = "Range"
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
            return response

    def _begin_refresh(self) -> bool:
        """Reserve refresh state before a worker starts."""
        with self._state_lock:
            if self.refresh_status["running"] or not self._media_lock.acquire(
                blocking=False
            ):
                return False
            self.refresh_status.update(running=True, start_time=time.time(), duration=0)
            self._clear_logs()
            return True

    def _finish_refresh(self) -> None:
        """Release the refresh reservation even after a worker fails."""
        with self._state_lock:
            start = self.refresh_status["start_time"]
            self.refresh_status.update(
                running=False, duration=time.time() - start if start else 0
            )
            self._media_lock.release()

    def _scheduled_channel_refresh(self, channel_name: str) -> None:
        """Use the same admission control for scheduled and manual refreshes."""
        if self._begin_refresh():
            self._run_single_channel_refresh(channel_name)

    def _run_refresh(self, channel_name: str | None = None) -> None:
        """Refresh selected channels by queueing their missing downloads.

        Discovery keeps the existing admission control (it runs while the
        refresh reservation holds the shared media lock) but enqueues new
        videos into the durable download queue instead of downloading
        inline; the queue worker performs the actual downloads afterwards.
        """
        handler = UILogHandler(self)
        downloader = None
        try:
            try:
                from .downloader import YouTubeDownloader
            except ImportError:
                from downloader import YouTubeDownloader
            config = self._load_channel_config()
            channels = config.get("channels", [])
            if channel_name is not None:
                channels = [
                    channel for channel in channels if channel["name"] == channel_name
                ]
            global_config = {
                key: value for key, value in config.items() if key != "channels"
            }
            downloader = YouTubeDownloader(
                str(self.config_dir / "channels.yaml"), videos_dir=str(self.videos_dir)
            )
            downloader.logger.addHandler(handler)
            for channel in channels:
                try:
                    self._discover_channel_jobs(downloader, channel, global_config)
                    self._cleanup_old_episodes(channel["name"], channel["max_episodes"])
                    self._apply_channel_retention(channel, global_config)
                    self._add_log_message(f"Refresh completed for {channel['name']}")
                except Exception as error:
                    self.logger.error(f"Refresh failed for {channel['name']}: {error}")
                    self._add_log_message(
                        f"Refresh failed for {channel['name']}: {error}"
                    )
        except Exception as error:
            self.logger.error(f"Refresh failed: {error}")
            self._add_log_message(f"Refresh failed: {error}")
        finally:
            if downloader is not None:
                downloader.logger.removeHandler(handler)
            self._finish_refresh()

    def _discover_channel_jobs(
        self,
        downloader: Any,
        channel: dict[str, Any],
        global_config: dict[str, Any],
    ) -> int:
        """Enqueue queue jobs for a channel's new downloadable videos.

        Mirrors the checks ``process_channel`` used to apply inline: skip
        already downloaded videos, fetch per-video metadata, and respect
        ``download_delay_hours`` plus ``check_delay_seconds``. Videos that
        would exceed the storage quota or the free-space minimum are not
        queued; the block is logged as a warning instead. Videos excluded
        by the channel's episode filters (title rules, duration bounds,
        Shorts, livestreams) or remembered as deleted (tombstones) are
        never queued; each skip is logged with the matched rule.

        Args:
            downloader: Downloader used for discovery only.
            channel: Channel configuration entry.
            global_config: Configuration keys outside ``channels``.

        Returns:
            The number of newly queued jobs.
        """
        channel_name = channel["name"]
        self._add_log_message(f"Refresh started for {channel_name}")
        max_episodes = channel.get("max_episodes", 10)
        videos = downloader.get_channel_videos(channel["url"], max_episodes)
        if not videos:
            self._add_log_message(f"No videos found for {channel_name}")
            return 0

        format_type = channel.get("format", "video")
        download_delay_hours = channel.get("download_delay_hours", 0)
        check_delay_seconds = channel.get(
            "check_delay_seconds", global_config.get("check_delay_seconds", 0)
        )
        snapshot = {
            "format": format_type,
            "quality": channel.get("quality", "max"),
            "sponsorblock_categories": channel.get("sponsorblock_categories", []),
            "max_episodes": max_episodes,
            "download_delay_hours": download_delay_hours,
            "download_delay_seconds": global_config.get("download_delay_seconds", 60),
        }
        settings = load_storage_settings(global_config)
        usage = measure_media_usage(self.videos_dir)
        try:
            free_bytes: int | None = free_disk_bytes(self.videos_dir)
            free_error = None
        except OSError as error:
            free_bytes = None
            free_error = str(error)
        channel_dir = contained_path(self.videos_dir, channel_name)

        # Filtered and manually deleted videos never reach the queue; the
        # queue re-checks both again at execution time.
        filters = load_episode_filters(channel)
        deleted = tombstoned_ids(self.config_dir, channel_name)

        queued_count = 0
        for index, video in enumerate(videos, 1):
            video_id = video["id"]
            title = video.get("title") or "Unknown Title"

            if video_id in deleted:
                message = f"🪦 Skipping episode deleted by the user: {title}"
                self.logger.info(message)
                self._add_log_message(message)
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue

            # Cheap filters (title rules, known durations, live markers)
            # run before the per-video metadata request.
            skip_reason = filter_skip_reason(video, filters)
            if skip_reason is not None:
                self._log_filter_skip(title, skip_reason)
                continue

            if downloader.video_exists(channel_name, video_id, format_type):
                self._add_log_message(f"⏭️  Already downloaded: {title}")
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue

            detailed_metadata = downloader.get_video_metadata(video["url"])
            if not detailed_metadata:
                self._add_log_message(f"❌ Failed to get metadata, skipping: {title}")
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue
            video.update(detailed_metadata)

            # Duration bounds and Shorts detection are re-applied now that
            # the authoritative metadata replaced the flat entry values.
            skip_reason = filter_skip_reason(video, filters)
            if skip_reason is not None:
                self._log_filter_skip(title, skip_reason)
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue

            if downloader.is_video_too_new(video, download_delay_hours):
                self._add_log_message(
                    f"⌛ Video too new (< {download_delay_hours}h old): {title}"
                )
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue

            if free_error is not None:
                blocked = f"Free disk space check failed: {free_error}"
            else:
                blocked = admission_error(
                    settings=settings,
                    channel=channel,
                    channel_dir=channel_dir,
                    videos_dir=self.videos_dir,
                    format_type=format_type,
                    metadata_bytes=video.get("filesize"),
                    usage_totals=usage,
                    free_bytes=free_bytes,
                )
            if blocked is not None:
                # Log instead of enqueueing so a full disk does not create
                # endless retry jobs; the next refresh tries again.
                self.logger.warning(f"⛔ Not queuing {title}: {blocked}")
                self._add_log_message(f"⛔ Not queuing {title}: {blocked}")
                self._apply_check_delay(check_delay_seconds, index, len(videos))
                continue

            estimated_bytes = estimate_download_bytes(
                channel_dir, format_type, video.get("filesize")
            )
            job = self.queue.enqueue(
                channel=channel_name,
                video_id=video_id,
                title=title,
                url=video["url"],
                snapshot=snapshot,
                estimated_bytes=estimated_bytes,
            )
            if job is None:
                self._add_log_message(f"↩️  Already queued: {title}")
            else:
                queued_count += 1
                self._add_log_message(f"➕ Queued download: {title}")
            self._apply_check_delay(check_delay_seconds, index, len(videos))

        self._add_log_message(
            f"📥 Queued {queued_count} new download(s) for {channel_name}"
        )
        return queued_count

    def _apply_check_delay(
        self, check_delay_seconds: float, index: int, total: int
    ) -> None:
        """Pause between metadata checks like the inline loop used to."""
        if check_delay_seconds > 0 and index < total:
            self._add_log_message(
                f"   ⏸️  Waiting {check_delay_seconds}s before next video check..."
            )
            time.sleep(check_delay_seconds)

    def _log_filter_skip(self, title: str, reason: str) -> None:
        """Record that a video was excluded, naming the matched rule."""
        message = f"🚫 Skipped ({reason}): {title}"
        self.logger.info(message)
        self._add_log_message(message)

    def _run_single_channel_refresh(self, channel_name: str):
        """Run a reserved refresh for one channel."""
        self._run_refresh(channel_name)

    def _cleanup_old_episodes(self, channel_name: str, max_episodes: int):
        """Remove completed episodes exceeding the configured limit."""
        try:
            cleanup_episodes(
                self.videos_dir,
                channel_name,
                max_episodes,
                pinned=pinned_ids(self.config_dir, channel_name),
            )
        except Exception as error:
            self.logger.error(f"Error cleaning up {channel_name}: {error}")

    def _apply_channel_retention(
        self, channel: dict[str, Any], global_config: dict[str, Any]
    ) -> int:
        """Delete episodes past the channel's effective retention window.

        Args:
            channel: Channel configuration entry.
            global_config: Configuration keys outside ``channels``.

        Returns:
            The number of episodes deleted.
        """
        try:
            effective = effective_channel_storage(
                load_storage_settings(global_config), channel
            )
            if effective["retention_days"] <= 0:
                return 0
            pinned = pinned_ids(self.config_dir, channel["name"])
            removed = apply_retention(
                self.videos_dir,
                channel["name"],
                effective["retention_days"],
                pinned=pinned,
                logger=self.logger,
            )
            if removed:
                self._add_log_message(
                    f"🗑️  Retention removed {removed} old episode(s) "
                    f"from {channel['name']}"
                )
            return removed
        except Exception as error:
            self.logger.error(f"Retention failed for {channel['name']}: {error}")
            return 0

    def _run_storage_maintenance(self) -> None:
        """Apply retention cleanup for every channel (startup and daily)."""
        if not self._media_lock.acquire(blocking=False):
            self.logger.info(
                "Storage maintenance skipped; a media operation is in progress"
            )
            return
        try:
            config = self._load_channel_config()
            total = 0
            for channel in config.get("channels", []):
                total += self._apply_channel_retention(channel, config)
            if total:
                self.logger.info(
                    f"Storage maintenance removed {total} episode(s) by retention"
                )
        except Exception as error:
            self.logger.error(f"Storage maintenance failed: {error}")
        finally:
            self._media_lock.release()

    def _run_automation_script(self):
        """Run a reserved refresh for all channels."""
        self._run_refresh()

    def _handle_refresh_trigger(self):
        """Handle refresh trigger request."""
        reserved = False
        try:
            if not self._begin_refresh():
                return jsonify({"error": "Refresh already in progress"}), 409

            reserved = True
            # Start the refresh in a background thread
            thread = threading.Thread(target=self._run_automation_script)
            thread.daemon = True
            thread.start()

            return jsonify({"message": "Refresh started successfully"}), 200

        except Exception as e:
            if reserved:
                self._finish_refresh()
            self.logger.error(f"Error triggering refresh: {e}")
            return jsonify({"error": "Failed to start refresh"}), 500

    def _handle_refresh_status(self):
        """Handle refresh status request."""
        try:
            status = self.refresh_status.copy()

            # Calculate current duration if running
            if status["running"] and status["start_time"]:
                status["duration"] = int(time.time() - status["start_time"])
            else:
                status["duration"] = (
                    int(status["duration"]) if status["duration"] else 0
                )

            # Add recent logs to the status
            status["logs"] = self.recent_logs.copy()

            # Queue counts let the UI distinguish discovery (running)
            # from the download queue draining in the background.
            status["queue"] = self.queue.counts()

            return jsonify(status), 200

        except Exception as e:
            self.logger.error(f"Error getting refresh status: {e}")
            return jsonify({"error": "Failed to get refresh status"}), 500

    def _handle_get_queue(self):
        """List download queue jobs, optionally filtered by state."""
        try:
            state = request.args.get("state")
            if state is not None and state not in JOB_STATES:
                return jsonify(
                    {
                        "error": "Invalid state; expected one of: "
                        + ", ".join(JOB_STATES)
                    }
                ), 400
            jobs = self.queue.jobs()
            if state is not None:
                jobs = [job for job in jobs if job["state"] == state]
            jobs.sort(key=lambda job: job.get("created_at") or "")
            return jsonify({"jobs": jobs, "summary": self.queue.counts()}), 200
        except Exception as e:
            self.logger.error(f"Error listing download queue: {e}")
            return jsonify({"error": "Failed to list download queue"}), 500

    def _handle_get_queue_job(self, job_id: str):
        """Return a single download queue job."""
        job = self.queue.get(job_id)
        if job is None:
            return jsonify({"error": "Job not found"}), 404
        return jsonify(job), 200

    def _handle_queue_cancel(self, job_id: str):
        """Cancel a pending job or request cancellation of a running one."""
        try:
            job = self.queue.get(job_id)
            if job is None:
                return jsonify({"error": "Job not found"}), 404
            if job["state"] not in ACTIVE_STATES:
                return jsonify({"error": f"Cannot cancel a {job['state']} job"}), 409
            updated = self.queue.request_cancel(job_id)
            if updated is None:  # Raced with a worker state transition.
                return jsonify({"error": "Job is no longer active"}), 409
            message = (
                "Job cancelled"
                if updated["state"] == "cancelled"
                else "Cancellation requested; aborting download"
            )
            return jsonify({"message": message, "job": updated}), 200
        except Exception as e:
            self.logger.error(f"Error cancelling job {job_id}: {e}")
            return jsonify({"error": "Failed to cancel job"}), 500

    def _handle_queue_retry(self, job_id: str):
        """Requeue a failed or cancelled job with a fresh attempt budget."""
        try:
            job = self.queue.get(job_id)
            if job is None:
                return jsonify({"error": "Job not found"}), 404
            if job["state"] not in ("failed", "cancelled"):
                return jsonify({"error": f"Cannot retry a {job['state']} job"}), 409
            updated = self.queue.retry(job_id)
            if updated is None:  # Raced with a worker state transition.
                return jsonify({"error": "Job is no longer retryable"}), 409
            return jsonify({"message": "Job requeued", "job": updated}), 200
        except Exception as e:
            self.logger.error(f"Error retrying job {job_id}: {e}")
            return jsonify({"error": "Failed to retry job"}), 500

    def _handle_queue_dismiss(self, job_id: str):
        """Remove a terminal job from the queue."""
        try:
            job = self.queue.get(job_id)
            if job is None:
                return jsonify({"error": "Job not found"}), 404
            if job["state"] not in TERMINAL_STATES:
                return jsonify(
                    {
                        "error": "Only completed, failed or cancelled jobs can be dismissed"
                    }
                ), 409
            self.queue.dismiss(job_id)
            return jsonify({"message": "Job dismissed"}), 200
        except Exception as e:
            self.logger.error(f"Error dismissing job {job_id}: {e}")
            return jsonify({"error": "Failed to dismiss job"}), 500

    def _handle_queue_clear_completed(self):
        """Remove every completed job from the queue."""
        try:
            removed = self.queue.clear_completed()
            return jsonify(
                {"message": f"Removed {removed} completed job(s)", "removed": removed}
            ), 200
        except Exception as e:
            self.logger.error(f"Error clearing completed jobs: {e}")
            return jsonify({"error": "Failed to clear completed jobs"}), 500

    def _handle_get_refresh_interval(self):
        """Handle get refresh interval request."""
        try:
            config = self._load_channel_config()
            default_interval_hours = config.get("default_interval_hours", 24)

            # Get next scheduled runs for all channels
            next_runs = []
            for job in self.scheduler.get_jobs():
                if job.id.startswith("channel_refresh_") and job.next_run_time:
                    next_runs.append(
                        {
                            "channel": job.id.replace("channel_refresh_", ""),
                            "next_run": job.next_run_time.isoformat(),
                            "name": job.name,
                        }
                    )

            # Sort by next run time
            next_runs.sort(key=lambda x: x["next_run"])

            return jsonify(
                {
                    "refresh_interval_hours": default_interval_hours,
                    "next_runs": next_runs,
                    "next_run": next_runs[0]["next_run"] if next_runs else None,
                }
            ), 200

        except Exception as e:
            self.logger.error(f"Error getting refresh interval: {e}")
            return jsonify({"error": "Failed to get refresh interval"}), 500

    def _handle_update_refresh_interval(self):
        """Handle update refresh interval request."""
        try:
            data = request.get_json()
            if not data or "refresh_interval_hours" not in data:
                return jsonify({"error": "refresh_interval_hours is required"}), 400

            new_interval = data["refresh_interval_hours"]

            # Validate interval
            if not isinstance(new_interval, (int, float)) or new_interval <= 0:
                return jsonify(
                    {"error": "refresh_interval_hours must be a positive number"}
                ), 400

            # Restart scheduler with new default interval
            if not self._restart_scheduler(new_interval):
                return jsonify({"error": "Failed to restart scheduler"}), 500

            # Get updated schedule info
            interval_data = self._handle_get_refresh_interval()
            if interval_data[1] == 200:  # Success
                response_data = interval_data[0].get_json()
                return jsonify(
                    {
                        "message": "Default refresh interval updated successfully",
                        "refresh_interval_hours": new_interval,
                        "next_run": response_data.get("next_run"),
                        "next_runs": response_data.get("next_runs", []),
                    }
                ), 200
            else:
                return jsonify({"error": "Failed to get updated schedule info"}), 500

        except Exception as e:
            self.logger.error(f"Error updating refresh interval: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _handle_get_storage(self):
        """Report media usage, quotas, retention and free disk space.

        Accounting counts only regular media files (``MEDIA_EXTENSIONS``);
        thumbnails and ``*.json`` metadata are excluded. Symlinks are never
        followed, so nothing outside the storage root is counted.
        """
        try:
            config = self._load_channel_config()
            settings = load_storage_settings(config)
            pins = load_pins(self.config_dir)
            usage = measure_media_usage(self.videos_dir)
            free_bytes = free_disk_bytes(self.videos_dir)

            channels = []
            totals_bytes = 0
            totals_episodes = 0
            totals_pinned = 0
            for channel in config.get("channels", []):
                name = channel["name"]
                stats = usage.get(name, {"bytes": 0, "episodes": 0})
                effective = effective_channel_storage(settings, channel)
                pinned_count = len(pins.get(name, []))
                quota_bytes = effective["quota_gb"] * GB
                quota_override = channel.get("quota_gb", 0)
                retention_override = channel.get("retention_days", 0)
                channels.append(
                    {
                        "name": name,
                        "display_name": channel.get("display_name", name),
                        "used_bytes": stats["bytes"],
                        "episodes": stats["episodes"],
                        "pinned": pinned_count,
                        "quota_gb": effective["quota_gb"],
                        "quota_overridden": is_positive_number(quota_override),
                        "retention_days": effective["retention_days"],
                        "retention_overridden": is_positive_number(retention_override),
                        "over_quota": bool(quota_bytes)
                        and stats["bytes"] > quota_bytes,
                    }
                )
                totals_bytes += stats["bytes"]
                totals_episodes += stats["episodes"]
                totals_pinned += pinned_count

            return jsonify(
                {
                    "totals": {
                        "used_bytes": totals_bytes,
                        "used_gb": round(totals_bytes / GB, 2),
                        "episodes": totals_episodes,
                        "pinned": totals_pinned,
                    },
                    "free_bytes": free_bytes,
                    "free_gb": round(free_bytes / GB, 2),
                    "settings": settings,
                    "channels": channels,
                    "accounting": (
                        "media files only; thumbnails and metadata excluded"
                    ),
                }
            ), 200
        except OSError as e:
            self.logger.error(f"Error measuring storage: {e}")
            return jsonify({"error": "Failed to measure storage"}), 500
        except Exception as e:
            self.logger.error(f"Error getting storage info: {e}")
            return jsonify({"error": "Failed to get storage info"}), 500

    def _handle_get_storage_config(self):
        """Return the effective global storage settings."""
        try:
            config = self._load_channel_config()
            return jsonify(load_storage_settings(config)), 200
        except Exception as e:
            self.logger.error(f"Error getting storage settings: {e}")
            return jsonify({"error": "Failed to get storage settings"}), 500

    @config_transaction
    def _handle_update_storage_config(self):
        """Validate, persist and return the global storage settings."""
        try:
            data = request.get_json(silent=True)
            if not isinstance(data, dict) or not data:
                return jsonify(
                    {"error": "No data provided; expected a JSON object"}
                ), 400
            try:
                validate_storage_settings(data)
            except (ValueError, OverflowError) as error:
                return jsonify({"error": str(error)}), 400

            config = self._load_channel_config()
            settings = load_storage_settings(config)
            settings.update(data)
            config["storage"] = settings

            if not self._save_channel_config(config):
                return jsonify({"error": "Failed to save storage settings"}), 500
            return jsonify(
                {
                    "message": "Storage settings updated successfully",
                    "storage": settings,
                }
            ), 200
        except Exception as e:
            self.logger.error(f"Error updating storage settings: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _handle_pin_episode(self, channel_name: str, episode_id: str, pinned: bool):
        """Pin or unpin one downloaded episode.

        Pins survive metadata rewrites and redownloads; cleanup and
        retention never delete pinned episodes. Deleting the episode or
        its channel removes the pin.
        """
        try:
            config = self._load_channel_config()
            if not any(ch["name"] == channel_name for ch in config.get("channels", [])):
                return jsonify({"error": "Channel not found"}), 404

            channel_dir = self.videos_dir / channel_name
            has_media = any(
                (channel_dir / f"{episode_id}{ext}").is_file()
                for ext in MEDIA_EXTENSIONS
            )
            if not (channel_dir / f"{episode_id}.json").is_file() or not has_media:
                return jsonify({"error": "Episode not found"}), 404

            if not set_episode_pinned(
                self.config_dir, channel_name, episode_id, pinned
            ):
                return jsonify({"error": "Failed to save pin"}), 500
            return jsonify(
                {
                    "message": "Episode pinned" if pinned else "Episode unpinned",
                    "episode_id": episode_id,
                    "pinned": pinned,
                }
            ), 200
        except Exception as e:
            self.logger.error(
                f"Error toggling pin for {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500

    def _validate_display_name(
        self,
        display_name: str,
        existing_ids: list[str] | None = None,
        existing_display_names: list[str] | None = None,
    ) -> tuple[bool, str, str]:
        """Validate display name and generate sanitized ID.

        Returns:
            Tuple[bool, str, str]: (is_valid, error_message, sanitized_id)
        """
        if not display_name:
            return False, "Display name cannot be empty", ""

        if not isinstance(display_name, str):
            return False, "Display name must be a string", ""

        # Check length
        if len(display_name.strip()) < 2:
            return False, "Display name must be at least 2 characters long", ""

        if len(display_name) > 100:
            return False, "Display name must be 100 characters or less", ""

        # Generate sanitized ID
        sanitized_id = sanitize_channel_name(display_name)

        if not sanitized_id:
            return (
                False,
                "Display name must contain at least some alphanumeric characters",
                "",
            )

        # Check ID uniqueness
        if existing_ids and sanitized_id in existing_ids:
            return (
                False,
                f"A channel with similar name already exists (would create same ID: {sanitized_id})",
                "",
            )

        # Check display name uniqueness
        if existing_display_names and display_name in existing_display_names:
            return False, "A channel with this display name already exists", ""

        return True, "", sanitized_id

    def _verify_youtube_channel(self, url: str) -> tuple[bool, str, dict]:
        """Verify if YouTube channel URL is accessible and valid.

        Returns:
            Tuple[bool, str, dict]: (is_valid, error_message, channel_info)
        """
        if not url:
            return False, "YouTube URL cannot be empty", {}

        # Basic URL format validation
        if not isinstance(url, str) or not url.startswith(
            ("https://www.youtube.com/", "https://youtube.com/")
        ):
            return False, "URL must be a valid YouTube channel URL", {}

        # Normalize URL to use www
        if url.startswith("https://youtube.com/"):
            url = url.replace("https://youtube.com/", "https://www.youtube.com/")

        # Check if it's a valid channel URL format
        valid_patterns = [
            r"https://www\.youtube\.com/@[\w\.-]+",
            r"https://www\.youtube\.com/c/[\w\.-]+",
            r"https://www\.youtube\.com/channel/[\w\.-]+",
            r"https://www\.youtube\.com/user/[\w\.-]+",
            r"https://www\.youtube\.com/playlist\?list=[\w\.-]+",
        ]

        if not any(re.match(pattern, url) for pattern in valid_patterns):
            return False, "Invalid YouTube channel URL format", {}

        # Try to extract channel information using yt-dlp
        try:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "extract_flat": True,
                "playlistend": 1,  # Only get first video to verify channel exists
                **self._get_proxy_opts(),
            }

            with yt_dlp.YoutubeDL(cast(Any, ydl_opts)) as ydl:
                info = ydl.extract_info(url, download=False)

                if not info:
                    return False, "Channel not found or inaccessible", {}

                # Extract channel information
                channel_info = {
                    "title": info.get("title", "Unknown"),
                    "id": info.get("id", ""),
                    "url": info.get("webpage_url", url),
                    "subscriber_count": info.get("subscriber_count"),
                    "video_count": info.get("playlist_count", 0),
                }

                return True, "", channel_info

        except DownloadError as e:
            error_msg = str(e).lower()
            if "private" in error_msg or "unavailable" in error_msg:
                return False, "Channel is private or unavailable", {}
            elif "not found" in error_msg or "does not exist" in error_msg:
                return False, "Channel does not exist", {}
            else:
                return False, f"Failed to access channel: {e!s}", {}
        except Exception as e:
            self.logger.error(f"Error verifying YouTube channel {url}: {e}")
            return False, "Unable to verify channel accessibility", {}

    @config_transaction
    def _handle_add_channel(self):
        """Handle adding a new channel."""
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            # Validate required fields - now expecting display_name instead of name
            required_fields = ["display_name", "url"]
            for field in required_fields:
                if field not in data:
                    return jsonify({"error": f"Missing required field: {field}"}), 400

            # Load current config
            config = self._load_channel_config()
            existing_ids = [ch["name"] for ch in config.get("channels", [])]
            existing_display_names = [
                ch.get("display_name", ch["name"]) for ch in config.get("channels", [])
            ]

            # Validate display name and generate ID
            name_valid, name_error, sanitized_id = self._validate_display_name(
                data["display_name"], existing_ids, existing_display_names
            )
            if not name_valid:
                return jsonify({"error": name_error}), 400

            # Verify YouTube channel accessibility
            url_valid, url_error, channel_info = self._verify_youtube_channel(
                data["url"]
            )
            if not url_valid:
                return jsonify({"error": url_error}), 400

            # Create new channel with defaults
            new_channel = {
                "name": sanitized_id,  # This is the sanitized ID for filesystem/URL usage
                "display_name": data["display_name"],  # This is the human-readable name
                "url": data["url"],
                "max_episodes": data.get("max_episodes", 10),
                "download_delay_hours": data.get("download_delay_hours", 6),
                "format": data.get("format", "video"),
                "quality": data.get("quality", "max"),
                **(
                    {"refresh_interval_hours": data["refresh_interval_hours"]}
                    if "refresh_interval_hours" in data
                    else {}
                ),
                **({"quota_gb": data["quota_gb"]} if "quota_gb" in data else {}),
                **(
                    {"retention_days": data["retention_days"]}
                    if "retention_days" in data
                    else {}
                ),
                # Episode filters; all optional and validated by
                # ``validate_channel_settings`` before this handler runs.
                **{
                    key: data[key]
                    for key in (
                        "title_include",
                        "title_exclude",
                        "min_duration_seconds",
                        "max_duration_seconds",
                        "skip_shorts",
                        "skip_livestreams",
                    )
                    if key in data
                },
                "sponsorblock_categories": data.get("sponsorblock_categories", []),
                "feed_token": generate_feed_token(),
            }

            # Add to config
            if "channels" not in config:
                config["channels"] = []
            config["channels"].append(new_channel)

            # Save config
            if self._save_channel_config(config):
                # Restart scheduler to include new channel
                self._setup_scheduler()

                response_data = {
                    "message": "Channel added successfully",
                    "channel": new_channel,
                    "channel_info": channel_info,
                }
                return jsonify(response_data), 201
            else:
                return jsonify({"error": "Failed to save configuration"}), 500

        except Exception as e:
            self.logger.error(f"Error adding channel: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @config_transaction
    def _handle_update_channel(self, channel_name: str):
        """Handle updating an existing channel."""
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            # Load current config
            config = self._load_channel_config()
            channels = config.get("channels", [])

            # Find the channel to update
            channel_index = None
            for i, channel in enumerate(channels):
                if channel["name"] == channel_name:
                    channel_index = i
                    break

            if channel_index is None:
                return jsonify({"error": "Channel not found"}), 404

            # Update channel fields
            channel = channels[channel_index]
            channel_info = {}

            if "display_name" in data:
                # Validate new display name but keep existing ID
                existing_ids = [
                    ch["name"] for i, ch in enumerate(channels) if i != channel_index
                ]
                existing_display_names = [
                    ch.get("display_name", ch["name"])
                    for i, ch in enumerate(channels)
                    if i != channel_index
                ]

                name_valid, name_error, _ = self._validate_display_name(
                    data["display_name"], existing_ids, existing_display_names
                )
                if not name_valid:
                    return jsonify({"error": name_error}), 400

                # Update display name but KEEP the existing ID
                channel["display_name"] = data["display_name"]

            if "url" in data:
                # Verify YouTube channel accessibility
                url_valid, url_error, channel_info = self._verify_youtube_channel(
                    data["url"]
                )
                if not url_valid:
                    return jsonify({"error": url_error}), 400
                channel["url"] = data["url"]
            if "max_episodes" in data:
                channel["max_episodes"] = data["max_episodes"]
            if "download_delay_hours" in data:
                channel["download_delay_hours"] = data["download_delay_hours"]
            if "format" in data:
                channel["format"] = data["format"]
            if "quality" in data:
                channel["quality"] = data["quality"]
            if "sponsorblock_categories" in data:
                channel["sponsorblock_categories"] = data["sponsorblock_categories"]

            if "refresh_interval_hours" in data:
                channel["refresh_interval_hours"] = data["refresh_interval_hours"]
            if "quota_gb" in data:
                channel["quota_gb"] = data["quota_gb"]
            if "retention_days" in data:
                channel["retention_days"] = data["retention_days"]
            for field in (
                "title_include",
                "title_exclude",
                "min_duration_seconds",
                "max_duration_seconds",
                "skip_shorts",
                "skip_livestreams",
            ):
                if field in data:
                    channel[field] = data[field]

            # Save config
            if self._save_channel_config(config):
                # Restart scheduler to update channel schedules
                self._setup_scheduler()

                response_data = {
                    "message": "Channel updated successfully",
                    "channel": channel,
                }
                if channel_info:
                    response_data["channel_info"] = channel_info
                return jsonify(response_data), 200
            else:
                return jsonify({"error": "Failed to save configuration"}), 500

        except Exception as e:
            self.logger.error(f"Error updating channel: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @config_transaction
    def _handle_delete_channel(self, channel_name: str):
        """Handle deleting a channel."""
        try:
            # Load current config
            config = self._load_channel_config()
            channels = config.get("channels", [])

            # Find and remove the channel
            original_length = len(channels)
            config["channels"] = [ch for ch in channels if ch["name"] != channel_name]

            if len(config["channels"]) == original_length:
                return jsonify({"error": "Channel not found"}), 404

            # Save config
            if self._save_channel_config(config):
                # Restart scheduler to remove deleted channel's job
                self._setup_scheduler()
                # Pins and tombstones have no meaning once the channel is
                # gone.
                remove_channel_pins(self.config_dir, channel_name)
                remove_channel_tombstones(self.config_dir, channel_name)

                return jsonify({"message": "Channel deleted successfully"}), 200
            else:
                return jsonify({"error": "Failed to save configuration"}), 500

        except Exception as e:
            self.logger.error(f"Error deleting channel: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _handle_rotate_feed_token(self, channel_name: str):
        """Rotate a channel's private feed token."""
        try:
            token = self._rotate_feed_token(channel_name)
            if token is None:
                return jsonify({"error": "Channel not found"}), 404
            return jsonify(
                {
                    "message": "Feed token rotated successfully",
                    "feed_token": token,
                }
            ), 200
        except Exception as e:
            self.logger.error(f"Error rotating feed token for {channel_name}: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @exclusive_media_operation
    def _handle_purge_episodes(self, channel_name: str):
        """Handle purging episodes for a channel.

        Purge is an explicit reset: the channel's tombstones (remembered
        deletions) are cleared as well, so a later refresh may download
        previously deleted episodes again.
        """
        try:
            import shutil

            # Validate channel exists in config
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_exists = any(ch["name"] == channel_name for ch in channels)

            if not channel_exists:
                return jsonify({"error": "Channel not found"}), 404

            # Path to channel's video directory
            channel_video_dir = self.videos_dir / channel_name

            if not channel_video_dir.exists():
                # Purge is an explicit reset: remembered deletions are
                # forgotten so a later refresh may download everything.
                removed = remove_channel_tombstones(self.config_dir, channel_name)
                if removed:
                    self.logger.info(
                        f"Forgot {removed} tombstone(s) for channel: {channel_name}"
                    )
                return jsonify({"message": "No episodes found to purge"}), 200

            # Remove all video files and directories for this channel
            try:
                shutil.rmtree(str(channel_video_dir))
                remove_channel_pins(self.config_dir, channel_name)
                # Purge is an explicit reset: remembered deletions are
                # forgotten so deleted episodes may be downloaded again.
                forgotten = remove_channel_tombstones(self.config_dir, channel_name)
                if forgotten:
                    self.logger.info(
                        f"Forgot {forgotten} tombstone(s) for channel: {channel_name}"
                    )
                self.logger.info(f"Purged all episodes for channel: {channel_name}")
            except FileNotFoundError:
                pass  # Directory already doesn't exist
            except Exception as e:
                self.logger.error(
                    f"Error removing channel directory {channel_video_dir}: {e}"
                )
                return jsonify({"error": "Failed to delete episode files"}), 500

            # Clear refresh timestamp for this channel
            timestamps_file = self.config_dir / "refresh_timestamps.json"
            if timestamps_file.exists():
                try:
                    with open(timestamps_file, "r", encoding="utf-8") as f:
                        timestamps = json.load(f)

                    if channel_name in timestamps:
                        del timestamps[channel_name]

                        atomic_write_text(
                            timestamps_file, json.dumps(timestamps, indent=2)
                        )

                        self.logger.info(
                            f"Cleared refresh timestamp for channel: {channel_name}"
                        )
                except Exception as e:
                    self.logger.error(f"Error updating refresh timestamps: {e}")
                    # Don't fail the whole operation for this

            return jsonify(
                {
                    "message": f"Successfully purged all episodes for channel '{channel_name}'. RSS feed will reflect changes immediately."
                }
            ), 200

        except Exception as e:
            self.logger.error(f"Error purging episodes for channel {channel_name}: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _handle_get_tombstones(self, channel_name: str):
        """List the episodes remembered as deleted for a channel."""
        try:
            if not self._channel_exists(channel_name):
                return jsonify({"error": "Channel not found"}), 404
            tombstones = load_tombstones(self.config_dir).get(channel_name, {})
            return jsonify(
                {
                    "channel": channel_name,
                    "tombstones": tombstones,
                    "total_count": len(tombstones),
                }
            ), 200
        except Exception as e:
            self.logger.error(f"Error loading tombstones for {channel_name}: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @config_transaction
    def _handle_clear_tombstones(self, channel_name: str):
        """Forget every remembered deletion for a channel."""
        try:
            if not self._channel_exists(channel_name):
                return jsonify({"error": "Channel not found"}), 404
            removed = remove_channel_tombstones(self.config_dir, channel_name)
            self.logger.info(
                f"Forgot {removed} tombstone(s) for channel: {channel_name}"
            )
            return jsonify(
                {
                    "message": f"Forgot {removed} remembered deletion(s)",
                    "removed": removed,
                }
            ), 200
        except Exception as e:
            self.logger.error(f"Error clearing tombstones for {channel_name}: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @config_transaction
    def _handle_forget_tombstone(self, channel_name: str, episode_id: str):
        """Forget one remembered deletion."""
        try:
            if not self._channel_exists(channel_name):
                return jsonify({"error": "Channel not found"}), 404
            if not remove_episode_tombstone(self.config_dir, channel_name, episode_id):
                return jsonify({"error": "Tombstone not found"}), 404
            self.logger.info(f"Forgot tombstone for {channel_name}/{episode_id}")
            return jsonify(
                {"message": "Tombstone removed", "episode_id": episode_id}
            ), 200
        except Exception as e:
            self.logger.error(
                f"Error forgetting tombstone {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500

    def _handle_get_episodes(self, channel_name: str):
        """Handle getting episodes for a channel.

        Episodes excluded by the channel's current filters or remembered
        as deleted (tombstones) stay hidden, consistently with the RSS
        feed and the episode counts; ``total_count`` reflects that view.
        """
        try:
            # Validate channel exists in config
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_config = None
            for ch in channels:
                if ch["name"] == channel_name:
                    channel_config = ch
                    break

            if not channel_config:
                return jsonify({"error": "Channel not found"}), 404

            # Filtered and tombstoned episodes stay hidden, consistent
            # with the RSS feed and the episode counts.
            episodes_data = self._visible_episodes(channel_name, channel_config)
            pinned = pinned_ids(self.config_dir, channel_name)

            # Format episodes for the frontend
            episodes = []
            for episode in episodes_data:
                # Convert duration to a readable format
                duration = episode.get("duration", 0)
                if isinstance(duration, (int, float)):
                    hours = int(duration // 3600)
                    minutes = int((duration % 3600) // 60)
                    seconds = int(duration % 60)
                    if hours > 0:
                        duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
                    else:
                        duration_str = f"{minutes}:{seconds:02d}"
                else:
                    duration_str = str(duration) if duration else "0:00"

                # Format upload date
                upload_date = episode.get("upload_date", "")
                if upload_date:
                    try:
                        # Parse upload_date (usually in YYYYMMDD format)
                        if len(upload_date) == 8:
                            date_obj = datetime.strptime(upload_date, "%Y%m%d")
                            formatted_date = date_obj.strftime("%B %d, %Y")
                        else:
                            formatted_date = upload_date
                    except (ValueError, TypeError):
                        formatted_date = upload_date
                else:
                    formatted_date = "Unknown"

                # Truncate description to 150 characters
                description = episode.get("description", "")
                if len(description) > 150:
                    description = description[:147] + "..."

                episodes.append(
                    {
                        "title": episode.get("title", "Unknown Title"),
                        "duration": duration_str,
                        "date": formatted_date,
                        "description": description,
                        "id": episode.get("id", ""),
                        "thumbnail": episode.get("thumbnail", ""),
                        "uploader": episode.get("uploader", ""),
                        "view_count": episode.get("view_count", 0),
                        "file_extension": episode.get("file_extension", ".mp4"),
                        "upload_date": episode.get(
                            "upload_date", ""
                        ),  # Keep original upload_date for sorting
                        "pinned": episode.get("id", "") in pinned,
                    }
                )

            # Sort episodes by upload date (newest first)
            episodes.sort(key=lambda x: x.get("upload_date", ""), reverse=True)

            return jsonify(
                {
                    "channel_name": channel_config.get("display_name", channel_name),
                    "episodes": episodes,
                    "total_count": len(episodes),
                }
            ), 200

        except Exception as e:
            self.logger.error(f"Error getting episodes for channel {channel_name}: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _handle_single_channel_refresh(self, channel_name: str):
        """Handle single channel refresh request."""
        reserved = False
        try:
            if self.refresh_status["running"]:
                return jsonify({"error": "Refresh already in progress"}), 409

            # Validate channel exists
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_exists = any(ch["name"] == channel_name for ch in channels)

            if not channel_exists:
                return jsonify({"error": "Channel not found"}), 404

            if not self._begin_refresh():
                return jsonify({"error": "Refresh already in progress"}), 409

            reserved = True
            # Start the single channel refresh in a background thread
            thread = threading.Thread(
                target=self._run_single_channel_refresh, args=(channel_name,)
            )
            thread.daemon = True
            thread.start()

            return jsonify(
                {"message": f"Refresh started for channel '{channel_name}'"}
            ), 200

        except Exception as e:
            if reserved:
                self._finish_refresh()
            self.logger.error(f"Error triggering single channel refresh: {e}")
            return jsonify({"error": "Failed to start channel refresh"}), 500

    def _generate_download_filename(
        self, channel_name: str, episode_id: str, extension: str
    ) -> str:
        """Generate a clean download filename from channel and episode metadata."""
        import re

        try:
            # Get channel display name
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel = next((ch for ch in channels if ch["name"] == channel_name), None)
            channel_display_name = (
                channel.get("display_name", channel_name) if channel else channel_name
            )

            # Load episode metadata
            channel_dir = self.videos_dir / channel_name
            episode_json = channel_dir / f"{episode_id}.json"
            episode_title = episode_id  # fallback

            if episode_json.exists():
                try:
                    with open(episode_json, "r", encoding="utf-8") as f:
                        episode_data = json.load(f)
                        episode_title = episode_data.get("title", episode_id)
                except Exception:
                    pass  # use fallback

            # Create clean filename: "Channel_Name - Episode_Title.ext"
            filename = f"{channel_display_name} - {episode_title}"

            # Clean filename: replace spaces with underscores, remove special chars
            filename = re.sub(
                r"[^\w\s-]", "", filename
            )  # Keep alphanumeric, spaces, hyphens
            filename = re.sub(r"\s+", "_", filename)  # Replace spaces with underscores
            filename = re.sub(r"_+", "_", filename)  # Collapse multiple underscores
            filename = filename.strip("_")  # Remove leading/trailing underscores

            return f"{filename}{extension}"

        except Exception:
            # Fallback to simple filename
            return f"{episode_id}{extension}"

    def _handle_mp3_download(self, channel_name: str, episode_id: str):
        """Handle MP3 conversion and download for an episode."""
        import tempfile

        temp_mp3_path = None
        response_owns_temp_file = False
        try:
            # Validate channel exists
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_exists = any(ch["name"] == channel_name for ch in channels)

            if not channel_exists:
                return jsonify({"error": "Channel not found"}), 404

            # Find the episode file
            channel_dir = self.videos_dir / channel_name
            if not channel_dir.exists():
                return jsonify({"error": "No episodes found for this channel"}), 404

            # Look for episode file with various extensions
            episode_file = None
            supported_extensions = [".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi"]

            for ext in supported_extensions:
                candidate_file = channel_dir / f"{episode_id}{ext}"
                if candidate_file.exists():
                    episode_file = candidate_file
                    break

            if not episode_file:
                return jsonify({"error": "Episode file not found"}), 404

            # If already MP3 or M4A, serve directly
            if episode_file.suffix.lower() in [".mp3", ".m4a"]:
                file_ext = episode_file.suffix.lower()
                if file_ext == ".mp3":
                    mimetype = "audio/mpeg"
                    download_name = self._generate_download_filename(
                        channel_name, episode_id, ".mp3"
                    )
                else:  # .m4a
                    mimetype = "audio/mp4"
                    download_name = self._generate_download_filename(
                        channel_name, episode_id, ".m4a"
                    )

                try:
                    return send_file(
                        str(episode_file),
                        mimetype=mimetype,
                        as_attachment=True,
                        download_name=download_name,
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error serving {file_ext.upper()} file {episode_file}: {e}"
                    )
                    return jsonify(
                        {"error": f"Failed to serve {file_ext.upper()} file"}
                    ), 500

            # Convert to MP3 using FFmpeg
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".mp3", delete=False
                ) as temp_mp3:
                    temp_mp3_path = temp_mp3.name

                # Use FFmpeg to convert to MP3
                ffmpeg_cmd = [
                    "ffmpeg",
                    "-i",
                    str(episode_file),
                    "-acodec",
                    "mp3",
                    "-ab",
                    "192k",
                    "-ar",
                    "44100",
                    "-y",  # Overwrite output file
                    temp_mp3_path,
                ]

                self.logger.info(
                    f"Converting {episode_file} to MP3: {' '.join(ffmpeg_cmd)}"
                )

                # Run FFmpeg with extended timeout for conversion
                result = subprocess.run(
                    ffmpeg_cmd,
                    capture_output=True,
                    text=True,
                    timeout=600,  # 10 minute timeout
                )

                if result.returncode != 0:
                    self.logger.error(f"FFmpeg conversion failed: {result.stderr}")
                    return jsonify({"error": "Audio conversion failed"}), 500

                # Verify the converted file exists and has content
                if (
                    not Path(temp_mp3_path).exists()
                    or Path(temp_mp3_path).stat().st_size == 0
                ):
                    return jsonify({"error": "Conversion produced empty file"}), 500

                try:
                    # Send the converted file
                    response = send_file(
                        temp_mp3_path,
                        mimetype="audio/mpeg",
                        as_attachment=True,
                        download_name=self._generate_download_filename(
                            channel_name, episode_id, ".mp3"
                        ),
                    )

                    # send_file uses direct passthrough, so close the file wrapper
                    # before unlinking, including when the client disconnects.
                    converted_path = Path(temp_mp3_path)
                    response.direct_passthrough = False
                    response.call_on_close(
                        lambda: converted_path.unlink(missing_ok=True)
                    )
                    response_owns_temp_file = True
                    return response

                except Exception as e:
                    # Cleanup on error
                    Path(temp_mp3_path).unlink(missing_ok=True)
                    self.logger.error(f"Error serving converted MP3: {e}")
                    return jsonify({"error": "Failed to serve converted file"}), 500

            except subprocess.TimeoutExpired:
                self.logger.error(f"FFmpeg conversion timeout for {episode_file}")
                return jsonify(
                    {"error": "Conversion timeout - file may be too large"}
                ), 500
            except FileNotFoundError:
                self.logger.error("FFmpeg not found - ensure it's installed")
                return jsonify({"error": "Audio conversion not available"}), 500
            except Exception as e:
                self.logger.error(f"Conversion error for {episode_file}: {e}")
                return jsonify({"error": "Conversion failed"}), 500

        except Exception as e:
            self.logger.error(
                f"Error in MP3 download for {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500
        finally:
            if temp_mp3_path is not None and not response_owns_temp_file:
                Path(temp_mp3_path).unlink(missing_ok=True)

    def _handle_original_download(self, channel_name: str, episode_id: str):
        """Handle download of original episode file with clean filename."""
        try:
            # Validate channel exists
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_exists = any(ch["name"] == channel_name for ch in channels)

            if not channel_exists:
                return jsonify({"error": "Channel not found"}), 404

            # Find the episode file
            channel_dir = self.videos_dir / channel_name
            if not channel_dir.exists():
                return jsonify({"error": "No episodes found for this channel"}), 404

            # Look for episode file with various extensions
            episode_file = None
            supported_extensions = [".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi"]

            for ext in supported_extensions:
                candidate_file = channel_dir / f"{episode_id}{ext}"
                if candidate_file.exists():
                    episode_file = candidate_file
                    break

            if not episode_file:
                return jsonify({"error": "Episode file not found"}), 404

            # Determine MIME type
            file_ext = episode_file.suffix.lower()
            mimetype_map = {
                ".mp4": "video/mp4",
                ".m4a": "audio/mp4",
                ".mp3": "audio/mpeg",
                ".webm": "video/webm",
                ".mkv": "video/x-matroska",
                ".avi": "video/x-msvideo",
            }
            mimetype = mimetype_map.get(file_ext, "application/octet-stream")

            # Generate clean download filename
            download_name = self._generate_download_filename(
                channel_name, episode_id, file_ext
            )

            try:
                return send_file(
                    str(episode_file),
                    mimetype=mimetype,
                    as_attachment=True,
                    download_name=download_name,
                )
            except Exception as e:
                self.logger.error(f"Error serving original file {episode_file}: {e}")
                return jsonify({"error": "Failed to serve file"}), 500

        except Exception as e:
            self.logger.error(
                f"Error in original download for {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500

    @exclusive_media_operation
    def _handle_delete_episode(self, channel_name: str, episode_id: str):
        """Handle deletion of a single episode.

        The deletion is recorded as a tombstone so discovery and queue
        execution never re-download this episode; an explicit redownload
        clears the tombstone again.
        """
        try:
            # Validate channel exists
            channel_dir = self.videos_dir / channel_name
            if not channel_dir.exists():
                return jsonify({"error": "Channel not found"}), 404

            # Check if episode exists
            json_file = channel_dir / f"{episode_id}.json"
            if not json_file.exists():
                return jsonify({"error": "Episode not found"}), 404

            # Remember the title for the tombstone record before the
            # metadata file disappears.
            episode_title = episode_id
            try:
                with open(json_file, "r", encoding="utf-8") as handle:
                    metadata = json.load(handle)
                if isinstance(metadata, dict) and metadata.get("title"):
                    episode_title = str(metadata["title"])
            except (OSError, ValueError):
                pass

            # Remove video files
            removed_files = []
            for ext in [".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi"]:
                video_file = channel_dir / f"{episode_id}{ext}"
                if video_file.exists():
                    video_file.unlink()
                    removed_files.append(f"video{ext}")
                    self.logger.info(f"Removed episode file: {video_file}")

            # Remove JSON metadata
            if json_file.exists():
                json_file.unlink()
                removed_files.append("metadata")
                self.logger.info(f"Removed metadata: {json_file}")

            # Remove thumbnail
            thumbnails_dir = channel_dir / "thumbnails"
            if thumbnails_dir.exists():
                for thumb_file in thumbnails_dir.glob(f"{episode_id}.*"):
                    thumb_file.unlink()
                    removed_files.append("thumbnail")
                    self.logger.info(f"Removed thumbnail: {thumb_file}")

            if not removed_files:
                return jsonify({"error": "No files found to delete"}), 404

            # The pin dies with the episode it protected.
            remove_episode_pin(self.config_dir, channel_name, episode_id)

            # Remember the deletion so refreshes never re-download this
            # episode (until it is forgotten explicitly or redownloaded).
            if record_tombstone(
                self.config_dir, channel_name, episode_id, episode_title
            ):
                self.logger.info(f"Recorded tombstone for deleted episode {episode_id}")
            else:
                self.logger.warning(
                    f"Could not record tombstone for deleted episode {episode_id}"
                )

            # RSS feed will automatically reflect changes on next request (dynamic generation)
            self.logger.info(
                f"Successfully deleted episode {episode_id}: {', '.join(removed_files)}"
            )
            return jsonify(
                {
                    "success": True,
                    "message": "Episode deleted successfully",
                    "removed": removed_files,
                }
            ), 200

        except Exception as e:
            self.logger.error(
                f"Error deleting episode {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500

    @exclusive_media_operation
    def _handle_redownload_episode(self, channel_name: str, episode_id: str):
        """Handle redownloading a single episode.

        Explicit redownloads are allowed even for remembered-deleted
        (tombstoned) episodes; success clears the tombstone.
        """
        try:
            # Validate channel exists in config
            config = self._load_channel_config()
            channels = config.get("channels", [])
            channel_config = None
            for ch in channels:
                if ch["name"] == channel_name:
                    channel_config = ch
                    break

            if not channel_config:
                return jsonify({"error": "Channel not found"}), 404

            # Redownload replaces the stored episode; for a remembered-
            # deleted (tombstoned) episode there are no files left, but an
            # explicit redownload is allowed and clears its tombstone once
            # it succeeds.
            channel_dir = self.videos_dir / channel_name
            json_file = channel_dir / f"{episode_id}.json"

            tombstone = (
                load_tombstones(self.config_dir).get(channel_name, {}).get(episode_id)
            )
            metadata: dict[str, Any]
            if not json_file.exists():
                if tombstone is None:
                    return jsonify({"error": "Episode not found"}), 404
                metadata = {"title": tombstone.get("title") or episode_id}
            else:
                # Load episode metadata to get video URL
                try:
                    with open(json_file, "r") as f:
                        metadata = json.load(f)
                except Exception as e:
                    self.logger.error(f"Error reading episode metadata: {e}")
                    return jsonify({"error": "Failed to read episode metadata"}), 500

            video_url = f"https://www.youtube.com/watch?v={episode_id}"
            video_title = metadata.get("title", "Unknown")

            import tempfile

            try:
                from .downloader import YouTubeDownloader
            except ImportError:
                from downloader import YouTubeDownloader

            video_info = {
                "id": episode_id,
                "title": video_title,
                "url": video_url,
                "upload_date": metadata.get("upload_date", ""),
                "duration": metadata.get("duration", 0),
            }
            with tempfile.TemporaryDirectory(
                dir=self.videos_dir, prefix=".redownload-"
            ) as staging:
                downloader = YouTubeDownloader(
                    str(self.config_dir / "channels.yaml"), videos_dir=staging
                )
                success = downloader.download_video(
                    video_info,
                    channel_name,
                    sponsorblock_categories=channel_config.get(
                        "sponsorblock_categories"
                    ),
                    format_type=channel_config.get("format", "video"),
                    quality=channel_config.get("quality", "max"),
                )
                staged_channel = Path(staging) / channel_name
                staged_metadata = staged_channel / f"{episode_id}.json"
                staged_media = [
                    staged_channel / f"{episode_id}{ext}" for ext in MEDIA_EXTENSIONS
                ]
                staged_media = [
                    path
                    for path in staged_media
                    if path.is_file() and path.stat().st_size > 0
                ]
                if not success or not staged_metadata.is_file() or not staged_media:
                    return jsonify(
                        {"error": "Failed to redownload episode; original preserved"}
                    ), 500
                channel_dir.mkdir(parents=True, exist_ok=True)
                thumbnails = channel_dir / "thumbnails"
                thumbnails.mkdir(exist_ok=True)
                for thumbnail in (staged_channel / "thumbnails").glob(
                    f"{episode_id}.*"
                ):
                    thumbnail.replace(thumbnails / thumbnail.name)
                for media in staged_media:
                    media.replace(channel_dir / media.name)
                staged_metadata.replace(json_file)
                new_names = {path.name for path in staged_media}
                for ext in MEDIA_EXTENSIONS:
                    old_media = channel_dir / f"{episode_id}{ext}"
                    if old_media.name not in new_names:
                        old_media.unlink(missing_ok=True)

            # A successful explicit redownload lifts the tombstone so the
            # episode is a normal, downloadable episode again.
            if remove_episode_tombstone(self.config_dir, channel_name, episode_id):
                self.logger.info(f"Cleared tombstone for redownloaded {episode_id}")

            # RSS feed will automatically reflect changes on next request (dynamic generation)
            self.logger.info(f"Successfully redownloaded episode {episode_id}")
            return jsonify(
                {
                    "success": True,
                    "message": f"Episode '{video_title}' redownloaded successfully",
                }
            ), 200

        except Exception as e:
            self.logger.error(
                f"Error redownloading episode {channel_name}/{episode_id}: {e}"
            )
            return jsonify({"error": "Internal server error"}), 500

    def _serve_index(self) -> str:
        """Generate and serve modern index page."""
        # Load channel configuration and refresh timestamps
        config = self._load_channel_config()
        channels = config.get("channels", [])
        refresh_timestamps = self._load_refresh_timestamps()

        # Prepare podcast data
        podcasts = []
        for channel in channels:
            channel_id = channel.get("name", "")
            display_name = channel.get(
                "display_name", channel_id.replace("_", " ").title()
            )
            # Use BASE_URL environment variable if set, otherwise use request host
            base_url = os.getenv("BASE_URL", request.host_url.rstrip("/"))
            feed_url = f"{base_url}/feeds/{channel_id}"
            if self.auth_enabled:
                feed_token = self._get_or_create_feed_token(channel_id)
                if feed_token:
                    feed_url = f"{feed_url}?token={quote(feed_token, safe='')}"

            # Episode count matches the feed and episodes API: filtered
            # and tombstoned episodes are not counted.
            episode_count = len(self._visible_episodes(channel_id, channel))

            # Get last refresh time
            last_refresh_raw = refresh_timestamps.get(channel_id)
            last_refresh = (
                self._format_timestamp(last_refresh_raw) if last_refresh_raw else None
            )

            podcasts.append(
                {
                    "name": display_name,
                    "original_name": channel_id,
                    "display_name": display_name,
                    "url": channel.get("url", ""),
                    "feed_url": feed_url,
                    "episode_count": episode_count,
                    "max_episodes": channel.get("max_episodes", "N/A"),
                    "download_delay_hours": channel.get("download_delay_hours", "N/A"),
                    "format": channel.get("format", "video"),
                    "quality": channel.get("quality", "max"),
                    "sponsorblock_categories": channel.get(
                        "sponsorblock_categories", []
                    ),
                    "last_refresh": last_refresh,
                }
            )

        return render_template(
            "index.html",
            podcasts=podcasts,
            auth_enabled=self.auth_enabled,
            csrf_token=session.get("csrf_token", "") if self.auth_enabled else "",
        )

    def _serve_rss_feed(self, channel_name: str) -> Response:
        """Dynamically generate and serve RSS feed."""

        # Load channel config to get display name
        config = self._load_channel_config()
        channels = config.get("channels", [])
        channel_config = None
        for ch in channels:
            if ch["name"] == channel_name:
                channel_config = ch
                break

        if not channel_config:
            self.logger.warning(f"Channel configuration not found: {channel_name}")
            abort(404)

        # Initialize RSS generator with base URL
        base_url = os.getenv("BASE_URL", request.host_url.rstrip("/"))
        feed_token = (
            self._get_or_create_feed_token(channel_name) if self.auth_enabled else None
        )
        rss_generator = RSSGenerator(base_url, feed_token=feed_token)

        # Generate RSS feed dynamically from filesystem
        try:
            display_name = channel_config.get("display_name")
            episodes = self._visible_episodes(channel_name, channel_config)
            rss_content = rss_generator.generate_rss_feed(
                channel_name, episodes, display_name
            )
            self.logger.info(
                f"Generated dynamic RSS feed for {display_name or channel_name} "
                f"({channel_name}) with {len(episodes)} visible episode(s)"
            )

            # Return RSS content directly (works even with empty feeds)
            return Response(
                rss_content,
                mimetype="application/rss+xml",
                headers={"Content-Type": "application/rss+xml; charset=utf-8"},
            )

        except Exception as e:
            self.logger.error(f"Error generating RSS feed for {channel_name}: {e}")
            abort(500)

    def _serve_video_file(self, channel_name: str, filename: str) -> Response:
        """Serve episode file (video/audio) with range request support."""
        episode_file = self.videos_dir / channel_name / filename

        if not episode_file.is_file():
            self.logger.warning(f"Episode file not found: {episode_file}")
            abort(404)

        # Determine MIME type based on file extension
        file_ext = episode_file.suffix.lower()
        mime_type_map = {
            ".mp4": "video/mp4",
            ".m4a": "audio/mp4",
            ".mp3": "audio/mpeg",
            ".webm": "video/webm",
            ".mkv": "video/x-matroska",
            ".avi": "video/avi",
        }
        mime_type = mime_type_map.get(file_ext, "video/mp4")

        if file_ext not in MEDIA_EXTENSIONS:
            abort(404)
        return send_file(str(episode_file), mimetype=mime_type, conditional=True)

    def _serve_thumbnail_file(self, channel_name: str, filename: str) -> Response:
        """Serve thumbnail image."""
        thumbnail_file = self.videos_dir / channel_name / "thumbnails" / filename

        if not thumbnail_file.is_file() or thumbnail_file.suffix.lower() not in (
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
        ):
            self.logger.warning(f"Thumbnail file not found: {thumbnail_file}")
            abort(404)

        # Determine MIME type based on file extension
        mime_type, _ = mimetypes.guess_type(str(thumbnail_file))
        if not mime_type:
            mime_type = "image/jpeg"  # Default fallback

        try:
            return send_file(
                str(thumbnail_file), mimetype=mime_type, as_attachment=False
            )
        except Exception as e:
            self.logger.error(f"Error serving thumbnail {thumbnail_file}: {e}")
            abort(500)

    def run(self, debug: bool = False):
        """Start the Flask server."""
        self.logger.info(f"Starting YouTube Podcast Server on {self.host}:{self.port}")

        # Log scheduler status
        config = self._load_channel_config()
        default_interval_hours = config.get("default_interval_hours", 24)
        channel_jobs = [
            job
            for job in self.scheduler.get_jobs()
            if job.id.startswith("channel_refresh_")
        ]
        if channel_jobs:
            self.logger.info(
                f"Per-channel refresh scheduled (default: {default_interval_hours}h), {len(channel_jobs)} channels configured"
            )
            for job in channel_jobs[:3]:  # Show first 3 jobs
                self.logger.info(f"  - {job.name}: next run {job.next_run_time}")
            if len(channel_jobs) > 3:
                self.logger.info(f"  ... and {len(channel_jobs) - 3} more channels")

        self.app.run(host=self.host, port=self.port, debug=debug)


def main():
    """Main entry point for web server."""
    server = YouTubePodcastServer()
    server.run()


if __name__ == "__main__":
    main()
