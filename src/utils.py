import json
import logging
import math
import os
import tempfile
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

try:
    import fcntl
except ImportError:  # The web UI can still run with thread locking on Windows.
    fcntl = None

MEDIA_EXTENSIONS = (".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi")
# Sane upper bounds for storage controls (GB / days).
MAX_QUOTA_GB = 16384
MAX_MIN_FREE_GB = 1024
MAX_RETENTION_DAYS = 36500
# Episode filters: YouTube defines Shorts as videos of up to 60 seconds;
# unknown durations are never classified as Shorts.
SHORTS_MAX_DURATION_SECONDS = 60
# yt-dlp ``live_status`` values that mark a currently live or scheduled stream.
LIVE_ACTIVE_STATUSES = ("is_live", "is_upcoming")
# Sane bounds for title filter rule lists (rules, characters per rule).
MAX_TITLE_FILTER_RULES = 100
MAX_TITLE_FILTER_LENGTH = 500
SPONSORBLOCK_CATEGORIES = {
    "sponsor",
    "selfpromo",
    "interaction",
    "intro",
    "outro",
    "preview",
    "music_offtopic",
    "filler",
}


class MediaLock:
    """Coordinate media changes between threads and POSIX server/cron processes."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._thread_lock = threading.Lock()
        self._file: TextIO | None = None

    def acquire(self, blocking: bool = True) -> bool:
        """Acquire the shared media lock, returning False when it is busy."""
        if not self._thread_lock.acquire(blocking=blocking):
            return False
        try:
            if fcntl is not None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._file = self.path.open("a+")
                flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
                fcntl.flock(self._file.fileno(), flags)
            return True
        except OSError as error:
            self.release()
            if isinstance(error, BlockingIOError):
                return False
            raise

    def release(self) -> None:
        """Release the lock without unlinking its shared inode."""
        if self._file is not None:
            self._file.close()
            self._file = None
        self._thread_lock.release()


def safe_component(value: object) -> bool:
    """Check a single filesystem component without rewriting its identity."""
    return (
        isinstance(value, str)
        and value not in ("", ".", "..")
        and not any(c in value for c in "/\\\x00*?[]")
        and all(ord(c) >= 32 for c in value)
    )


def contained_path(root: Path, *parts: str) -> Path:
    """Resolve a path and reject traversal, including escaping symlinks."""
    if not all(safe_component(part) for part in parts):
        raise ValueError("Invalid path component")
    path = root.joinpath(*parts).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Path is outside the storage directory")
    return path


def atomic_write_text(path: Path, content: str) -> None:
    """Replace a text file atomically so readers never see partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            temporary.chmod(path.stat().st_mode & 0o777)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


# Identity of a state file as read: ``(st_ino, st_size, st_mtime_ns)``.
StateIdentity = tuple[int, int, int]


class StateFileError(OSError):
    """A JSON state file exists but could not be read.

    Raised for I/O failures such as permission errors, EIO or exhausted
    file descriptors. The data on disk may be perfectly valid, so such a
    file is never quarantined and callers must never overwrite it.
    """


class CorruptStateFileError(ValueError):
    """A JSON state file was read but holds invalid content.

    Attributes:
        identity: Identity of the file that was parsed, used to make sure
            only that exact file is ever quarantined.
    """

    def __init__(self, message: str, identity: StateIdentity) -> None:
        super().__init__(message)
        self.identity = identity


def require_json_object(data: Any) -> None:
    """Validate that a decoded state file is a JSON object.

    Raises:
        ValueError: When ``data`` is not a dict.
    """
    if not isinstance(data, dict):
        raise ValueError("state file must contain a JSON object")


def _state_identity(info: os.stat_result) -> StateIdentity:
    """Reduce a stat result to the fields that identify one written file."""
    return (info.st_ino, info.st_size, info.st_mtime_ns)


def _read_state_bytes(path: Path) -> tuple[bytes, StateIdentity] | None:
    """Read a state file together with the identity of the inode read.

    Returns:
        ``(content, identity)``, or None when the file does not exist.

    Raises:
        StateFileError: When the file exists but could not be read.
    """
    try:
        with open(path, "rb") as handle:
            identity = _state_identity(os.fstat(handle.fileno()))
            return handle.read(), identity
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as error:
        raise StateFileError(f"Cannot read {path}: {error}") from error


def read_json_state_strict(
    path: Path, *, validate: Callable[[Any], None] | None = None
) -> Any | None:
    """Read a JSON state file, raising for unreadable or invalid content.

    Use this for state that must fail closed: invalid content raises
    instead of reading as empty, and the file is always left in place.

    Args:
        path: State file location.
        validate: Optional shape check raising ValueError or TypeError.

    Returns:
        The decoded JSON value, or None when the file does not exist.

    Raises:
        StateFileError: When the file exists but could not be read.
        CorruptStateFileError: When the content is not UTF-8 JSON or
            fails ``validate``.
    """
    snapshot = _read_state_bytes(path)
    if snapshot is None:
        return None
    raw, identity = snapshot
    try:
        data = json.loads(raw.decode("utf-8"))
        if validate is not None:
            validate(data)
    except (ValueError, TypeError) as error:
        raise CorruptStateFileError(
            f"{path.name} is corrupt: {error}", identity
        ) from error
    return data


def read_json_state(
    path: Path,
    *,
    validate: Callable[[Any], None] | None = None,
    quarantine: bool = True,
    logger: logging.Logger | None = None,
) -> Any | None:
    """Read a JSON state file, treating corrupt content as empty.

    I/O errors always propagate so callers never mistake an unreadable
    file for an empty one. Corrupt content is logged and, when
    ``quarantine`` is set, moved aside by :func:`quarantine_state_file`.

    Args:
        path: State file location.
        validate: Optional shape check raising ValueError or TypeError.
        quarantine: Move a corrupt file aside. Lock-free readers pass
            False and leave that to writers.
        logger: Optional logger; defaults to this module's logger.

    Returns:
        The decoded JSON value, or None when the file is missing or
        corrupt.

    Raises:
        StateFileError: When the file exists but could not be read; it is
            never quarantined for that.
    """
    log = logger if logger is not None else logging.getLogger(__name__)
    try:
        return read_json_state_strict(path, validate=validate)
    except CorruptStateFileError as error:
        if not quarantine:
            log.error(f"{error}; treating it as empty")
            return None
        log.error(f"{error}; starting empty")
        quarantine_state_file(path, error.identity, log)
        return None


def _free_backup_path(path: Path) -> Path:
    """Pick a quarantine name that never overwrites an earlier backup."""
    candidate = path.with_name(f"{path.name}.corrupt")
    if not os.path.lexists(candidate):
        return candidate
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    candidate = path.with_name(f"{path.name}.corrupt.{stamp}")
    counter = 1
    while os.path.lexists(candidate):
        candidate = path.with_name(f"{path.name}.corrupt.{stamp}.{counter}")
        counter += 1
    return candidate


def quarantine_state_file(
    path: Path, identity: StateIdentity, logger: logging.Logger
) -> Path | None:
    """Move a corrupt state file aside unless it changed after parsing.

    Writers replace state files atomically, so by the time a reader has
    found its copy corrupt the path may already hold a fresh, valid file.
    Comparing inode, size and mtime with the parsed file keeps such a file
    in place. The first backup is ``<name>.corrupt``; later ones get a UTC
    timestamp (and a counter) so earlier backups are never overwritten.

    Args:
        path: State file location.
        identity: Identity of the file that was found corrupt.
        logger: Logger for the outcome.

    Returns:
        The backup path, or None when the file was left in place.
    """
    try:
        if _state_identity(path.stat()) != identity:
            logger.warning(f"{path.name} changed after it was read; left in place")
            return None
        backup = _free_backup_path(path)
        path.rename(backup)
    except OSError as error:
        logger.error(f"Could not back up corrupt {path.name}: {error}")
        return None
    logger.info(f"Kept corrupt {path.name} as {backup.name}")
    return backup


def validate_bounded_number(field: str, value: Any, maximum: float) -> None:
    """Require a finite number in ``[0, maximum]``; booleans are rejected.

    Args:
        field: Setting name used in the error message.
        value: Candidate value.
        maximum: Inclusive upper bound.

    Raises:
        ValueError: When the value is not a finite number in bounds.
    """
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or value < 0
        or value > maximum
    ):
        raise ValueError(f"{field} must be a number between 0 and {maximum:g}")


def validate_channel_settings(data: dict[str, Any]) -> None:
    """Reject invalid settings before they reach scheduling or deletion logic."""
    if "max_episodes" in data:
        value = data["max_episodes"]
        if type(value) is not int or value <= 0:
            raise ValueError("max_episodes must be a positive integer")
    for field, maximum in (
        ("global_quota_gb", MAX_QUOTA_GB),
        ("min_free_space_gb", MAX_MIN_FREE_GB),
        ("retention_days", MAX_RETENTION_DAYS),
        ("quota_gb", MAX_QUOTA_GB),
    ):
        if field in data:
            validate_bounded_number(field, data[field], maximum)
    for field in (
        "download_delay_hours",
        "check_delay_seconds",
        "download_delay_seconds",
        "refresh_interval_hours",
    ):
        if field not in data:
            continue
        value = data[field]
        if (
            type(value) not in (int, float)
            or not math.isfinite(value)
            or value < 0
            or (field == "refresh_interval_hours" and (value == 0 or value > 8760))
        ):
            message = (
                "must be a positive number, finite and <= 8760"
                if field == "refresh_interval_hours"
                else "must be a finite non-negative number"
            )
            raise ValueError(f"{field} {message}")
    if "format" in data and data["format"] not in ("video", "audio"):
        raise ValueError("format must be video or audio")
    if "quality" in data and data["quality"] not in (
        "max",
        "best",
        "480p",
        "720p",
        "1080p",
    ):
        raise ValueError("Unsupported quality")
    if "sponsorblock_categories" in data:
        categories = data["sponsorblock_categories"]
        if not isinstance(categories, list) or any(
            not isinstance(category, str) or category not in SPONSORBLOCK_CATEGORIES
            for category in categories
        ):
            raise ValueError(
                "sponsorblock_categories must be a list of supported categories"
            )
    for field in ("title_include", "title_exclude"):
        if field not in data:
            continue
        rules = data[field]
        if not isinstance(rules, list) or len(rules) > MAX_TITLE_FILTER_RULES:
            raise ValueError(
                f"{field} must be a list of at most {MAX_TITLE_FILTER_RULES} strings"
            )
        for rule in rules:
            if not isinstance(rule, str) or not rule.strip():
                raise ValueError(f"{field} entries must be non-empty strings")
            if len(rule) > MAX_TITLE_FILTER_LENGTH:
                raise ValueError(
                    f"{field} entries must be at most "
                    f"{MAX_TITLE_FILTER_LENGTH} characters"
                )
    for field in ("min_duration_seconds", "max_duration_seconds"):
        if field in data and (type(data[field]) is not int or data[field] < 0):
            raise ValueError(
                f"{field} must be a non-negative integer (0 disables the limit)"
            )
    if (
        type(data.get("min_duration_seconds")) is int
        and type(data.get("max_duration_seconds")) is int
        and data["min_duration_seconds"] > data["max_duration_seconds"]
    ):
        raise ValueError("min_duration_seconds must not exceed max_duration_seconds")
    for field in ("skip_shorts", "skip_livestreams"):
        if field in data and type(data[field]) is not bool:
            raise ValueError(f"{field} must be true or false")


def load_episode_filters(channel: dict[str, Any]) -> dict[str, Any]:
    """Resolve a channel's episode filters, defaulting invalid values.

    Mirrors :func:`storage.load_storage_settings`: values written through
    the API are already validated, but a hand-edited ``channels.yaml`` can
    carry anything, so wrong types fall back to "no filter" instead of
    crashing discovery or downloads.

    Args:
        channel: Channel configuration entry.

    Returns:
        A new dict with ``title_include``/``title_exclude`` rule lists,
        ``min_duration_seconds``/``max_duration_seconds`` (``0`` = no
        bound) and the ``skip_shorts``/``skip_livestreams`` flags.
    """

    def rules(field: str) -> list[str]:
        value = channel.get(field)
        if not isinstance(value, list):
            return []
        return [rule for rule in value if isinstance(rule, str) and rule.strip()]

    def seconds(field: str) -> int:
        value = channel.get(field)
        return value if type(value) is int and value > 0 else 0

    return {
        "title_include": rules("title_include"),
        "title_exclude": rules("title_exclude"),
        "min_duration_seconds": seconds("min_duration_seconds"),
        "max_duration_seconds": seconds("max_duration_seconds"),
        "skip_shorts": channel.get("skip_shorts") is True,
        "skip_livestreams": channel.get("skip_livestreams") is True,
    }


def is_livestream_entry(entry: dict[str, Any]) -> bool:
    """Detect livestreams in yt-dlp entries and episode metadata.

    Flat channel entries carry ``live_status`` (``is_live``, ``is_upcoming``,
    ...); older extractions expose the ``is_live``/``was_live`` booleans
    instead. Past broadcasts flagged ``was_live`` also count as livestreams.

    Args:
        entry: Flat yt-dlp entry or episode metadata mapping.

    Returns:
        True when the entry represents a livestream.
    """
    if not isinstance(entry, dict):
        return False
    status = entry.get("live_status")
    if isinstance(status, str) and status in LIVE_ACTIVE_STATUSES:
        return True
    return bool(entry.get("is_live")) or bool(entry.get("was_live"))


def _matched_title_rule(title: str, rules: list[str]) -> str | None:
    """Return the first rule contained in the title, or None.

    Matching is case-insensitive (``casefold`` for unicode stability) and
    ignores surrounding whitespace on the configured rules.
    """
    folded = title.casefold()
    for rule in rules:
        if rule.strip().casefold() in folded:
            return rule
    return None


def filter_skip_reason(video: dict[str, Any], filters: dict[str, Any]) -> str | None:
    """Explain why a video is excluded by a channel's episode filters.

    Exclusion wins over inclusion. Durations and live status that a source
    does not provide are never used against the video: unknown durations
    cannot be classified as Shorts and cannot violate duration bounds, and
    entries without live markers are not livestreams.

    Args:
        video: Flat yt-dlp entry or on-disk episode metadata.
        filters: Resolved filters from :func:`load_episode_filters`.

    Returns:
        A human-readable reason mentioning the matched rule, or None when
        the video passes every configured filter.
    """
    title = str(video.get("title") or "")
    excluded = _matched_title_rule(title, filters.get("title_exclude") or [])
    if excluded is not None:
        return f"title matches exclude rule '{excluded}'"
    include = filters.get("title_include") or []
    if include:
        included = _matched_title_rule(title, include)
        if included is None:
            return f"title matches no include rule ({', '.join(include)})"
    if filters.get("skip_livestreams") and is_livestream_entry(video):
        status = video.get("live_status")
        detail = f"live_status={status}" if status else "live flags set"
        return f"livestream ({detail})"
    duration = video.get("duration")
    # Unknown durations never violate bounds and are never Shorts.
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or duration < 0
    ):
        return None
    minimum = filters.get("min_duration_seconds") or 0
    maximum = filters.get("max_duration_seconds") or 0
    if minimum > 0 and duration < minimum:
        return f"duration {duration}s is below min_duration_seconds ({minimum}s)"
    if maximum > 0 and duration > maximum:
        return f"duration {duration}s exceeds max_duration_seconds ({maximum}s)"
    if filters.get("skip_shorts") and duration <= SHORTS_MAX_DURATION_SECONDS:
        return (
            f"YouTube Short (duration {duration:g}s <= {SHORTS_MAX_DURATION_SECONDS}s)"
        )
    return None


def cleanup_episodes(
    videos_dir: Path,
    channel_name: str,
    max_episodes: int,
    pinned: set[str] | None = None,
) -> None:
    """Prune completed episodes using filenames as the trusted identity.

    Pinned episodes are never removed; when pins occupy slots the channel
    may keep more than ``max_episodes`` episodes.

    Args:
        videos_dir: Storage root containing the channel directory.
        channel_name: Sanitized channel identifier.
        max_episodes: Number of newest episodes to keep.
        pinned: Episode ids that must never be deleted.
    """
    validate_channel_settings({"max_episodes": max_episodes})
    pinned = pinned or set()
    channel_dir = contained_path(videos_dir, channel_name)
    episodes = []
    for metadata_file in channel_dir.glob("*.json"):
        if metadata_file.name.endswith(".info.json") or metadata_file.is_symlink():
            continue
        episode_id = metadata_file.stem
        if not safe_component(episode_id):
            continue
        metadata = load_json_file(str(metadata_file))
        if not metadata or metadata.get("id") != episode_id:
            continue
        if not any(
            contained_path(channel_dir, f"{episode_id}{ext}").is_file()
            for ext in MEDIA_EXTENSIONS
        ):
            continue
        episodes.append(
            (str(metadata.get("upload_date") or ""), episode_id, metadata_file)
        )
    episodes.sort()
    deletable = [entry for entry in episodes if entry[1] not in pinned]
    for _, episode_id, metadata_file in deletable[:-max_episodes]:
        for ext in MEDIA_EXTENSIONS:
            contained_path(channel_dir, f"{episode_id}{ext}").unlink(missing_ok=True)
        thumbnails = contained_path(channel_dir, "thumbnails")
        for thumbnail in thumbnails.glob(f"{episode_id}.*"):
            thumbnail.unlink()
        metadata_file.unlink(missing_ok=True)


def setup_logging() -> logging.Logger:
    """Setup logging configuration to stdout only."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    return logging.getLogger(__name__)


def ensure_directory(path: str) -> Path:
    """Ensure directory exists and return Path object."""
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def load_json_file(file_path: str) -> dict[str, Any]:
    """Load JSON file and return data."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as e:
        logging.error(f"Failed to load JSON file {file_path}: {e}")
        return {}


def save_json_file(data: dict[str, Any], file_path: str) -> bool:
    """Save data to JSON file."""
    try:
        atomic_write_text(Path(file_path), json.dumps(data, indent=2))
        return True
    except Exception as e:
        logging.error(f"Failed to save JSON file {file_path}: {e}")
        return False


def get_episode_files(channel_dir: str) -> list[dict[str, Any]]:
    """Get all episode files (video/audio) and metadata from channel directory."""
    channel_path = Path(channel_dir)
    if not channel_path.exists():
        return []

    episodes = []
    # Support both video and audio file extensions
    supported_extensions = [".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi"]

    for json_file in channel_path.glob("*.json"):
        episode_id = json_file.stem
        episode_file = None

        # Check for episode file with any supported extension
        for ext in supported_extensions:
            potential_file = channel_path / f"{episode_id}{ext}"
            if potential_file.exists():
                episode_file = potential_file
                break

        if episode_file and episode_file.exists():
            metadata = load_json_file(str(json_file))
            if metadata:
                metadata["file_path"] = str(episode_file)
                metadata["id"] = episode_id
                episodes.append(metadata)

    # Sort by upload date (newest first)
    episodes.sort(key=lambda x: str(x.get("upload_date") or ""), reverse=True)
    return episodes


def get_video_files(channel_dir: str) -> list[dict[str, Any]]:
    """Legacy function name for backward compatibility."""
    return get_episode_files(channel_dir)


def clean_filename(filename: str) -> str:
    """Clean filename for filesystem compatibility."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")
    return filename.strip()


def format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable format."""
    if not seconds:
        return "Unknown"

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except Exception:
        return 0.0


def sanitize_channel_name(display_name: str) -> str:
    """Sanitize display name to create a valid channel ID for filesystem usage.

    Args:
        display_name: The display name that may contain spaces and special characters

    Returns:
        A sanitized string safe for use as filesystem directory names and URLs
    """
    if not display_name:
        return ""

    import re

    # Convert to lowercase
    sanitized = display_name.lower()

    # Replace spaces and common separators with underscores
    sanitized = re.sub(r"[\s\-\.]+", "_", sanitized)

    # Remove any characters that aren't alphanumeric or underscores
    sanitized = re.sub(r"[^a-z0-9_]", "", sanitized)

    # Remove leading/trailing underscores and collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")

    # Ensure minimum length
    if len(sanitized) < 2:
        sanitized = f"channel_{sanitized}" if sanitized else "channel"

    return sanitized
