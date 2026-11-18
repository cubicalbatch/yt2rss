import dataclasses
import json
import logging
import math
import os
import stat
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, TextIO
from urllib.parse import urlsplit

try:
    import fcntl
except ImportError:  # The web UI can still run with thread locking on Windows.
    fcntl = None

# Every supported episode media suffix and the MIME type it is served with
# (feed enclosures, streaming and downloads). Order is the lookup order.
MEDIA_MIME_TYPES: dict[str, str] = {
    ".mp4": "video/mp4",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".webm": "video/webm",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
}
MEDIA_EXTENSIONS: tuple[str, ...] = tuple(MEDIA_MIME_TYPES)
# Media a download of each channel format produces, preferred suffix first.
AUDIO_EXTENSIONS: tuple[str, ...] = (".m4a", ".mp3")
VIDEO_EXTENSIONS: tuple[str, ...] = (".mp4", ".webm", ".mkv", ".avi")
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
# Optional proxy URL applied to every yt-dlp call (see the README).
YT_DLP_PROXY_ENV = "YT_DLP_PROXY"
# Proxy schemes yt-dlp supports; any other scheme is never echoed to logs.
PROXY_SCHEMES = frozenset({"http", "https", "socks4", "socks4a", "socks5", "socks5h"})
# Logged instead of a proxy URL that cannot be reduced to scheme://host[:port].
PROXY_LOG_PLACEHOLDER = "<configured proxy>"


class MediaLock:
    """Coordinate media changes between threads and POSIX server/cron processes."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._thread_lock = threading.Lock()
        self._file: TextIO | None = None
        self._owner: int | None = None

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
            self._owner = threading.get_ident()
            return True
        except OSError as error:
            self.release()
            if isinstance(error, BlockingIOError):
                return False
            raise

    def release(self) -> None:
        """Release the lock without unlinking its shared inode."""
        self._owner = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._thread_lock.release()

    def held_by_current_thread(self) -> bool:
        """Report whether the calling thread acquired this lock."""
        return self._owner == threading.get_ident()


# One MediaLock per resolved lock file, shared by every caller in a process.
_state_file_locks: dict[Path, MediaLock] = {}
_state_file_locks_guard = threading.Lock()


def state_lock_path(path: Path) -> Path:
    """Return the sibling lock file guarding updates of state file ``path``."""
    return path.with_name(f"{path.name}.lock")


def _state_file_lock_for(path: Path) -> MediaLock:
    """Return the process-wide lock for ``path``, keyed by its resolved path."""
    lock_path = state_lock_path(Path(path).resolve())
    with _state_file_locks_guard:
        return _state_file_locks.setdefault(lock_path, MediaLock(lock_path))


def _acquire_state_lock(lock: MediaLock, path: Path) -> bool:
    """Block until ``lock`` is held; False (logged) when it cannot be taken."""
    try:
        return lock.acquire(blocking=True)
    except OSError as error:
        logging.getLogger(__name__).error(
            f"Cannot lock {path.name} for an update: {error}"
        )
        return False


@contextmanager
def state_file_lock(path: Path) -> Iterator[bool]:
    """Serialize read-modify-write cycles of a JSON state file.

    Threads and processes (the web server and the cron runner may share one
    config directory) are serialized with a :class:`MediaLock` on the
    sibling ``<name>.lock`` file (see :func:`state_lock_path`). The data
    file itself is never locked because writers replace it atomically, and
    the lock file is never unlinked, so every process always locks the same
    inode. Without ``fcntl`` only threads are serialized. A missing
    directory is created like :func:`atomic_write_text` does.

    The lock is not reentrant. Two flocks taken by one process on the same
    file block each other, so every caller in a process shares one lock per
    resolved path, and call sites must hold it only around a single
    load-modify-save of that file: never nest it for the same path and
    never do slow work such as network requests while holding it. Nesting
    raises RuntimeError instead of deadlocking. Read-only paths need no
    lock because writes are atomic renames.

    Args:
        path: The state file whose update is being guarded.

    Yields:
        True while the lock is held; False (logged) when it could not be
        taken, e.g. on a read-only directory. The caller must then skip its
        write.

    Raises:
        RuntimeError: When the calling thread already holds this lock.
    """
    lock = _state_file_lock_for(path)
    if lock.held_by_current_thread():
        raise RuntimeError(f"The {path.name} lock is already held by this thread")
    if not _acquire_state_lock(lock, path):
        yield False
        return
    try:
        yield True
    finally:
        lock.release()


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


# ---------- episode files ----------
#
# One episode is stored as ``<channel>/<id>.json`` (metadata),
# ``<channel>/<id><media suffix>`` and ``<channel>/thumbnails/<id>.*``.


def media_extensions_for(format_type: str) -> tuple[str, ...]:
    """Return the media suffixes a channel format produces, preferred first.

    Args:
        format_type: Channel format, ``audio`` or ``video``; anything else
            is treated as ``video`` like the downloader does.
    """
    return AUDIO_EXTENSIONS if format_type == "audio" else VIDEO_EXTENSIONS


def find_episode_media(
    channel_dir: Path,
    episode_id: str,
    extensions: tuple[str, ...] = MEDIA_EXTENSIONS,
) -> Path | None:
    """Find an episode's media file, trying ``extensions`` in order.

    Only regular files inside ``channel_dir`` count: directories, symlinks
    resolving outside it and unsafe episode ids are skipped.

    Args:
        channel_dir: The channel's media directory.
        episode_id: Episode (YouTube video) identifier.
        extensions: Media suffixes in preference order.

    Returns:
        The resolved media path, or None when no candidate exists.
    """
    if not safe_component(episode_id):
        return None
    for extension in extensions:
        try:
            candidate = contained_path(channel_dir, f"{episode_id}{extension}")
        except ValueError:
            continue
        if candidate.is_file():
            return candidate
    return None


class StoredEpisode(NamedTuple):
    """A complete episode on disk: matching metadata plus a media file."""

    episode_id: str
    metadata: dict[str, Any]
    metadata_file: Path
    media_file: Path


def collect_episodes(channel_dir: Path) -> list[StoredEpisode]:
    """List complete episodes, using filenames as the trusted identity.

    yt-dlp ``*.info.json`` leftovers, symlinked or unsafely named metadata,
    metadata whose ``id`` differs from its filename and episodes without
    media are skipped.

    Args:
        channel_dir: The channel's media directory; may be missing.

    Returns:
        The complete episodes in no particular order.
    """
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
        media_file = find_episode_media(channel_dir, episode_id)
        if media_file is not None:
            episodes.append(
                StoredEpisode(episode_id, metadata, metadata_file, media_file)
            )
    return episodes


@dataclasses.dataclass
class RemovedEpisodeFiles:
    """Files :func:`delete_episode_files` removed for one episode.

    Attributes:
        media: Removed media files in ``MEDIA_EXTENSIONS`` order.
        thumbnails: Removed thumbnail files.
        metadata: The removed metadata file, or None when there was none.
    """

    media: list[Path] = dataclasses.field(default_factory=list)
    thumbnails: list[Path] = dataclasses.field(default_factory=list)
    metadata: Path | None = None

    @property
    def paths(self) -> list[Path]:
        """All removed files: media, thumbnails, then metadata."""
        metadata = [self.metadata] if self.metadata is not None else []
        return [*self.media, *self.thumbnails, *metadata]


def _remove_entry(path: Path) -> bool:
    """Unlink one directory entry without following symlinks.

    Returns:
        True when the entry was removed; False when it does not exist or
        is a directory.

    Raises:
        OSError: When the entry exists but cannot be removed.
    """
    try:
        if stat.S_ISDIR(path.lstat().st_mode):
            return False
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def _remove_thumbnails(
    channel_dir: Path, episode_id: str, logger: logging.Logger
) -> list[Path]:
    """Remove an episode's thumbnails; failures are logged, never raised."""
    try:
        thumbnails_dir = contained_path(channel_dir, "thumbnails")
        thumbnails = list(thumbnails_dir.glob(f"{episode_id}.*"))
    except (OSError, ValueError) as error:
        logger.warning(f"Could not list thumbnails of {episode_id}: {error}")
        return []
    removed = []
    for thumbnail in thumbnails:
        try:
            thumbnail.unlink()
        except OSError as error:
            logger.warning(f"Could not remove thumbnail {thumbnail}: {error}")
            continue
        removed.append(thumbnail)
    return removed


def delete_episode_files(
    channel_dir: Path, episode_id: str, logger: logging.Logger
) -> RemovedEpisodeFiles:
    """Remove one episode's media, thumbnails and metadata.

    Entries are unlinked without following symlinks, so nothing outside
    ``channel_dir`` is touched. Thumbnail failures are logged and never
    stop the deletion. Metadata goes last: when media cannot be removed
    the episode stays listed and a later pass retries it.

    Args:
        channel_dir: The channel's (already contained) media directory.
        episode_id: Episode identifier; must be a safe filename component.
        logger: Receives thumbnail failures and one debug line per file.

    Returns:
        The files that were removed.

    Raises:
        ValueError: When ``episode_id`` is not a safe filename component.
        OSError: When a media or metadata file cannot be removed.
    """
    if not safe_component(episode_id):
        raise ValueError("Invalid episode id")
    removed = RemovedEpisodeFiles()
    for extension in MEDIA_EXTENSIONS:
        media = channel_dir / f"{episode_id}{extension}"
        if _remove_entry(media):
            removed.media.append(media)
    removed.thumbnails = _remove_thumbnails(channel_dir, episode_id, logger)
    metadata = channel_dir / f"{episode_id}.json"
    if _remove_entry(metadata):
        removed.metadata = metadata
    for path in removed.paths:
        logger.debug(f"Removed {path}")
    return removed


def _episode_timestamp(metadata: dict[str, Any]) -> float:
    """Return the metadata's epoch ``timestamp``; missing or invalid is -inf."""
    value = metadata.get("timestamp")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return -math.inf
    if isinstance(value, float) and math.isnan(value):
        return -math.inf
    return value


def _episode_age_key(episode: StoredEpisode) -> tuple[str, float, str]:
    """Order episodes oldest first.

    ``upload_date`` (``YYYYMMDD``) decides; same-day uploads are ordered
    by ``timestamp`` with a missing one counting as oldest, and finally by
    id so the order is deterministic.
    """
    return (
        str(episode.metadata.get("upload_date") or ""),
        _episode_timestamp(episode.metadata),
        episode.episode_id,
    )


def cleanup_episodes(
    videos_dir: Path,
    channel_name: str,
    max_episodes: int,
    pinned: set[str] | None = None,
) -> None:
    """Prune completed episodes using filenames as the trusted identity.

    The oldest episodes go first (see :func:`_episode_age_key`). Pinned
    episodes are never removed; when pins occupy slots the channel may
    keep more than ``max_episodes`` episodes. A thumbnail that cannot be
    removed is logged and never stops the pass.

    Args:
        videos_dir: Storage root containing the channel directory.
        channel_name: Sanitized channel identifier.
        max_episodes: Number of newest episodes to keep.
        pinned: Episode ids that must never be deleted.
    """
    validate_channel_settings({"max_episodes": max_episodes})
    pinned = pinned or set()
    channel_dir = contained_path(videos_dir, channel_name)
    episodes = sorted(collect_episodes(channel_dir), key=_episode_age_key)
    deletable = [episode for episode in episodes if episode.episode_id not in pinned]
    logger = logging.getLogger(__name__)
    for episode in deletable[:-max_episodes]:
        delete_episode_files(channel_dir, episode.episode_id, logger)


# Proxy values already announced in this process (see ``yt_dlp_proxy_opts``).
_announced_proxies: set[str] = set()
_announced_proxies_guard = threading.Lock()


def redact_proxy_url(proxy_url: str) -> str:
    """Reduce a proxy URL to ``scheme://host[:port]`` for logging.

    Userinfo, path, query and fragment are always dropped. The placeholder
    is returned when the value does not parse, has no hostname, uses a
    scheme yt-dlp does not support, or has an ``@`` after the host (an
    unescaped ``/``, ``?`` or ``#`` in credentials pushes them there), so
    credentials can never reach the logs.

    Args:
        proxy_url: The configured proxy URL.

    Returns:
        The redacted proxy address, or :data:`PROXY_LOG_PLACEHOLDER`.
    """
    try:
        parts = urlsplit(proxy_url)
        hostname = parts.hostname
        port = parts.port
    except ValueError:
        return PROXY_LOG_PLACEHOLDER
    trailing = parts.path + parts.query + parts.fragment
    if parts.scheme not in PROXY_SCHEMES or not hostname or "@" in trailing:
        return PROXY_LOG_PLACEHOLDER
    host = f"[{hostname}]" if ":" in hostname else hostname
    address = f"{parts.scheme}://{host}"
    return address if port is None else f"{address}:{port}"


def _claim_proxy_announcement(proxy_url: str) -> bool:
    """Record ``proxy_url`` as announced, reporting whether it was new.

    Args:
        proxy_url: The configured proxy URL.

    Returns:
        True for exactly one caller per distinct value in this process.
    """
    with _announced_proxies_guard:
        if proxy_url in _announced_proxies:
            return False
        _announced_proxies.add(proxy_url)
        return True


def yt_dlp_proxy_opts(logger: logging.Logger | None = None) -> dict[str, str]:
    """Build yt-dlp proxy options from the ``YT_DLP_PROXY`` environment variable.

    The value is passed to yt-dlp unchanged apart from surrounding whitespace.
    Each distinct value is announced at INFO once per process, redacted by
    :func:`redact_proxy_url`, so frequent metadata fetches and downloads do
    not repeat it.

    Args:
        logger: Logger for the announcement; defaults to this module's logger.

    Returns:
        ``{"proxy": value}`` when a proxy is configured, otherwise ``{}``.
    """
    proxy_url = os.getenv(YT_DLP_PROXY_ENV, "").strip()
    if not proxy_url:
        return {}
    if _claim_proxy_announcement(proxy_url):
        (logger or logging.getLogger(__name__)).info(
            f"🔐 Using proxy: {redact_proxy_url(proxy_url)}"
        )
    return {"proxy": proxy_url}


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
