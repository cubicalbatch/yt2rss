"""Storage controls: quotas, low-space protection, retention, and pins.

Accounting definition: usage counts only regular media files whose suffix is
in :data:`MEDIA_EXTENSIONS`. Thumbnails and ``*.json`` metadata are excluded
so the numbers track playable episodes. Symlinks are never followed, so
media linked in from outside the storage root is neither counted nor
traversed.

Pins live in ``config_dir/pins.json`` as ``{"<channel>": ["<video_id>", ...]}``
so they survive episode metadata rewrites and redownloads; they are removed
only when the episode or its channel is deleted. Pins guard irreversible
deletions, so a corrupt or unreadable ``pins.json`` fails closed: it raises
:class:`PinsUnavailableError`, cleanup and retention pause, and the file is
never quarantined or overwritten automatically.

Tombstones live in ``config_dir/tombstones.json`` as
``{"<channel>": {"<video_id>": {"deleted_at": ..., "title": ...}}}`` and
remember manually deleted episodes so discovery, queue execution, the RSS
feed and the episodes API never bring them back. An explicit channel purge
(forget via the API) or channel deletion clears them. A corrupt file is
quarantined and reads as empty; an unreadable one raises
:class:`~utils.StateFileError` so callers fail closed instead of
forgetting every deletion.
"""

import json
import logging
import os
import shutil
import stat
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from .utils import (
        MAX_MIN_FREE_GB,
        MAX_QUOTA_GB,
        MAX_RETENTION_DAYS,
        MEDIA_EXTENSIONS,
        CorruptStateFileError,
        StateFileError,
        atomic_write_text,
        collect_episodes,
        contained_path,
        delete_episode_files,
        read_json_state,
        read_json_state_strict,
        require_json_object,
        validate_bounded_number,
        validate_channel_settings,
    )
except ImportError:
    from utils import (
        MAX_MIN_FREE_GB,
        MAX_QUOTA_GB,
        MAX_RETENTION_DAYS,
        MEDIA_EXTENSIONS,
        CorruptStateFileError,
        StateFileError,
        atomic_write_text,
        collect_episodes,
        contained_path,
        delete_episode_files,
        read_json_state,
        read_json_state_strict,
        require_json_object,
        validate_bounded_number,
        validate_channel_settings,
    )

GB = 1024**3

Number = int | float

DEFAULT_STORAGE_SETTINGS: dict[str, Number] = {
    "global_quota_gb": 0,  # 0 = unlimited
    "min_free_space_gb": 1,  # 0 = low-space protection disabled
    "retention_days": 0,  # 0 = retention disabled
}

STORAGE_FIELD_BOUNDS: dict[str, float] = {
    "global_quota_gb": MAX_QUOTA_GB,
    "min_free_space_gb": MAX_MIN_FREE_GB,
    "retention_days": MAX_RETENTION_DAYS,
}

# Worst-case size assumed for a download when nothing better is known.
DEFAULT_ESTIMATE_VIDEO_BYTES = 2 * GB
DEFAULT_ESTIMATE_AUDIO_BYTES = 200 * 1024 * 1024

SECONDS_PER_DAY = 86400

PINS_FILENAME = "pins.json"
_pins_lock = threading.Lock()


def is_positive_number(value: Any) -> bool:
    """Check a finite number above zero, rejecting bools."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


# ---------- settings ----------


def validate_storage_settings(data: dict[str, Any]) -> None:
    """Reject invalid global storage settings with a readable error.

    Args:
        data: Mapping of setting names to candidate values.

    Raises:
        ValueError: When a key is unknown or a value is not a finite
            number within its bounds. Booleans and strings are rejected.
    """
    if not isinstance(data, dict):
        raise ValueError("storage settings must be an object")
    for key, value in data.items():
        maximum = STORAGE_FIELD_BOUNDS.get(key)
        if maximum is None:
            raise ValueError(f"Unknown storage setting: {key}")
        validate_bounded_number(key, value, maximum)


def load_storage_settings(config: dict[str, Any]) -> dict[str, Number]:
    """Read global storage settings, filling defaults for missing keys.

    Invalid values (wrong type, negative) fall back to their default so a
    hand-edited ``channels.yaml`` can never disable protection silently.

    Args:
        config: Full parsed configuration including the ``storage`` key.

    Returns:
        A new dict with all three settings resolved.
    """
    raw = config.get("storage")
    settings = dict(DEFAULT_STORAGE_SETTINGS)
    if isinstance(raw, dict):
        for key in settings:
            value = raw.get(key)
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and value >= 0
            ):
                settings[key] = value
    return settings


def effective_channel_storage(
    settings: dict[str, Number], channel: dict[str, Any]
) -> dict[str, Number]:
    """Resolve a channel's quota and retention overrides against globals.

    A per-channel ``quota_gb`` or ``retention_days`` of 0 (or absent) means
    "inherit the global setting".

    Args:
        settings: Global settings from :func:`load_storage_settings`.
        channel: Channel configuration entry.

    Returns:
        Effective ``quota_gb``, ``retention_days`` and
        ``min_free_space_gb`` for the channel.
    """
    quota = channel.get("quota_gb", 0)
    quota = quota if is_positive_number(quota) else settings["global_quota_gb"]
    retention = channel.get("retention_days", 0)
    retention = (
        retention if is_positive_number(retention) else settings["retention_days"]
    )
    return {
        "quota_gb": quota,
        "retention_days": retention,
        "min_free_space_gb": settings["min_free_space_gb"],
    }


# ---------- accounting ----------


def _iter_media_files(root: Path) -> Iterator[tuple[Path, int]]:
    """Yield ``(path, size)`` for regular media files under ``root``.

    Directory and file symlinks are skipped entirely so nothing outside
    the storage root is followed or counted.
    """
    if not root.is_dir():
        return
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            if not name.endswith(MEDIA_EXTENSIONS):
                continue
            path = Path(dirpath) / name
            try:
                info = path.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            yield path, info.st_size


def measure_media_usage(videos_dir: Path) -> dict[str, dict[str, int]]:
    """Measure media usage per channel directory.

    Args:
        videos_dir: Storage root containing one directory per channel.

    Returns:
        Mapping of channel directory name to ``{"bytes": size,
        "episodes": count}``. Only regular media files count; thumbnails,
        metadata and symlinks are excluded.
    """
    usage: dict[str, dict[str, int]] = {}
    root = Path(videos_dir)
    if not root.is_dir():
        return usage
    with os.scandir(root) as entries:
        for entry in entries:
            if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                continue
            total = 0
            count = 0
            for _path, size in _iter_media_files(Path(entry.path)):
                total += size
                count += 1
            usage[entry.name] = {"bytes": total, "episodes": count}
    return usage


def channel_usage(videos_dir: Path, channel_name: str) -> dict[str, int]:
    """Measure media usage for a single channel.

    Args:
        videos_dir: Storage root.
        channel_name: Sanitized channel identifier.

    Returns:
        ``{"bytes": size, "episodes": count}`` for the channel.

    Raises:
        ValueError: When the channel name escapes the storage root.
    """
    total = 0
    count = 0
    for _path, size in _iter_media_files(
        contained_path(Path(videos_dir), channel_name)
    ):
        total += size
        count += 1
    return {"bytes": total, "episodes": count}


def free_disk_bytes(path: Path) -> int:
    """Return free bytes on the filesystem that holds (or will hold) ``path``.

    When ``path`` does not exist yet, the nearest existing ancestor is
    queried so a fresh storage root never disables the low-space check.

    Raises:
        OSError: When the filesystem cannot be queried.
    """
    probe = Path(path)
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            break
        probe = parent
    return shutil.disk_usage(probe).free


def estimate_download_bytes(
    channel_dir: Path, format_type: str, metadata_bytes: int | float | None = None
) -> int:
    """Estimate the worst-case size of one new download.

    The estimate is the largest of: a metadata size hint, the largest
    episode already stored in the channel, and a per-format default.

    Args:
        channel_dir: The channel's media directory.
        format_type: ``video`` or ``audio``.
        metadata_bytes: Optional size hint from yt-dlp metadata.

    Returns:
        A conservative byte estimate for admission checks.
    """
    candidates = [
        DEFAULT_ESTIMATE_AUDIO_BYTES
        if format_type == "audio"
        else DEFAULT_ESTIMATE_VIDEO_BYTES
    ]
    if (
        isinstance(metadata_bytes, (int, float))
        and not isinstance(metadata_bytes, bool)
        and metadata_bytes > 0
    ):
        candidates.append(int(metadata_bytes))
    largest = max((size for _path, size in _iter_media_files(channel_dir)), default=0)
    if largest > 0:
        candidates.append(largest)
    return max(candidates)


def admission_error(
    *,
    settings: dict[str, Number],
    channel: dict[str, Any],
    channel_dir: Path,
    videos_dir: Path,
    format_type: str,
    metadata_bytes: int | float | None = None,
    estimate_bytes: int | None = None,
    usage_totals: dict[str, dict[str, int]] | None = None,
    free_bytes: int | None = None,
) -> str | None:
    """Decide whether one more download may be written to storage.

    Args:
        settings: Global storage settings.
        channel: Channel configuration entry (may carry overrides).
        channel_dir: The channel's media directory.
        videos_dir: Storage root.
        format_type: ``video`` or ``audio``.
        metadata_bytes: Optional size hint from discovery metadata.
        estimate_bytes: Precomputed estimate; derived when None.
        usage_totals: Precomputed per-channel usage; measured when None.
        free_bytes: Precomputed free space; queried when None.

    Returns:
        ``None`` when the download may proceed, otherwise an actionable
        blocking reason suitable for logs and job errors.
    """
    effective = effective_channel_storage(settings, channel)
    if usage_totals is None:
        usage_totals = measure_media_usage(videos_dir)
    if free_bytes is None:
        try:
            free_bytes = free_disk_bytes(videos_dir)
        except OSError as error:
            return f"Free disk space check failed: {error}"
    minimum_bytes = effective["min_free_space_gb"] * GB
    if minimum_bytes > 0 and free_bytes < minimum_bytes:
        return (
            f"Insufficient free disk space: {free_bytes / GB:.1f} GB free, "
            f"{effective['min_free_space_gb']:g} GB required"
        )
    if estimate_bytes is None:
        estimate_bytes = estimate_download_bytes(
            channel_dir, format_type, metadata_bytes
        )
    channel_bytes = usage_totals.get(Path(channel_dir).name, {}).get("bytes", 0)
    quota_override = channel.get("quota_gb", 0)
    quota_bytes = effective["quota_gb"] * GB
    if (
        is_positive_number(quota_override)
        and quota_bytes > 0
        and channel_bytes + estimate_bytes > quota_bytes
    ):
        return (
            f"Storage quota exceeded: {(channel_bytes + estimate_bytes) / GB:.1f}"
            f"/{effective['quota_gb']:g} GB"
        )
    total_bytes = sum(entry["bytes"] for entry in usage_totals.values())
    global_quota_bytes = settings["global_quota_gb"] * GB
    if global_quota_bytes > 0 and total_bytes + estimate_bytes > global_quota_bytes:
        return (
            f"Global storage quota exceeded: {(total_bytes + estimate_bytes) / GB:.1f}"
            f"/{settings['global_quota_gb']:g} GB"
        )
    return None


# ---------- pins ----------


def pins_path(config_dir: str | Path) -> Path:
    """Return the pins file location inside ``config_dir``."""
    return Path(config_dir) / PINS_FILENAME


class PinsUnavailableError(RuntimeError):
    """Raised when ``pins.json`` exists but is unreadable or corrupt.

    Treating such a file as "nothing pinned" would let cleanup and
    retention delete pinned episodes irreversibly, so callers must skip
    every destructive pass and never write pins until it is repaired.
    """


def _validate_pins(data: Any) -> None:
    """Require ``{channel: [video_id, ...]}``; anything else is corrupt.

    Raises:
        ValueError: When the top level is not an object or a channel's
            pins are not a list.
    """
    require_json_object(data)
    for channel, ids in data.items():
        if not isinstance(ids, list):
            raise ValueError(f"pins for channel '{channel}' must be a list")


def load_pins(config_dir: str | Path) -> dict[str, list[str]]:
    """Load pinned episode ids per channel.

    A missing file means nothing is pinned. A corrupt or unreadable file
    is never read as empty and never quarantined automatically.

    Raises:
        PinsUnavailableError: When ``pins.json`` exists but cannot be read
            or holds invalid content.
    """
    try:
        data = read_json_state_strict(pins_path(config_dir), validate=_validate_pins)
    except (StateFileError, CorruptStateFileError) as error:
        raise PinsUnavailableError(
            f"{error}. Episode cleanup and retention are paused and pins "
            f"cannot change until {PINS_FILENAME} is repaired or removed"
        ) from error
    if data is None:
        return {}
    return {
        str(channel): [entry for entry in ids if isinstance(entry, str)]
        for channel, ids in data.items()
    }


def _load_pins_for_update(config_dir: str | Path) -> dict[str, list[str]] | None:
    """Load pins before a write; None (logged) means the write must not happen."""
    try:
        return load_pins(config_dir)
    except PinsUnavailableError as error:
        logging.error(f"Not changing pins: {error}")
        return None


def save_pins(config_dir: str | Path, pins: dict[str, list[str]]) -> bool:
    """Persist pins atomically; return False when the write failed."""
    try:
        atomic_write_text(
            pins_path(config_dir), json.dumps(pins, indent=2, sort_keys=True)
        )
        return True
    except OSError as error:
        logging.error(f"Failed to save pins: {error}")
        return False


def pinned_ids(config_dir: str | Path, channel_name: str) -> set[str]:
    """Return the set of pinned video ids for one channel.

    Raises:
        PinsUnavailableError: When ``pins.json`` is unreadable or corrupt.
    """
    return set(load_pins(config_dir).get(channel_name, ()))


def pins_for_deletion(
    config_dir: str | Path,
    channel_name: str,
    logger: logging.Logger,
    action: str,
) -> set[str] | None:
    """Return a channel's pins for a destructive pass, or None to skip it.

    Deleting without knowing the pins could remove pinned episodes
    irreversibly, so an unusable ``pins.json`` pauses the pass and logs an
    actionable error instead.

    Args:
        config_dir: Directory holding ``pins.json``.
        channel_name: Sanitized channel identifier.
        logger: Logger receiving the error.
        action: Name of the skipped pass, e.g. ``"retention"``.

    Returns:
        The pinned video ids, or None when the pass must be skipped.
    """
    try:
        return pinned_ids(config_dir, channel_name)
    except PinsUnavailableError as error:
        logger.error(f"Skipping {action} for {channel_name}: {error}")
        return None


def set_episode_pinned(
    config_dir: str | Path, channel_name: str, video_id: str, pinned: bool
) -> bool:
    """Add or remove one pin.

    Returns:
        False when ``pins.json`` could not be read (it is then left
        untouched) or the write failed.
    """
    with _pins_lock:
        pins = _load_pins_for_update(config_dir)
        if pins is None:
            return False
        ids = pins.setdefault(channel_name, [])
        if pinned and video_id not in ids:
            ids.append(video_id)
        if not pinned and video_id in ids:
            ids.remove(video_id)
        if not ids:
            pins.pop(channel_name, None)
        return save_pins(config_dir, pins)


def remove_episode_pin(
    config_dir: str | Path, channel_name: str, video_id: str
) -> bool:
    """Drop one pin, e.g. after the episode was deleted.

    Returns:
        True when a pin was removed and persisted; False when there was
        none, ``pins.json`` could not be read (left untouched) or the
        write failed.
    """
    with _pins_lock:
        pins = _load_pins_for_update(config_dir)
        if pins is None:
            return False
        ids = pins.get(channel_name)
        if ids is None or video_id not in ids:
            return False
        ids.remove(video_id)
        if not ids:
            pins.pop(channel_name, None)
        return save_pins(config_dir, pins)


def remove_channel_pins(config_dir: str | Path, channel_name: str) -> bool:
    """Drop every pin for a channel (purge or channel deletion).

    Returns:
        True when pins were removed and persisted; False when there were
        none, ``pins.json`` could not be read (left untouched) or the
        write failed.
    """
    with _pins_lock:
        pins = _load_pins_for_update(config_dir)
        if pins is None or channel_name not in pins:
            return False
        del pins[channel_name]
        return save_pins(config_dir, pins)


# ---------- tombstones ----------


TOMBSTONES_FILENAME = "tombstones.json"
_tombstones_lock = threading.Lock()


def tombstones_path(config_dir: str | Path) -> Path:
    """Return the tombstones file location inside ``config_dir``."""
    return Path(config_dir) / TOMBSTONES_FILENAME


def load_tombstones(
    config_dir: str | Path, *, quarantine: bool = True
) -> dict[str, dict[str, dict[str, str]]]:
    """Load remembered deletions per channel; corrupt data reads as empty.

    A corrupt file is backed up as ``tombstones.json.corrupt`` (later
    backups get a timestamp suffix) so the original bytes are never lost.

    Args:
        config_dir: Directory holding ``tombstones.json``.
        quarantine: Move a corrupt file aside; lock-free readers pass
            False and leave that to the locked writers.

    Returns:
        ``{channel: {video_id: {"deleted_at": iso, "title": str}}}`` with
        invalid entries dropped.

    Raises:
        StateFileError: When the file exists but cannot be read; it is
            left untouched.
    """
    data = read_json_state(
        tombstones_path(config_dir),
        validate=require_json_object,
        quarantine=quarantine,
    )
    if data is None:
        return {}
    tombstones: dict[str, dict[str, dict[str, str]]] = {}
    for channel, entries in data.items():
        if not isinstance(entries, dict):
            continue
        cleaned: dict[str, dict[str, str]] = {}
        for video_id, record in entries.items():
            if isinstance(video_id, str) and isinstance(record, dict):
                cleaned[video_id] = {
                    "deleted_at": str(record.get("deleted_at") or ""),
                    "title": str(record.get("title") or ""),
                }
        if cleaned:
            tombstones[str(channel)] = cleaned
    return tombstones


def save_tombstones(
    config_dir: str | Path, tombstones: dict[str, dict[str, dict[str, str]]]
) -> bool:
    """Persist tombstones atomically; return False when the write failed."""
    try:
        atomic_write_text(
            tombstones_path(config_dir),
            json.dumps(tombstones, indent=2, sort_keys=True),
        )
        return True
    except OSError as error:
        logging.error(f"Failed to save tombstones: {error}")
        return False


def _load_tombstones_for_update(
    config_dir: str | Path,
) -> dict[str, dict[str, dict[str, str]]] | None:
    """Load tombstones before a write; None (logged) means do not write."""
    try:
        return load_tombstones(config_dir)
    except StateFileError as error:
        logging.error(f"Not changing tombstones: {error}")
        return None


def tombstoned_ids(config_dir: str | Path, channel_name: str) -> set[str]:
    """Return the set of remembered-deleted video ids for one channel.

    This lock-free read never quarantines a corrupt file.

    Raises:
        StateFileError: When ``tombstones.json`` cannot be read. Callers
            deciding what to download must fail closed on it.
    """
    return set(load_tombstones(config_dir, quarantine=False).get(channel_name, ()))


def record_tombstone(
    config_dir: str | Path,
    channel_name: str,
    video_id: str,
    title: str,
    deleted_at: str | None = None,
) -> bool:
    """Remember a manually deleted episode so refreshes skip it forever.

    Args:
        config_dir: Directory holding ``tombstones.json``.
        channel_name: Sanitized channel identifier.
        video_id: YouTube video identifier.
        title: Episode title kept for identification in the UI.
        deleted_at: ISO-8601 deletion time; defaults to now (UTC).

    Returns:
        False when ``tombstones.json`` could not be read (it is then left
        untouched) or the write failed.
    """
    with _tombstones_lock:
        tombstones = _load_tombstones_for_update(config_dir)
        if tombstones is None:
            return False
        tombstones.setdefault(channel_name, {})[video_id] = {
            "deleted_at": deleted_at or datetime.now(UTC).isoformat(),
            "title": str(title or video_id),
        }
        return save_tombstones(config_dir, tombstones)


def remove_episode_tombstone(
    config_dir: str | Path, channel_name: str, video_id: str
) -> bool:
    """Forget one deletion, e.g. after an explicit redownload succeeded.

    Returns:
        True when a tombstone record was removed and persisted; False when
        there was none, the file could not be read (left untouched) or the
        write failed.
    """
    with _tombstones_lock:
        tombstones = _load_tombstones_for_update(config_dir)
        if tombstones is None:
            return False
        channel = tombstones.get(channel_name)
        if not channel or video_id not in channel:
            return False
        del channel[video_id]
        if not channel:
            tombstones.pop(channel_name, None)
        return save_tombstones(config_dir, tombstones)


def remove_channel_tombstones(config_dir: str | Path, channel_name: str) -> int:
    """Drop every tombstone for a channel; return how many were removed.

    Used by an explicit purge (a reset: deleted episodes may be downloaded
    again) and by channel deletion. Returns 0 without writing when the
    file could not be read, and 0 when the write failed.
    """
    with _tombstones_lock:
        tombstones = _load_tombstones_for_update(config_dir)
        if not tombstones or not tombstones.get(channel_name):
            return 0
        removed = len(tombstones.pop(channel_name))
        return removed if save_tombstones(config_dir, tombstones) else 0


# ---------- retention ----------


def _episode_age_reference(metadata: dict[str, Any], media_file: Path) -> float:
    """Return the episode's age reference in epoch seconds.

    The upload date from the episode metadata wins; the media file's mtime
    is the fallback when the date is missing or unparsable.
    """
    upload_date = str(metadata.get("upload_date") or "")
    try:
        return datetime.strptime(upload_date, "%Y%m%d").timestamp()
    except ValueError:
        pass
    try:
        return media_file.stat().st_mtime
    except OSError:
        return time.time()


def apply_retention(
    videos_dir: Path,
    channel_name: str,
    retention_days: float,
    pinned: set[str] | None = None,
    logger: logging.Logger | None = None,
    now: float | None = None,
) -> int:
    """Delete unpinned episodes older than ``retention_days``; return count.

    Episodes are evaluated by upload date (metadata) with the media file's
    mtime as fallback and deleted oldest first. Each deletion is logged.
    Running again after a pass deletes nothing new.

    Args:
        videos_dir: Storage root.
        channel_name: Sanitized channel identifier.
        retention_days: Age window in days; ``0`` (or less) disables it.
        pinned: Episode ids that must never be deleted.
        logger: Optional logger; defaults to this module's logger.
        now: Override the current epoch time (used by tests).

    Returns:
        The number of episodes deleted.

    Raises:
        ValueError: When the channel name escapes the storage root.
    """
    log = logger if logger is not None else logging.getLogger(__name__)
    if (
        not isinstance(retention_days, (int, float))
        or isinstance(retention_days, bool)
        or retention_days <= 0
    ):
        return 0
    validate_channel_settings({"retention_days": retention_days})
    channel_dir = contained_path(Path(videos_dir), channel_name)
    pinned = pinned or set()
    current = time.time() if now is None else now
    cutoff = current - retention_days * SECONDS_PER_DAY
    aged = (
        (
            _episode_age_reference(episode.metadata, episode.media_file),
            episode.episode_id,
        )
        for episode in collect_episodes(channel_dir)
        if episode.episode_id not in pinned
    )
    eligible = sorted(
        (entry for entry in aged if entry[0] < cutoff), key=lambda entry: entry[0]
    )
    removed = 0
    for reference, episode_id in eligible:
        delete_episode_files(channel_dir, episode_id, log)
        age_days = (current - reference) / SECONDS_PER_DAY
        log.info(
            f"Retention: deleted {episode_id} from {channel_name} "
            f"(age {age_days:.1f}d > {retention_days:g}d)"
        )
        removed += 1
    return removed


def channel_retention_days(
    global_config: dict[str, Any], channel: dict[str, Any]
) -> Number:
    """Return a channel's effective retention window in days.

    A per-channel ``retention_days`` of 0 (or absent) inherits the global
    ``storage.retention_days``. Callers check this before reading pins so a
    broken ``pins.json`` never reports a skipped pass that is disabled.

    Args:
        global_config: Configuration holding the ``storage`` key.
        channel: Channel configuration entry.

    Returns:
        The effective window; ``0`` means retention is disabled.
    """
    settings = load_storage_settings(global_config)
    return effective_channel_storage(settings, channel)["retention_days"]


def apply_channel_retention(
    videos_dir: Path,
    channel: dict[str, Any],
    global_config: dict[str, Any],
    *,
    pinned: set[str],
    logger: logging.Logger,
) -> int:
    """Apply a channel's effective retention window; return the count deleted.

    Shared by refreshes, queued downloads, daily maintenance and the cron
    runner. Pins must come from :func:`pins_for_deletion` so an unusable
    ``pins.json`` skips the pass before this is called.

    Args:
        videos_dir: Storage root.
        channel: Channel configuration entry (may carry an override).
        global_config: Configuration holding the ``storage`` key.
        pinned: Episode ids that must never be deleted.
        logger: Logger receiving one line per deleted episode.

    Returns:
        The number of episodes deleted; ``0`` when retention is disabled.

    Raises:
        ValueError: When the channel name escapes the storage root or the
            effective window is out of bounds.
    """
    return apply_retention(
        videos_dir,
        channel["name"],
        channel_retention_days(global_config, channel),
        pinned=pinned,
        logger=logger,
    )
