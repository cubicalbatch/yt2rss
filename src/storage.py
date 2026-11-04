"""Storage controls: quotas, low-space protection, retention, and pins.

Accounting definition: usage counts only regular media files whose suffix is
in :data:`MEDIA_EXTENSIONS`. Thumbnails and ``*.json`` metadata are excluded
so the numbers track playable episodes. Symlinks are never followed, so
media linked in from outside the storage root is neither counted nor
traversed.

Pins live in ``config_dir/pins.json`` as ``{"<channel>": ["<video_id>", ...]}``
so they survive episode metadata rewrites and redownloads; they are removed
only when the episode or its channel is deleted.

Tombstones live in ``config_dir/tombstones.json`` as
``{"<channel>": {"<video_id>": {"deleted_at": ..., "title": ...}}}`` and
remember manually deleted episodes so discovery, queue execution, the RSS
feed and the episodes API never bring them back. An explicit channel purge
(forget via the API) or channel deletion clears them.
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
        atomic_write_text,
        contained_path,
        load_json_file,
        safe_component,
        validate_bounded_number,
        validate_channel_settings,
    )
except ImportError:
    from utils import (
        MAX_MIN_FREE_GB,
        MAX_QUOTA_GB,
        MAX_RETENTION_DAYS,
        MEDIA_EXTENSIONS,
        atomic_write_text,
        contained_path,
        load_json_file,
        safe_component,
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


def load_pins(config_dir: str | Path) -> dict[str, list[str]]:
    """Load pinned episode ids per channel; malformed data reads as empty."""
    path = pins_path(config_dir)
    if not path.is_file():
        return {}
    data = load_json_file(str(path))
    pins: dict[str, list[str]] = {}
    if isinstance(data, dict):
        for channel, ids in data.items():
            if isinstance(ids, list):
                pins[str(channel)] = [entry for entry in ids if isinstance(entry, str)]
    return pins


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
    """Return the set of pinned video ids for one channel."""
    return set(load_pins(config_dir).get(channel_name, ()))


def set_episode_pinned(
    config_dir: str | Path, channel_name: str, video_id: str, pinned: bool
) -> bool:
    """Add or remove one pin; returns False only when persistence failed."""
    with _pins_lock:
        pins = load_pins(config_dir)
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
) -> None:
    """Drop one pin, e.g. after the episode was deleted."""
    with _pins_lock:
        pins = load_pins(config_dir)
        ids = pins.get(channel_name)
        if ids is None or video_id not in ids:
            return
        ids.remove(video_id)
        if not ids:
            pins.pop(channel_name, None)
        save_pins(config_dir, pins)


def remove_channel_pins(config_dir: str | Path, channel_name: str) -> None:
    """Drop every pin for a channel (purge or channel deletion)."""
    with _pins_lock:
        pins = load_pins(config_dir)
        if channel_name in pins:
            del pins[channel_name]
            save_pins(config_dir, pins)


# ---------- tombstones ----------


TOMBSTONES_FILENAME = "tombstones.json"
_tombstones_lock = threading.Lock()


def tombstones_path(config_dir: str | Path) -> Path:
    """Return the tombstones file location inside ``config_dir``."""
    return Path(config_dir) / TOMBSTONES_FILENAME


def load_tombstones(config_dir: str | Path) -> dict[str, dict[str, dict[str, str]]]:
    """Load remembered deletions per channel; corrupt data reads as empty.

    A corrupt file is backed up as ``tombstones.json.corrupt`` so the
    original bytes are never lost; loading must never crash the server.

    Returns:
        ``{channel: {video_id: {"deleted_at": iso, "title": str}}}`` with
        invalid entries dropped.
    """
    path = tombstones_path(config_dir)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("tombstones file must be an object")
    except (OSError, json.JSONDecodeError, ValueError) as error:
        logging.error(f"Tombstones file is corrupt ({error}); starting empty")
        backup = Path(f"{path}.corrupt")
        try:
            path.replace(backup)
            logging.info(f"Kept corrupt tombstones file as {backup.name}")
        except OSError as backup_error:
            logging.error(f"Could not back up corrupt tombstones file: {backup_error}")
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


def tombstoned_ids(config_dir: str | Path, channel_name: str) -> set[str]:
    """Return the set of remembered-deleted video ids for one channel."""
    return set(load_tombstones(config_dir).get(channel_name, ()))


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
        False only when persistence failed.
    """
    with _tombstones_lock:
        tombstones = load_tombstones(config_dir)
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
        True when a tombstone record was removed.
    """
    with _tombstones_lock:
        tombstones = load_tombstones(config_dir)
        channel = tombstones.get(channel_name)
        if not channel or video_id not in channel:
            return False
        del channel[video_id]
        if not channel:
            tombstones.pop(channel_name, None)
        save_tombstones(config_dir, tombstones)
        return True


def remove_channel_tombstones(config_dir: str | Path, channel_name: str) -> int:
    """Drop every tombstone for a channel; return how many were removed.

    Used by an explicit purge (a reset: deleted episodes may be downloaded
    again) and by channel deletion.
    """
    with _tombstones_lock:
        tombstones = load_tombstones(config_dir)
        channel = tombstones.get(channel_name)
        if not channel:
            return 0
        removed = len(channel)
        del tombstones[channel_name]
        save_tombstones(config_dir, tombstones)
        return removed


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


def _collect_episodes(
    channel_dir: Path,
) -> list[tuple[str, dict[str, Any], Path, Path]]:
    """List valid episodes as ``(episode_id, metadata, metadata_file, media_file)``."""
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
        media_file = next(
            (
                contained_path(channel_dir, f"{episode_id}{ext}")
                for ext in MEDIA_EXTENSIONS
                if contained_path(channel_dir, f"{episode_id}{ext}").is_file()
            ),
            None,
        )
        if media_file is None:
            continue
        episodes.append((episode_id, metadata, metadata_file, media_file))
    return episodes


def _delete_episode_files(
    channel_dir: Path, episode_id: str, metadata_file: Path, logger: logging.Logger
) -> None:
    """Remove media, thumbnail and metadata for one episode."""
    for ext in MEDIA_EXTENSIONS:
        contained_path(channel_dir, f"{episode_id}{ext}").unlink(missing_ok=True)
    for thumbnail in contained_path(channel_dir, "thumbnails").glob(f"{episode_id}.*"):
        try:
            thumbnail.unlink()
        except OSError as error:
            logger.warning(f"Could not remove thumbnail {thumbnail}: {error}")
    metadata_file.unlink(missing_ok=True)


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
    eligible = sorted(
        (
            (reference, episode_id, metadata_file, media_file)
            for episode_id, metadata, metadata_file, media_file in _collect_episodes(
                channel_dir
            )
            if episode_id not in pinned
            and (reference := _episode_age_reference(metadata, media_file)) < cutoff
        ),
        key=lambda entry: entry[0],
    )
    removed = 0
    for reference, episode_id, metadata_file, media_file in eligible:
        _delete_episode_files(channel_dir, episode_id, metadata_file, log)
        age_days = (current - reference) / SECONDS_PER_DAY
        log.info(
            f"Retention: deleted {episode_id} from {channel_name} "
            f"(age {age_days:.1f}d > {retention_days:g}d)"
        )
        removed += 1
    return removed
