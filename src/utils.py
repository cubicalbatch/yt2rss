import json
import logging
import math
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, TextIO

try:
    import fcntl
except ImportError:  # The web UI can still run with thread locking on Windows.
    fcntl = None

MEDIA_EXTENSIONS = (".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".avi")
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


def validate_channel_settings(data: dict[str, Any]) -> None:
    """Reject invalid settings before they reach scheduling or deletion logic."""
    if "max_episodes" in data:
        value = data["max_episodes"]
        if type(value) is not int or value <= 0:
            raise ValueError("max_episodes must be a positive integer")
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


def cleanup_episodes(videos_dir: Path, channel_name: str, max_episodes: int) -> None:
    """Prune completed episodes using filenames as the trusted identity."""
    validate_channel_settings({"max_episodes": max_episodes})
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
    for _, episode_id, metadata_file in episodes[:-max_episodes]:
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
