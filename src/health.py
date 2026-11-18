"""Refresh health tracking and webhook notifications.

Health lives in ``config_dir/health.json``::

    {
      "channels": {
        "<channel>": {
          "consecutive_failures": 0,
          "last_success_at": null,
          "last_failure_at": null,
          "last_error": null,
          "unavailable": false,
          "history": [{"at": ..., "status": ..., ...}]
        }
      },
      "notifications": {
        "last_sent": {"<event key>": "<iso>"},
        "last_attempts": {"<event key>": {"at": ..., "ok": ..., "error": ...}},
        "last_error": null
      }
    }

Every channel refresh appends a bounded history entry. Repeated failures and
unavailable channels produce notification events; a low free-disk check is
evaluated separately. Events are delivered to a single generic webhook as
JSON ``POST`` requests with a per-event cooldown so a broken channel cannot
flood the target.

Notifications are opt-in: without ``notifications.webhook_url`` in
``channels.yaml`` events are recorded but nothing leaves the process.

A corrupt ``health.json`` is quarantined and reads as empty. An unreadable
one is never treated as empty data: readers degrade to an empty view with
a log, and writers skip saving so the file is never overwritten.

The web server and the cron runner both update the file, so every
read-modify-write holds :func:`~utils.state_file_lock`; reads stay
lock-free because writes are atomic.
"""

import json
import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

try:
    from .utils import (
        StateFileError,
        atomic_write_text,
        read_json_state,
        require_json_object,
        state_file_lock,
    )
except ImportError:
    from utils import (
        StateFileError,
        atomic_write_text,
        read_json_state,
        require_json_object,
        state_file_lock,
    )

HEALTH_FILENAME = "health.json"
HISTORY_LIMIT = 50
DEFAULT_FAILURE_THRESHOLD = 3
DEFAULT_COOLDOWN_MINUTES = 360
MAX_FAILURE_THRESHOLD = 100
MAX_COOLDOWN_MINUTES = 10080
MAX_WEBHOOK_URL_LENGTH = 2000
WEBHOOK_TIMEOUT_SECONDS = 10
GB = 1024**3


class ChannelUnavailableError(RuntimeError):
    """Raised when a channel cannot be fetched (removed, private, blocked)."""


def health_path(config_dir: str | Path) -> Path:
    """Return the health file location inside ``config_dir``."""
    return Path(config_dir) / HEALTH_FILENAME


def _empty_state() -> dict[str, Any]:
    """Return a fresh per-channel health state."""
    return {
        "consecutive_failures": 0,
        "last_success_at": None,
        "last_failure_at": None,
        "last_error": None,
        "unavailable": False,
        "history": [],
    }


def _empty_health() -> dict[str, Any]:
    """Return the health data of a fresh installation."""
    return {"channels": {}, "notifications": {}}


def load_health(config_dir: str | Path, *, quarantine: bool = True) -> dict[str, Any]:
    """Load health data; a corrupt file is quarantined and reads as empty.

    Args:
        config_dir: Directory holding ``health.json``.
        quarantine: Move a corrupt file aside; lock-free readers pass
            False and leave that to the locked writers.

    Returns:
        ``{"channels": ..., "notifications": ...}`` with invalid entries
        dropped.

    Raises:
        StateFileError: When the file exists but cannot be read; it is
            left untouched.
    """
    data = read_json_state(
        health_path(config_dir), validate=require_json_object, quarantine=quarantine
    )
    if data is None:
        return _empty_health()

    channels = data.get("channels")
    if not isinstance(channels, dict):
        channels = {}
    cleaned_channels: dict[str, dict[str, Any]] = {}
    for name, state in channels.items():
        if not isinstance(name, str) or not isinstance(state, dict):
            continue
        cleaned = _empty_state()
        cleaned["consecutive_failures"] = _non_negative_int(
            state.get("consecutive_failures")
        )
        for field in ("last_success_at", "last_failure_at", "last_error"):
            value = state.get(field)
            if isinstance(value, str):
                cleaned[field] = value
        cleaned["unavailable"] = state.get("unavailable") is True
        history = state.get("history")
        cleaned["history"] = (
            [entry for entry in history if isinstance(entry, dict)][-HISTORY_LIMIT:]
            if isinstance(history, list)
            else []
        )
        cleaned_channels[name] = cleaned

    notifications = data.get("notifications")
    if not isinstance(notifications, dict):
        notifications = {}
    cleaned_notifications: dict[str, Any] = {
        "last_sent": {},
        "last_attempts": {},
        "last_error": None,
    }
    last_sent = notifications.get("last_sent")
    if isinstance(last_sent, dict):
        cleaned_notifications["last_sent"] = {
            key: value
            for key, value in last_sent.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    attempts = notifications.get("last_attempts")
    if isinstance(attempts, dict):
        cleaned_notifications["last_attempts"] = {
            key: value
            for key, value in attempts.items()
            if isinstance(key, str) and isinstance(value, dict)
        }
    if isinstance(notifications.get("last_error"), str):
        cleaned_notifications["last_error"] = notifications["last_error"]
    return {"channels": cleaned_channels, "notifications": cleaned_notifications}


def _read_health_view(config_dir: str | Path) -> dict[str, Any]:
    """Read health lock-free for display or evaluation.

    Never quarantines; an unreadable file degrades to empty data (logged)
    so health views keep working while the file itself stays untouched.
    """
    try:
        return load_health(config_dir, quarantine=False)
    except StateFileError as error:
        logging.error(f"Health data unavailable: {error}")
        return _empty_health()


def _load_health_for_update(
    config_dir: str | Path, action: str
) -> dict[str, Any] | None:
    """Load health before a write; None (logged) means skip the save.

    Args:
        config_dir: Directory holding ``health.json``.
        action: Description of the skipped update for the log.
    """
    try:
        return load_health(config_dir)
    except StateFileError as error:
        logging.error(f"Not saving health data ({action}): {error}")
        return None


@contextmanager
def _locked_health(
    config_dir: str | Path, action: str
) -> Iterator[dict[str, Any] | None]:
    """Hold the ``health.json`` lock and load the data for an update.

    The web server and the cron runner both write health data, so every
    read-modify-write runs under this cross-process lock. Callers save
    before leaving the block and never nest it.

    Args:
        config_dir: Directory holding ``health.json``.
        action: Description of the update for the log.

    Yields:
        The health data, or None (logged) when the lock could not be taken
        or the file cannot be read; the caller must then skip saving.
    """
    with state_file_lock(health_path(config_dir)) as locked:
        yield _load_health_for_update(config_dir, action) if locked else None


def save_health(config_dir: str | Path, data: dict[str, Any]) -> bool:
    """Persist health data atomically; return False when the write failed."""
    try:
        atomic_write_text(
            health_path(config_dir), json.dumps(data, indent=2, sort_keys=True)
        )
        return True
    except OSError as error:
        logging.error(f"Failed to save health data: {error}")
        return False


def _non_negative_int(value: Any) -> int:
    """Coerce persisted counters to a non-negative integer."""
    return value if isinstance(value, int) and value > 0 else 0


def record_refresh(
    config_dir: str | Path,
    channel_name: str,
    *,
    success: bool,
    error: str | None = None,
    queued: int = 0,
    unavailable: bool = False,
    duration_seconds: float | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Append a refresh outcome and update the channel's failure counters.

    Args:
        config_dir: Directory holding ``health.json``.
        channel_name: Sanitized channel identifier.
        success: Whether discovery/refresh finished without an error.
        error: Failure description; ignored on success.
        queued: Number of episodes queued by this refresh.
        unavailable: True when the channel itself could not be fetched.
        duration_seconds: Refresh duration when known.
        at: ISO timestamp override (tests).

    Returns:
        The updated per-channel state. When ``health.json`` cannot be
        read or locked nothing is saved and the state reflects this
        refresh only.
    """
    timestamp = at or datetime.now(UTC).isoformat()
    entry: dict[str, Any] = {
        "at": timestamp,
        "status": "success" if success else "failure",
        "queued": max(0, int(queued)),
        "unavailable": bool(unavailable),
    }
    if duration_seconds is not None:
        entry["duration_seconds"] = round(float(duration_seconds), 3)
    if error:
        entry["error"] = str(error)

    with _locked_health(config_dir, f"refresh of {channel_name}") as data:
        channels = data["channels"] if data is not None else {}
        state = _apply_refresh(
            channels.get(channel_name) or _empty_state(), entry, error=error
        )
        if data is not None:
            channels[channel_name] = state
            save_health(config_dir, data)
        return dict(state)


def _apply_refresh(
    state: dict[str, Any], entry: dict[str, Any], *, error: str | None
) -> dict[str, Any]:
    """Update a channel state in place with one refresh history entry.

    Args:
        state: The channel's current health state.
        entry: History entry built by :func:`record_refresh`.
        error: Failure description; ignored on success.

    Returns:
        The updated ``state``.
    """
    timestamp = entry["at"]
    if entry["status"] == "success":
        state["consecutive_failures"] = 0
        state["last_success_at"] = timestamp
        state["last_error"] = None
        state["unavailable"] = False
    else:
        state["consecutive_failures"] = (
            _non_negative_int(state.get("consecutive_failures")) + 1
        )
        state["last_failure_at"] = timestamp
        state["last_error"] = error or "Unknown error"
        state["unavailable"] = entry["unavailable"]
    history = state.get("history")
    if not isinstance(history, list):
        history = []
    history.append(entry)
    state["history"] = history[-HISTORY_LIMIT:]
    return state


def remove_channel_health(config_dir: str | Path, channel_name: str) -> bool:
    """Forget one channel's health state (used when the channel is deleted).

    Returns:
        True when the state was removed; False when there was none or
        ``health.json`` could not be read or locked (it is then left
        untouched).
    """
    with _locked_health(config_dir, f"removal of {channel_name}") as data:
        if data is None or channel_name not in data["channels"]:
            return False
        del data["channels"][channel_name]
        save_health(config_dir, data)
        return True


def load_notification_settings(global_config: dict[str, Any]) -> dict[str, Any]:
    """Resolve notification settings from the global config with defaults.

    Args:
        global_config: ``channels.yaml`` keys outside the ``channels`` list.

    Returns:
        Settings with ``webhook_url`` (``""`` disables delivery),
        ``failure_threshold``, ``cooldown_minutes`` and
        ``notify_low_space``.
    """
    raw = global_config.get("notifications")
    if not isinstance(raw, dict):
        raw = {}

    webhook_url = raw.get("webhook_url")
    if not isinstance(webhook_url, str):
        webhook_url = ""
    threshold = raw.get("failure_threshold")
    cooldown = raw.get("cooldown_minutes")
    return {
        "webhook_url": webhook_url.strip(),
        "failure_threshold": threshold
        if isinstance(threshold, int)
        and not isinstance(threshold, bool)
        and 1 <= threshold <= MAX_FAILURE_THRESHOLD
        else DEFAULT_FAILURE_THRESHOLD,
        "cooldown_minutes": cooldown
        if isinstance(cooldown, int)
        and not isinstance(cooldown, bool)
        and 0 <= cooldown <= MAX_COOLDOWN_MINUTES
        else DEFAULT_COOLDOWN_MINUTES,
        "notify_low_space": raw.get("notify_low_space") is not False,
    }


def validate_notification_settings(data: dict[str, Any]) -> None:
    """Validate a notification settings payload; raise ValueError if invalid."""
    if not isinstance(data, dict):
        raise ValueError("Notification settings must be a JSON object")

    if "webhook_url" in data:
        url = data["webhook_url"]
        if not isinstance(url, str):
            raise ValueError("webhook_url must be a string")
        if len(url) > MAX_WEBHOOK_URL_LENGTH:
            raise ValueError("webhook_url is too long")
        if url.strip():
            parsed = urlsplit(url.strip())
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise ValueError("webhook_url must be an http(s) URL")
    if "failure_threshold" in data:
        value = data["failure_threshold"]
        if type(value) is not int or value < 1 or value > MAX_FAILURE_THRESHOLD:
            raise ValueError(
                f"failure_threshold must be an integer between 1 and "
                f"{MAX_FAILURE_THRESHOLD}"
            )
    if "cooldown_minutes" in data:
        value = data["cooldown_minutes"]
        if type(value) is not int or value < 0 or value > MAX_COOLDOWN_MINUTES:
            raise ValueError(
                f"cooldown_minutes must be an integer between 0 and "
                f"{MAX_COOLDOWN_MINUTES}"
            )
    if "notify_low_space" in data and type(data["notify_low_space"]) is not bool:
        raise ValueError("notify_low_space must be true or false")


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO timestamp defensively."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _cooldown_active(
    last_sent: dict[str, Any], key: str, cooldown_minutes: int, now: datetime
) -> bool:
    """Return True when the event's cooldown has not elapsed yet."""
    if cooldown_minutes <= 0:
        return False
    sent_at = _parse_timestamp(last_sent.get(key))
    if sent_at is None:
        return False
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=UTC)
    return (now - sent_at).total_seconds() < cooldown_minutes * 60


def collect_events(
    config_dir: str | Path,
    settings: dict[str, Any],
    *,
    free_bytes: int | None = None,
    min_free_space_gb: float = 0,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Build notification events that are due, honoring the cooldown.

    Args:
        config_dir: Directory holding ``health.json``.
        settings: Resolved notification settings.
        free_bytes: Current free disk space, or None when unknown.
        min_free_space_gb: Configured minimum free space (0 disables).
        now: Current time override (tests).

    Returns:
        Event dicts shaped as ``{"key", "event", "channel", "message",
        "occurred_at", ...}`` ready for :func:`dispatch_events`.
    """
    current = now or datetime.now(UTC)
    data = _read_health_view(config_dir)
    last_sent = data["notifications"].get("last_sent", {})
    cooldown = int(settings.get("cooldown_minutes", DEFAULT_COOLDOWN_MINUTES))
    threshold = int(settings.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD))
    events: list[dict[str, Any]] = []

    for channel_name, state in sorted(data.get("channels", {}).items()):
        if state.get("unavailable"):
            key = f"channel_unavailable:{channel_name}"
            if not _cooldown_active(last_sent, key, cooldown, current):
                events.append(
                    {
                        "key": key,
                        "event": "channel_unavailable",
                        "channel": channel_name,
                        "message": (
                            f"Channel '{channel_name}' could not be refreshed: "
                            f"{state.get('last_error') or 'unavailable'}"
                        ),
                        "occurred_at": current.isoformat(),
                        "consecutive_failures": state.get("consecutive_failures", 0),
                        "last_error": state.get("last_error"),
                    }
                )
            continue
        failures = _non_negative_int(state.get("consecutive_failures"))
        if failures >= threshold:
            key = f"channel_failure:{channel_name}"
            if not _cooldown_active(last_sent, key, cooldown, current):
                events.append(
                    {
                        "key": key,
                        "event": "channel_failure",
                        "channel": channel_name,
                        "message": (
                            f"Channel '{channel_name}' failed {failures} "
                            f"consecutive refresh(es): "
                            f"{state.get('last_error') or 'unknown error'}"
                        ),
                        "occurred_at": current.isoformat(),
                        "consecutive_failures": failures,
                        "last_error": state.get("last_error"),
                    }
                )

    if (
        settings.get("notify_low_space", True)
        and min_free_space_gb > 0
        and free_bytes is not None
    ):
        minimum = int(min_free_space_gb * GB)
        if free_bytes < minimum:
            key = "low_disk_space"
            if not _cooldown_active(last_sent, key, cooldown, current):
                events.append(
                    {
                        "key": key,
                        "event": "low_disk_space",
                        "channel": None,
                        "message": (
                            f"Free disk space is {free_bytes / GB:.2f} GB, below "
                            f"the {min_free_space_gb:g} GB minimum"
                        ),
                        "occurred_at": current.isoformat(),
                        "free_bytes": int(free_bytes),
                        "min_free_space_gb": min_free_space_gb,
                    }
                )
    return events


def _webhook_payload(event: dict[str, Any]) -> bytes:
    """Serialize one event for the webhook request body."""
    body = {
        "source": "yt2rss",
        "event": event.get("event"),
        "channel": event.get("channel"),
        "message": event.get("message"),
        "occurred_at": event.get("occurred_at"),
    }
    for extra in (
        "consecutive_failures",
        "last_error",
        "free_bytes",
        "min_free_space_gb",
    ):
        if extra in event:
            body[extra] = event[extra]
    return json.dumps(body).encode("utf-8")


def _post_webhook(webhook_url: str, event: dict[str, Any]) -> None:
    """POST one event; raise on network or HTTP errors."""
    request = Request(
        webhook_url,
        data=_webhook_payload(event),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "yt2rss/1.0",
        },
        method="POST",
    )
    with urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS) as response:
        status = getattr(response, "status", 200)
        if isinstance(status, int) and status >= 400:
            raise RuntimeError(f"Webhook returned HTTP {status}")


def dispatch_events(
    config_dir: str | Path,
    events: Iterable[dict[str, Any]],
    webhook_url: str,
) -> list[dict[str, Any]]:
    """Deliver events to the webhook and persist the attempt results.

    Delivery failures never raise: they are recorded so the UI can show the
    last error, and the cooldown only advances after a successful delivery
    (a transient outage is retried on the next health evaluation).

    Args:
        config_dir: Directory holding ``health.json``.
        events: Events from :func:`collect_events`.
        webhook_url: Destination URL; empty disables delivery.

    Returns:
        One result per event: ``{"key", "event", "ok", "error"}``.
    """
    results: list[dict[str, Any]] = []
    if not webhook_url:
        return results

    now = datetime.now(UTC).isoformat()
    attempts: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for event in events:
        result = {"key": event.get("key"), "event": event.get("event"), "ok": True}
        try:
            _post_webhook(webhook_url, event)
        except Exception as error:  # Network errors must never break refreshes.
            logging.warning(f"Health webhook delivery failed: {error}")
            result.update(ok=False, error=str(error))
        else:
            logging.info(f"Health notification sent: {event.get('event')}")
        attempts.append((event, result))
        results.append(result)

    with _locked_health(config_dir, "notification bookkeeping") as data:
        if data is None:
            return results
        notifications = data["notifications"]
        for event, result in attempts:
            key = str(event.get("key") or "")
            if not key:
                continue
            notifications.setdefault("last_attempts", {})[key] = {
                "at": now,
                "ok": result["ok"],
                "error": result.get("error"),
            }
            if result["ok"]:
                notifications.setdefault("last_sent", {})[key] = now
            else:
                notifications["last_error"] = result.get("error")
        save_health(config_dir, data)
    return results


def health_summary(config_dir: str | Path) -> dict[str, Any]:
    """Return the stored channel states and notification bookkeeping.

    Never quarantines and never raises for an unreadable file: the summary
    is then empty and the error is logged.
    """
    data = _read_health_view(config_dir)
    return {
        "channels": data.get("channels", {}),
        "notifications": data.get("notifications", {}),
    }
