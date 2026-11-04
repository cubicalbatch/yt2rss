"""Optional password authentication helpers for the web server.

Everything here is inert unless the operator enables authentication with the
``YT2RSS_PASSWORD`` environment variable. The web server wires these helpers
into its Flask app; the cron runner never serves HTTP and never imports them.
"""

import hmac
import os
import secrets
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import TypeGuard
from urllib.parse import urlsplit

# Session signing key persisted inside the config directory.
SESSION_SECRET_FILE = ".session_secret"

# Failed-login throttle defaults: five bad attempts per address per window.
LOGIN_MAX_FAILURES = 5
LOGIN_FAILURE_WINDOW_SECONDS = 900
LOGIN_MAX_TRACKED_ADDRESSES = 1000


def generate_feed_token() -> str:
    """Generate an opaque per-channel feed token."""
    return secrets.token_urlsafe(32)


def generate_csrf_token() -> str:
    """Generate a session-bound CSRF token for logout forms."""
    return secrets.token_hex(32)


def constant_time_equals(left: str | None, right: str | None) -> bool:
    """Compare two strings in constant time, tolerating missing values."""
    if left is None or right is None:
        return False
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def is_safe_relative_path(target: object) -> TypeGuard[str]:
    """Check a redirect target is a same-origin relative path."""
    if not isinstance(target, str):
        return False
    if not target.startswith("/") or target.startswith("//"):
        return False
    if "\\" in target:
        return False
    parts = urlsplit(target)
    return not parts.scheme and not parts.netloc


def _read_secret_file(secret_file: Path) -> str:
    """Read a session signing key, returning empty on any read problem."""
    try:
        return secret_file.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def load_or_create_session_secret(config_dir: Path) -> str:
    """Load the persisted session signing key, creating it on first use.

    The key lives in the config directory with mode 0600 so the signing key
    survives restarts without being stored in channels.yaml.

    Args:
        config_dir: Resolved application config directory.

    Returns:
        The signing key as a string.
    """
    secret_file = config_dir / SESSION_SECRET_FILE
    secret = _read_secret_file(secret_file)
    if not secret:
        secret = secrets.token_hex(32)
        try:
            descriptor = os.open(
                secret_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
        except FileExistsError:
            # Another process created the key first; use its key.
            secret = _read_secret_file(secret_file) or secret
        else:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(secret)
    try:
        secret_file.chmod(0o600)
    except OSError:
        pass  # Filesystems without permission support; keep the key usable.
    return secret


class LoginThrottler:
    """Bounded in-memory failed-login throttle keyed by client address."""

    def __init__(
        self,
        max_failures: int = LOGIN_MAX_FAILURES,
        window_seconds: int = LOGIN_FAILURE_WINDOW_SECONDS,
        max_tracked_addresses: int = LOGIN_MAX_TRACKED_ADDRESSES,
    ):
        """Configure the throttle window and memory bounds.

        Args:
            max_failures: Failed attempts allowed per window before blocking.
            window_seconds: Length of the rolling failure window in seconds.
            max_tracked_addresses: Maximum addresses kept in memory at once.
        """
        self.max_failures = max_failures
        self.window_seconds = window_seconds
        self.max_tracked_addresses = max_tracked_addresses
        self._failures: OrderedDict[str, list[float]] = OrderedDict()
        self._lock = threading.Lock()

    def is_blocked(self, address: str) -> bool:
        """Check whether the address recently exceeded the failure limit."""
        with self._lock:
            return len(self._recent_failures(address)) >= self.max_failures

    def record_failure(self, address: str) -> None:
        """Record a failed attempt and evict stale addresses when over capacity."""
        with self._lock:
            failures = self._recent_failures(address)
            failures.append(time.monotonic())
            self._failures[address] = failures
            self._failures.move_to_end(address)
            while len(self._failures) > self.max_tracked_addresses:
                self._failures.popitem(last=False)

    def record_success(self, address: str) -> None:
        """Clear failures for the address after a successful login."""
        with self._lock:
            self._failures.pop(address, None)

    def _recent_failures(self, address: str) -> list[float]:
        """Return the address's failures inside the window, pruning the rest."""
        cutoff = time.monotonic() - self.window_seconds
        failures = [
            timestamp
            for timestamp in self._failures.get(address, [])
            if timestamp > cutoff
        ]
        if failures:
            self._failures[address] = failures
            self._failures.move_to_end(address)
        else:
            self._failures.pop(address, None)
        return failures
