"""Durable download queue shared by the web server and the cron runner.

Every download triggered by a refresh runs as a persisted job in
``appdata/config/download_queue.json``. Jobs survive restarts, retry with a
deterministic exponential backoff, can be cancelled or retried manually, and
report progress back to the UI.

Concurrency model:

- All mutations of the queue file run under a :class:`MediaLock` on
  ``download_queue.lock`` so server threads and cron processes serialize
  their read-modify-write cycles. Reads stay lock-free because writes
  replace the file atomically; only locked reads quarantine a corrupt
  file, and a queue file that cannot be read is never overwritten.
- Actually downloading a video additionally requires the shared
  ``media.lock`` flock (the same lock refreshes and destructive media
  operations use), so the web server and ``cron_runner`` can never work on
  the same download concurrently.
"""

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

try:
    from .storage import (
        admission_error,
        apply_retention,
        effective_channel_storage,
        load_storage_settings,
        pins_for_deletion,
        tombstoned_ids,
    )
    from .utils import (
        MediaLock,
        StateFileError,
        atomic_write_text,
        cleanup_episodes,
        contained_path,
        filter_skip_reason,
        load_episode_filters,
        read_json_state,
    )
except ImportError:
    from storage import (
        admission_error,
        apply_retention,
        effective_channel_storage,
        load_storage_settings,
        pins_for_deletion,
        tombstoned_ids,
    )
    from utils import (
        MediaLock,
        StateFileError,
        atomic_write_text,
        cleanup_episodes,
        contained_path,
        filter_skip_reason,
        load_episode_filters,
        read_json_state,
    )

QUEUE_FILENAME = "download_queue.json"
COMPLETED_HISTORY_LIMIT = 200
MAX_ATTEMPTS = 5
RETRY_BASE_DELAY_SECONDS = 60.0
PROGRESS_MIN_INTERVAL_SECONDS = 1.0

JOB_STATES = ("pending", "running", "completed", "failed", "cancelled")
ACTIVE_STATES = ("pending", "running")
TERMINAL_STATES = ("completed", "failed", "cancelled")

_JOB_DEFAULTS: dict[str, Any] = {
    "channel": "",
    "video_id": "",
    "title": "",
    "url": "",
    "config_snapshot": {},
    "state": "pending",
    "attempts": 0,
    "max_attempts": MAX_ATTEMPTS,
    "next_attempt_at": None,
    "created_at": None,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "progress": None,
    "cancel_requested": False,
    "estimated_bytes": None,
}


class DownloadCancelled(Exception):
    """Raised inside yt-dlp progress hooks to abort a running download."""


class ChannelMissingError(Exception):
    """Raised when a queued job's channel is no longer configured."""


class ConfigUnavailableError(Exception):
    """Raised when ``channels.yaml`` cannot be read or parsed.

    Unlike :class:`ChannelMissingError` this says nothing about whether the
    job's channel still exists, so the worker defers such jobs without
    consuming an attempt instead of cancelling them. A typo while
    hand-editing the file therefore never wipes out the pending queue.
    """


class EpisodeFilteredError(Exception):
    """Raised when a queued video may no longer be downloaded.

    Execution re-checks the channel's *current* episode filters and the
    tombstones of manually deleted episodes, so a job enqueued before a
    configuration change (or before the user deleted the episode) is
    retired as ``cancelled`` instead of downloading.
    """


class StorageLimitError(RuntimeError):
    """Raised when a download is blocked by quota or free-space limits."""


def _validate_queue_data(data: Any) -> None:
    """Require the persisted ``{"jobs": [...]}`` shape.

    Raises:
        ValueError: When the decoded queue file has another shape.
    """
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), list):
        raise ValueError("queue file must be an object with a jobs list")


def backoff_delay_seconds(attempt: int) -> float:
    """Return the deterministic wait after a failed attempt (no jitter).

    Args:
        attempt: The 1-based number of the attempt that just failed.

    Returns:
        The delay in seconds before the next attempt.
    """
    return RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1))


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def _new_job(
    channel: str,
    video_id: str,
    title: str,
    url: str,
    snapshot: dict[str, Any] | None,
    estimated_bytes: int | None = None,
) -> dict[str, Any]:
    """Build a job record with the full persistent schema."""
    return {
        "id": uuid.uuid4().hex,
        "channel": channel,
        "video_id": video_id,
        "title": title,
        "url": url,
        "config_snapshot": dict(snapshot or {}),
        "state": "pending",
        "attempts": 0,
        "max_attempts": MAX_ATTEMPTS,
        "next_attempt_at": None,
        "created_at": _utc_now_iso(),
        "started_at": None,
        "finished_at": None,
        "error": None,
        "progress": None,
        "cancel_requested": False,
        "estimated_bytes": estimated_bytes,
    }


class DownloadQueue:
    """Persistent store and state machine for download jobs."""

    def __init__(
        self, config_dir: str | Path, logger: logging.Logger | None = None
    ) -> None:
        """Open (or create) the queue stored in ``config_dir``.

        Jobs left in ``running`` by a crashed process are recovered to
        ``pending`` with their attempt count preserved so interrupted work
        resumes automatically.

        Args:
            config_dir: Directory holding ``download_queue.json``.
            logger: Optional logger; defaults to this module's logger.
        """
        self.config_dir = Path(config_dir)
        self.file_path = self.config_dir / QUEUE_FILENAME
        self.lock_path = self.config_dir / "download_queue.lock"
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        self._lock = MediaLock(self.lock_path)
        # In-memory cancel flags give the same-process worker instant
        # feedback; cross-process flags are picked up from the file.
        self._memory_flags_lock = threading.Lock()
        self._memory_cancel_flags: set[str] = set()
        self.recover_interrupted()

    # ---------- read helpers (lock-free; writes are atomic) ----------

    def jobs(self) -> list[dict[str, Any]]:
        """Return deep copies of all persisted jobs."""
        return deepcopy(self._read_jobs())

    def get(self, job_id: str) -> dict[str, Any] | None:
        """Return a deep copy of one job, or None when unknown."""
        for job in self._read_jobs():
            if job.get("id") == job_id:
                return deepcopy(job)
        return None

    def counts(self) -> dict[str, int]:
        """Return per-state job counts for summaries."""
        counts = {state: 0 for state in JOB_STATES}
        for job in self._read_jobs():
            state = job.get("state")
            if state in counts:
                counts[state] += 1
        return counts

    def peek_due(self, now: float | None = None) -> dict[str, Any] | None:
        """Return the oldest due pending job without claiming it."""
        current = time.time() if now is None else now
        for job in self._read_jobs():
            if job.get("state") != "pending" or job.get("cancel_requested"):
                continue
            next_attempt = job.get("next_attempt_at")
            if next_attempt is not None and next_attempt > current:
                continue
            return deepcopy(job)
        return None

    def memory_cancel_requested(self, job_id: str) -> bool:
        """Check the in-process cancel flag for a running job."""
        with self._memory_flags_lock:
            return job_id in self._memory_cancel_flags

    def disk_cancel_requested(self, job_id: str) -> bool:
        """Re-read the queue file to observe cross-process cancel flags."""
        job = self.get(job_id)
        return bool(job and job.get("cancel_requested"))

    # ---------- mutations (serialized by the queue file lock) ----------

    def enqueue(
        self,
        *,
        channel: str,
        video_id: str,
        title: str,
        url: str,
        snapshot: dict[str, Any] | None = None,
        estimated_bytes: int | None = None,
    ) -> dict[str, Any] | None:
        """Add a download job unless an active one already covers it.

        Args:
            channel: Sanitized channel identifier.
            video_id: YouTube video identifier.
            title: Human-readable video title for the UI.
            url: Watch URL used by the downloader.
            snapshot: Channel settings captured at enqueue time.
            estimated_bytes: Worst-case size estimate used by quota checks.

        Returns:
            The created job, or None when an active job for the same
            (channel, video_id) pair already exists.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            for job in job_list:
                if (
                    job.get("state") in ACTIVE_STATES
                    and job.get("channel") == channel
                    and job.get("video_id") == video_id
                ):
                    return None
            job = _new_job(channel, video_id, title, url, snapshot, estimated_bytes)
            job_list.append(job)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def claim_next(self, now: float | None = None) -> dict[str, Any] | None:
        """Atomically claim the oldest due pending job for execution.

        Only call this while holding the shared media lock so no other
        process can claim and download concurrently. Pending jobs whose
        cancellation was requested are retired as ``cancelled`` instead of
        being claimed.

        Args:
            now: Override the current epoch time (used by tests).

        Returns:
            The claimed job copy with ``state`` set to ``running`` and its
            attempt count incremented, or None when nothing is claimable.
        """
        current = time.time() if now is None else now

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            candidates = sorted(
                (job for job in job_list if job.get("state") == "pending"),
                key=lambda job: job.get("created_at") or "",
            )
            for job in candidates:
                next_attempt = job.get("next_attempt_at")
                if next_attempt is not None and next_attempt > current:
                    continue
                if job.get("cancel_requested"):
                    job.update(
                        state="cancelled",
                        finished_at=_utc_now_iso(),
                        error="Cancelled before start",
                        cancel_requested=False,
                    )
                    self._forget_memory_flag(job["id"])
                    continue
                job.update(
                    state="running",
                    started_at=_utc_now_iso(),
                    attempts=int(job.get("attempts", 0)) + 1,
                    progress=None,
                    next_attempt_at=None,
                    error=None,
                    cancel_requested=False,
                )
                return deepcopy(job)
            return None

        return self._mutate(mutate, None)

    def mark_completed(self, job_id: str) -> dict[str, Any] | None:
        """Mark a running job completed and clear its cancel flag."""
        return self._transition_terminal(job_id, "completed", None)

    def mark_cancelled(
        self, job_id: str, error: str | None = None
    ) -> dict[str, Any] | None:
        """Mark a running or pending job cancelled."""
        return self._transition_terminal(job_id, "cancelled", error)

    def mark_failed(self, job_id: str, error: str) -> dict[str, Any] | None:
        """Record a failure, scheduling a retry or a terminal failure.

        Args:
            job_id: Identifier of the running job.
            error: Human-readable description of the last error.

        Returns:
            The updated job, or None when the job is unknown or not running.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return None
            attempts = int(job.get("attempts", 0))
            max_attempts = int(job.get("max_attempts", MAX_ATTEMPTS))
            updates: dict[str, Any] = {
                "error": error,
                "progress": None,
                "cancel_requested": False,
            }
            if attempts >= max_attempts:
                updates.update(state="failed", finished_at=_utc_now_iso())
            else:
                updates.update(
                    state="pending",
                    started_at=None,
                    next_attempt_at=time.time() + backoff_delay_seconds(attempts),
                )
            job.update(updates)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def mark_failed_terminal(self, job_id: str, error: str) -> dict[str, Any] | None:
        """Retire a running job as failed immediately, with no retry attempt.

        Used for deterministic blockers such as storage quota limits: the
        job lands in the terminal ``failed`` state with an actionable
        message and can be retried manually once the blocker is resolved.

        Args:
            job_id: Identifier of the running job.
            error: Human-readable, actionable description of the blocker.

        Returns:
            The updated job, or None when the job is unknown or not running.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return None
            job.update(
                state="failed",
                finished_at=_utc_now_iso(),
                error=error,
                progress=None,
                cancel_requested=False,
            )
            self._forget_memory_flag(job_id)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def defer(
        self, job_id: str, error: str, delay: float = RETRY_BASE_DELAY_SECONDS
    ) -> dict[str, Any] | None:
        """Return a running job to ``pending`` without consuming an attempt.

        Used when a job could not start for an environmental reason that
        says nothing about the download itself, such as an unreadable
        ``channels.yaml``. The attempt counted by :meth:`claim_next` is
        given back, so repeated deferrals never exhaust the retry budget.
        A cancellation requested while the job was running still wins: the
        job is retired as ``cancelled`` instead of being deferred.

        Args:
            job_id: Identifier of the running job.
            error: Actionable message shown while the job waits.
            delay: Seconds until the job becomes due again.

        Returns:
            The updated job, or None when the job is unknown or not running.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return None
            if job.get("cancel_requested") or self.memory_cancel_requested(job_id):
                job.update(
                    state="cancelled",
                    finished_at=_utc_now_iso(),
                    error="Cancelled by user",
                    progress=None,
                    cancel_requested=False,
                )
            else:
                job.update(
                    state="pending",
                    attempts=max(0, int(job.get("attempts", 0)) - 1),
                    next_attempt_at=time.time() + delay,
                    started_at=None,
                    progress=None,
                    error=error,
                )
            self._forget_memory_flag(job_id)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def request_cancel(self, job_id: str) -> dict[str, Any] | None:
        """Cancel a pending job immediately or flag a running job.

        Args:
            job_id: Identifier of an active (pending or running) job.

        Returns:
            The updated job, or None when the job is unknown or already in
            a terminal state.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") not in ACTIVE_STATES:
                return None
            if job["state"] == "running":
                job["cancel_requested"] = True
                self._remember_memory_flag(job_id)
            else:
                job.update(
                    state="cancelled",
                    finished_at=_utc_now_iso(),
                    error="Cancelled by user",
                    cancel_requested=False,
                )
            return deepcopy(job)

        return self._mutate(mutate, None)

    def retry(self, job_id: str) -> dict[str, Any] | None:
        """Requeue a failed or cancelled job with a fresh attempt budget."""

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") not in ("failed", "cancelled"):
                return None
            job.update(
                state="pending",
                attempts=0,
                next_attempt_at=None,
                started_at=None,
                finished_at=None,
                error=None,
                progress=None,
                cancel_requested=False,
            )
            self._forget_memory_flag(job_id)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def dismiss(self, job_id: str) -> bool:
        """Remove a terminal (completed, failed, cancelled) job."""

        def mutate(job_list: list[dict[str, Any]]) -> bool:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") not in TERMINAL_STATES:
                return False
            job_list.remove(job)
            self._forget_memory_flag(job_id)
            return True

        result = self._mutate(mutate, False)
        return bool(result)

    def clear_completed(self) -> int:
        """Remove every completed job, returning how many were removed."""

        def mutate(job_list: list[dict[str, Any]]) -> int:
            before = len(job_list)
            job_list[:] = [job for job in job_list if job.get("state") != "completed"]
            return before - len(job_list)

        result = self._mutate(mutate, 0)
        return int(result)

    def update_progress(self, job_id: str, progress: dict[str, Any]) -> None:
        """Persist progress for a running job (callers throttle frequency)."""

        def mutate(job_list: list[dict[str, Any]]) -> bool:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return False
            job["progress"] = dict(progress)
            return True

        self._mutate(mutate, False)

    def modify(
        self, job_id: str, mutator: Callable[[dict[str, Any]], None]
    ) -> dict[str, Any] | None:
        """Apply a caller callback to a stored job (maintenance hook).

        Args:
            job_id: Identifier of the job to modify.
            mutator: Callable receiving the mutable job record.

        Returns:
            A copy of the updated job, or None when the job is unknown.
        """

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None:
                return None
            mutator(job)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def recover_interrupted(self) -> int:
        """Requeue jobs left ``running`` by a crashed process."""
        with self._locked() as usable:
            if not usable:
                return 0
            job_list = self._load_for_update()
            if job_list is None:
                return 0
            recovered = [job for job in job_list if job.get("state") == "running"]
            for job in recovered:
                job.update(
                    state="pending",
                    started_at=None,
                    progress=None,
                    next_attempt_at=None,
                )
            if recovered:
                self._save_unlocked(job_list)
                self.logger.info(
                    f"♻️  Resumed {len(recovered)} interrupted download job(s)"
                )
            return len(recovered)

    def requeue_running(self, job_id: str) -> bool:
        """Return one running job to ``pending`` (its worker is stopping).

        Unlike :meth:`recover_interrupted`, this only touches the job the
        stopping worker actually claimed, so stopping an idle worker can
        never requeue a job another process is executing.
        """
        with self._locked() as usable:
            if not usable:
                return False
            job_list = self._load_for_update()
            if job_list is None:
                return False
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return False
            job.update(
                state="pending",
                started_at=None,
                progress=None,
                next_attempt_at=None,
            )
            self._save_unlocked(job_list)
            self.logger.info(f"♻️  Requeued interrupted job {job_id}")
            return True

    # ---------- internals ----------

    @contextmanager
    def _locked(self) -> Iterator[bool]:
        """Guard mutations; yields False when the config dir has vanished."""
        if not self.config_dir.is_dir():
            yield False
            return
        if not self._lock.acquire(blocking=True):  # pragma: no cover
            yield False
            return
        try:
            yield True
        finally:
            self._lock.release()

    def _read_jobs(self) -> list[dict[str, Any]]:
        """Read jobs without acquiring the lock (writes are atomic).

        A corrupt file is left for a locked reader to quarantine: by the
        time this reader parsed it, a writer may already have replaced it.
        An unreadable file reads as empty (logged) and is left untouched.
        """
        try:
            return self._load(quarantine=False)
        except StateFileError as error:
            self.logger.error(f"Error reading download queue: {error}")
            return []

    def _load_for_update(self) -> list[dict[str, Any]] | None:
        """Load jobs while holding the queue lock, quarantining corruption.

        Returns:
            The jobs, or None (logged) when the file cannot be read; the
            caller must then abort without saving.
        """
        try:
            return self._load(quarantine=True)
        except StateFileError as error:
            self.logger.error(f"Download queue not modified: {error}")
            return None

    def _load(self, *, quarantine: bool) -> list[dict[str, Any]]:
        """Read, validate and normalize the persisted jobs.

        Args:
            quarantine: Move a corrupt queue file aside.

        Raises:
            StateFileError: When the queue file cannot be read.
        """
        data = read_json_state(
            self.file_path,
            validate=_validate_queue_data,
            quarantine=quarantine,
            logger=self.logger,
        )
        if data is None:
            return []
        jobs: list[dict[str, Any]] = []
        for entry in data["jobs"]:
            try:
                jobs.append(self._normalize_job(entry))
            except (TypeError, ValueError, AttributeError) as error:
                self.logger.warning(f"Dropping invalid queue entry: {error}")
        return jobs

    def _normalize_job(self, job: Any) -> dict[str, Any]:
        """Fill defaults for older records and validate required identity."""
        if not isinstance(job, dict) or not isinstance(job.get("id"), str):
            raise TypeError("queue entries must be objects with an id")
        normalized = dict(_JOB_DEFAULTS)
        normalized.update({k: v for k, v in job.items() if v is not None})
        if normalized["state"] not in JOB_STATES:
            normalized["state"] = "pending"
        normalized["attempts"] = max(0, int(normalized["attempts"]))
        normalized["max_attempts"] = max(1, int(normalized["max_attempts"]))
        normalized["cancel_requested"] = bool(normalized["cancel_requested"])
        estimate = normalized.get("estimated_bytes")
        normalized["estimated_bytes"] = (
            int(estimate)
            if isinstance(estimate, int)
            and not isinstance(estimate, bool)
            and estimate > 0
            else None
        )
        # Copy the snapshot: the defaults dict must never be shared.
        snapshot = normalized["config_snapshot"]
        normalized["config_snapshot"] = (
            dict(snapshot) if isinstance(snapshot, dict) else {}
        )
        return normalized

    def _save_unlocked(self, job_list: list[dict[str, Any]]) -> None:
        """Bound the completed history and persist atomically."""
        completed = sorted(
            (job for job in job_list if job.get("state") == "completed"),
            key=lambda job: job.get("finished_at") or "",
            reverse=True,
        )
        overflow = len(completed) - COMPLETED_HISTORY_LIMIT
        if overflow > 0:
            stale_ids = {job["id"] for job in completed[COMPLETED_HISTORY_LIMIT:]}
            job_list[:] = [job for job in job_list if job.get("id") not in stale_ids]
        atomic_write_text(self.file_path, json.dumps({"jobs": job_list}, indent=2))

    def _mutate(self, mutator: Callable[[Any], Any], empty: Any) -> Any:
        """Run a read-modify-write cycle under the queue lock.

        Returns ``empty`` without saving when the config directory vanished
        or the queue file cannot be read, so a failed read never replaces
        the persisted jobs with an empty list.
        """
        with self._locked() as usable:
            if not usable:
                return empty
            job_list = self._load_for_update()
            if job_list is None:
                return empty
            result = mutator(job_list)
            self._save_unlocked(job_list)
            return result

    @staticmethod
    def _find(job_list: list[dict[str, Any]], job_id: str) -> dict[str, Any] | None:
        """Locate a job record inside the list being mutated."""
        for job in job_list:
            if job.get("id") == job_id:
                return job
        return None

    def _transition_terminal(
        self, job_id: str, state: str, error: str | None
    ) -> dict[str, Any] | None:
        """Move a running job into a terminal state."""

        def mutate(job_list: list[dict[str, Any]]) -> dict[str, Any] | None:
            job = self._find(job_list, job_id)
            if job is None or job.get("state") != "running":
                return None
            job.update(
                state=state,
                finished_at=_utc_now_iso(),
                error=error,
                cancel_requested=False,
            )
            if state == "completed":
                job["progress"] = {"percent": 100, "speed": None, "eta": None}
            self._forget_memory_flag(job_id)
            return deepcopy(job)

        return self._mutate(mutate, None)

    def _remember_memory_flag(self, job_id: str) -> None:
        """Record an in-process cancel request for instant hook checks."""
        with self._memory_flags_lock:
            self._memory_cancel_flags.add(job_id)

    def _forget_memory_flag(self, job_id: str) -> None:
        """Drop the in-process cancel flag once the job leaves running."""
        with self._memory_flags_lock:
            self._memory_cancel_flags.discard(job_id)


def format_progress(d: dict[str, Any]) -> dict[str, Any]:
    """Convert a yt-dlp progress hook payload into the stored shape."""
    total = d.get("total_bytes") or d.get("total_bytes_estimate")
    downloaded = d.get("downloaded_bytes")
    percent = None
    if (
        isinstance(total, (int, float))
        and total > 0
        and isinstance(downloaded, (int, float))
    ):
        percent = round(min(100.0, downloaded / total * 100.0), 1)
    return {
        "percent": percent,
        "speed": d.get("speed"),
        "eta": d.get("eta"),
    }


def execute_queue_job(
    job: dict[str, Any],
    *,
    videos_dir: Path,
    config_path: Path,
    cancel_check: Callable[[], bool],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    log_handler: logging.Handler | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Execute one queued download using the standard downloader pipeline.

    The channel's *current* configuration wins over the snapshot taken at
    enqueue time, so format/quality edits apply to queued work. When the
    channel was deleted the job fails with :class:`ChannelMissingError` so
    the worker can retire it as cancelled; when ``channels.yaml`` cannot be
    read at all it fails with :class:`ConfigUnavailableError` so the worker
    defers it instead. After a successful (or already
    present) download the per-channel refresh timestamp is updated and the
    episode cleanup that ``process_channel`` used to perform runs; a
    failure of that cleanup is logged and never fails the job.

    Args:
        job: Persisted job record.
        videos_dir: Media storage root.
        config_path: Path to ``channels.yaml``.
        cancel_check: Returns True when a cancellation was requested.
        progress_callback: Receives progress dicts for persistence.
        log_handler: Optional handler attached to the downloader's logger.
        logger: Optional logger for queue-level messages.

    Raises:
        DownloadCancelled: When ``cancel_check`` reports a cancellation.
        ConfigUnavailableError: When ``channels.yaml`` is missing,
            unreadable, unparsable, or structurally invalid.
        ChannelMissingError: When the channel is absent from a valid
            configuration.
        EpisodeFilteredError: When the channel's current filters exclude
            the video or it was deleted (tombstoned) by the user; the
            worker retires such jobs as cancelled.
        StorageLimitError: When quota or free-space protection blocks the
            download; the worker retires such jobs as terminal failures.
        StateFileError: When ``tombstones.json`` cannot be read; the job
            fails closed and is retried with the normal backoff.
        RuntimeError: When the download failed.
    """
    queue_logger = logger if logger is not None else logging.getLogger(__name__)
    try:
        from .downloader import YouTubeDownloader
    except ImportError:
        from downloader import YouTubeDownloader

    downloader = YouTubeDownloader(str(config_path), videos_dir=str(videos_dir))
    if log_handler is not None:
        downloader.logger.addHandler(log_handler)
    try:
        channels, global_config = downloader.load_config_strict()
        channel_name = job["channel"]
        channel = next(
            (entry for entry in channels if entry.get("name") == channel_name),
            None,
        )
        if channel is None:
            raise ChannelMissingError(
                f"Channel '{channel_name}' is no longer configured"
            )
        snapshot = job.get("config_snapshot") or {}
        format_type = channel.get("format", snapshot.get("format", "video"))
        quality = channel.get("quality", snapshot.get("quality", "max"))
        sponsorblock_categories = channel.get(
            "sponsorblock_categories",
            snapshot.get("sponsorblock_categories", []),
        )
        max_episodes = channel.get("max_episodes", snapshot.get("max_episodes", 10))

        # The channel's current configuration wins over the enqueue
        # snapshot: a video that is now filtered out or was deleted by the
        # user must never be downloaded.
        _check_episode_filters(
            job,
            channel=channel,
            config_dir=config_path.parent,
            downloader=downloader,
        )

        if downloader.video_exists(channel_name, job["video_id"], format_type):
            queue_logger.info(
                f"⏭️  {job['video_id']} already downloaded; marking complete"
            )
        else:
            _check_storage_admission(
                job,
                channel=channel,
                videos_dir=videos_dir,
                global_config=global_config,
                format_type=format_type,
            )

            def progress_hook(d: dict[str, Any]) -> None:
                """Cooperatively abort and report progress from yt-dlp."""
                if cancel_check():
                    raise DownloadCancelled("Cancelled by user")
                if progress_callback is not None:
                    progress_callback(format_progress(d))

            video_info = {
                "id": job["video_id"],
                "title": job.get("title") or "Unknown Title",
                "url": job["url"],
            }
            success = downloader.download_video(
                video_info,
                channel_name,
                sponsorblock_categories=sponsorblock_categories,
                format_type=format_type,
                quality=quality,
                progress_hook=progress_hook,
            )
            if not success:
                raise RuntimeError(
                    f"Download failed for '{job.get('title') or job['video_id']}'"
                )

        downloader.rss_generator.update_channel_refresh_time(channel_name)
        _prune_after_download(
            channel=channel,
            max_episodes=max_episodes,
            videos_dir=videos_dir,
            config_dir=config_path.parent,
            global_config=global_config,
            logger=queue_logger,
        )
    finally:
        if log_handler is not None:
            downloader.logger.removeHandler(log_handler)


def _job_estimate_bytes(job: dict[str, Any]) -> int | None:
    """Return the job's persisted size estimate when it is usable."""
    estimate = job.get("estimated_bytes")
    if isinstance(estimate, int) and not isinstance(estimate, bool) and estimate > 0:
        return estimate
    return None


def _check_episode_filters(
    job: dict[str, Any],
    *,
    channel: dict[str, Any],
    config_dir: Path,
    downloader: Any,
) -> None:
    """Raise :class:`EpisodeFilteredError` when the job may not download.

    Tombstones always apply. Title rules are checked against the job's
    recorded title; duration bounds, Shorts and livestream filters need
    video details the job does not carry, so metadata is fetched from the
    current channel configuration whenever those filters are active. A
    failed metadata fetch never blocks the download on its own — the
    download itself reports such failures.

    Args:
        job: Persisted job record.
        channel: The channel's *current* configuration entry.
        config_dir: Directory holding ``tombstones.json``.
        downloader: Downloader used for on-demand metadata fetches.

    Raises:
        EpisodeFilteredError: When the video is filtered or tombstoned.
        StateFileError: When ``tombstones.json`` cannot be read; nothing
            may be downloaded without knowing the user's deletions.
    """
    video_id = job["video_id"]
    if video_id in tombstoned_ids(config_dir, job["channel"]):
        raise EpisodeFilteredError(
            f"Episode {video_id} was deleted by the user and is remembered as deleted"
        )
    filters = load_episode_filters(channel)
    video: dict[str, Any] = {
        "id": video_id,
        "title": job.get("title") or "",
        "url": job.get("url") or "",
    }
    if (
        filters["min_duration_seconds"] > 0
        or filters["max_duration_seconds"] > 0
        or filters["skip_shorts"]
        or filters["skip_livestreams"]
    ):
        video.update(downloader.get_video_metadata(job["url"]))
    reason = filter_skip_reason(video, filters)
    if reason is not None:
        raise EpisodeFilteredError(f"Filtered by current channel config: {reason}")


def _check_storage_admission(
    job: dict[str, Any],
    *,
    channel: dict[str, Any],
    videos_dir: Path,
    global_config: dict[str, Any],
    format_type: str,
) -> None:
    """Raise :class:`StorageLimitError` when quota or free space blocks the job."""
    settings = load_storage_settings(global_config)
    blocked = admission_error(
        settings=settings,
        channel=channel,
        channel_dir=contained_path(videos_dir, job["channel"]),
        videos_dir=videos_dir,
        format_type=format_type,
        estimate_bytes=_job_estimate_bytes(job),
    )
    if blocked is not None:
        raise StorageLimitError(blocked)


def _prune_after_download(
    *,
    channel: dict[str, Any],
    max_episodes: int,
    videos_dir: Path,
    config_dir: Path,
    global_config: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Run ``max_episodes`` cleanup and retention after a completed download.

    The download already succeeded, so failures here are logged and never
    turn the job into a failed or retried one. An unusable ``pins.json``
    skips both passes: deleting without knowing the pins could remove
    pinned episodes irreversibly.

    Args:
        channel: The channel's current configuration entry.
        max_episodes: Number of newest unpinned episodes to keep.
        videos_dir: Media storage root.
        config_dir: Directory holding ``pins.json``.
        global_config: Configuration keys outside ``channels``.
        logger: Logger for queue-level messages.
    """
    channel_name = channel["name"]
    pinned = pins_for_deletion(
        config_dir, channel_name, logger, "post-download cleanup and retention"
    )
    if pinned is None:
        return
    try:
        cleanup_episodes(videos_dir, channel_name, max_episodes, pinned=pinned)
        _apply_job_retention(
            channel=channel,
            videos_dir=videos_dir,
            global_config=global_config,
            pinned=pinned,
            logger=logger,
        )
    except Exception as error:  # The download is complete; never fail the job.
        logger.error(f"Post-download cleanup failed for {channel_name}: {error}")


def _apply_job_retention(
    *,
    channel: dict[str, Any],
    videos_dir: Path,
    global_config: dict[str, Any],
    pinned: set[str],
    logger: logging.Logger,
) -> int:
    """Apply the channel's effective retention after its download completed."""
    effective = effective_channel_storage(load_storage_settings(global_config), channel)
    if effective["retention_days"] <= 0:
        return 0
    removed = apply_retention(
        videos_dir,
        channel["name"],
        effective["retention_days"],
        pinned=pinned,
        logger=logger,
    )
    if removed:
        logger.info(
            f"🗑️  Retention removed {removed} old episode(s) from {channel['name']}"
        )
    return removed


def _config_unavailable_message(error: ConfigUnavailableError) -> str:
    """Build the actionable job error shown while channels.yaml is unusable."""
    detail = " ".join(str(error).split())  # YAML errors span several lines.
    return (
        f"Waiting for a readable channels.yaml ({detail}). The job retries "
        f"every {RETRY_BASE_DELAY_SECONDS:.0f}s without using an attempt; "
        "fix the file to resume downloads."
    )


class QueueWorker:
    """Single worker thread draining a :class:`DownloadQueue`."""

    def __init__(
        self,
        queue: DownloadQueue,
        executor: Callable[..., None],
        media_lock: MediaLock | None = None,
        logger: logging.Logger | None = None,
        poll_interval: float = 2.0,
    ) -> None:
        """Wire the worker to a queue and a job executor.

        Args:
            queue: The persistent queue to drain.
            executor: Callable executed per claimed job; receives the job
                plus ``cancel_check`` and ``progress_callback`` keyword
                arguments.
            media_lock: Shared flock guarding actual downloads. None only
                when the caller already holds the lock (cron drain).
            logger: Optional logger; defaults to this module's logger.
            poll_interval: Seconds between idle polls for due jobs.
        """
        self.queue = queue
        self.executor = executor
        self.media_lock = media_lock
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # Job id claimed by this worker; used to requeue only our own work
        # when stopping, instead of every running job in the queue.
        self._current_job_id: str | None = None

    def start(self) -> None:
        """Start the background worker thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="download-queue-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop without blocking shutdown and requeue this worker's own job.

        A job claimed by this worker that is still running after the join
        timeout is returned to ``pending`` so the next worker resumes it.
        Jobs claimed by other workers or processes are left untouched.
        """
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)
        job_id = self._current_job_id
        if job_id is not None:
            self.queue.requeue_running(job_id)

    def run_pending(self) -> int:
        """Drain due jobs synchronously; the caller holds the media lock.

        Deferred and retry-scheduled jobs are not due again within the same
        drain, so the loop always terminates. Like the background loop, the
        politeness pause only follows successful downloads.

        Returns:
            The number of jobs processed (completed, failed, cancelled or
            deferred).
        """
        processed = 0
        while not self._stop_event.is_set():
            job = self.queue.claim_next()
            if job is None:
                break
            state = self._execute(job)
            processed += 1
            if state == "completed":
                self._wait_inter_job_delay(job)
        return processed

    def _loop(self) -> None:
        """Poll for due jobs until stopped."""
        while not self._stop_event.is_set():
            try:
                if self._process_next():
                    continue
            except Exception as error:  # Keep the worker alive no matter what.
                self.logger.error(f"Download queue worker error: {error}")
            self._stop_event.wait(self.poll_interval)

    def _process_next(self) -> bool:
        """Claim and execute one job; returns True when a job was handled."""
        if self.queue.peek_due() is None:
            return False
        if self.media_lock is not None and not self.media_lock.acquire(blocking=False):
            return False
        job: dict[str, Any] | None = None
        state = ""
        try:
            job = self.queue.claim_next()
            if job is None:
                return False
            self._current_job_id = job["id"]
            try:
                state = self._execute(job)
            finally:
                self._current_job_id = None
        finally:
            if self.media_lock is not None:
                self.media_lock.release()
        # Politeness pauses must not hold the media lock: otherwise a manual
        # refresh, delete or redownload returns 409 for the whole delay.
        if job is not None and state == "completed":
            self._wait_inter_job_delay(job)
        return True

    def _execute(self, job: dict[str, Any]) -> str:
        """Run the executor and translate the outcome into job state."""
        job_id = job["id"]
        last_cancel_check = 0.0
        last_progress_write = 0.0

        def cancel_check() -> bool:
            """Check in-process flags instantly, file flags at most 1/sec."""
            nonlocal last_cancel_check
            if self.queue.memory_cancel_requested(job_id):
                return True
            now = time.monotonic()
            if now - last_cancel_check >= PROGRESS_MIN_INTERVAL_SECONDS:
                last_cancel_check = now
                return self.queue.disk_cancel_requested(job_id)
            return False

        def progress_callback(progress: dict[str, Any]) -> None:
            """Persist progress at most once per second per job."""
            nonlocal last_progress_write
            now = time.monotonic()
            if now - last_progress_write < PROGRESS_MIN_INTERVAL_SECONDS:
                return
            last_progress_write = now
            try:
                self.queue.update_progress(job_id, progress)
            except Exception as error:
                self.logger.warning(f"Could not persist progress: {error}")

        try:
            self.executor(
                job, cancel_check=cancel_check, progress_callback=progress_callback
            )
        except DownloadCancelled as cancellation:
            self.queue.mark_cancelled(job_id, error=str(cancellation))
        except ConfigUnavailableError as unavailable:
            self.logger.warning(f"Deferring job ({job_id}): {unavailable}")
            self.queue.defer(job_id, error=_config_unavailable_message(unavailable))
        except ChannelMissingError as missing:
            self.logger.warning(f"Channel removed; cancelling job: {missing}")
            self.queue.mark_cancelled(job_id, error=str(missing))
        except EpisodeFilteredError as filtered:
            self.logger.info(f"Skipping filtered job ({job_id}): {filtered}")
            self.queue.mark_cancelled(job_id, error=str(filtered))
        except StorageLimitError as limit:
            self.logger.warning(f"Storage limit blocked job ({job_id}): {limit}")
            self.queue.mark_failed_terminal(job_id, error=str(limit))
        except Exception as error:
            self.logger.error(f"Download job failed ({job_id}): {error}")
            self.queue.mark_failed(job_id, str(error))
        else:
            self.queue.mark_completed(job_id)
        final = self.queue.get(job_id)
        return str(final.get("state")) if final else "pending"

    def _wait_inter_job_delay(self, job: dict[str, Any]) -> None:
        """Honor the configured pause between successful downloads."""
        snapshot = job.get("config_snapshot") or {}
        delay = snapshot.get("download_delay_seconds", 0)
        if isinstance(delay, (int, float)) and delay > 0:
            self._stop_event.wait(float(delay))
