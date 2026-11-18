"""Tests for storage controls: accounting, settings, pins, retention, quotas.

The HTTP surface runs through the Flask test client; accounting, retention
and admission logic run directly against temporary storage trees.
"""

import json
import logging
import os
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from src.download_queue import QueueWorker, StorageLimitError
from src.storage import (
    DEFAULT_ESTIMATE_AUDIO_BYTES,
    DEFAULT_ESTIMATE_VIDEO_BYTES,
    GB,
    PinsUnavailableError,
    admission_error,
    apply_retention,
    channel_usage,
    effective_channel_storage,
    estimate_download_bytes,
    free_disk_bytes,
    load_pins,
    load_storage_settings,
    measure_media_usage,
    pinned_ids,
    pins_path,
    remove_channel_pins,
    remove_episode_pin,
    set_episode_pinned,
    validate_storage_settings,
)
from src.utils import (
    CorruptStateFileError,
    StateFileError,
    atomic_write_text,
    cleanup_episodes,
    read_json_state,
    read_json_state_strict,
    require_json_object,
    validate_channel_settings,
)
from tests.test_auth import auth_client as auth_client
from tests.test_auth import auth_server as auth_server
from tests.test_download_queue import StubDownloader, enqueue_job
from tests.test_download_queue import config_dir as config_dir
from tests.test_download_queue import queue as queue
from tests.test_web_server import (
    another_episode_metadata as another_episode_metadata,
)
from tests.test_web_server import (
    app_config as app_config,
)
from tests.test_web_server import (
    client as client,
)
from tests.test_web_server import (
    episode_metadata as episode_metadata,
)
from tests.test_web_server import (
    setup_test_episodes as setup_test_episodes,
)
from tests.test_web_server import (
    test_server as test_server,
)

NOW = 1_700_000_000.0


def write_config(test_server, **mutations):
    """Persist a modified copy of the server's channels.yaml."""
    config_file = test_server.config_dir / "channels.yaml"
    config = yaml.safe_load(config_file.read_text())
    config.update(mutations)
    config_file.write_text(yaml.safe_dump(config))
    return config


def make_sparse(path, size):
    """Create a file whose reported size is ``size`` without writing bytes."""
    with open(path, "wb") as handle:
        handle.truncate(size)
    return path


def make_episode(channel_dir, episode_id, upload_date, size=1000, mtime=None):
    """Write one valid episode (metadata + media) with optional mtime."""
    channel_dir.mkdir(parents=True, exist_ok=True)
    (channel_dir / f"{episode_id}.json").write_text(
        json.dumps(
            {
                "id": episode_id,
                "title": f"Episode {episode_id}",
                "upload_date": upload_date,
            }
        )
    )
    media = channel_dir / f"{episode_id}.m4a"
    media.write_bytes(b"x" * size)
    if mtime is not None:
        os.utime(media, (mtime, mtime))
    return media


def state_backups(path: Path) -> list[Path]:
    """List quarantine backups (``<name>.corrupt*``) of a state file."""
    return sorted(path.parent.glob(f"{path.name}.corrupt*"))


def break_pins(config_dir: Path, mode: str, unreadable_files) -> bytes:
    """Make ``pins.json`` corrupt or unreadable and return its bytes.

    The unreadable variant holds valid pins for ``a`` in ``test_channel``,
    so any deletion of ``a`` would prove the pins were ignored.
    """
    path = pins_path(config_dir)
    if mode == "corrupt":
        path.write_text('{"test_channel": ["a", ', encoding="utf-8")
    else:
        path.write_text(json.dumps({"test_channel": ["a"]}), encoding="utf-8")
        unreadable_files(path)
    return path.read_bytes()


class TestStateFileHelper:
    """read_json_state: missing, unreadable, corrupt and replaced files."""

    def test_missing_file_reads_as_none(self, tmp_path):
        assert read_json_state(tmp_path / "state.json") is None
        assert read_json_state_strict(tmp_path / "state.json") is None

    def test_valid_file_is_returned(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text('{"a": 1}')
        assert read_json_state(path, validate=require_json_object) == {"a": 1}

    def test_io_error_raises_and_leaves_file_untouched(
        self, tmp_path, unreadable_files
    ):
        path = tmp_path / "state.json"
        # Even corrupt bytes must never be quarantined after a failed read.
        path.write_text("{not json")
        unreadable_files(path)
        with pytest.raises(StateFileError, match="Permission denied"):
            read_json_state(path)
        with pytest.raises(StateFileError):
            read_json_state_strict(path)
        assert path.read_bytes() == b"{not json"
        assert state_backups(path) == []

    def test_corrupt_file_is_quarantined(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("{not json")
        assert read_json_state(path) is None
        assert not path.exists()
        assert Path(f"{path}.corrupt").read_text() == "{not json"

    def test_failed_validation_counts_as_corrupt(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("[1, 2]")
        assert read_json_state(path, validate=require_json_object) is None
        assert Path(f"{path}.corrupt").read_text() == "[1, 2]"

    def test_undecodable_bytes_count_as_corrupt(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_bytes(b"\xff\xfe\x00garbage")
        assert read_json_state(path) is None
        assert Path(f"{path}.corrupt").read_bytes() == b"\xff\xfe\x00garbage"

    def test_later_corruptions_never_overwrite_earlier_backups(self, tmp_path):
        path = tmp_path / "state.json"
        for content in ("first", "second", "third"):
            path.write_text(f"{content} corruption")
            assert read_json_state(path) is None
        first = Path(f"{path}.corrupt")
        assert first.read_text() == "first corruption"
        backups = state_backups(path)
        assert len(backups) == 3
        assert sorted(backup.read_text() for backup in backups) == [
            "first corruption",
            "second corruption",
            "third corruption",
        ]
        assert all(
            backup.name.startswith("state.json.corrupt.")
            for backup in backups
            if backup != first
        )

    def test_quarantine_disabled_leaves_file_in_place(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("{not json")
        assert read_json_state(path, quarantine=False) is None
        assert path.read_text() == "{not json"
        assert state_backups(path) == []

    def test_file_replaced_after_parsing_is_not_quarantined(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("[1]")

        def replace_then_reject(data):
            # A writer atomically replaces the file while this reader is
            # still deciding that its (old) copy is invalid.
            atomic_write_text(path, '{"fresh": true}')
            raise ValueError("wrong shape")

        assert read_json_state(path, validate=replace_then_reject) is None
        assert json.loads(path.read_text()) == {"fresh": True}
        assert state_backups(path) == []

    def test_strict_read_raises_and_keeps_corrupt_file(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("{not json")
        with pytest.raises(CorruptStateFileError, match="state.json is corrupt"):
            read_json_state_strict(path)
        assert path.read_text() == "{not json"
        assert state_backups(path) == []


class TestSettingsValidation:
    """Storage settings validation, defaults and inheritance."""

    def test_defaults_when_storage_key_missing(self):
        assert load_storage_settings({}) == {
            "global_quota_gb": 0,
            "min_free_space_gb": 1,
            "retention_days": 0,
        }

    def test_invalid_yaml_values_fall_back_to_defaults(self):
        config = {
            "storage": {
                "global_quota_gb": "ten",
                "min_free_space_gb": -5,
                "retention_days": True,
            }
        }
        assert load_storage_settings(config) == load_storage_settings({})

    @pytest.mark.parametrize(
        "payload",
        [
            {"global_quota_gb": -1},
            {"global_quota_gb": True},
            {"global_quota_gb": "10"},
            {"global_quota_gb": 999_999},
            {"min_free_space_gb": -0.5},
            {"min_free_space_gb": 2048},
            {"retention_days": -1},
            {"retention_days": 40_000},
            {"unknown_key": 1},
        ],
    )
    def test_validate_storage_settings_rejects_invalid(self, payload):
        with pytest.raises(ValueError):
            validate_storage_settings(payload)

    def test_validate_storage_settings_accepts_bounds(self):
        validate_storage_settings(
            {"global_quota_gb": 16384, "min_free_space_gb": 0, "retention_days": 36500}
        )

    @pytest.mark.parametrize(
        "payload",
        [
            {"quota_gb": -2},
            {"quota_gb": False},
            {"retention_days": "30"},
            {"retention_days": 1e9},
        ],
    )
    def test_validate_channel_settings_rejects_invalid_overrides(self, payload):
        with pytest.raises(ValueError):
            validate_channel_settings(payload)

    def test_validate_channel_settings_accepts_overrides(self):
        validate_channel_settings({"quota_gb": 10, "retention_days": 90})

    def test_effective_settings_inheritance(self):
        settings: dict[str, int | float] = {
            "global_quota_gb": 5,
            "min_free_space_gb": 2,
            "retention_days": 14,
        }
        # Absent and zero both inherit the global values.
        for channel in ({}, {"quota_gb": 0, "retention_days": 0}):
            effective = effective_channel_storage(settings, channel)
            assert effective == {
                "quota_gb": 5,
                "retention_days": 14,
                "min_free_space_gb": 2,
            }
        # Explicit overrides win.
        effective = effective_channel_storage(
            settings, {"quota_gb": 1.5, "retention_days": 3}
        )
        assert effective["quota_gb"] == 1.5
        assert effective["retention_days"] == 3


class TestAccounting:
    """Media usage measurement with extension filtering and symlink safety."""

    def test_counts_only_media_extensions(self, tmp_path):
        channel = tmp_path / "chan"
        channel.mkdir()
        make_sparse(channel / "ep1.mp4", 3000)
        (channel / "ep2.m4a").write_bytes(b"b" * 250)
        (channel / "notes.txt").write_bytes(b"c" * 100_000)
        (channel / "ep1.json").write_text("{}")
        thumbs = channel / "thumbnails"
        thumbs.mkdir()
        (thumbs / "ep1.jpg").write_bytes(b"d" * 50_000)

        usage = measure_media_usage(tmp_path)
        assert usage == {"chan": {"bytes": 3250, "episodes": 2}}

    def test_channel_usage_reports_zeros_for_unknown_channel(self, tmp_path):
        assert channel_usage(tmp_path, "ghost") == {"bytes": 0, "episodes": 0}

    def test_symlinks_are_never_followed(self, tmp_path):
        outside = tmp_path / "outside.bin"
        outside.write_bytes(b"x" * 400_000)
        videos = tmp_path / "videos"
        channel = videos / "chan"
        channel.mkdir(parents=True)
        (channel / "ep.mp4").write_bytes(b"a" * 1000)
        os.symlink(outside, channel / "linked.mp4")
        inner = channel / "inner"
        inner.mkdir()
        os.symlink(outside, inner / "nested.webm")
        os.symlink(outside, videos / "linked_channel_dir")

        usage = measure_media_usage(videos)
        assert usage == {"chan": {"bytes": 1000, "episodes": 1}}

    def test_free_disk_bytes_reports_free_space(self, tmp_path):
        assert free_disk_bytes(tmp_path) > 0


class TestEstimatesAndAdmission:
    """Download size estimates and quota/free-space admission checks."""

    def test_estimate_uses_largest_existing_episode(self, tmp_path):
        channel = tmp_path / "chan"
        channel.mkdir()
        make_sparse(channel / "big.mp4", 3 * GB)
        estimate = estimate_download_bytes(channel, "video", 100)
        assert estimate == 3 * GB

    def test_estimate_metadata_hint_can_dominate(self, tmp_path):
        channel = tmp_path / "chan"
        channel.mkdir()
        (channel / "small.mp4").write_bytes(b"a" * 1000)
        estimate = estimate_download_bytes(channel, "video", 5 * GB)
        assert estimate == 5 * GB

    def test_estimate_falls_back_to_format_default(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        assert estimate_download_bytes(empty, "video") == DEFAULT_ESTIMATE_VIDEO_BYTES
        assert estimate_download_bytes(empty, "audio") == DEFAULT_ESTIMATE_AUDIO_BYTES

    BASE_SETTINGS: dict[str, int | float] = {
        "global_quota_gb": 0,
        "min_free_space_gb": 1,
        "retention_days": 0,
    }

    def test_blocks_when_free_space_below_minimum(self, tmp_path):
        blocked = admission_error(
            settings=self.BASE_SETTINGS,
            channel={},
            channel_dir=tmp_path / "chan",
            videos_dir=tmp_path,
            format_type="video",
            usage_totals={},
            free_bytes=int(0.5 * GB),
        )
        assert blocked is not None
        assert "Insufficient free disk space" in blocked
        assert "0.5 GB free" in blocked

    def test_allows_when_free_space_above_minimum(self, tmp_path):
        assert (
            admission_error(
                settings=self.BASE_SETTINGS,
                channel={},
                channel_dir=tmp_path / "chan",
                videos_dir=tmp_path,
                format_type="video",
                usage_totals={},
                free_bytes=2 * GB,
            )
            is None
        )

    def test_disabled_minimum_never_blocks(self, tmp_path):
        settings = dict(self.BASE_SETTINGS, min_free_space_gb=0)
        assert (
            admission_error(
                settings=settings,
                channel={},
                channel_dir=tmp_path / "chan",
                videos_dir=tmp_path,
                format_type="video",
                usage_totals={},
                free_bytes=0,
            )
            is None
        )

    def test_channel_quota_block_message_is_actionable(self, tmp_path):
        channel_dir = tmp_path / "chan"
        channel_dir.mkdir()
        make_sparse(channel_dir / "ep.mp4", 3 * GB)
        usage = measure_media_usage(tmp_path)
        blocked = admission_error(
            settings=self.BASE_SETTINGS,
            channel={"quota_gb": 4},
            channel_dir=channel_dir,
            videos_dir=tmp_path,
            format_type="video",
            usage_totals=usage,
            free_bytes=100 * GB,
        )
        assert blocked is not None
        assert "Storage quota exceeded: 6.0/4 GB" in blocked

    def test_channel_quota_allows_when_within_budget(self, tmp_path):
        channel_dir = tmp_path / "chan"
        channel_dir.mkdir()
        make_sparse(channel_dir / "ep.mp4", 3 * GB)
        assert (
            admission_error(
                settings=self.BASE_SETTINGS,
                channel={"quota_gb": 6},
                channel_dir=channel_dir,
                videos_dir=tmp_path,
                format_type="video",
                usage_totals=measure_media_usage(tmp_path),
                free_bytes=100 * GB,
            )
            is None
        )

    def test_global_quota_block(self, tmp_path):
        settings = dict(self.BASE_SETTINGS, global_quota_gb=1)
        blocked = admission_error(
            settings=settings,
            channel={},
            channel_dir=tmp_path / "chan",
            videos_dir=tmp_path,
            format_type="video",
            usage_totals={"chan": {"bytes": int(0.5 * GB), "episodes": 1}},
            free_bytes=100 * GB,
        )
        assert blocked is not None
        assert "Global storage quota exceeded" in blocked

    def test_unlimited_when_quota_is_zero(self, tmp_path):
        blocked = admission_error(
            settings=self.BASE_SETTINGS,
            channel={},
            channel_dir=tmp_path / "chan",
            videos_dir=tmp_path,
            format_type="video",
            usage_totals={"chan": {"bytes": 999 * GB, "episodes": 1}},
            free_bytes=100 * GB,
        )
        assert blocked is None

    def test_free_space_queried_when_not_supplied(self, tmp_path):
        # tmp_path exists so the real disk_usage query succeeds and, with a
        # huge minimum, turns into a block.
        settings = dict(self.BASE_SETTINGS, min_free_space_gb=10_000_000)
        blocked = admission_error(
            settings=settings,
            channel={},
            channel_dir=tmp_path / "chan",
            videos_dir=tmp_path,
            format_type="video",
            usage_totals={},
        )
        assert blocked is not None
        assert "Insufficient free disk space" in blocked

    def test_measured_usage_and_free_space_used_when_not_supplied(self, tmp_path):
        settings = dict(self.BASE_SETTINGS, global_quota_gb=1)
        channel_dir = tmp_path / "chan"
        channel_dir.mkdir()
        make_sparse(channel_dir / "ep.mp4", 2 * GB)
        blocked = admission_error(
            settings=settings,
            channel={},
            channel_dir=channel_dir,
            videos_dir=tmp_path,
            format_type="video",
        )
        assert blocked is not None
        assert "Global storage quota exceeded" in blocked


class TestRetention:
    """Age-based deletion semantics."""

    def seed(self, tmp_path, episodes):
        channel = tmp_path / "chan"
        for episode_id, upload_date, mtime in episodes:
            make_episode(channel, episode_id, upload_date, mtime=mtime)
        return channel

    def test_deletes_by_upload_date_not_mtime(self, tmp_path):
        channel = self.seed(
            tmp_path,
            [
                ("olddate", "20200101", NOW - 3600),  # fresh mtime, old upload
                ("newdate", "20991231", NOW - 40 * 86400),  # old mtime, new upload
            ],
        )
        removed = apply_retention(
            tmp_path, "chan", 30, logger=logging.getLogger("unused"), now=NOW
        )
        assert removed == 1
        assert not (channel / "olddate.m4a").exists()
        assert (channel / "newdate.m4a").exists()

    def test_mtime_fallback_when_upload_date_missing(self, tmp_path):
        channel = tmp_path / "chan"
        channel.mkdir()
        (channel / "old.json").write_text(
            json.dumps({"id": "old", "title": "No date", "upload_date": ""})
        )
        media = channel / "old.m4a"
        media.write_bytes(b"x" * 10)
        os.utime(media, (NOW - 100 * 86400, NOW - 100 * 86400))

        removed = apply_retention(tmp_path, "chan", 30, now=NOW)
        assert removed == 1
        assert not media.exists()
        assert not (channel / "old.json").exists()

    def test_boundary_age_is_kept(self, tmp_path):
        cutoff = NOW - 10 * 86400
        channel = self.seed(
            tmp_path,
            [
                ("exactly", "", cutoff),
                ("older", "", cutoff - 1),
                ("fresh", "", NOW - 60),
            ],
        )
        removed = apply_retention(tmp_path, "chan", 10, now=NOW)
        assert removed == 1
        assert (channel / "exactly.m4a").exists()
        assert (channel / "fresh.m4a").exists()

    def test_deletes_media_thumbnail_and_metadata(self, tmp_path):
        channel = self.seed(tmp_path, [("gone", "20200101", 0)])
        thumbs = channel / "thumbnails"
        thumbs.mkdir()
        (thumbs / "gone.jpg").write_bytes(b"img")

        removed = apply_retention(tmp_path, "chan", 30, now=NOW)
        assert removed == 1
        assert not (channel / "gone.m4a").exists()
        assert not (channel / "gone.json").exists()
        assert not (thumbs / "gone.jpg").exists()

    def test_pinned_episodes_are_never_deleted(self, tmp_path):
        channel = self.seed(
            tmp_path, [("keepme", "20200101", 0), ("victim", "20200201", 0)]
        )
        removed = apply_retention(tmp_path, "chan", 30, pinned={"keepme"}, now=NOW)
        assert removed == 1
        assert (channel / "keepme.m4a").exists()
        assert (channel / "keepme.json").exists()

    def test_oldest_deleted_first(self, tmp_path):
        channel = self.seed(
            tmp_path,
            [
                ("mid", "20210301", 0),
                ("oldest", "20200101", 0),
                ("newest", "20220601", 0),
            ],
        )
        records = []
        logger = logging.getLogger("retention-order")
        logger.setLevel(logging.INFO)
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        logger.addHandler(handler)
        logger.propagate = False

        removed = apply_retention(tmp_path, "chan", 30, logger=logger, now=NOW)
        assert removed == 3
        assert ["oldest", "mid", "newest"] == [
            message.split()[2] for message in records
        ]
        assert not any(
            (channel / f"{episode_id}.m4a").exists()
            for episode_id in ("oldest", "mid", "newest")
        )

    def test_second_run_is_idempotent(self, tmp_path):
        self.seed(tmp_path, [("old", "20200101", 0)])
        assert apply_retention(tmp_path, "chan", 30, now=NOW) == 1
        assert apply_retention(tmp_path, "chan", 30, now=NOW) == 0

    def test_zero_disables_retention(self, tmp_path):
        channel = self.seed(tmp_path, [("ancient", "19990101", 0)])
        assert apply_retention(tmp_path, "chan", 0, now=NOW) == 0
        assert (channel / "ancient.m4a").exists()

    def test_missing_channel_is_safe(self, tmp_path):
        assert apply_retention(tmp_path, "ghost", 30, now=NOW) == 0


class TestCleanupPins:
    """max_episodes pruning respects pins."""

    def test_pinned_excess_episode_survives(self, tmp_path):
        channel = tmp_path / "chan"
        for episode_id, upload_date in (
            ("a", "20200101"),
            ("b", "20210101"),
            ("c", "20220101"),
            ("d", "20230101"),
        ):
            make_episode(channel, episode_id, upload_date)

        cleanup_episodes(tmp_path, "chan", 2, pinned={"a"})
        remaining = sorted(path.stem for path in channel.glob("*.json"))
        # a is pinned; among the unpinned episodes only the two newest stay.
        assert remaining == ["a", "c", "d"]

    def test_all_pinned_channel_never_shrinks(self, tmp_path):
        channel = tmp_path / "chan"
        for episode_id, upload_date in (
            ("a", "20200101"),
            ("b", "20210101"),
            ("c", "20220101"),
        ):
            make_episode(channel, episode_id, upload_date)

        cleanup_episodes(tmp_path, "chan", 1, pinned={"a", "b", "c"})
        assert len(list(channel.glob("*.json"))) == 3

    def test_without_pins_prunes_to_max(self, tmp_path):
        channel = tmp_path / "chan"
        for episode_id, upload_date in (
            ("a", "20200101"),
            ("b", "20210101"),
            ("c", "20220101"),
        ):
            make_episode(channel, episode_id, upload_date)

        cleanup_episodes(tmp_path, "chan", 1)
        remaining = sorted(path.stem for path in channel.glob("*.json"))
        assert remaining == ["c"]


class TestStorageAPI:
    """Usage endpoint and global storage settings endpoints."""

    def test_storage_endpoint_reports_usage(self, client, setup_test_episodes):
        response = client.get("/api/storage")
        assert response.status_code == 200
        data = response.get_json()

        assert data["settings"] == {
            "global_quota_gb": 0,
            "min_free_space_gb": 1,
            "retention_days": 0,
        }
        assert data["totals"]["episodes"] == 2
        assert data["totals"]["used_bytes"] > 0
        assert data["totals"]["pinned"] == 0
        assert data["free_bytes"] > 0
        assert (
            data["accounting"] == "media files only; thumbnails and metadata excluded"
        )

        entry = next(c for c in data["channels"] if c["name"] == "test_channel")
        assert entry["episodes"] == 2
        assert entry["used_bytes"] == data["totals"]["used_bytes"]
        assert entry["pinned"] == 0
        assert entry["quota_gb"] == 0
        assert entry["over_quota"] is False
        assert entry["quota_overridden"] is False

    def test_storage_endpoint_flags_over_quota_channels(
        self, client, test_server, setup_test_episodes
    ):
        channel_dir = test_server.videos_dir / "test_channel"
        make_sparse(channel_dir / "big.m4a", 1_000_000)
        config = yaml.safe_load((test_server.config_dir / "channels.yaml").read_text())
        config["channels"][0]["quota_gb"] = 0.0005  # ~524 kB
        (test_server.config_dir / "channels.yaml").write_text(yaml.safe_dump(config))

        data = client.get("/api/storage").get_json()
        entry = next(c for c in data["channels"] if c["name"] == "test_channel")
        assert entry["over_quota"] is True
        assert entry["quota_overridden"] is True
        other = next(c for c in data["channels"] if c["name"] == "another_channel")
        assert other["over_quota"] is False
        assert other["quota_overridden"] is False

    def test_storage_endpoint_counts_pins(
        self, client, test_server, setup_test_episodes
    ):
        client.post("/api/channels/test_channel/episodes/test123abc/pin")
        data = client.get("/api/storage").get_json()
        assert data["totals"]["pinned"] == 1
        entry = next(c for c in data["channels"] if c["name"] == "test_channel")
        assert entry["pinned"] == 1

    def test_storage_config_get_returns_defaults(self, client):
        data = client.get("/api/config/storage").get_json()
        assert data == {
            "global_quota_gb": 0,
            "min_free_space_gb": 1,
            "retention_days": 0,
        }

    def test_storage_config_put_roundtrip(self, client, test_server):
        payload = {
            "global_quota_gb": 100,
            "min_free_space_gb": 2.5,
            "retention_days": 60,
        }
        response = client.put("/api/config/storage", json=payload)
        assert response.status_code == 200
        assert response.get_json()["storage"] == payload

        saved = yaml.safe_load((test_server.config_dir / "channels.yaml").read_text())
        assert saved["storage"] == payload
        assert client.get("/api/config/storage").get_json() == payload

    def test_storage_config_put_merges_partial_update(self, client, test_server):
        assert (
            client.put("/api/config/storage", json={"global_quota_gb": 5}).status_code
            == 200
        )
        assert (
            client.put("/api/config/storage", json={"retention_days": 14}).status_code
            == 200
        )
        data = client.get("/api/config/storage").get_json()
        assert data == {
            "global_quota_gb": 5,
            "min_free_space_gb": 1,
            "retention_days": 14,
        }

    @pytest.mark.parametrize(
        "payload",
        [
            {"global_quota_gb": -1},
            {"global_quota_gb": True},
            {"retention_days": "never"},
            {"min_free_space_gb": 10_000},
            {"bogus": 1},
        ],
    )
    def test_storage_config_put_rejects_invalid_without_saving(
        self, client, test_server, payload
    ):
        config_file = test_server.config_dir / "channels.yaml"
        original = config_file.read_bytes()
        response = client.put("/api/config/storage", json=payload)
        assert response.status_code == 400
        assert "error" in response.get_json()
        assert config_file.read_bytes() == original

    def test_channel_update_accepts_quota_and_retention(self, client, test_server):
        response = client.put(
            "/api/channels/test_channel", json={"quota_gb": 2, "retention_days": 45}
        )
        assert response.status_code == 200
        saved = yaml.safe_load((test_server.config_dir / "channels.yaml").read_text())
        channel = saved["channels"][0]
        assert channel["quota_gb"] == 2
        assert channel["retention_days"] == 45

    def test_channel_update_rejects_invalid_quota(self, client, test_server):
        config_file = test_server.config_dir / "channels.yaml"
        original = config_file.read_bytes()
        assert (
            client.put(
                "/api/channels/test_channel", json={"quota_gb": "lots"}
            ).status_code
            == 400
        )
        assert config_file.read_bytes() == original


class TestPinAPI:
    """Pin endpoints, persistence and lifecycle."""

    def test_pin_unpin_roundtrip(self, client, test_server, setup_test_episodes):
        response = client.post("/api/channels/test_channel/episodes/test123abc/pin")
        assert response.status_code == 200
        assert response.get_json()["pinned"] is True
        assert pinned_ids(test_server.config_dir, "test_channel") == {"test123abc"}

        episodes = client.get("/api/channels/test_channel/episodes").get_json()
        flags = {episode["id"]: episode["pinned"] for episode in episodes["episodes"]}
        assert flags == {"test123abc": True, "test456def": False}

        # Pinning twice is idempotent.
        assert (
            client.post(
                "/api/channels/test_channel/episodes/test123abc/pin"
            ).status_code
            == 200
        )

        unpin = client.delete("/api/channels/test_channel/episodes/test123abc/pin")
        assert unpin.status_code == 200
        assert unpin.get_json()["pinned"] is False
        assert pinned_ids(test_server.config_dir, "test_channel") == set()

    def test_pin_unknown_channel_404(self, client, setup_test_episodes):
        response = client.post("/api/channels/ghost/episodes/test123abc/pin")
        assert response.status_code == 404

    def test_pin_unknown_episode_404(self, client, setup_test_episodes):
        response = client.post("/api/channels/test_channel/episodes/missing/pin")
        assert response.status_code == 404

    def test_pin_requires_authentication(self, auth_client):
        assert (
            auth_client.post("/api/channels/test_channel/episodes/ep/pin").status_code
            == 401
        )
        assert (
            auth_client.delete("/api/channels/test_channel/episodes/ep/pin").status_code
            == 401
        )
        assert auth_client.get("/api/storage").status_code == 401
        assert auth_client.get("/api/config/storage").status_code == 401
        assert (
            auth_client.put(
                "/api/config/storage", json={"global_quota_gb": 1}
            ).status_code
            == 401
        )

    def test_pin_survives_redownload(self, client, setup_test_episodes):
        client.post("/api/channels/test_channel/episodes/test123abc/pin")

        def download(downloader, video, channel_name, **kwargs) -> bool:
            channel = downloader.videos_dir / channel_name
            channel.mkdir(parents=True, exist_ok=True)
            # A fresh metadata rewrite knows nothing about pins.
            (channel / f"{video['id']}.json").write_text(
                json.dumps({"id": video["id"], "title": "Rewritten"})
            )
            (channel / f"{video['id']}.mp4").write_bytes(b"replacement")
            return True

        with patch(
            "src.downloader.YouTubeDownloader.download_video",
            autospec=True,
            side_effect=download,
        ):
            assert (
                client.post(
                    "/api/channels/test_channel/episodes/test123abc/redownload"
                ).status_code
                == 200
            )

        episodes = client.get("/api/channels/test_channel/episodes").get_json()
        flags = {episode["id"]: episode["pinned"] for episode in episodes["episodes"]}
        assert flags["test123abc"] is True

    def test_pin_removed_on_episode_delete(
        self, client, test_server, setup_test_episodes
    ):
        client.post("/api/channels/test_channel/episodes/test123abc/pin")
        assert (
            client.delete(
                "/api/channels/test_channel/episodes/test123abc/delete"
            ).status_code
            == 200
        )
        assert load_pins(test_server.config_dir) == {}

    def test_pins_removed_on_purge(self, client, test_server, setup_test_episodes):
        client.post("/api/channels/test_channel/episodes/test123abc/pin")
        client.post("/api/channels/test_channel/episodes/test456def/pin")
        assert client.post("/api/channels/test_channel/purge").status_code == 200
        assert load_pins(test_server.config_dir) == {}

    def test_pins_removed_on_channel_delete(
        self, client, test_server, setup_test_episodes
    ):
        client.post("/api/channels/test_channel/episodes/test123abc/pin")
        assert client.delete("/api/channels/test_channel").status_code == 200
        assert load_pins(test_server.config_dir) == {}


class TestEnforcement:
    """Quota and free-space enforcement at discovery and execution time."""

    MB = 1024 * 1024

    @pytest.fixture
    def discovery_stub(self, test_server):
        """Downloader double with two queueable videos and no local files."""
        mb = self.MB
        videos = [
            {
                "id": "blocked1",
                "title": "Blocked One",
                "url": "https://www.youtube.com/watch?v=blocked1",
            },
            {
                "id": "blocked2",
                "title": "Blocked Two",
                "url": "https://www.youtube.com/watch?v=blocked2",
            },
        ]

        class DiscoveryStub:
            def __init__(
                self,
                config_path: str = "channels.yaml",
                base_dir: str = ".",
                videos_dir: str | None = None,
            ):
                self.videos_dir = Path(videos_dir or ".")
                self.logger = logging.getLogger("discovery-stub")

            def load_config(self):
                config = yaml.safe_load(
                    (test_server.config_dir / "channels.yaml").read_text()
                )
                return config["channels"], {
                    key: value for key, value in config.items() if key != "channels"
                }

            def get_channel_videos(self, url, max_episodes=10):
                return videos

            def get_video_metadata(self, url):
                return {"id": url.rsplit("=", 1)[-1], "filesize": 500 * mb}

            def video_exists(self, channel_name, video_id, format_type="video"):
                return False

            def is_video_too_new(self, video_info, download_delay_hours):
                return False

        return DiscoveryStub

    def test_discovery_blocked_by_quota_never_enqueues(
        self, test_server, discovery_stub
    ):
        test_server.queue_worker.stop()
        write_config(
            test_server,
            storage={
                "global_quota_gb": 0.000001,
                "min_free_space_gb": 0,
                "retention_days": 0,
            },
        )
        test_server.logger.warning = Mock()

        assert test_server._begin_refresh()
        with patch("src.downloader.YouTubeDownloader", discovery_stub):
            test_server._run_refresh("test_channel")

        assert test_server.queue.jobs() == []
        assert test_server.logger.warning.call_count == 2
        logged = test_server.logger.warning.call_args_list[0].args[0]
        assert "Global storage quota exceeded" in logged

    def test_discovery_enqueues_when_quota_is_unlimited(
        self, test_server, discovery_stub
    ):
        test_server.queue_worker.stop()
        assert test_server._begin_refresh()
        with patch("src.downloader.YouTubeDownloader", discovery_stub):
            test_server._run_refresh("test_channel")

        jobs = test_server.queue.jobs()
        assert [job["video_id"] for job in jobs] == ["blocked1", "blocked2"]
        assert all(job["estimated_bytes"] > 0 for job in jobs)

    def _drain_job(self, test_server, job):
        """Claim and run one job through the real worker translation."""
        worker = QueueWorker(
            test_server.queue,
            executor=test_server._execute_queue_job,
            media_lock=None,
            logger=test_server.logger,
        )
        claimed = test_server.queue.claim_next()
        assert claimed is not None and claimed["id"] == job["id"]
        worker._execute(claimed)

    def test_execution_blocked_job_fails_terminally_with_actionable_error(
        self, test_server
    ):
        test_server.queue_worker.stop()
        write_config(
            test_server,
            storage={
                "global_quota_gb": 0.000001,
                "min_free_space_gb": 0,
                "retention_days": 0,
            },
        )
        job = enqueue_job(test_server.queue, video_id="quota victim")
        with (
            patch("src.downloader.YouTubeDownloader", StubDownloader),
            patch("src.download_queue.cleanup_episodes"),
        ):
            self._drain_job(test_server, job)
        stored = test_server.queue.get(job["id"])
        assert stored["state"] == "failed"
        assert "Global storage quota exceeded" in stored["error"]
        # Terminal, not scheduled for automatic retry.
        assert stored["next_attempt_at"] is None
        assert stored["finished_at"] is not None

    def test_retry_succeeds_after_quota_raised(self, test_server):
        test_server.queue_worker.stop()
        write_config(
            test_server,
            storage={
                "global_quota_gb": 0.000001,
                "min_free_space_gb": 0,
                "retention_days": 0,
            },
        )
        job = enqueue_job(test_server.queue, video_id="blocked")
        with (
            patch("src.downloader.YouTubeDownloader", StubDownloader),
            patch("src.download_queue.cleanup_episodes"),
        ):
            self._drain_job(test_server, job)
        assert test_server.queue.get(job["id"])["state"] == "failed"

        # User frees space / raises the quota, then retries manually.
        write_config(
            test_server,
            storage={"global_quota_gb": 0, "min_free_space_gb": 0, "retention_days": 0},
        )
        assert test_server.queue.retry(job["id"])["state"] == "pending"
        StubDownloader.behavior = "success"
        with (
            patch("src.downloader.YouTubeDownloader", StubDownloader),
            patch("src.download_queue.cleanup_episodes"),
        ):
            self._drain_job(test_server, job)
        assert test_server.queue.get(job["id"])["state"] == "completed"

    def test_worker_translates_storage_limit_into_terminal_failure(self, queue):
        executor = Mock(
            side_effect=StorageLimitError("Storage quota exceeded: 5.0/4 GB")
        )
        worker = QueueWorker(queue, executor=executor, media_lock=None)
        job = enqueue_job(queue)
        queue.claim_next()
        worker._execute(queue.get(job["id"]))
        stored = queue.get(job["id"])
        assert stored["state"] == "failed"
        assert stored["error"] == "Storage quota exceeded: 5.0/4 GB"
        assert stored["next_attempt_at"] is None
        # Manually retryable once the blocker is resolved.
        assert queue.retry(job["id"])["state"] == "pending"

    def test_already_downloaded_job_skips_admission(self, test_server):
        test_server.queue_worker.stop()
        write_config(
            test_server,
            storage={
                "global_quota_gb": 0.000001,
                "min_free_space_gb": 0,
                "retention_days": 0,
            },
        )
        channel_dir = test_server.videos_dir / "test_channel"
        channel_dir.mkdir(parents=True, exist_ok=True)
        # test_channel downloads audio, so the stub's video_exists check
        # looks for <id>.m4a alongside the metadata.
        make_sparse(channel_dir / "video1.m4a", 10)
        (channel_dir / "video1.json").write_text(json.dumps({"id": "video1"}))
        job = enqueue_job(test_server.queue, video_id="video1")
        with (
            patch("src.downloader.YouTubeDownloader", StubDownloader),
            patch("src.download_queue.cleanup_episodes"),
        ):
            self._drain_job(test_server, job)
        assert test_server.queue.get(job["id"])["state"] == "completed"

    def test_queue_cleanup_respects_pins(self, test_server):
        test_server.queue_worker.stop()
        channel_dir = test_server.videos_dir / "test_channel"
        for video_id, upload_date in (
            ("a", "20200101"),
            ("b", "20210101"),
            ("c", "20220101"),
        ):
            make_episode(channel_dir, video_id, upload_date)
        set_episode_pinned(test_server.config_dir, "test_channel", "c", True)
        # The channel's current config wins over the job snapshot, so the
        # prune limit must come from channels.yaml.
        config = yaml.safe_load((test_server.config_dir / "channels.yaml").read_text())
        config["channels"][0]["max_episodes"] = 1
        (test_server.config_dir / "channels.yaml").write_text(yaml.safe_dump(config))
        job = enqueue_job(
            test_server.queue,
            video_id="c",
            snapshot={
                "format": "audio",
                "quality": "max",
                "max_episodes": 1,
                "download_delay_seconds": 0,
            },
        )
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            self._drain_job(test_server, job)
        assert test_server.queue.get(job["id"])["state"] == "completed"
        # Pruning to one episode removed the oldest unpinned episode; the
        # pinned newest episode stays even though it exceeds the limit.
        assert not (channel_dir / "a.m4a").exists()
        assert (channel_dir / "b.m4a").exists()
        assert (channel_dir / "c.m4a").exists()


class TestInlineDownloadEnforcement:
    """The legacy inline download path honors storage controls too."""

    @staticmethod
    def _make_downloader(tmp_path, settings):
        from src.downloader import YouTubeDownloader

        config_path = tmp_path / "channels.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {
                    "channels": [
                        {
                            "name": "chan",
                            "url": "https://www.youtube.com/@x",
                            "max_episodes": 5,
                            "format": "video",
                        }
                    ],
                    "storage": settings,
                }
            )
        )
        downloader = YouTubeDownloader(
            str(config_path), videos_dir=str(tmp_path / "podcasts")
        )
        channel = {
            "name": "chan",
            "url": "https://www.youtube.com/@x",
            "max_episodes": 5,
            "format": "video",
            "quality": "max",
            "sponsorblock_categories": [],
        }
        video = {
            "id": "vid1",
            "title": "Video",
            "url": "https://www.youtube.com/watch?v=vid1",
            "filesize": 100,
            "timestamp": time.time(),
        }
        return downloader, channel, video

    def test_process_channel_skips_downloads_over_quota(self, tmp_path):
        settings = {
            "global_quota_gb": 0.001,
            "min_free_space_gb": 0,
            "retention_days": 0,
        }
        downloader, channel, video = self._make_downloader(tmp_path, settings)
        with (
            patch.object(downloader, "get_channel_videos", return_value=[video]),
            patch.object(downloader, "get_video_metadata", return_value=dict(video)),
            patch.object(downloader, "video_exists", return_value=False),
            patch.object(downloader, "is_video_too_new", return_value=False),
            patch.object(downloader, "download_video", return_value=True) as download,
        ):
            count = downloader.process_channel(channel, {"storage": settings})
        assert count == 0
        download.assert_not_called()

    def test_process_channel_downloads_within_quota(self, tmp_path):
        settings = {"global_quota_gb": 0, "min_free_space_gb": 0, "retention_days": 0}
        downloader, channel, video = self._make_downloader(tmp_path, settings)
        with (
            patch.object(downloader, "get_channel_videos", return_value=[video]),
            patch.object(downloader, "get_video_metadata", return_value=dict(video)),
            patch.object(downloader, "video_exists", return_value=False),
            patch.object(downloader, "is_video_too_new", return_value=False),
            patch.object(downloader, "download_video", return_value=True) as download,
        ):
            count = downloader.process_channel(channel, {"storage": settings})
        assert count == 1
        download.assert_called_once()


class TestMaintenance:
    """Startup/daily retention maintenance."""

    def test_maintenance_applies_effective_retention(self, test_server):
        config = write_config(test_server)
        config["channels"][0]["retention_days"] = 30
        (test_server.config_dir / "channels.yaml").write_text(yaml.safe_dump(config))
        channel_dir = test_server.videos_dir / "test_channel"
        make_episode(channel_dir, "ancient", "", mtime=0)
        make_episode(channel_dir, "fresh", "", mtime=None)

        test_server._run_storage_maintenance()

        assert not (channel_dir / "ancient.m4a").exists()
        assert (channel_dir / "fresh.m4a").exists()

    def test_maintenance_skipped_while_media_busy(self, test_server):
        channel_dir = test_server.videos_dir / "test_channel"
        make_episode(channel_dir, "ancient", "", mtime=0)
        config = write_config(test_server)
        config["channels"][0]["retention_days"] = 30
        (test_server.config_dir / "channels.yaml").write_text(yaml.safe_dump(config))

        assert test_server._begin_refresh()
        try:
            test_server._run_storage_maintenance()
            assert (channel_dir / "ancient.m4a").exists()
        finally:
            test_server._finish_refresh()

    def test_maintenance_without_retention_is_a_no_op(self, test_server):
        channel_dir = test_server.videos_dir / "test_channel"
        make_episode(channel_dir, "kept", "", mtime=0)
        test_server._run_storage_maintenance()
        assert (channel_dir / "kept.m4a").exists()


BROKEN_PIN_MODES = pytest.mark.parametrize("mode", ["corrupt", "unreadable"])


class TestPinsFailClosed:
    """A corrupt or unreadable pins.json never reads as "nothing pinned"."""

    @staticmethod
    def seed_old_episodes(test_server) -> Path:
        """Store three old test_channel episodes and enable retention."""
        channel_dir = test_server.videos_dir / "test_channel"
        for episode_id, upload_date in (
            ("a", "20200101"),
            ("b", "20200201"),
            ("c", "20200301"),
        ):
            make_episode(channel_dir, episode_id, upload_date)
        config = write_config(test_server)
        config["channels"][0]["retention_days"] = 30
        config["channels"][0]["max_episodes"] = 1
        (test_server.config_dir / "channels.yaml").write_text(yaml.safe_dump(config))
        return channel_dir

    @staticmethod
    def surviving(channel_dir: Path) -> list[str]:
        return sorted(path.stem for path in channel_dir.glob("*.m4a"))

    @BROKEN_PIN_MODES
    def test_load_pins_raises_and_keeps_file(self, tmp_path, unreadable_files, mode):
        original = break_pins(tmp_path, mode, unreadable_files)
        with pytest.raises(PinsUnavailableError, match="repaired or removed"):
            load_pins(tmp_path)
        with pytest.raises(PinsUnavailableError):
            pinned_ids(tmp_path, "test_channel")
        assert pins_path(tmp_path).read_bytes() == original
        assert state_backups(pins_path(tmp_path)) == []

    def test_wrong_shape_is_corrupt(self, tmp_path):
        pins_path(tmp_path).write_text(json.dumps({"test_channel": "a"}))
        with pytest.raises(PinsUnavailableError):
            load_pins(tmp_path)

    def test_missing_file_means_nothing_pinned(self, tmp_path):
        assert load_pins(tmp_path) == {}
        assert pinned_ids(tmp_path, "test_channel") == set()

    @BROKEN_PIN_MODES
    def test_pin_writers_never_overwrite(self, tmp_path, unreadable_files, mode):
        original = break_pins(tmp_path, mode, unreadable_files)
        assert set_episode_pinned(tmp_path, "test_channel", "b", True) is False
        assert set_episode_pinned(tmp_path, "test_channel", "a", False) is False
        assert remove_episode_pin(tmp_path, "test_channel", "a") is False
        assert remove_channel_pins(tmp_path, "test_channel") is False
        assert pins_path(tmp_path).read_bytes() == original
        assert state_backups(pins_path(tmp_path)) == []

    @BROKEN_PIN_MODES
    def test_cleanup_and_retention_delete_nothing(
        self, test_server, unreadable_files, mode
    ):
        channel_dir = self.seed_old_episodes(test_server)
        break_pins(test_server.config_dir, mode, unreadable_files)
        test_server.logger.error = Mock()
        config = test_server._load_channel_config()

        test_server._cleanup_old_episodes("test_channel", 1)
        assert test_server._apply_channel_retention(config["channels"][0], config) == 0
        test_server._run_storage_maintenance()

        # Pinned "a" and the unpinned episodes all survive.
        assert self.surviving(channel_dir) == ["a", "b", "c"]
        logged = " ".join(
            str(call.args[0]) for call in test_server.logger.error.call_args_list
        )
        assert "Skipping cleanup for test_channel" in logged
        assert "Skipping retention for test_channel" in logged
        assert "pins.json is repaired or removed" in logged

    def test_cleanup_resumes_once_pins_are_repaired(self, test_server):
        channel_dir = self.seed_old_episodes(test_server)
        pins_path(test_server.config_dir).write_text("{broken")
        test_server._cleanup_old_episodes("test_channel", 1)
        assert self.surviving(channel_dir) == ["a", "b", "c"]

        pins_path(test_server.config_dir).write_text(
            json.dumps({"test_channel": ["a"]})
        )
        test_server._cleanup_old_episodes("test_channel", 1)
        assert self.surviving(channel_dir) == ["a", "c"]

    @BROKEN_PIN_MODES
    def test_pin_toggle_refuses_and_keeps_file(
        self, client, test_server, setup_test_episodes, unreadable_files, mode
    ):
        original = break_pins(test_server.config_dir, mode, unreadable_files)
        url = "/api/channels/test_channel/episodes/test123abc/pin"
        for response in (client.post(url), client.delete(url)):
            assert response.status_code == 500
            assert "pins.json is corrupt or unreadable" in response.get_json()["error"]
        assert pins_path(test_server.config_dir).read_bytes() == original
        assert state_backups(pins_path(test_server.config_dir)) == []

    @BROKEN_PIN_MODES
    def test_read_only_views_still_respond(
        self, client, test_server, setup_test_episodes, unreadable_files, mode
    ):
        break_pins(test_server.config_dir, mode, unreadable_files)

        storage = client.get("/api/storage")
        assert storage.status_code == 200
        data = storage.get_json()
        assert data["totals"]["episodes"] == 2
        assert data["totals"]["pinned"] == 0
        assert "pins.json" in data["pins_error"]

        episodes = client.get("/api/channels/test_channel/episodes")
        assert episodes.status_code == 200
        flags = [episode["pinned"] for episode in episodes.get_json()["episodes"]]
        assert flags == [False, False]

    def test_storage_report_has_no_pins_error_when_healthy(
        self, client, setup_test_episodes
    ):
        assert client.get("/api/storage").get_json()["pins_error"] is None

    @staticmethod
    def drain_one(test_server, job):
        """Claim and run one job through the real worker translation."""
        worker = QueueWorker(
            test_server.queue,
            executor=test_server._execute_queue_job,
            media_lock=None,
            logger=test_server.logger,
        )
        claimed = test_server.queue.claim_next()
        assert claimed is not None and claimed["id"] == job["id"]
        worker._execute(claimed)

    @BROKEN_PIN_MODES
    def test_completed_download_survives_skipped_cleanup(
        self, test_server, unreadable_files, mode
    ):
        test_server.queue_worker.stop()
        channel_dir = self.seed_old_episodes(test_server)
        break_pins(test_server.config_dir, mode, unreadable_files)
        job = enqueue_job(test_server.queue, video_id="fresh")
        with patch("src.downloader.YouTubeDownloader", StubDownloader):
            self.drain_one(test_server, job)

        stored = test_server.queue.get(job["id"])
        assert stored["state"] == "completed"
        assert stored["error"] is None
        assert (channel_dir / "fresh.mp4").exists()
        # Neither max_episodes cleanup nor retention deleted anything.
        assert self.surviving(channel_dir) == ["a", "b", "c"]

    def test_cleanup_crash_after_download_keeps_job_completed(self, test_server):
        test_server.queue_worker.stop()
        job = enqueue_job(test_server.queue, video_id="fresh")
        with (
            patch("src.downloader.YouTubeDownloader", StubDownloader),
            patch(
                "src.download_queue.cleanup_episodes",
                side_effect=RuntimeError("disk hiccup"),
            ),
        ):
            self.drain_one(test_server, job)
        stored = test_server.queue.get(job["id"])
        assert stored["state"] == "completed"
        assert stored["attempts"] == 1
