"""Refresh health tracking and webhook notifications."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
import yaml

from src.health import (
    DEFAULT_COOLDOWN_MINUTES,
    DEFAULT_FAILURE_THRESHOLD,
    HISTORY_LIMIT,
    collect_events,
    dispatch_events,
    health_path,
    health_summary,
    load_health,
    load_notification_settings,
    record_refresh,
    remove_channel_health,
    save_health,
    validate_notification_settings,
)
from src.utils import StateFileError
from tests.test_web_server import app_config as app_config
from tests.test_web_server import client as client
from tests.test_web_server import test_server as test_server


def fake_response(status=200):
    """Minimal context-manager response for urlopen patches."""
    response = Mock()
    response.status = status
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    return response


def settings(**overrides):
    """Resolved notification settings with a webhook and short cooldown."""
    base = {
        "webhook_url": "https://example.test/hook",
        "failure_threshold": 3,
        "cooldown_minutes": 60,
        "notify_low_space": True,
    }
    base.update(overrides)
    return base


def health_backups(config_dir) -> list[Path]:
    """List quarantine backups of ``health.json``."""
    path = health_path(config_dir)
    return sorted(path.parent.glob(f"{path.name}.corrupt*"))


def make_health_unreadable(config_dir, unreadable_files) -> bytes:
    """Record history for ``test_channel``, then block reads; return bytes."""
    record_failure(config_dir, count=3, error="boom")
    unreadable_files(health_path(config_dir))
    return health_path(config_dir).read_bytes()


def record_failure(config_dir, channel="test_channel", count=1, **kwargs):
    """Record ``count`` consecutive failures for a channel."""
    state = {}
    for _ in range(count):
        state = record_refresh(config_dir, channel, success=False, **kwargs)
    return state


class TestHealthStore:
    """Persistence, counters, and corruption recovery."""

    def test_success_records_history_and_resets_failures(self, tmp_path):
        record_failure(tmp_path, count=2)
        state = record_refresh(tmp_path, "test_channel", success=True, queued=2)
        assert state["consecutive_failures"] == 0
        assert state["last_error"] is None
        assert state["unavailable"] is False
        assert state["last_success_at"]
        assert [entry["status"] for entry in state["history"]] == [
            "failure",
            "failure",
            "success",
        ]
        assert state["history"][-1]["queued"] == 2

    def test_failure_increments_and_keeps_error(self, tmp_path):
        state = record_refresh(
            tmp_path,
            "test_channel",
            success=False,
            error="boom",
            unavailable=True,
        )
        assert state["consecutive_failures"] == 1
        assert state["last_error"] == "boom"
        assert state["unavailable"] is True
        assert state["last_failure_at"]

    def test_history_is_bounded(self, tmp_path):
        for _ in range(HISTORY_LIMIT + 10):
            record_refresh(tmp_path, "test_channel", success=True)
        state = load_health(tmp_path)["channels"]["test_channel"]
        assert len(state["history"]) == HISTORY_LIMIT

    def test_persists_across_reloads(self, tmp_path):
        record_refresh(tmp_path, "test_channel", success=True)
        reloaded = load_health(tmp_path)
        assert reloaded["channels"]["test_channel"]["history"]

    def test_corrupt_file_is_quarantined(self, tmp_path):
        health_path(tmp_path).write_text("{not json")
        data = load_health(tmp_path)
        assert data["channels"] == {}
        assert health_path(tmp_path).with_suffix(".json.corrupt").is_file()

    def test_unreadable_file_raises_and_is_untouched(self, tmp_path, unreadable_files):
        original = make_health_unreadable(tmp_path, unreadable_files)
        with pytest.raises(StateFileError, match="Permission denied"):
            load_health(tmp_path)
        assert health_path(tmp_path).read_bytes() == original
        assert health_backups(tmp_path) == []

    def test_writers_never_overwrite_unreadable_file(self, tmp_path, unreadable_files):
        original = make_health_unreadable(tmp_path, unreadable_files)
        state = record_refresh(tmp_path, "test_channel", success=False, error="x")
        # Reported for this refresh only; the stored history is not reset.
        assert state["consecutive_failures"] == 1
        assert remove_channel_health(tmp_path, "test_channel") is False
        with patch("src.health.urlopen", return_value=fake_response()):
            results = dispatch_events(
                tmp_path,
                [{"key": "x", "event": "test", "message": "m"}],
                "https://example.test/hook",
            )
        assert results[0]["ok"] is True
        assert health_path(tmp_path).read_bytes() == original
        assert health_backups(tmp_path) == []

    def test_readers_degrade_to_empty_when_unreadable(self, tmp_path, unreadable_files):
        make_health_unreadable(tmp_path, unreadable_files)
        assert health_summary(tmp_path) == {"channels": {}, "notifications": {}}
        assert collect_events(tmp_path, settings(failure_threshold=1)) == []
        events = collect_events(tmp_path, settings(), free_bytes=1, min_free_space_gb=5)
        assert [event["event"] for event in events] == ["low_disk_space"]

    def test_lock_free_readers_leave_corrupt_file_for_writers(self, tmp_path):
        health_path(tmp_path).write_text("{not json")
        assert health_summary(tmp_path)["channels"] == {}
        assert collect_events(tmp_path, settings()) == []
        assert health_path(tmp_path).read_text() == "{not json"
        record_refresh(tmp_path, "test_channel", success=True)
        assert Path(f"{health_path(tmp_path)}.corrupt").read_text() == "{not json"
        assert "test_channel" in load_health(tmp_path)["channels"]

    def test_invalid_entries_are_dropped(self, tmp_path):
        health_path(tmp_path).write_text(
            json.dumps(
                {
                    "channels": {
                        "ok": {
                            "consecutive_failures": "many",
                            "unavailable": "yes",
                            "history": "nope",
                        },
                        "bad": "not an object",
                    }
                }
            )
        )
        data = load_health(tmp_path)
        assert set(data["channels"]) == {"ok"}
        assert data["channels"]["ok"]["consecutive_failures"] == 0
        assert data["channels"]["ok"]["history"] == []

    def test_remove_channel_health(self, tmp_path):
        record_refresh(tmp_path, "test_channel", success=True)
        assert remove_channel_health(tmp_path, "test_channel") is True
        assert remove_channel_health(tmp_path, "test_channel") is False
        assert load_health(tmp_path)["channels"] == {}

    def test_save_health_reports_failure(self, tmp_path):
        with patch("src.health.atomic_write_text", side_effect=OSError("disk full")):
            assert save_health(tmp_path, {"channels": {}}) is False

    def test_health_summary_shape(self, tmp_path):
        record_refresh(tmp_path, "test_channel", success=True)
        summary = health_summary(tmp_path)
        assert set(summary) == {"channels", "notifications"}
        assert "test_channel" in summary["channels"]


class TestNotificationSettings:
    """Defaults and validation for the ``notifications`` config block."""

    def test_defaults(self):
        resolved = load_notification_settings({})
        assert resolved["webhook_url"] == ""
        assert resolved["failure_threshold"] == DEFAULT_FAILURE_THRESHOLD
        assert resolved["cooldown_minutes"] == DEFAULT_COOLDOWN_MINUTES
        assert resolved["notify_low_space"] is True

    def test_invalid_config_falls_back(self):
        resolved = load_notification_settings(
            {
                "notifications": {
                    "webhook_url": 5,
                    "failure_threshold": "3",
                    "cooldown_minutes": -1,
                    "notify_low_space": "yes",
                }
            }
        )
        assert resolved["webhook_url"] == ""
        assert resolved["failure_threshold"] == DEFAULT_FAILURE_THRESHOLD
        assert resolved["cooldown_minutes"] == DEFAULT_COOLDOWN_MINUTES
        assert resolved["notify_low_space"] is True

    def test_validate_accepts_valid_payload(self):
        validate_notification_settings(
            {
                "webhook_url": "https://example.test/hook",
                "failure_threshold": 5,
                "cooldown_minutes": 30,
                "notify_low_space": False,
            }
        )

    def test_validate_empty_webhook_disables(self):
        validate_notification_settings({"webhook_url": ""})

    @pytest.mark.parametrize(
        "payload",
        [
            {"webhook_url": "ftp://example.test/hook"},
            {"webhook_url": "not a url"},
            {"webhook_url": "https://"},
            {"webhook_url": "file:///etc/passwd"},
            {"webhook_url": 3},
            {"failure_threshold": 0},
            {"failure_threshold": 101},
            {"failure_threshold": True},
            {"failure_threshold": 2.5},
            {"cooldown_minutes": -1},
            {"cooldown_minutes": 10081},
            {"notify_low_space": 1},
        ],
    )
    def test_validate_rejects_invalid_payloads(self, payload):
        with pytest.raises(ValueError):
            validate_notification_settings(payload)


class TestEventCollection:
    """Thresholds, unavailability, low disk, and cooldown behavior."""

    def test_failure_threshold(self, tmp_path):
        record_failure(tmp_path, count=2)
        assert collect_events(tmp_path, settings(failure_threshold=3)) == []
        record_failure(tmp_path, count=1)
        events = collect_events(tmp_path, settings(failure_threshold=3))
        assert [event["event"] for event in events] == ["channel_failure"]
        assert "failed 3 consecutive" in events[0]["message"]

    def test_unavailable_channel_takes_precedence(self, tmp_path):
        record_failure(tmp_path, count=5, error="private video", unavailable=True)
        events = collect_events(tmp_path, settings(failure_threshold=2))
        assert [event["event"] for event in events] == ["channel_unavailable"]
        assert events[0]["key"] == "channel_unavailable:test_channel"

    def test_low_disk_event(self, tmp_path):
        events = collect_events(
            tmp_path,
            settings(),
            free_bytes=2 * 1024**3,
            min_free_space_gb=5,
        )
        assert [event["event"] for event in events] == ["low_disk_space"]
        assert events[0]["free_bytes"] == 2 * 1024**3

    def test_low_disk_respects_toggle_and_unknown_space(self, tmp_path):
        assert (
            collect_events(
                tmp_path,
                settings(notify_low_space=False),
                free_bytes=1,
                min_free_space_gb=5,
            )
            == []
        )
        assert (
            collect_events(tmp_path, settings(), free_bytes=None, min_free_space_gb=5)
            == []
        )

    def test_cooldown_suppresses_repeats(self, tmp_path):
        record_failure(tmp_path, count=3)
        now = datetime(2026, 1, 1, tzinfo=UTC)
        events = collect_events(tmp_path, settings(), now=now)
        assert len(events) == 1
        # simulate a successful delivery timestamp without network
        save_health(
            tmp_path,
            {
                "channels": load_health(tmp_path)["channels"],
                "notifications": {
                    "last_sent": {"channel_failure:test_channel": now.isoformat()},
                    "last_attempts": {},
                },
            },
        )
        assert collect_events(tmp_path, settings(), now=now) == []
        later = now + timedelta(minutes=61)
        assert len(collect_events(tmp_path, settings(), now=later)) == 1

    def test_zero_cooldown_repeats(self, tmp_path):
        record_failure(tmp_path, count=3)
        now = datetime(2026, 1, 1, tzinfo=UTC)
        assert len(collect_events(tmp_path, settings(cooldown_minutes=0), now=now)) == 1
        save_health(
            tmp_path,
            {
                "channels": load_health(tmp_path)["channels"],
                "notifications": {
                    "last_sent": {"channel_failure:test_channel": now.isoformat()},
                    "last_attempts": {},
                },
            },
        )
        assert len(collect_events(tmp_path, settings(cooldown_minutes=0), now=now)) == 1


class TestDispatch:
    """Webhook delivery and persisted attempt bookkeeping."""

    def test_empty_webhook_is_a_noop(self, tmp_path):
        assert dispatch_events(tmp_path, [{"key": "x", "event": "test"}], "") == []

    def test_successful_delivery_updates_last_sent(self, tmp_path):
        events = [
            {
                "key": "channel_failure:a",
                "event": "channel_failure",
                "channel": "a",
                "message": "boom",
                "occurred_at": datetime.now(UTC).isoformat(),
                "consecutive_failures": 3,
            }
        ]
        calls = []

        def fake_urlopen(request, timeout):
            calls.append((request, timeout))
            return fake_response()

        with patch("src.health.urlopen", fake_urlopen):
            results = dispatch_events(tmp_path, events, "https://example.test/hook")
        assert results[0]["ok"] is True
        assert calls and calls[0][0].full_url == "https://example.test/hook"
        body = json.loads(calls[0][0].data)
        assert body["event"] == "channel_failure"
        assert body["consecutive_failures"] == 3
        stored = load_health(tmp_path)["notifications"]
        assert "channel_failure:a" in stored["last_sent"]
        assert stored["last_attempts"]["channel_failure:a"]["ok"] is True

    def test_failed_delivery_is_recorded_and_not_cooled_down(self, tmp_path):
        record_failure(tmp_path, count=3)
        events = collect_events(tmp_path, settings())
        with patch("src.health.urlopen", side_effect=URLError("offline")):
            results = dispatch_events(tmp_path, events, "https://example.test/hook")
        assert results[0]["ok"] is False
        stored = load_health(tmp_path)["notifications"]
        assert stored["last_sent"] == {}
        assert stored["last_attempts"]["channel_failure:test_channel"]["ok"] is False
        assert stored["last_error"]
        assert len(collect_events(tmp_path, settings())) == 1

    def test_http_error_status_is_a_failure(self, tmp_path):
        with patch("src.health.urlopen", return_value=fake_response(500)):
            results = dispatch_events(
                tmp_path,
                [{"key": "x", "event": "test", "message": "m"}],
                "https://example.test/hook",
            )
        assert results[0]["ok"] is False


class TestHealthApi:
    """Management endpoints, validation, auth and CSRF behavior."""

    def test_get_health_reports_channels_and_settings(self, client, test_server):
        record_refresh(test_server.config_dir, "test_channel", success=True, queued=1)
        response = client.get("/api/health")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["channels"]["test_channel"]["consecutive_failures"] == 0
        assert data["notifications"]["settings"]["webhook_url"] == ""
        assert "free_bytes" in data["disk"]

    def test_get_health_reports_pending_events(self, client, test_server):
        record_failure(test_server.config_dir, count=3)
        data = json.loads(client.get("/api/health").data)
        assert [event["event"] for event in data["pending_events"]] == [
            "channel_failure"
        ]

    def test_update_notification_settings_persists(self, client, test_server):
        response = client.put(
            "/api/config/notifications",
            json={
                "webhook_url": "https://example.test/hook",
                "failure_threshold": 4,
                "cooldown_minutes": 15,
                "notify_low_space": False,
            },
        )
        assert response.status_code == 200
        settings_data = json.loads(client.get("/api/config/notifications").data)
        assert settings_data["webhook_url"] == "https://example.test/hook"
        assert settings_data["failure_threshold"] == 4
        assert settings_data["notify_low_space"] is False
        config = yaml.safe_load(
            (test_server.config_dir / "channels.yaml").read_text(encoding="utf-8")
        )
        assert config["notifications"]["webhook_url"] == "https://example.test/hook"

    def test_update_rejects_invalid_settings(self, client, test_server):
        response = client.put(
            "/api/config/notifications",
            json={"webhook_url": "ftp://example.test"},
        )
        assert response.status_code == 400
        assert "http" in json.loads(response.data)["error"]

    def test_update_requires_a_body(self, client):
        assert client.put("/api/config/notifications", json={}).status_code == 400

    def test_check_health_returns_events(self, client, test_server):
        record_failure(test_server.config_dir, count=3)
        response = client.post("/api/health/check")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert [event["event"] for event in data["events"]] == ["channel_failure"]
        assert data["webhook_configured"] is False

    def test_check_health_delivers_when_configured(self, client, test_server):
        record_failure(test_server.config_dir, count=3)
        client.put(
            "/api/config/notifications",
            json={"webhook_url": "https://example.test/hook"},
        )
        with patch("src.health.urlopen", return_value=fake_response()):
            response = client.post("/api/health/check")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["delivered"][0]["ok"] is True

    def test_test_notification_requires_webhook(self, client):
        response = client.post("/api/notifications/test")
        assert response.status_code == 400

    def test_test_notification_delivers(self, client, test_server):
        client.put(
            "/api/config/notifications",
            json={"webhook_url": "https://example.test/hook"},
        )
        with patch("src.health.urlopen", return_value=fake_response()):
            response = client.post("/api/notifications/test")
        assert response.status_code == 200
        assert json.loads(response.data)["ok"] is True

    def test_test_notification_reports_delivery_failure(self, client, test_server):
        client.put(
            "/api/config/notifications",
            json={"webhook_url": "https://example.test/hook"},
        )
        with patch("src.health.urlopen", side_effect=URLError("offline")):
            response = client.post("/api/notifications/test")
        assert response.status_code == 502
        assert json.loads(response.data)["ok"] is False


class TestRefreshIntegration:
    """Refreshes record health and unavailable channels are detected."""

    def test_refresh_success_is_recorded(self, test_server):
        downloader = Mock()
        downloader.get_channel_videos.return_value = []
        downloader.last_channel_error = None
        channel = yaml.safe_load(
            (test_server.config_dir / "channels.yaml").read_text(encoding="utf-8")
        )["channels"][0]

        assert test_server._begin_refresh()
        with patch("src.downloader.YouTubeDownloader", return_value=downloader):
            test_server._run_refresh(channel["name"])

        state = load_health(test_server.config_dir)["channels"][channel["name"]]
        assert state["consecutive_failures"] == 0
        assert state["last_success_at"]

    def test_unavailable_channel_is_recorded(self, test_server):
        downloader = Mock()
        downloader.get_channel_videos.return_value = []
        downloader.last_channel_error = "This channel does not exist"
        channel = yaml.safe_load(
            (test_server.config_dir / "channels.yaml").read_text(encoding="utf-8")
        )["channels"][0]

        assert test_server._begin_refresh()
        with patch("src.downloader.YouTubeDownloader", return_value=downloader):
            test_server._run_refresh(channel["name"])

        state = load_health(test_server.config_dir)["channels"][channel["name"]]
        assert state["unavailable"] is True
        assert state["consecutive_failures"] == 1
        assert "does not exist" in state["last_error"]

    def test_deleting_a_channel_removes_health(self, client, test_server):
        record_refresh(test_server.config_dir, "test_channel", success=True)
        assert client.delete("/api/channels/test_channel").status_code == 200
        assert "test_channel" not in load_health(test_server.config_dir)["channels"]

    def test_scheduler_falls_back_to_timestamps_when_unreadable(
        self, test_server, unreadable_files
    ):
        fallback = "2026-01-01T00:00:00+00:00"
        (test_server.config_dir / "refresh_timestamps.json").write_text(
            json.dumps({"test_channel": fallback})
        )
        record_refresh(
            test_server.config_dir,
            "test_channel",
            success=True,
            at="2026-02-01T00:00:00+00:00",
        )
        assert test_server._last_refresh_attempts()["test_channel"] == datetime(
            2026, 2, 1, tzinfo=UTC
        )
        original = health_path(test_server.config_dir).read_bytes()
        unreadable_files(health_path(test_server.config_dir))

        attempts = test_server._last_refresh_attempts()

        assert attempts["test_channel"] == datetime(2026, 1, 1, tzinfo=UTC)
        assert health_path(test_server.config_dir).read_bytes() == original
        assert health_backups(test_server.config_dir) == []
