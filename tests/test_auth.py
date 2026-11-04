"""
Tests for optional password authentication and private feed tokens.

Covers both modes: auth disabled (open access regression) and auth enabled
(login/logout, throttling, route authorization, feed tokens, rotation).
"""

import json
import re
import time
from unittest.mock import Mock, patch

import pytest
import yaml
from flask.sessions import SecureCookieSessionInterface

from src.auth import LOGIN_MAX_FAILURES, is_safe_relative_path
from src.cron_runner import AutomationRunner
from src.web_server import YouTubePodcastServer


@pytest.fixture
def app_config():
    """Create test configuration data."""
    return {
        "channels": [
            {
                "name": "test_channel",
                "display_name": "Test Channel",
                "url": "https://www.youtube.com/@test",
                "max_episodes": 5,
                "download_delay_hours": 6,
                "format": "audio",
                "quality": "best",
                "sponsorblock_categories": ["sponsor", "intro"],
                "refresh_interval_hours": 12,
            },
            {
                "name": "another_channel",
                "display_name": "Another Channel",
                "url": "https://www.youtube.com/@another",
                "max_episodes": 10,
                "download_delay_hours": 12,
                "format": "video",
                "quality": "720p",
                "sponsorblock_categories": [],
            },
        ],
        "default_interval_hours": 24,
    }


@pytest.fixture
def open_server(temp_directory, app_config, mock_logging, mock_atexit, monkeypatch):
    """Create a server with authentication disabled."""
    monkeypatch.delenv("YT2RSS_PASSWORD", raising=False)
    monkeypatch.delenv("YT2RSS_USERNAME", raising=False)
    monkeypatch.delenv("YT2RSS_SECRET_KEY", raising=False)
    return _build_server(temp_directory, app_config)


@pytest.fixture
def auth_server(temp_directory, app_config, mock_logging, mock_atexit, monkeypatch):
    """Create a server with password authentication enabled."""
    monkeypatch.setenv("YT2RSS_PASSWORD", "secret-pass")
    monkeypatch.delenv("YT2RSS_USERNAME", raising=False)
    monkeypatch.delenv("YT2RSS_SECRET_KEY", raising=False)
    monkeypatch.delenv("BASE_URL", raising=False)
    return _build_server(temp_directory, app_config)


@pytest.fixture
def auth_client(auth_server):
    """Create a test client for the auth-enabled server."""
    return auth_server.app.test_client()


@pytest.fixture
def open_episodes(open_server):
    """Write test episodes into the open-access server."""
    return _write_episodes(open_server)


@pytest.fixture
def setup_test_episodes(auth_server):
    """Write test episodes into the auth-enabled server."""
    return _write_episodes(auth_server)


def _build_server(temp_directory, app_config, write_config=True):
    """Construct a server on temporary directories with a mocked scheduler."""
    videos_dir = temp_directory / "podcasts"
    config_dir = temp_directory / "config"
    videos_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    if write_config:
        with open(config_dir / "channels.yaml", "w") as f:
            yaml.safe_dump(app_config, f)

    with patch("src.web_server.BackgroundScheduler"):
        server = YouTubePodcastServer(
            host="127.0.0.1",
            port=5000,
            videos_dir=str(videos_dir),
            config_dir=str(config_dir),
        )
        server.scheduler = Mock()
        server.scheduler.get_jobs.return_value = []
        server.scheduler.add_job = Mock()
        server.scheduler.remove_job = Mock()
        server.app.config["TESTING"] = True
        return server


def _write_episodes(server):
    """Create episode files and metadata for both channels."""
    channel_dir = server.videos_dir / "test_channel"
    thumbnails_dir = channel_dir / "thumbnails"
    thumbnails_dir.mkdir(parents=True)

    metadata = {
        "id": "test123abc",
        "title": "Test Episode",
        "upload_date": "20241201",
        "duration": 1234,
        "thumbnail": "thumbnails/test123abc.jpg",
    }
    (channel_dir / "test123abc.json").write_text(json.dumps(metadata))
    (channel_dir / "test123abc.m4a").write_text("fake audio data")
    (thumbnails_dir / "test123abc.jpg").write_text("fake image data")

    other_dir = server.videos_dir / "another_channel"
    other_dir.mkdir(parents=True)
    (other_dir / "test456def.json").write_text(
        json.dumps({"id": "test456def", "title": "Other", "upload_date": "20241202"})
    )
    (other_dir / "test456def.mp4").write_text("fake video data")
    return channel_dir


def _login(client, **overrides):
    """Log in with the default test credentials."""
    data = {"username": "admin", "password": "secret-pass"}
    data.update(overrides)
    return client.post("/login", data=data)


def _csrf_token(client):
    """Extract the logout CSRF token from the rendered index page."""
    page = client.get("/").data.decode()
    match = re.search(r'name="csrf_token" value="([^"]+)"', page)
    assert match, "Logout form with CSRF token missing from index page"
    return match.group(1)


def _feed_token(server, client, channel_name="test_channel"):
    """Fetch a channel's tokenized feed URL from the index page."""
    page = client.get("/").data.decode()
    match = re.search(rf"/feeds/{channel_name}\?token=([^\"']+)", page)
    assert match, f"Tokenized feed URL for {channel_name} missing from index page"
    return match.group(1)


def _anonymous_client(server):
    """Create a cookie-less client that cannot authenticate by session."""
    return server.app.test_client()


class TestSafeRelativePath:
    """Test the open-redirect guard."""

    @pytest.mark.parametrize(
        "target",
        ["/", "/feeds/x", "/api/channels?a=b", "/#anchor"],
    )
    def test_accepts_same_origin_paths(self, target):
        """Relative same-origin paths are allowed."""
        assert is_safe_relative_path(target)

    @pytest.mark.parametrize(
        "target",
        [
            None,
            "",
            "https://evil.com",
            "http://evil.com/",
            "//evil.com",
            "/\\evil.com",
            "relative/path",
        ],
    )
    def test_rejects_foreign_targets(self, target):
        """Absolute URLs, protocol-relative and backslash targets are rejected."""
        assert not is_safe_relative_path(target)


class TestAuthDisabledRegression:
    """Without YT2RSS_PASSWORD everything stays open access."""

    def test_index_open(self, open_server, open_episodes):
        """Index page renders without login."""
        client = _anonymous_client(open_server)
        assert client.get("/").status_code == 200

    def test_feed_open_without_token(self, open_server, open_episodes):
        """Plain feed URLs keep working."""
        client = _anonymous_client(open_server)
        response = client.get("/feeds/test_channel")
        assert response.status_code == 200
        assert b"Test Episode" in response.data

    def test_media_open_without_token(self, open_server, open_episodes):
        """Media and thumbnails serve without credentials."""
        client = _anonymous_client(open_server)
        assert client.get("/podcasts/test_channel/test123abc.m4a").status_code == 200
        assert client.get("/thumbnails/test_channel/test123abc.jpg").status_code == 200

    def test_api_open(self, open_server, open_episodes):
        """Management API responds without login."""
        client = _anonymous_client(open_server)
        assert client.get("/api/channels").status_code == 200

    def test_no_session_secret_written(self, open_server):
        """Auth disabled must not create or require a signing key."""
        assert open_server.app.secret_key is None
        assert not (open_server.config_dir / ".session_secret").exists()

    def test_login_inert_when_disabled(self, open_server):
        """Login endpoints redirect to the index without auth enabled."""
        client = _anonymous_client(open_server)
        assert client.get("/login").status_code == 302
        assert client.post("/login", data={}).status_code == 302


class TestAuthEnabledRouteMatrix:
    """Route authorization with authentication enabled."""

    def test_anonymous_page_redirects_to_login(self, auth_client):
        """Pages redirect to /login with a next parameter."""
        response = auth_client.get("/")
        assert response.status_code == 302
        location = response.headers["Location"]
        assert "/login" in location
        assert "next=/" in location

    def test_anonymous_api_returns_401_json(self, auth_client):
        """API requests without a session get JSON 401."""
        response = auth_client.get("/api/channels")
        assert response.status_code == 401
        assert response.is_json
        assert "error" in response.get_json()

    def test_anonymous_feed_denied(self, auth_client, setup_test_episodes):
        """A feed without token never leaks content."""
        response = auth_client.get("/feeds/test_channel")
        assert response.status_code == 404
        assert b"Test Episode" not in response.data

    def test_anonymous_media_denied(self, auth_client, setup_test_episodes):
        """Media requests without credentials return 401, not bytes."""
        response = auth_client.get("/podcasts/test_channel/test123abc.m4a")
        assert response.status_code == 401
        assert b"fake audio data" not in response.data

    def test_anonymous_thumbnail_denied(self, auth_client, setup_test_episodes):
        """Thumbnail requests without credentials return 401."""
        response = auth_client.get("/thumbnails/test_channel/test123abc.jpg")
        assert response.status_code == 401
        assert b"fake image data" not in response.data

    def test_login_page_open_and_static_open(self, auth_client):
        """Login page and static assets stay reachable when anonymous."""
        assert auth_client.get("/login").status_code == 200
        assert auth_client.get("/static/js/script.js").status_code == 200


class TestLoginLogout:
    """Login, logout and session behavior."""

    def test_login_success_grants_access(self, auth_client):
        """Correct credentials set a cookie and unlock pages and API."""
        response = _login(auth_client)
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/")

        assert auth_client.get("/").status_code == 200
        assert auth_client.get("/api/channels").status_code == 200

    def test_login_sets_hardened_cookie(self, auth_client):
        """The session cookie is HttpOnly and SameSite=Lax."""
        response = _login(auth_client)
        cookie = response.headers["Set-Cookie"]
        assert "HttpOnly" in cookie
        assert "SameSite=Lax" in cookie
        assert "Secure" not in cookie

    def test_secure_cookie_when_base_url_https(
        self, temp_directory, app_config, mock_logging, mock_atexit, monkeypatch
    ):
        """BASE_URL starting with https:// enables the Secure cookie flag."""
        monkeypatch.setenv("YT2RSS_PASSWORD", "secret-pass")
        monkeypatch.setenv("BASE_URL", "https://feeds.example.com")
        server = _build_server(temp_directory, app_config)
        response = server.app.test_client().post(
            "/login", data={"username": "admin", "password": "secret-pass"}
        )
        assert "Secure" in response.headers["Set-Cookie"]

    def test_login_failure_wrong_password(self, auth_client):
        """Wrong passwords are rejected with 401 and no access."""
        response = _login(auth_client, password="wrong")
        assert response.status_code == 401
        assert b"Invalid username or password." in response.data
        assert auth_client.get("/api/channels").status_code == 401

    def test_login_failure_wrong_username(self, auth_client):
        """Wrong usernames are rejected even with the right password."""
        assert _login(auth_client, username="root").status_code == 401

    def test_custom_username_from_env(
        self, temp_directory, app_config, mock_logging, mock_atexit, monkeypatch
    ):
        """YT2RSS_USERNAME replaces the default admin username."""
        monkeypatch.setenv("YT2RSS_PASSWORD", "secret-pass")
        monkeypatch.setenv("YT2RSS_USERNAME", "keeper")
        server = _build_server(temp_directory, app_config)
        client = server.app.test_client()
        wrong = client.post(
            "/login", data={"username": "admin", "password": "secret-pass"}
        )
        assert wrong.status_code == 401
        right = client.post(
            "/login", data={"username": "keeper", "password": "secret-pass"}
        )
        assert right.status_code == 302

    def test_login_redirects_to_next(self, auth_client):
        """A safe next path is honored after login."""
        response = _login(auth_client, next="/api/channels")
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/api/channels")

    @pytest.mark.parametrize(
        "next_target", ["https://evil.com", "//evil.com", "/\\evil.com"]
    )
    def test_login_rejects_open_redirect(self, auth_client, next_target):
        """Off-site or malformed next targets fall back to the index page."""
        response = _login(auth_client, next=next_target)
        assert response.status_code == 302
        assert "evil.com" not in response.headers["Location"]

    def test_logout_requires_csrf_token(self, auth_client):
        """Logout POST without a valid CSRF token is rejected."""
        _login(auth_client)
        response = auth_client.post("/logout", data={"csrf_token": "forged"})
        assert response.status_code == 400
        assert auth_client.get("/api/channels").status_code == 200

    def test_logout_clears_session(self, auth_client):
        """Logout with the CSRF token invalidates access."""
        _login(auth_client)
        token = _csrf_token(auth_client)
        response = auth_client.post("/logout", data={"csrf_token": token})
        assert response.status_code == 302
        assert auth_client.get("/api/channels").status_code == 401
        assert auth_client.get("/").status_code == 302

    def test_logout_only_via_post(self, auth_client):
        """GET /logout is not routable."""
        _login(auth_client)
        assert auth_client.get("/logout").status_code == 405

    def test_login_throttles_repeated_failures(self, auth_client, auth_server):
        """After repeated failures even correct credentials are throttled."""
        for _ in range(LOGIN_MAX_FAILURES):
            assert _login(auth_client, password="wrong").status_code == 401
        assert _login(auth_client).status_code == 429
        assert auth_client.get("/api/channels").status_code == 401

        # Once the throttle window is clear, logins work again.
        auth_server._login_throttler._failures.clear()
        assert _login(auth_client).status_code == 302

    def test_expired_session_is_rejected(self, auth_server):
        """A session cookie older than the lifetime no longer authenticates."""
        client = _anonymous_client(auth_server)
        serializer = SecureCookieSessionInterface().get_signing_serializer(
            auth_server.app
        )
        assert serializer is not None
        expired = time.time() - 8 * 24 * 3600
        with patch("time.time", return_value=expired):
            cookie_value = serializer.dumps({"authenticated": True, "_permanent": True})
        client.set_cookie("session", cookie_value)
        assert client.get("/api/channels").status_code == 401
        assert client.get("/").status_code == 302


class TestFeedTokens:
    """Per-channel feed token issuance and validation."""

    def test_index_shows_tokenized_urls(self, auth_client, setup_test_episodes):
        """The index page emits tokenized feed URLs when logged in."""
        _login(auth_client)
        assert "/feeds/test_channel?token=" in auth_client.get("/").data.decode()

    def test_index_warns_plain_urls_require_login(
        self, auth_client, setup_test_episodes
    ):
        """The UI warns that plain feed URLs require login."""
        _login(auth_client)
        assert "plain feed URLs require login" in auth_client.get("/").data.decode()

    def test_correct_token_grants_feed(
        self, auth_server, auth_client, setup_test_episodes
    ):
        """A valid token serves the feed without a session."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        response = _anonymous_client(auth_server).get(
            f"/feeds/test_channel?token={token}"
        )
        assert response.status_code == 200
        assert b"Test Episode" in response.data

    def test_feed_urls_carry_token(self, auth_server, auth_client, setup_test_episodes):
        """Enclosures, thumbnails and the self link carry the token."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        feed = (
            _anonymous_client(auth_server)
            .get(f"/feeds/test_channel?token={token}")
            .data.decode()
        )
        assert f"/podcasts/test_channel/test123abc.m4a?token={token}" in feed
        assert f"/thumbnails/test_channel/test123abc.jpg?token={token}" in feed
        assert f"/feeds/test_channel?token={token}" in feed

    def test_enclosure_fetchable_without_cookies(
        self, auth_server, auth_client, setup_test_episodes
    ):
        """The enclosure URL from the feed works without a login session."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        feed = (
            _anonymous_client(auth_server)
            .get(f"/feeds/test_channel?token={token}")
            .data.decode()
        )
        match = re.search(r'url="([^"]*/podcasts/[^"]+)"', feed)
        assert match, "Enclosure URL missing from feed"
        response = _anonymous_client(auth_server).get(match.group(1))
        assert response.status_code == 200
        assert b"fake audio data" in response.data

    def test_thumbnail_token_grants_access(
        self, auth_server, auth_client, setup_test_episodes
    ):
        """A matching channel token unlocks thumbnails."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        response = _anonymous_client(auth_server).get(
            f"/thumbnails/test_channel/test123abc.jpg?token={token}"
        )
        assert response.status_code == 200

    def test_media_range_request_with_token(
        self, auth_server, auth_client, setup_test_episodes
    ):
        """Tokenized media requests still honor HTTP Range."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        response = _anonymous_client(auth_server).get(
            f"/podcasts/test_channel/test123abc.m4a?token={token}",
            headers={"Range": "bytes=0-3"},
        )
        assert response.status_code == 206
        assert response.data == b"fake"

    def test_missing_token_denied(self, auth_server, auth_client):
        """A tokenized setup rejects plain feed URLs."""
        _login(auth_client)  # ensures the token exists
        response = _anonymous_client(auth_server).get("/feeds/test_channel")
        assert response.status_code == 404

    def test_wrong_token_denied(self, auth_server, auth_client):
        """Garbage tokens are rejected without leaking the feed."""
        _login(auth_client)
        response = _anonymous_client(auth_server).get("/feeds/test_channel?token=nope")
        assert response.status_code == 404

    def test_cross_channel_token_denied(self, auth_server, auth_client):
        """A token is only valid for its own channel."""
        _login(auth_client)
        other_token = _feed_token(auth_server, auth_client, "another_channel")
        client = _anonymous_client(auth_server)
        feed = client.get(f"/feeds/test_channel?token={other_token}")
        assert feed.status_code == 404
        media = client.get(f"/podcasts/test_channel/test123abc.m4a?token={other_token}")
        assert media.status_code == 401

    def test_token_grants_no_api_access(self, auth_server, auth_client):
        """Feed tokens must never unlock the management API."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        response = _anonymous_client(auth_server).get(f"/api/channels?token={token}")
        assert response.status_code == 401

    def test_token_survives_config_reload(
        self, temp_directory, app_config, mock_logging, mock_atexit, monkeypatch
    ):
        """Tokens persist in channels.yaml across server restarts."""
        monkeypatch.setenv("YT2RSS_PASSWORD", "secret-pass")
        server = _build_server(temp_directory, app_config)
        token = server._get_or_create_feed_token("test_channel")
        assert token

        with open(server.config_dir / "channels.yaml") as f:
            stored = yaml.safe_load(f)
        assert stored["channels"][0]["feed_token"] == token

        reloaded = _build_server(temp_directory, app_config, write_config=False)
        assert reloaded._stored_feed_token("test_channel") == token
        response = _anonymous_client(reloaded).get(f"/feeds/test_channel?token={token}")
        assert response.status_code == 200

    def test_rotation_invalidates_old_token(self, auth_server, auth_client):
        """Rotating a token immediately revokes previous URLs."""
        _login(auth_client)
        old_token = _feed_token(auth_server, auth_client)

        response = auth_client.post("/api/channels/test_channel/rotate-token")
        assert response.status_code == 200
        new_token = response.get_json()["feed_token"]
        assert new_token != old_token

        client = _anonymous_client(auth_server)
        assert client.get(f"/feeds/test_channel?token={old_token}").status_code == 404
        assert client.get(f"/feeds/test_channel?token={new_token}").status_code == 200

    def test_rotation_requires_known_channel(self, auth_client):
        """Rotating an unknown channel returns 404."""
        _login(auth_client)
        assert auth_client.post("/api/channels/ghost/rotate-token").status_code == 404

    def test_rotation_requires_login(self, auth_server, auth_client):
        """Anonymous visitors cannot rotate tokens."""
        _login(auth_client)
        token = _feed_token(auth_server, auth_client)
        client = _anonymous_client(auth_server)
        assert client.post("/api/channels/test_channel/rotate-token").status_code == 401
        assert client.get(f"/feeds/test_channel?token={token}").status_code == 200

    def test_channel_created_via_api_has_token(self, auth_server, auth_client):
        """API-created channels receive a feed token immediately."""
        _login(auth_client)
        with patch.object(
            auth_server,
            "_verify_youtube_channel",
            return_value=(True, "", {"title": "New"}),
        ):
            response = auth_client.post(
                "/api/channels",
                json={
                    "display_name": "Fresh Channel",
                    "url": "https://www.youtube.com/@fresh",
                },
            )
        assert response.status_code == 201
        created = response.get_json()["channel"]
        assert created["feed_token"]
        feed = _anonymous_client(auth_server).get(
            f"/feeds/fresh_channel?token={created['feed_token']}"
        )
        assert feed.status_code == 200

    def test_session_browser_playback_without_token(
        self, auth_client, setup_test_episodes
    ):
        """Logged-in browsers keep playing media without tokens in URLs."""
        _login(auth_client)
        assert (
            auth_client.get("/podcasts/test_channel/test123abc.m4a").status_code == 200
        )
        assert auth_client.get("/feeds/test_channel").status_code == 200


class TestCronRunnerWithAuth:
    """The headless cron runner is unaffected by authentication."""

    def test_runner_runs_with_auth_enabled(self, temp_directory, monkeypatch):
        """run() succeeds with YT2RSS_PASSWORD set and keeps feed tokens."""
        monkeypatch.setenv("YT2RSS_PASSWORD", "secret-pass")
        config_dir = temp_directory / "appdata" / "config"
        config_dir.mkdir(parents=True)
        config = {
            "channels": [
                {
                    "name": "test_channel",
                    "display_name": "Test Channel",
                    "url": "https://www.youtube.com/@test",
                    "max_episodes": 5,
                    "feed_token": "existing-token",
                }
            ],
            "default_interval_hours": 24,
        }
        with open(config_dir / "channels.yaml", "w") as f:
            yaml.safe_dump(config, f)

        with (
            patch.object(AutomationRunner, "setup_logging"),
            patch("src.cron_runner.YouTubeDownloader"),
        ):
            runner = AutomationRunner(str(temp_directory))
            runner.logger = Mock()
            runner.downloader = Mock()
            runner.downloader.process_channel.return_value = 1
            exit_code = runner.run()

        assert exit_code == 0
        with open(config_dir / "channels.yaml") as f:
            preserved = yaml.safe_load(f)
        assert preserved["channels"][0]["feed_token"] == "existing-token"
