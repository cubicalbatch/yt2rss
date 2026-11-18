"""Shared yt-dlp proxy options built from ``YT_DLP_PROXY``.

The proxy is passed to yt-dlp unchanged, announced at INFO once per process
per distinct value, and logged only as ``scheme://host[:port]`` so
credentials never reach the logs. Nothing here touches the network:
``yt_dlp.YoutubeDL`` is always mocked.
"""

import logging
import sys
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from src import utils
from src.downloader import YouTubeDownloader
from src.utils import PROXY_LOG_PLACEHOLDER, YT_DLP_PROXY_ENV, yt_dlp_proxy_opts
from src.web_server import YouTubePodcastServer
from tests.test_web_server import app_config as app_config
from tests.test_web_server import test_server as test_server

LOGGER_NAME = "yt2rss.tests.proxy"
ANNOUNCEMENT_PREFIX = "🔐 Using proxy: "
PROXY = "socks5://alice:s3cret@proxy.example.com:1080"
OTHER_PROXY = "http://proxy.example.org:8080"
CHANNEL_URL = "https://www.youtube.com/@test"
VIDEO_URL = "https://www.youtube.com/watch?v=video123"
THREADS = 8
CALLS_PER_THREAD = 200
JOIN_TIMEOUT_SECONDS = 30.0
SWITCH_INTERVAL_SECONDS = 1e-6


@pytest.fixture(autouse=True)
def fresh_proxy_state(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Start every test with no proxy configured and nothing announced."""
    monkeypatch.delenv(YT_DLP_PROXY_ENV, raising=False)
    monkeypatch.setattr(utils, "_announced_proxies", set())
    caplog.set_level(logging.INFO)


@pytest.fixture
def logger() -> logging.Logger:
    """Provide a dedicated logger whose records reach ``caplog``."""
    return logging.getLogger(LOGGER_NAME)


@pytest.fixture
def downloader(tmp_path: Path) -> YouTubeDownloader:
    """Create a downloader rooted in a temporary directory."""
    with patch("src.downloader.RSSGenerator"):
        return YouTubeDownloader(str(tmp_path / "channels.yaml"), str(tmp_path))


@pytest.fixture
def fast_thread_switching() -> Iterator[None]:
    """Switch threads very often so racing announcements genuinely interleave."""
    previous = sys.getswitchinterval()
    sys.setswitchinterval(SWITCH_INTERVAL_SECONDS)
    yield
    sys.setswitchinterval(previous)


def announcements(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the logged proxy addresses, in order.

    Args:
        caplog: The pytest log capture fixture.

    Returns:
        The text after the announcement prefix of every proxy log line.
    """
    return [
        record.getMessage().removeprefix(ANNOUNCEMENT_PREFIX)
        for record in caplog.records
        if record.getMessage().startswith(ANNOUNCEMENT_PREFIX)
    ]


def constructed_opts(mock_ytdl: Mock) -> dict[str, Any]:
    """Return the options the mocked ``yt_dlp.YoutubeDL`` was last built with.

    Args:
        mock_ytdl: The patched ``yt_dlp.YoutubeDL`` class.

    Returns:
        The options mapping passed to the constructor.
    """
    return mock_ytdl.call_args.args[0]


@pytest.mark.parametrize("value", [None, "", "   ", " \t\n "])
def test_unset_or_blank_proxy_yields_no_opts_and_no_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
    value: str | None,
) -> None:
    """No proxy is configured when the variable is unset, empty or blank."""
    if value is not None:
        monkeypatch.setenv(YT_DLP_PROXY_ENV, value)

    assert yt_dlp_proxy_opts(logger) == {}
    assert yt_dlp_proxy_opts() == {}
    assert caplog.records == []


@pytest.mark.parametrize(
    "value, logged, secrets",
    [
        ("socks5://proxy.example.com:1080", "socks5://proxy.example.com:1080", ()),
        (PROXY, "socks5://proxy.example.com:1080", ("alice", "s3cret")),
        ("http://proxy.example.com:8080", "http://proxy.example.com:8080", ()),
        ("https://proxy.example.com", "https://proxy.example.com", ()),
        (
            "socks5h://alice:s3cret@[::1]:1080",
            "socks5h://[::1]:1080",
            ("alice", "s3cret"),
        ),
        (
            "SOCKS5://Alice:S3cret@Proxy.Example.com:1080",
            "socks5://proxy.example.com:1080",
            ("Alice", "S3cret"),
        ),
        # Raw "@" and ":" in the password, percent-encoded "/".
        (
            "socks5://alice:p@ss:w%2Frd@proxy.example.com:1080",
            "socks5://proxy.example.com:1080",
            ("alice", "p@ss", "w%2Frd", "w/rd"),
        ),
        (
            "http://proxy.example.com:3128/?token=t0ps3cret",
            "http://proxy.example.com:3128",
            ("t0ps3cret",),
        ),
        (
            "http://alice:s3cret@proxy.example.com:8080/path?token=t0ps3cret#frag",
            "http://proxy.example.com:8080",
            ("alice", "s3cret", "t0ps3cret", "path", "frag"),
        ),
    ],
)
def test_valid_proxy_is_passed_through_and_logged_redacted(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
    value: str,
    logged: str,
    secrets: tuple[str, ...],
) -> None:
    """yt-dlp gets the exact value; the log shows only scheme, host and port."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, value)

    assert yt_dlp_proxy_opts(logger) == {"proxy": value}
    assert announcements(caplog) == [logged]
    assert caplog.records[0].name == LOGGER_NAME
    assert caplog.records[0].levelno == logging.INFO
    for secret in secrets:
        assert secret not in caplog.text


def test_surrounding_whitespace_is_stripped(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Whitespace around the value is not passed to yt-dlp."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, f"  {PROXY}\n")

    assert yt_dlp_proxy_opts() == {"proxy": PROXY}
    assert announcements(caplog) == ["socks5://proxy.example.com:1080"]


def test_default_logger_is_the_utils_module_logger(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Without an explicit logger the announcement uses ``src.utils``."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, OTHER_PROXY)

    yt_dlp_proxy_opts()

    assert [record.name for record in caplog.records] == ["src.utils"]


@pytest.mark.parametrize(
    "value",
    [
        "not a url",
        "socks5://:1080",
        "http://alice:s3cret@",
        # No scheme, so nothing is a reliable host.
        "alice:s3cret@proxy.example.com:1080",
        # Unescaped "/", "?" or "#" in the password ends the host early.
        "socks5://alice:pa/ss@proxy.example.com:1080",
        "socks5://alice:12/34@proxy.example.com:1080",
        "socks5://alice:12?34@proxy.example.com:1080",
        "socks5://alice:12#34@proxy.example.com:1080",
        "socks5://alice:s3cret@proxy.example.com:1080/x@y",
        # Out-of-range port and unbalanced IPv6 brackets fail to parse.
        "socks5://alice:s3cret@proxy.example.com:99999",
        "socks5://alice:s3cret@[::1:1080",
        # A schemeless "user:" plus a password starting with "//".
        "alice://s3cret@proxy.example.com:1080",
        "ftp://proxy.example.com:21",
    ],
)
def test_unredactable_proxy_logs_placeholder(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
    value: str,
) -> None:
    """Malformed values log a placeholder but still reach yt-dlp."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, value)

    assert yt_dlp_proxy_opts(logger) == {"proxy": value}
    assert announcements(caplog) == [PROXY_LOG_PLACEHOLDER]
    for secret in ("alice", "s3cret", "pa/ss", "12/34", "12?34", "12#34"):
        assert secret not in caplog.text


def test_repeated_calls_announce_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
) -> None:
    """The same proxy is announced only on the first call."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, PROXY)

    for _ in range(50):
        assert yt_dlp_proxy_opts(logger) == {"proxy": PROXY}

    assert len(announcements(caplog)) == 1


@pytest.mark.usefixtures("fast_thread_switching")
def test_concurrent_calls_announce_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
) -> None:
    """Racing threads still produce exactly one announcement."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, PROXY)
    barrier = threading.Barrier(THREADS)
    results: list[dict[str, str]] = []
    results_guard = threading.Lock()

    def worker() -> None:
        barrier.wait(timeout=JOIN_TIMEOUT_SECONDS)
        for _ in range(CALLS_PER_THREAD):
            opts = yt_dlp_proxy_opts(logger)
            with results_guard:
                results.append(opts)

    threads = [threading.Thread(target=worker) for _ in range(THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=JOIN_TIMEOUT_SECONDS)
        assert not thread.is_alive()

    assert results == [{"proxy": PROXY}] * (THREADS * CALLS_PER_THREAD)
    assert announcements(caplog) == ["socks5://proxy.example.com:1080"]


def test_changed_proxy_is_announced_again(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    logger: logging.Logger,
) -> None:
    """Each distinct value is announced once, however often it is used."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, PROXY)
    yt_dlp_proxy_opts(logger)
    yt_dlp_proxy_opts(logger)

    monkeypatch.setenv(YT_DLP_PROXY_ENV, OTHER_PROXY)
    assert yt_dlp_proxy_opts(logger) == {"proxy": OTHER_PROXY}
    yt_dlp_proxy_opts(logger)

    monkeypatch.setenv(YT_DLP_PROXY_ENV, PROXY)
    assert yt_dlp_proxy_opts(logger) == {"proxy": PROXY}

    assert announcements(caplog) == [
        "socks5://proxy.example.com:1080",
        "http://proxy.example.org:8080",
    ]


DOWNLOADER_CALLS: dict[str, Callable[[YouTubeDownloader], object]] = {
    "listing": lambda downloader: downloader.get_channel_videos(CHANNEL_URL, 3),
    "metadata": lambda downloader: downloader.get_video_metadata(VIDEO_URL),
    "download": lambda downloader: downloader.download_video(
        {"id": "video123", "title": "Test Video", "url": VIDEO_URL},
        "test_channel",
    ),
}


@pytest.mark.parametrize("call", DOWNLOADER_CALLS)
@pytest.mark.parametrize("proxy", [PROXY, None])
def test_downloader_yt_dlp_calls_include_proxy_opts(
    monkeypatch: pytest.MonkeyPatch,
    downloader: YouTubeDownloader,
    call: str,
    proxy: str | None,
) -> None:
    """Listing, metadata and download options carry the configured proxy."""
    if proxy is not None:
        monkeypatch.setenv(YT_DLP_PROXY_ENV, proxy)

    with patch("src.downloader.yt_dlp.YoutubeDL") as mock_ytdl:
        ydl = mock_ytdl.return_value.__enter__.return_value
        ydl.extract_info.return_value = {}
        ydl.download.return_value = 0
        DOWNLOADER_CALLS[call](downloader)

    mock_ytdl.assert_called_once()
    assert constructed_opts(mock_ytdl).get("proxy") == proxy


def test_downloader_announces_proxy_once_across_calls(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    downloader: YouTubeDownloader,
) -> None:
    """A refresh's many yt-dlp calls log the proxy a single time."""
    monkeypatch.setenv(YT_DLP_PROXY_ENV, PROXY)

    with patch("src.downloader.yt_dlp.YoutubeDL") as mock_ytdl:
        ydl = mock_ytdl.return_value.__enter__.return_value
        ydl.extract_info.return_value = {}
        ydl.download.return_value = 0
        for _ in range(3):
            for call in DOWNLOADER_CALLS.values():
                call(downloader)

    assert mock_ytdl.call_count == 3 * len(DOWNLOADER_CALLS)
    assert announcements(caplog) == ["socks5://proxy.example.com:1080"]
    assert "s3cret" not in caplog.text


@pytest.mark.parametrize("proxy", [PROXY, None])
def test_verify_youtube_channel_includes_proxy_opts(
    monkeypatch: pytest.MonkeyPatch,
    test_server: YouTubePodcastServer,
    proxy: str | None,
) -> None:
    """Channel verification in the web server uses the configured proxy."""
    if proxy is not None:
        monkeypatch.setenv(YT_DLP_PROXY_ENV, proxy)

    with patch("src.web_server.yt_dlp.YoutubeDL") as mock_ytdl:
        mock_ytdl.return_value.__enter__.return_value.extract_info.return_value = {
            "title": "Test Channel",
            "id": "UCtest123",
            "webpage_url": CHANNEL_URL,
        }
        valid, error, _ = test_server._verify_youtube_channel(CHANNEL_URL)

    assert (valid, error) == (True, "")
    mock_ytdl.assert_called_once()
    assert constructed_opts(mock_ytdl).get("proxy") == proxy
