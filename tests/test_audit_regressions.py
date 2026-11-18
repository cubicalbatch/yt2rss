"""Regression coverage for the security, storage, and feed audit."""

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import Mock, patch
from xml.etree import ElementTree

import pytest

from src.downloader import YouTubeDownloader
from src.rss_generator import RSSGenerator
from src.utils import atomic_write_text, cleanup_episodes
from tests.test_redownload import redownload_and_run
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


@pytest.mark.parametrize("body", [[], [1], "text", 42, True, None])
def test_management_requires_json_object(client, body) -> None:
    response = client.post(
        "/api/channels", data=json.dumps(body), content_type="application/json"
    )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "settings",
    [
        {"max_episodes": 0},
        {"max_episodes": -1},
        {"max_episodes": True},
        {"max_episodes": 2.5},
        {"max_episodes": "5"},
        {"download_delay_hours": -1},
        {"download_delay_hours": float("inf")},
        {"refresh_interval_hours": False},
        {"refresh_interval_hours": float("nan")},
        {"refresh_interval_hours": 9000},
        {"format": "bad"},
        {"quality": []},
        {"sponsorblock_categories": "sponsor"},
        {"sponsorblock_categories": [{}]},
    ],
)
def test_invalid_settings_never_modify_config(client, test_server, settings) -> None:
    original = (test_server.config_dir / "channels.yaml").read_bytes()
    assert client.put("/api/channels/test_channel", json=settings).status_code == 400
    assert (test_server.config_dir / "channels.yaml").read_bytes() == original


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "https://evil.example"},
        {"Origin": "null"},
        {"Sec-Fetch-Site": "cross-site"},
        {"Sec-Fetch-Site": "same-site"},
    ],
)
def test_cross_origin_mutations_blocked(client, setup_test_episodes, headers) -> None:
    assert (
        client.post("/api/channels/test_channel/purge", headers=headers).status_code
        == 403
    )
    assert (setup_test_episodes / "test123abc.m4a").exists()


def test_same_origin_and_public_feeds_work(client, setup_test_episodes) -> None:
    response = client.put(
        "/api/channels/test_channel",
        json={"max_episodes": 6},
        headers={"Origin": "http://localhost"},
    )
    assert response.status_code == 200
    assert (
        client.get(
            "/feeds/test_channel", headers={"Origin": "https://podcast.example"}
        ).headers["Access-Control-Allow-Origin"]
        == "*"
    )
    assert (
        "Access-Control-Allow-Origin"
        not in client.options(
            "/api/channels", headers={"Origin": "https://evil.example"}
        ).headers
    )


@pytest.mark.parametrize(
    "channel,episode",
    [
        ("..", "victim"),
        ("test_channel", "*"),
        ("test_channel", ".."),
        ("test_channel", "[ab]"),
    ],
)
def test_destructive_paths_rejected(
    client, test_server, setup_test_episodes, channel, episode
) -> None:
    victim = test_server.videos_dir.parent / "victim.json"
    victim.write_text("protected")
    assert (
        client.delete(f"/api/channels/{channel}/episodes/{episode}/delete").status_code
        == 404
    )
    assert victim.read_text() == "protected"
    assert (setup_test_episodes / "test123abc.m4a").exists()


def test_symlink_cannot_escape_media_directory(
    client, test_server, setup_test_episodes
) -> None:
    secret = test_server.videos_dir.parent / "secret.mp4"
    secret.write_bytes(b"private")
    (setup_test_episodes / "escape.mp4").symlink_to(secret)
    (setup_test_episodes / "thumbnails" / "escape.jpg").symlink_to(secret)
    assert client.get("/podcasts/test_channel/escape.mp4").status_code == 404
    assert client.get("/thumbnails/test_channel/escape.jpg").status_code == 404
    assert (
        client.post(
            "/api/channels/test_channel/episodes/escape/download-original"
        ).status_code
        == 404
    )


def test_media_endpoint_does_not_serve_metadata(client, setup_test_episodes) -> None:
    assert client.get("/podcasts/test_channel/test123abc.json").status_code == 404


def test_leading_underscore_video_id_keeps_identity(
    client, setup_test_episodes
) -> None:
    (setup_test_episodes / "_video.mp4").write_bytes(b"correct")
    (setup_test_episodes / "video.mp4").write_bytes(b"wrong")
    assert client.get("/podcasts/test_channel/_video.mp4").data == b"correct"
    assert (
        client.post("/api/channels/test_channel/episodes/_video/download-original").data
        == b"correct"
    )


@pytest.mark.parametrize(
    "header,expected,status",
    [
        ("bytes=0-3", b"0123", 206),
        ("bytes=-3", b"789", 206),
        ("bytes=7-", b"789", 206),
        ("bytes=7-999", b"789", 206),
        ("bytes=20-30", None, 416),
        ("bytes=5-2", None, 416),
        ("bytes=not-a-range", None, 416),
        ("bytes=0-1,3-4", None, 416),
    ],
)
def test_streaming_range_semantics(
    client, setup_test_episodes, header, expected, status
) -> None:
    (setup_test_episodes / "range.mp4").write_bytes(b"0123456789")
    response = client.get("/podcasts/test_channel/range.mp4", headers={"Range": header})
    assert response.status_code == status
    if expected is not None:
        assert response.data == expected
    else:
        assert response.headers["Content-Range"] == "bytes */10"


def test_head_and_if_range(client, setup_test_episodes) -> None:
    (setup_test_episodes / "range.mp4").write_bytes(b"0123456789")
    response = client.head(
        "/podcasts/test_channel/range.mp4", headers={"Range": "bytes=0-2"}
    )
    assert response.status_code == 206
    assert response.data == b""
    assert response.content_length == 3
    response = client.get(
        "/podcasts/test_channel/range.mp4",
        headers={"Range": "bytes=0-2", "If-Range": '"stale"'},
    )
    assert response.status_code == 200
    assert response.data == b"0123456789"


def test_refresh_reserved_before_worker_starts(client, test_server) -> None:
    with patch("src.web_server.threading.Thread") as thread:
        assert client.post("/api/refresh").status_code == 200
        assert client.get("/api/refresh/status").json["running"] is True
        assert client.post("/api/refresh").status_code == 409
        assert client.post("/api/channels/test_channel/refresh").status_code == 409
        test_server._scheduled_channel_refresh("test_channel")
        assert thread.call_count == 1
    test_server._finish_refresh()


def test_thread_start_failure_releases_reservation(client, test_server) -> None:
    with patch("src.web_server.threading.Thread") as thread:
        thread.return_value.start.side_effect = RuntimeError("cannot start")
        assert client.post("/api/channels/test_channel/refresh").status_code == 500
    assert test_server.refresh_status["running"] is False
    assert test_server._begin_refresh()
    test_server._finish_refresh()


def test_refresh_uses_custom_paths_and_recovers_from_failure(test_server) -> None:
    with patch("src.downloader.YouTubeDownloader") as downloader:
        downloader.return_value.get_channel_videos.side_effect = RuntimeError("offline")
        assert test_server._begin_refresh()
        test_server._run_single_channel_refresh("test_channel")
        assert downloader.call_args.kwargs["videos_dir"] == str(test_server.videos_dir)
        assert downloader.call_args.args[0] == str(
            test_server.config_dir / "channels.yaml"
        )
    assert test_server.queue.jobs() == []
    assert test_server.refresh_status["running"] is False
    assert test_server._begin_refresh()
    test_server._finish_refresh()


@pytest.mark.parametrize(
    "method,path",
    [
        ("post", "/api/channels/test_channel/purge"),
        ("delete", "/api/channels/test_channel/episodes/test123abc/delete"),
    ],
)
def test_destructive_actions_do_not_race_refresh(
    client, test_server, setup_test_episodes, method, path
) -> None:
    assert test_server._begin_refresh()
    try:
        assert getattr(client, method)(path).status_code == 409
        assert (setup_test_episodes / "test123abc.m4a").exists()
    finally:
        test_server._finish_refresh()


def test_redownload_waits_in_queue_while_refresh_runs(
    client, test_server, setup_test_episodes
) -> None:
    """Redownloads no longer take media.lock inside the request.

    They are accepted (202) and queued; the queue worker, which needs the
    lock, cannot start the job until the refresh releases it.
    """
    test_server.queue_worker.stop()
    assert test_server._begin_refresh()
    try:
        with patch("src.downloader.YouTubeDownloader.download_video") as download:
            response = client.post(
                "/api/channels/test_channel/episodes/test123abc/redownload"
            )
            assert response.status_code == 202
            assert test_server.queue_worker._process_next() is False
        download.assert_not_called()
        job = test_server.queue.get(response.get_json()["job"]["id"])
        assert job is not None and job["state"] == "pending"
        assert (setup_test_episodes / "test123abc.m4a").exists()
    finally:
        test_server._finish_refresh()


def test_fractional_intervals_are_not_truncated(client, test_server) -> None:
    assert (
        client.put(
            "/api/config/refresh-interval", json={"refresh_interval_hours": 0.5}
        ).status_code
        == 200
    )
    assert test_server._load_channel_config()["default_interval_hours"] == 0.5
    assert (
        client.put(
            "/api/channels/test_channel", json={"refresh_interval_hours": 1.25}
        ).status_code
        == 200
    )
    assert (
        test_server._load_channel_config()["channels"][0]["refresh_interval_hours"]
        == 1.25
    )


def test_concurrent_config_updates_preserve_all_channels(test_server) -> None:
    def add_channel(index: int) -> int:
        with test_server.app.test_client() as browser:
            return browser.post(
                "/api/channels",
                json={
                    "display_name": f"Channel {index}",
                    "url": "https://www.youtube.com/@test",
                },
            ).status_code

    with (
        patch.object(
            test_server, "_verify_youtube_channel", return_value=(True, "", {})
        ),
        ThreadPoolExecutor(max_workers=4) as pool,
    ):
        assert list(pool.map(add_channel, range(8))) == [201] * 8
    assert len(test_server._load_channel_config()["channels"]) == 10


def test_broken_config_is_never_overwritten(client, test_server) -> None:
    config = test_server.config_dir / "channels.yaml"
    config.write_text("channels: [invalid yaml")
    assert (
        client.put(
            "/api/config/refresh-interval", json={"refresh_interval_hours": 2}
        ).status_code
        == 500
    )
    assert config.read_text() == "channels: [invalid yaml"


def test_atomic_write_failure_preserves_original_and_cleans_temp(tmp_path) -> None:
    target = tmp_path / "config.yaml"
    target.write_text("original")
    with patch("src.utils.os.replace", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            atomic_write_text(target, "replacement")
    assert target.read_text() == "original"
    assert list(tmp_path.iterdir()) == [target]


def test_failed_redownload_preserves_episode(test_server, setup_test_episodes) -> None:
    original = {
        path.name: path.read_bytes()
        for path in setup_test_episodes.iterdir()
        if path.is_file()
    }
    with patch("src.downloader.YouTubeDownloader.download_video", return_value=False):
        job = redownload_and_run(test_server)
    assert job["state"] == "pending"  # Retried with the normal backoff.
    assert "original episode preserved" in job["error"]
    assert {
        path.name: path.read_bytes()
        for path in setup_test_episodes.iterdir()
        if path.is_file()
    } == original
    assert not list(setup_test_episodes.parent.glob(".redownload-*"))


def test_successful_redownload_publishes_replacement(
    test_server, setup_test_episodes
) -> None:
    def download(downloader, video, channel_name, **kwargs) -> bool:
        channel = downloader.videos_dir / channel_name
        channel.mkdir()
        (channel / f"{video['id']}.mp4").write_bytes(b"replacement")
        (channel / f"{video['id']}.json").write_text(json.dumps(video))
        return True

    with patch(
        "src.downloader.YouTubeDownloader.download_video",
        autospec=True,
        side_effect=download,
    ):
        assert redownload_and_run(test_server)["state"] == "completed"
    assert (setup_test_episodes / "test123abc.mp4").read_bytes() == b"replacement"
    assert not (setup_test_episodes / "test123abc.m4a").exists()


@pytest.mark.parametrize("failure", ["exit", "timeout", "missing", "empty", "success"])
def test_conversion_always_cleans_temporary_files(
    client, setup_test_episodes, tmp_path, failure
) -> None:
    (setup_test_episodes / "convert.mp4").write_bytes(b"video")
    converted = []

    def convert(command, **kwargs):
        output = Path(command[-1])
        converted.append(output)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 600)
        if failure == "missing":
            raise FileNotFoundError("ffmpeg")
        if failure == "success":
            output.write_bytes(b"converted audio")
        return Mock(returncode=1 if failure == "exit" else 0, stderr="failed")

    with patch("src.web_server.subprocess.run", side_effect=convert):
        response = client.post(
            "/api/channels/test_channel/episodes/convert/download-mp3"
        )
        assert response.status_code == (200 if failure == "success" else 500)
        if failure == "success":
            assert response.data == b"converted audio"
        response.close()
    assert converted and all(not path.exists() for path in converted)


def test_rss_handles_null_thumbnails_and_uses_real_size(setup_test_episodes) -> None:
    path = setup_test_episodes / "test123abc.json"
    metadata = json.loads(path.read_text())
    metadata["thumbnail"] = None
    metadata["file_size"] = None
    path.write_text(json.dumps(metadata))
    episodes = RSSGenerator().scan_channel_videos(setup_test_episodes)
    episode = next(episode for episode in episodes if episode["id"] == "test123abc")
    assert episode["thumbnail"] is None
    assert episode["file_size"] == len(b"fake audio data")


def test_feed_urls_resolve_and_xml_roundtrips(client, setup_test_episodes) -> None:
    path = setup_test_episodes / "test123abc.json"
    metadata = json.loads(path.read_text())
    metadata.update(
        title="Quotes ' & <tags>",
        description="a ]]> b &amp; <markup> \x00",
        duration=float("inf"),
    )
    path.write_text(json.dumps(metadata))
    response = client.get("/feeds/test_channel")
    root = ElementTree.fromstring(response.data)
    assert [item.findtext("guid") for item in root.findall("channel/item")] == [
        "test456def",
        "test123abc",
    ]
    item = root.findall("channel/item")[1]
    assert item.findtext("title") == metadata["title"]
    assert item.findtext("description") == "a ]]> b &amp; <markup> "
    namespace = {
        "atom": "http://www.w3.org/2005/Atom",
        "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
    }
    from urllib.parse import urlsplit

    self_link = root.find("channel/atom:link", namespace)
    artwork = item.find("itunes:image", namespace)
    enclosure = item.find("enclosure")
    assert self_link is not None and artwork is not None and enclosure is not None
    links = [self_link.get("href"), artwork.get("href"), enclosure.get("url")]
    for link in links:
        assert client.get(urlsplit(link).path).status_code == 200
    assert enclosure.get("length") == str(len(b"fake audio data"))


def test_cleanup_ignores_incomplete_metadata_and_removes_all_formats(tmp_path) -> None:
    channel = tmp_path / "channel"
    channel.mkdir()
    for index in range(3):
        (channel / f"v{index}.json").write_text(
            json.dumps({"id": f"v{index}", "upload_date": f"2024010{index + 1}"})
        )
        for extension in (".mp4", ".webm", ".m4a", ".mp3"):
            (channel / f"v{index}{extension}").write_bytes(b"episode")
    (channel / "pending.info.json").write_text(
        json.dumps({"id": "v2", "upload_date": "20990101"})
    )
    (channel / "fake.json").write_text(
        json.dumps({"id": "../victim", "upload_date": "19700101"})
    )
    victim = tmp_path / "victim.mp4"
    victim.write_bytes(b"protected")
    cleanup_episodes(tmp_path, "channel", 2)
    assert not list(channel.glob("v0.*"))
    assert (channel / "v1.mp4").exists() and (channel / "v2.mp4").exists()
    assert victim.read_bytes() == b"protected"


def write_episode(channel: Path, episode_id: str, **metadata: object) -> None:
    """Store one complete episode: metadata, media and a thumbnail."""
    thumbnails = channel / "thumbnails"
    thumbnails.mkdir(parents=True, exist_ok=True)
    (channel / f"{episode_id}.json").write_text(
        json.dumps({"id": episode_id, **metadata})
    )
    (channel / f"{episode_id}.m4a").write_bytes(b"episode")
    (thumbnails / f"{episode_id}.jpg").write_bytes(b"thumbnail")


@pytest.mark.parametrize(
    "timestamps,kept",
    [
        # Ids sort opposite to upload order: an id tie-break kept "zzz".
        ({"aaa": 1_704_100_000, "zzz": 1_704_090_000}, "aaa"),
        ({"aaa": 1_704_090_000, "zzz": 1_704_100_000}, "zzz"),
        # A missing timestamp counts as the oldest upload of its day.
        ({"aaa": None, "mmm": 1_704_100_000, "zzz": 1_704_090_000}, "mmm"),
    ],
    ids=["newer-has-lower-id", "newer-has-higher-id", "missing-timestamp"],
)
def test_cleanup_breaks_same_day_ties_by_timestamp(
    tmp_path, timestamps: dict[str, int | None], kept: str
) -> None:
    channel = tmp_path / "channel"
    for episode_id, timestamp in timestamps.items():
        metadata: dict[str, object] = {"upload_date": "20240101"}
        if timestamp is not None:
            metadata["timestamp"] = timestamp
        write_episode(channel, episode_id, **metadata)
    cleanup_episodes(tmp_path, "channel", 1)
    assert [path.stem for path in channel.glob("*.json")] == [kept]
    assert [path.stem for path in channel.glob("*.m4a")] == [kept]


def test_undeletable_thumbnail_does_not_stop_cleanup(tmp_path) -> None:
    channel = tmp_path / "channel"
    for day in range(1, 5):
        write_episode(channel, f"ep{day}", upload_date=f"2024010{day}")
    stuck = channel / "thumbnails" / "ep1.jpg"
    real_unlink = Path.unlink

    def flaky_unlink(path: Path, missing_ok: bool = False) -> None:
        if path.name == stuck.name:
            raise PermissionError("thumbnail is busy")
        real_unlink(path, missing_ok=missing_ok)

    with patch.object(Path, "unlink", flaky_unlink):
        cleanup_episodes(tmp_path, "channel", 1)
    # ep1 is deleted first; its stuck thumbnail no longer aborts the pass.
    assert [path.name for path in channel.glob("*.json")] == ["ep4.json"]
    assert [path.name for path in channel.glob("*.m4a")] == ["ep4.m4a"]
    assert sorted(path.name for path in stuck.parent.iterdir()) == [
        "ep1.jpg",
        "ep4.jpg",
    ]


def test_download_failure_code_never_reports_success(tmp_path) -> None:
    downloader = YouTubeDownloader(
        str(tmp_path / "config/channels.yaml"), str(tmp_path)
    )
    with patch("src.downloader.yt_dlp.YoutubeDL") as ytdlp:
        ytdlp.return_value.__enter__.return_value.download.return_value = 1
        assert not downloader.download_video(
            {"id": "video", "url": "https://www.youtube.com/watch?v=video"}, "channel"
        )


def test_alternate_formats_not_downloaded_again(tmp_path) -> None:
    downloader = YouTubeDownloader(
        str(tmp_path / "config/channels.yaml"), str(tmp_path)
    )
    channel = downloader.videos_dir / "channel"
    channel.mkdir()
    (channel / "video.webm").write_bytes(b"video")
    (channel / "video.json").write_text("{}")
    assert downloader.video_exists("channel", "video")
    assert not downloader.video_exists("channel", "video", "audio")
    assert (
        downloader.rss_generator.refresh_timestamps_file
        == tmp_path / "config/refresh_timestamps.json"
    )


def test_release_timestamp_used_for_download_delay(tmp_path) -> None:
    downloader = YouTubeDownloader(
        str(tmp_path / "config/channels.yaml"), str(tmp_path)
    )
    with patch("src.downloader.time.time", return_value=100000):
        assert downloader.is_video_too_new(
            {"timestamp": 99000, "upload_date": "19700101"}, 1
        )
        assert not downloader.is_video_too_new({"timestamp": 90000}, 1)


def test_channel_names_cannot_break_inline_javascript(client, test_server) -> None:
    payload = "');globalThis.pwned=true;// <script> & \""
    config = test_server._load_channel_config()
    config["channels"][0]["display_name"] = payload
    test_server._save_channel_config(config)

    class Handlers(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.handlers = []

        def handle_starttag(self, tag: str, attrs: list) -> None:
            handler = dict(attrs).get("onclick", "")
            if handler.startswith("openEpisodesModal("):
                self.handlers.append(handler)

    parser = Handlers()
    parser.feed(client.get("/").get_data(as_text=True))
    handler = parser.handlers[0]
    assert "globalThis.pwned=true" in handler
    # JSON strings keep quote characters encoded even after HTML attribute decoding.
    assert "\\u0027" in handler and "\\u003c" in handler
    arguments = json.loads(
        "[" + handler.removeprefix("openEpisodesModal(").removesuffix(")") + "]"
    )
    assert arguments == ["test_channel", payload]


def test_failed_format_change_preserves_existing_episode(tmp_path) -> None:
    downloader = YouTubeDownloader(
        str(tmp_path / "config/channels.yaml"), str(tmp_path)
    )
    channel = downloader.videos_dir / "channel"
    channel.mkdir()
    (channel / "video.mp4").write_bytes(b"original video")
    (channel / "video.json").write_text('{"id": "video"}')
    with patch("src.downloader.yt_dlp.YoutubeDL") as ytdlp:
        ytdlp.return_value.__enter__.return_value.download.side_effect = RuntimeError(
            "offline"
        )
        assert not downloader.download_video(
            {"id": "video", "url": "https://www.youtube.com/watch?v=video"},
            "channel",
            format_type="audio",
        )
    assert (channel / "video.mp4").read_bytes() == b"original video"
    assert (channel / "video.json").exists()


def test_audio_conversion_and_quality_options(tmp_path) -> None:
    downloader = YouTubeDownloader(
        str(tmp_path / "config/channels.yaml"), str(tmp_path)
    )
    video = {"id": "video", "url": "https://www.youtube.com/watch?v=video"}
    with patch("src.downloader.yt_dlp.YoutubeDL") as ytdlp:
        ytdlp.return_value.__enter__.return_value.download.return_value = 1
        downloader.download_video(
            video, "channel", format_type="audio", sponsorblock_categories=["sponsor"]
        )
        options = ytdlp.call_args.args[0]
        assert {"key": "FFmpegExtractAudio", "preferredcodec": "m4a"} in options[
            "postprocessors"
        ]
        assert any(
            processor["key"] == "SponsorBlock"
            for processor in options["postprocessors"]
        )
        downloader.download_video(video, "channel", quality="480p")
        options = ytdlp.call_args.args[0]
        assert all(
            "height<=480" in fallback for fallback in options["format"].split("/")
        )


def test_cron_lock_retains_inode_and_excludes_other_runners(tmp_path) -> None:
    from src.cron_runner import AutomationRunner

    first, second, third = [AutomationRunner(str(tmp_path)) for _ in range(3)]
    try:
        assert first.acquire_lock()
        inode = first.lock_file_path.stat().st_ino
        assert not second.acquire_lock()
        first.release_lock()
        assert first.lock_file_path.stat().st_ino == inode
        assert second.acquire_lock()
        assert not third.acquire_lock()
    finally:
        first.release_lock()
        second.release_lock()
        third.release_lock()


def test_web_and_cron_share_media_lock(client, test_server, tmp_path) -> None:
    from src.cron_runner import AutomationRunner

    cron = AutomationRunner(str(tmp_path))
    cron.lock_file_path = test_server.config_dir / "media.lock"
    assert cron.acquire_lock()
    try:
        assert client.post("/api/refresh").status_code == 409
    finally:
        cron.release_lock()
    assert test_server._begin_refresh()
    try:
        assert not cron.acquire_lock()
    finally:
        test_server._finish_refresh()


def test_scheduler_close_is_idempotent(test_server) -> None:
    test_server.scheduler.running = False
    test_server.close()
    test_server.scheduler.shutdown.assert_not_called()
