# YouTube to RSS Podcast Converter

A self-hosted solution that converts YouTube channels into podcast RSS feeds. Subscribe to your favorite YouTube channels in any podcast app, with automatic SponsorBlock integration to remove ads and promotional segments.

## Features

- **Automatic Downloads**: Monitor YouTube channels for video or M4A audio, with configurable video quality
- **SponsorBlock Integration**: Automatically remove sponsored segments, self-promotion, and other unwanted content
- **Podcast RSS Feeds**: Generate standard RSS feeds compatible with any podcast app
- **Per-Channel Scheduling**: Individual refresh controls with configurable intervals for each channel
- **Episode Playback**: Click episodes to play directly in your browser
- **Web Interface**: Web UI with episode, individual channel refresh, and status monitoring

![yt2rss screenshot](yt2rss_screenshot.png)

## Quick Start

The easiest way to get started is using the pre-built Docker image:

```bash
# Create necessary directories
mkdir -p appdata/config appdata/podcasts

# Run the container
docker run -d \
  --name yt2rss \
  -p 5000:5000 \
  -e BASE_URL=http://localhost:5000 \
  -v $(pwd)/appdata/config:/app/appdata/config \
  -v $(pwd)/appdata/podcasts:/app/appdata/podcasts \
  cubicalbatch/yt2rss

# Optional: Run with SOCKS5 proxy support
docker run -d \
  --name yt2rss \
  -p 5000:5000 \
  -e BASE_URL=http://localhost:5000 \
  -e YT_DLP_PROXY=socks5://proxy.example.com:1080 \
  -v $(pwd)/appdata/config:/app/appdata/config \
  -v $(pwd)/appdata/podcasts:/app/appdata/podcasts \
  cubicalbatch/yt2rss
```

### Access Your Podcasts

- **Web Interface**: Visit `http://localhost:5000` to manage channels and view status
- **RSS Feeds**: Subscribe to `http://localhost:5000/feeds/channel_name` in your podcast app
- **Direct Access**: Podcasts served at `http://localhost:5000/podcasts/channel_name/filename.mp4`

## Usage

### Web Interface

The web interface provides:

- **Channel Management**: Add, edit, and delete YouTube channels
- **Episode Browser**: Click episode counts to view detailed episode modals with thumbnails and descriptions
- **Episode Playback**: Click individual episodes to play them directly in your browser
- **Per-Channel Refresh**: Individual refresh buttons for each channel with real-time status
- **Status Monitoring**: View download progress and last refresh times
- **Configuration**: Adjust refresh intervals and settings per channel

### Podcast Apps

Any standard podcast app can subscribe to your feeds:

1. Copy the RSS feed URL from the web interface
2. Add it as a new podcast subscription
3. Videos will appear as episodes with thumbnails
4. Automatic updates based on your refresh interval

### Channel Configuration

Add new channels through the web interface or by editing `appdata/config/channels.yaml`:

```yaml
default_interval_hours: 24  # Default refresh interval for new channels
check_delay_seconds: 30     # Delay between video metadata checks to avoid rate limiting
channels:
  - name: example_channel
    url: https://www.youtube.com/@ExampleChannel
    max_episodes: 15
    download_delay_hours: 12
    refresh_interval_hours: 24  # Individual channel refresh interval
    format: video              # video or audio
    quality: max               # max, best, 480p, 720p, or 1080p
    sponsorblock_categories:
      - sponsor
      - selfpromo
```

Refresh intervals can be fractional (for example, `0.5` for 30 minutes), up to
8760 hours. Episode limits must be positive integers. The API validates settings
before saving them. Configuration files are replaced atomically; a malformed
existing file must be repaired before configuration changes can be saved.

Manual and scheduled refreshes share one worker slot. Purge, delete, and
redownload requests return HTTP 409 while a media operation is running. A failed
redownload preserves the existing episode. Cleanup counts completed episodes and
removes all supported media formats belonging to expired episodes.

### SponsorBlock Categories

Available categories for automatic removal:
- `sponsor`: Paid promotion segments
- `selfpromo`: Self-promotion and channel announcements
- `interaction`: Subscribe reminders and interaction requests
- `intro`: Channel intro sequences
- `outro`: End screens and credits
- `preview`: Video previews and recaps
- `music_offtopic`: Non-music segments in music videos
- `filler`: Filler content and tangents

## Environment Variables

- `BASE_URL`: Base URL for RSS feeds and podcast URLs (default: `http://localhost:5000`)
  - Example: `BASE_URL=https://your-domain.com`
  - Used in RSS feed URLs and media file links

- `YT_DLP_PROXY`: SOCKS5/HTTP/HTTPS proxy for yt-dlp downloads (optional)
  - Supports SOCKS5, HTTP, and HTTPS proxies
  - Examples:
    - `YT_DLP_PROXY=socks5://proxy.example.com:1080`
    - `YT_DLP_PROXY=socks5://user:pass@proxy.example.com:1080`
    - `YT_DLP_PROXY=http://proxy.example.com:8080`
  - When set, all YouTube downloads and metadata fetches will use the specified proxy

- `YT2RSS_PASSWORD`: Management password (optional)
  - When unset, the app stays open access and all URLs work exactly as before
  - When set, the management UI and API require a login; pages redirect to
    `/login` and API/media requests without credentials are rejected with 401
  - Cookie flags: `HttpOnly`, `SameSite=Lax`; `Secure` is added when `BASE_URL`
    starts with `https://`

- `YT2RSS_USERNAME`: Login username (optional, default `admin`)

- `YT2RSS_SECRET_KEY`: Flask session signing key (optional)
  - When unset, a key is generated once and persisted to
    `appdata/config/.session_secret` (mode 0600)

### Private (tokenized) feed URLs

When `YT2RSS_PASSWORD` is set, feeds become private:

- Each channel has an opaque `feed_token` stored in `channels.yaml`. It is
  generated on demand and shown in the UI.
- Share `/feeds/<channel>?token=<feed_token>` with podcast apps; the feed's
  enclosure and thumbnail URLs carry the same token, so apps fetch media and
  images without a login session.
- A token is only valid for its own channel and grants no access to the
  management API.
- "Rotate Token" in the channel card generates a new token; existing tokenized
  URLs for that channel stop working immediately.
- Without `YT2RSS_PASSWORD`, plain feed URLs keep working for everyone.
- The standalone cron runner (`src/cron_runner.py`) is headless and unaffected.

## Development Setup

For development or manual installation, this project uses UV for dependency management:

```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies and run
uv sync --frozen
uv run --frozen python main.py
```

Run the checks locally:

```bash
uv run --frozen pytest
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen pyright
node --test tests/test_ui.cjs
```

`uv.lock` is the dependency source of truth. Docker installs the pinned, hashed
runtime dependencies exported to `requirements.txt`. After updating dependencies,
regenerate that file with:

```bash
uv export --frozen --no-dev --no-emit-project -o requirements.txt
```

Requirements:
- Python 3.13+
- FFmpeg (for video processing)
- UV package manager

## Security Notes

- Designed for a trusted local network. Without `YT2RSS_PASSWORD` the
  management API has no authentication.
- With `YT2RSS_PASSWORD` set, management pages and API require login (signed
  session cookie, constant-time password check, per-address failed-login
  throttling). Feeds and media additionally accept a valid per-channel
  `feed_token` in the `?token=` query parameter; tokens never grant management
  API access.
- Browser management requests must originate from the same origin. Feeds and
  media remain available across origins for podcast clients. Command-line API
  clients can continue to call the API without an `Origin` header while logged
  in.
- The built-in server serves HTTP. Use an authenticated HTTPS reverse proxy for
  remote access and configure `BASE_URL` to match the public address (also
  enables the `Secure` cookie flag).
- Run one web server process for a data directory so scheduling and configuration
  edits stay coordinated. On POSIX systems, the web server and standalone cron
  runner also share a file lock to prevent overlapping media operations.

## Contributing

This is a self-hosted personal project. Feel free to fork and modify for your needs.

## Acknowledgments

This project is built with the help of several excellent open-source tools:

- **[yt-dlp](https://github.com/yt-dlp/yt-dlp)** - For downloading YouTube videos
- **[SponsorBlock](https://sponsor.ajay.app/)** - For community-driven sponsor segment detection
- **[FFmpeg](https://ffmpeg.org/)** - For video processing and manipulation

Special thanks to the maintainers and contributors of these projects.

## License

Open source - modify and distribute freely.
