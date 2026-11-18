# YouTube to RSS Podcast Converter

A self-hosted solution that converts YouTube channels into podcast RSS feeds. Subscribe to your favorite YouTube channels in any podcast app, with automatic SponsorBlock integration to remove ads and promotional segments.

## Features

- **Automatic Downloads**: Monitor YouTube channels for video or M4A audio, with configurable video quality
- **SponsorBlock Integration**: Automatically remove sponsored segments, self-promotion, and other unwanted content
- **Episode Filters**: Per-channel title include/exclude rules, duration limits, and skip Shorts/livestreams toggles
- **Remembered Deletions**: Manually deleted episodes are never re-downloaded by refreshes
- **Health Monitoring**: Refresh history, repeated-failure and unavailable-channel detection, low-space alerts, and webhook notifications
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
- **Download Queue**: Live panel with per-job progress, cancel, retry, and dismiss actions
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
    quota_gb: 20               # 0 or absent: inherit the global quota
    retention_days: 90         # 0 or absent: inherit the global retention
    sponsorblock_categories:
      - sponsor
      - selfpromo
    # Episode filters (all optional; absent keys disable the filter):
    title_include:             # when set, a title must contain at least one rule
      - review
      - interview
    title_exclude:             # titles matching any rule are never downloaded
      - trailer
    min_duration_seconds: 300  # skip episodes shorter than this (0 = no limit)
    max_duration_seconds: 7200 # skip episodes longer than this (0 = no limit)
    skip_shorts: true          # skip YouTube Shorts (60 seconds or less)
    skip_livestreams: true     # skip live, upcoming and was-live streams
```

Refresh intervals can be fractional (for example, `0.5` for 30 minutes), up to
8760 hours. Episode limits must be positive integers. The API validates settings
before saving them. Configuration files are replaced atomically; a malformed
existing file must be repaired before configuration changes can be saved.

Scheduled refreshes are anchored to each channel's last refresh attempt
(successful or failed, from `health.json`, falling back to
`refresh_timestamps.json`): the next run is that attempt plus the channel's
interval. Schedules therefore survive restarts and edits to other channels,
and changing a channel's interval reschedules it from its last attempt.
Channels already overdue when the server starts run shortly after startup,
staggered so they do not all start at once; channels without any recorded
attempt (for example after upgrading) wait one full interval.

### Episode Filters

Every filter is optional; channels without filter keys behave exactly as
before (backwards compatible). Matching semantics:

- **Title rules** are case-insensitive substrings, compared with Unicode
  case folding; surrounding whitespace on a rule is ignored. When
  `title_include` is non-empty, a title must contain at least one rule.
  **Exclusions win**: a title matching any `title_exclude` rule is skipped
  even when it matches an include rule.
- **Duration bounds** apply to `min_duration_seconds`/`max_duration_seconds`
  (non-negative integers; `0` or absent disables a bound). Videos with an
  unknown duration are never skipped by duration rules.
- **Shorts** are videos whose duration is at most 60 seconds — unknown
  durations are never classified as Shorts.
- **Livestreams** are detected from yt-dlp's flat entry `live_status`
  (`is_live`, `is_upcoming`) or the `is_live`/`was_live` flags; past
  broadcasts flagged `was_live` also count as livestreams.

Filters are enforced in three places: refresh discovery checks cheap fields
(title, Shorts, livestreams) before each metadata request and re-applies
duration bounds to the fetched metadata; queue execution re-checks the
channel's *current* filters (plus tombstones) when a job runs, so editing
filters also cancels queued jobs — such jobs are retired as `cancelled`
with the matched rule as the reason; and the RSS feed, the episodes API and
the episode counts hide episodes on disk that the current filters exclude
(files are never deleted). Downloaded episode metadata keeps the live
markers, so `skip_livestreams` also hides stored livestreams; episodes
downloaded before those markers were recorded stay visible. Every skip is
logged with the matched rule in the refresh logs.

Manual and scheduled refreshes share one worker slot. A manual refresh returns
HTTP 409 while another refresh or media operation is running; a scheduled
refresh that finds the worker busy (another refresh, a queued download, or a
purge/delete in progress) is not dropped but retried after 5 minutes, repeating
until it can run. Purge and delete requests return HTTP 409 while a media
operation is running. Redownload requests are never rejected for that reason:
they are queued like any other download (see below) and a failed redownload
preserves the existing episode. Cleanup counts completed episodes and removes
all supported media formats belonging to expired episodes.

### Download Queue

Refreshes no longer download videos inline. Discovery enqueues every new
video into a durable queue (`appdata/config/download_queue.json`), and a
single worker thread downloads them one at a time. Explicit episode
redownloads run through the same queue:

- Jobs survive restarts: a queue file is written atomically after every
  change, jobs left `running` by a crash are requeued on startup, and a
  corrupt queue file is quarantined as `download_queue.json.corrupt` while
  the app starts with an empty queue. Later corruptions get a timestamped
  backup name, so earlier backups are never overwritten. A queue file that
  cannot be read (permissions, I/O errors) is never discarded: the queue
  reads as empty and every change is refused until the file is readable
  again.
- Failed downloads retry automatically up to 5 attempts with a
  deterministic exponential backoff (60s, 120s, 240s, 480s between
  attempts); exhausted attempts stay `failed` with the last error until
  retried or dismissed.
- Jobs can be cancelled while pending or running (a running download aborts
  cooperatively and its partial files are removed). Completed history is
  capped at the newest 200 entries.
- Duplicate protection: at most one active job per (channel, video), whether
  it is a download or a redownload.
- The queue worker shares `media.lock` with refreshes and destructive
  actions, so downloads and redownloads never race purge/delete operations.
- The channel's *current* format/quality settings apply when a queued job
  runs; deleting a channel cancels its queued jobs.
- If `channels.yaml` cannot be read or parsed (missing, unreadable, invalid
  YAML, or no `channels` list), queued jobs are not cancelled: they wait and
  retry every minute without using any of their attempts, showing the
  reason as their error, and resume once the file is fixed. Only jobs whose
  channel is absent from a valid configuration are cancelled.
- **Redownloads** (`POST /api/channels/<channel>/episodes/<episode_id>/redownload`)
  return HTTP 202 immediately with `{"message": ..., "job": ...}`, a job of
  `kind` `redownload` (refresh-discovered jobs have `kind` `download`); the
  endpoint answers 404 for an unknown channel or episode and 409 (with the
  conflicting `job`) while an active job already covers the episode. The job
  gets progress, cancellation and retries like any download and uses the
  channel's current format/quality/SponsorBlock settings. The new copy is
  staged in a temporary `.redownload-*` directory and replaces the episode's
  media, metadata and thumbnail only once it is complete, so a failed,
  incomplete or cancelled redownload leaves the original untouched. As an
  explicit user action it skips episode filters and tombstones and ignores
  quotas (the episode replaces itself), but low free-space protection still
  blocks it as a terminal failure; it never triggers `max_episodes` cleanup
  or retention.

The UI shows a collapsible "Download Queue" panel with the active job's
progress bar, pending count, and failed jobs with Retry/Dismiss actions;
redownload jobs carry a "Redownload" label. The episode browser's Redownload
button follows its queued job, showing the progress percentage and the final
result.

Queue management API (session-authenticated when `YT2RSS_PASSWORD` is set):

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/queue?state=<state>` | GET | List jobs with summary counts; optional state filter (`pending`, `running`, `completed`, `failed`, `cancelled`) |
| `/api/queue/<job_id>` | GET | Single job detail (404 if unknown) |
| `/api/queue/<job_id>/cancel` | POST | Cancel a pending job or request cancellation of a running one (409 for terminal jobs) |
| `/api/queue/<job_id>/retry` | POST | Requeue a failed/cancelled job with a fresh attempt budget (409 otherwise) |
| `/api/queue/<job_id>/dismiss` | POST | Remove a terminal job (409 for active jobs) |
| `/api/queue/clear-completed` | POST | Remove all completed jobs |

`GET /api/refresh/status` additionally reports `queue` counts
(`pending`/`running`/`failed`/...) so polling clients can tell discovery
from queue draining. The standalone cron runner drains leftover queue jobs
with the same store before its normal discovery pass; it never corrupts the
queue file and holds the same `media.lock` while downloading.

### Storage Controls

Storage settings live under the `storage:` key in `channels.yaml`
(missing keys fall back to the defaults):

```yaml
storage:
  global_quota_gb: 0     # Stop new downloads when total media would exceed this. 0 = unlimited.
  min_free_space_gb: 1   # Pause new downloads below this much free disk space. 0 = disabled.
  retention_days: 0      # Delete episodes older than this many days. 0 = disabled.
```

How enforcement works:

- **Quotas and free space only block new downloads** — media is never
  deleted automatically because a quota is exceeded. Only an explicit
  `retention_days` (global or per channel) deletes episodes.
- The checks run twice: during refresh discovery (a blocked video is
  logged and skipped instead of queued, so a full disk cannot create
  endless retry jobs) and again before a queued job executes. A blocked
  job lands in the terminal `failed` state with an actionable message such
  as `Global storage quota exceeded: 12.3/10.0 GB`; use the queue's Retry
  action once space is freed.
- Quota estimates use the worst of: yt-dlp's reported size hint, the
  largest episode already stored for the channel, and a per-format default
  (2 GB video, 200 MB audio).
- Retention runs after every channel refresh, once at startup, and daily
  via the scheduler. The cron runner applies it to every channel on each
  pass, even when that channel's discovery failed. Episodes are evaluated
  by upload date (metadata) with the file mtime as fallback, deleted
  oldest first, and each deletion is logged. **Pinned episodes are never
  deleted**, by retention or by `max_episodes` cleanup.
- **Pins** are stored in `appdata/config/pins.json` and survive episode
  redownloads and metadata rewrites. They are removed only when the
  episode (delete) or its channel (purge/delete) is removed. A corrupt or
  unreadable `pins.json` is never treated as "nothing pinned" and never
  moved aside automatically: `max_episodes` cleanup and retention pause
  (with an error in the log), pin changes are refused, and the storage
  view reports `pins_error` until you repair or remove the file.

Storage API (session-authenticated when `YT2RSS_PASSWORD` is set):

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/storage` | GET | Totals, free disk space, configured quotas/retention, per-channel usage with over-quota flag, and pinned counts. Usage counts only media files — thumbnails and metadata are excluded; symlinks are never followed. |
| `/api/config/storage` | GET | Effective global storage settings (with defaults applied). |
| `/api/config/storage` | PUT | Validate, atomically persist and return the global storage settings. |
| `/api/channels/<channel>/episodes/<episode_id>/pin` | POST | Pin an episode against automatic cleanup (404 for unknown channel/episode). |
| `/api/channels/<channel>/episodes/<episode_id>/pin` | DELETE | Remove an episode's pin. |

`GET /api/channels/<channel>/episodes` includes a `pinned` flag per episode.
The web UI adds a storage panel with usage/quota bars and a free-space
warning, storage fields in the scheduler modal, per-channel quota and
retention fields in the channel modal, and a pin toggle in the episode list.

### Remembered Deletions (Tombstones)

Deleting an episode from the episode browser records it in
`appdata/config/tombstones.json` (`{channel: {video_id: {deleted_at,
title}}}`) so refreshes never download it again:

- Discovery skips tombstoned videos (logged as "deleted by the user"), and
  queue execution cancels a queued download job for a tombstoned video
  instead of downloading it. The RSS feed and episode counts never include them.
- A corrupt `tombstones.json` is quarantined as `tombstones.json.corrupt`
  (later corruptions get a timestamped name, so backups are never
  overwritten) and the app starts with an empty set — deletions are lost,
  not crashes. A file that cannot be read is never discarded: refreshes
  fail (recorded in the refresh health) and queued downloads retry instead
  of fetching possibly deleted episodes, deleting episodes and the
  tombstone endpoints return an error, while feeds and episode lists keep
  working.
- An **explicit redownload** of a remembered-deleted episode is allowed
  and clears its tombstone on success, making it a normal episode again.
- **Purge is an explicit reset**: it clears the channel's tombstones, so a
  later refresh may download previously deleted episodes again. Deleting a
  channel also removes its tombstones.
- The channel's danger zone shows "Deleted episodes remembered: N" with a
  one-click "Forget Remembered Deletions" button.

Tombstone API (session-authenticated when `YT2RSS_PASSWORD` is set; all
endpoints return 404 for unknown channels):

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/channels/<channel>/tombstones` | GET | List remembered deletions with `total_count` |
| `/api/channels/<channel>/tombstones` | DELETE | Forget all remembered deletions for the channel |
| `/api/channels/<channel>/tombstones/<video_id>` | DELETE | Forget one remembered deletion (404 if not remembered) |

### Health Monitoring and Notifications

Every channel refresh is recorded in `appdata/config/health.json`:
consecutive failures, last success/failure, the last error, an
"unavailable" flag, and a bounded history (last 50 outcomes). The web UI
shows a collapsible "System Health" panel with per-channel status, free
disk space, pending alerts, and "Run Check Now".

Notifications are opt-in. Configure them under the `notifications:` key in
`channels.yaml` (or in the scheduler modal):

```yaml
notifications:
  webhook_url: https://example.com/hooks/yt2rss  # empty disables delivery
  failure_threshold: 3      # consecutive failed refreshes before alerting
  cooldown_minutes: 360     # minimum time between repeats of the same alert
  notify_low_space: true    # alert when free space < storage.min_free_space_gb
```

Alerts are generic JSON `POST`s (works with ntfy, Gotify, Home Assistant,
Discord relays, custom endpoints):

```json
{
  "source": "yt2rss",
  "event": "channel_failure",
  "channel": "example_channel",
  "message": "Channel 'example_channel' failed 3 consecutive refresh(es): ...",
  "occurred_at": "2026-01-01T00:00:00+00:00",
  "consecutive_failures": 3,
  "last_error": "..."
}
```

Events: `channel_failure` (threshold reached), `channel_unavailable` (the
channel URL cannot be fetched — removed, private, blocked), and
`low_disk_space`. Delivery failures never break a refresh; they are
recorded in `health.json` (visible in the UI) and retried on the next
evaluation. The cooldown only advances after a successful delivery. An
hourly scheduled check evaluates health; refreshes evaluate it as well.

Health API (session-authenticated when `YT2RSS_PASSWORD` is set):

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/health` | GET | Per-channel state, notification bookkeeping, disk state, pending events |
| `/api/health/check` | POST | Evaluate now and deliver due notifications synchronously |
| `/api/config/notifications` | GET/PUT | Read or validate and save notification settings |
| `/api/notifications/test` | POST | Deliver a test event to the configured webhook |

A corrupt `health.json` is quarantined as `health.json.corrupt` (later
corruptions get a timestamped name, so backups are never overwritten) and
the app starts with empty history. A `health.json` that cannot be read is
never discarded: health views and scheduling treat it as empty and new
refresh results are not saved until it is readable again. Deleting a
channel removes its health state.

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
