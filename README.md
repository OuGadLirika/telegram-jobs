# Job Bot

Telegram bot for fetching, deduplicating, queueing and publishing job posts.

Source feed:
- https://remotive.com/remote-jobs/feed
- https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss
- https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss
- https://weworkremotely.com/categories/remote-front-end-programming-jobs.rss
- https://weworkremotely.com/categories/remote-management-and-finance-jobs.rss
- https://weworkremotely.com/categories/remote-programming-jobs.rss

## Requirements

- Python 3.11+
- Telegram bot token and bot admin account

## Setup

1. Create and activate virtual environment.

For a clean Linux machine (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
python3 -m venv .venv
source .venv/bin/activate
```

For Fedora/RHEL-based systems:

```bash
sudo dnf install -y python3 python3-pip
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create `.env` from `.env.example` and fill values:

```bash
cp .env.example .env
```

## Required Environment Variables

- `BOT_TOKEN`: Telegram bot token.
- `ADMIN_USER_ID`: Numeric Telegram user id for admin commands.
- `TARGET_CHANNEL`: Channel username (`@channel`) or numeric channel id (`-100...`).

## Optional Environment Variables

- `SQLITE_PATH` (default: `jobs.db`)
- `BANNER_PATH` (default: `banner.png`)
- `POST_CHAR_LIMIT` (default: `1000`)
- `SEND_INTERVAL_SECONDS` (default: `5400`)
- `REQUEST_TIMEOUT_SECONDS` (default: `20`)
- `REQUEST_RETRIES` (default: `3`)
- `RETRY_BASE_DELAY_SECONDS` (default: `0.5`)
- `RETRY_JITTER_SECONDS` (default: `0.4`)
- `MONITOR_INTERVAL_SECONDS` (default: `86400`)

## Run

```bash
python bot.py
```

Bot validates required env variables on startup and exits early if config is invalid.

## Storage

- Main storage: SQLite (`jobs`, `job_links`, `queue`, `posts_status`, `sources`, `failures`).
- On first run, bot migrates legacy `jobs.json`, `links.json`, `delayed_posts.json`, `posts_status.json` into SQLite.

## Admin Commands

- `/info` - show queued posts with queue id and scheduled publish time.
- `/fetch` - run one RSS fetch cycle immediately and return cycle metrics.
- `/delete` - delete next queued element.
- `/delete <queue_id>` - delete by queue id.
- `/delete <job_id>` - delete by job id.
- `/push` - publish next queued element immediately.
- `/push <queue_id>` - publish specific queue element immediately.
- `/push <job_id>` - publish specific job immediately.
- `/reschedule` - recalculate current queue schedule with configured `SEND_INTERVAL_SECONDS`.
- `/reschedule <seconds>` - recalculate current queue schedule using custom interval in seconds.

## Tests

```bash
pytest -q
```

Current suite covers legacy helpers and production-critical scenarios:

- deduplication by fingerprint,
- queue scheduling/idempotency,
- retry/timeout/backoff behavior,
- JSON -> SQLite migration,
- publication error handling.
# telegram-jobs
