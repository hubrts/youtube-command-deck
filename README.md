# YouTube Direct Bot

Telegram + web tooling for YouTube download flows, transcript extraction, Q&A, and market research workflows.

## Project Structure

- `src/youtube_direct_bot/`: modular internal package (state store, Telegram helpers, web OpenAPI spec)
- `tests/`: unit tests
- `docs/`: architecture and security notes
- `web/`: browser UI assets
- `scripts/`: operational scripts (cookie refresh, validation, maintenance)

## Prerequisites

- Python 3.11+
- `yt-dlp` in PATH
- PostgreSQL (with `pgvector` if `STATE_DB_REQUIRE_PGVECTOR=1`)

## Setup

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install python-telegram-bot requests yt-dlp websockets "psycopg[binary]"
```

2. Create local env file from template:

```bash
cp .env.example .env
```

3. Fill required values in `.env`:
- `YT_BOT_TOKEN`
- `STATE_DB_DSN`
- `COOKIES_FILE`
- `STORAGE_DIR`
- `PUBLIC_URL_BASE`

4. Keep real cookie exports outside git (for example `./secrets/cookies.txt`) and set `COOKIES_FILE` accordingly.

## Running

- Telegram bot:

```bash
python3 bot.py
```

- Web API/UI:

```bash
python3 web_app.py --host 0.0.0.0 --port 8080
```

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

---

## Restart Bot + Web Together

Use the all-in-one restart script. It validates cookies, syncs systemd units, and restarts both services:

```bash
bash scripts/restart_bot_ui.sh
```

What it does:
1. Regenerates the OpenAPI/Swagger spec
2. Validates or refreshes cookies from the browser
3. Copies unit files to `~/.config/systemd/user/`
4. Reloads the systemd user daemon
5. Restarts `ytdl-bot.service` and `ytdl-web.service`

Run this script **from the desktop session** where Chrome/keyring is available (not over a plain SSH session without `DISPLAY`).

To check service status at any time:

```bash
systemctl --user status ytdl-bot.service ytdl-web.service
```

To view live logs:

```bash
journalctl --user -u ytdl-bot.service -f
journalctl --user -u ytdl-web.service -f
```

---

## Cookie Management

Cookies must stay fresh for yt-dlp to access age-restricted or logged-in YouTube content.

### How it works

Cookies are exported from a **locally running Chrome browser** with an active YouTube login. The script reads cookies directly from the Chrome profile on disk — no manual export needed, but **Chrome must be open and logged into YouTube**.

### Manual refresh

```bash
DISPLAY=:0 YT_COOKIES_FROM_BROWSER=chrome:Default \
  python3 scripts/refresh_cookies_from_browser.py
```

After refreshing, validate:

```bash
python3 scripts/validate_cookies.py --cookies-file ./cookies.txt
```

### Automatic refresh via systemd timer

Install the timer (runs every 12 hours, also on boot):

```bash
cp systemd-user/ytdl-refresh-cookies.service ~/.config/systemd/user/
cp systemd-user/ytdl-refresh-cookies.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now ytdl-refresh-cookies.timer
```

Check timer status:

```bash
systemctl --user list-timers ytdl-refresh-cookies.timer
```

Key env vars that control refresh behavior:

| Variable | Default | Description |
|---|---|---|
| `YT_COOKIES_FROM_BROWSER` | `chrome` | Browser and profile (e.g. `chrome:Default`) |
| `COOKIE_TOUCH_YOUTUBE_BEFORE_EXPORT` | `1` | Open/focus YouTube tab before exporting |
| `COOKIE_REQUIRE_OPEN_YOUTUBE` | `1` | Refuse to export if no YouTube tab is open |
| `COOKIE_TOUCH_OPEN_NEW_WINDOW_IF_MISSING` | `0` | Don't open a new window automatically |
| `COOKIE_REFRESH_TIMEOUT_SEC` | `90` | Max seconds to wait for export |

### Healthcheck with Telegram alerts

The healthcheck script runs every 30 minutes via cron and sends a Telegram alert if cookies expire or become invalid:

```bash
python3 scripts/cookie_auth_healthcheck.py
```

---

## Cron Jobs

Install the full crontab:

```bash
crontab cron/ytdl_cleanup.cron
```

This sets up:

| Schedule | Job |
|---|---|
| Daily at 04:15 | Prune downloaded videos older than `RETENTION_DAYS` (default 60) |
| Every 30 min | Cookie/auth healthcheck with Telegram alerts on failure |

To add the optional cookie refresh cron instead of (or alongside) the systemd timer, uncomment the last entry in `cron/ytdl_cleanup.cron`.

View current crontab:

```bash
crontab -l
```

Logs are written to `data/cleanup_cron.log` and `data/cookie_healthcheck.log`.

---

## Secrets Hygiene

- `.gitignore` excludes `.env*`, `cookies.txt`, `data/`, logs, caches, and local-only settings.
- `cookies.txt.example` is a placeholder; never commit real cookie exports.
- Pre-commit secret scanning is configured in `.pre-commit-config.yaml`.

Recommended setup:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

See `docs/SECURITY.md` for credential-rotation steps.
