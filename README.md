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
