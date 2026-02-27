# YouTube Operation Hub

A self-hosted YouTube operations hub combining a web UI, REST API, and Telegram bot. Built to save time — instead of watching hours of video content, you get structured summaries, transcripts, and answers in seconds.

## Features

### Direct Download
Paste a YouTube URL and download the video or audio directly. Useful for offline viewing (flights, no Wi-Fi), extracting audio for editing, or archiving content — no YouTube Premium required.

### Saved Live
Records a live stream while it's in progress and saves it locally. Even if the creator deletes the stream afterward, the recording is preserved. Practical for sports events, live lectures, webinars, religious teachings, or any live content you can't afford to miss.

### Video Notes
Paste a YouTube link, run analysis, and get:
- 5–10 key bullet points summarizing the video
- Practical takeaways
- Uncertain points or risks mentioned
- Full transcript

Instead of watching a 40-minute or 2-hour video, you get the core information in seconds. The transcript and analysis are processed by a configurable LLM backend — local Ollama, OpenAI, or Claude.

### Knowledge Brew
Enter a keyword (e.g. "starting a bakery") and set a maximum number of videos to pull. The system fetches those videos from YouTube and produces a cross-video summary covering:
- Step-by-step process extracted across all videos
- Common patterns and shared advice
- Differences in approach between creators

Instead of watching six one-hour videos, you get the combined knowledge distilled into a structured report — sourced from real creators, not generic AI training data.

### Telegram Bot
Every feature above is available directly inside Telegram via interactive buttons. Paste a link, tap a button, get results — no browser required. Works from any device in seconds.

---

## Architecture

- `bot.py` — Telegram bot entry point
- `web_app.py` — web API and browser UI entry point
- `video_notes.py` — transcript extraction, LLM analysis, embeddings, Q&A
- `src/youtube_direct_bot/` — internal package: state store, Telegram helpers, OpenAPI spec
- `web/` — browser UI assets (HTML, JS, CSS, Swagger)
- `scripts/` — operational scripts (cookie refresh, validation, maintenance, healthcheck)
- `systemd-user/` — systemd unit files for bot, web, and cookie refresh timer
- `cron/` — crontab for cleanup and healthcheck
- `tests/` — unit tests
- `docs/` — architecture and security notes

---

## Prerequisites

- Python 3.11+
- `yt-dlp` in PATH
- PostgreSQL (with `pgvector` extension if `STATE_DB_REQUIRE_PGVECTOR=1`)
- Chrome/Chromium installed and logged into YouTube (for cookie extraction)

---

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
- `YT_BOT_TOKEN` — Telegram bot token from @BotFather
- `STATE_DB_DSN` — PostgreSQL connection string
- `COOKIES_FILE` — path to YouTube session cookies (outside git, e.g. `./secrets/cookies.txt`)
- `STORAGE_DIR` — local directory for downloaded files
- `PUBLIC_URL_BASE` — public-facing URL for download links

4. Keep real cookie exports outside git and set `COOKIES_FILE` accordingly.

---

## Running

Telegram bot:

```bash
python3 bot.py
```

Web API / UI:

```bash
python3 web_app.py --host 0.0.0.0 --port 8080
```

---

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

---

## AI / LLM — Transcript Analysis

The bot extracts transcripts from YouTube videos and uses an LLM to answer questions, summarize content, translate, and run market research workflows.

### How the pipeline works

1. **Transcript extraction** — yt-dlp pulls the transcript (subtitles or auto-generated captions)
2. **Chunking + embedding** — transcript is split into chunks and embedded into pgvector for semantic search
3. **Retrieval** — when a user asks a question, relevant chunks are retrieved by vector similarity + lexical scoring
4. **LLM answer** — retrieved chunks are sent to the configured LLM backend as context, and the answer is streamed back to the user in Telegram

The analysis can run on:
- A **VPS server** using a local Ollama model (no API cost)
- The **browser** on the client side
- **Claude or OpenAI API** for higher quality results

### Supported LLM backends

| Backend | Provider | Default model |
|---|---|---|
| `local` | Ollama (self-hosted) | `llama3.2:3b` |
| `openai` | OpenAI API | `gpt-4.1-mini` |
| `claude` / `anthropic` | Anthropic API | `claude-3-5-sonnet-latest` |
| `auto` | tries local → Claude → OpenAI | — |

Set `VIDEO_QA_BACKEND` (for Q&A) and `VIDEO_AI_BACKEND` (for summaries/notes) in `.env`.

### Connecting a remote LLM via API

**OpenAI (or any OpenAI-compatible API):**
```env
VIDEO_QA_BACKEND=openai
VIDEO_QA_MODEL=gpt-4.1-mini
OPENAI_API_KEY=sk-...
```

**Anthropic Claude:**
```env
VIDEO_QA_BACKEND=claude
VIDEO_CLAUDE_MODEL=claude-3-5-sonnet-latest
ANTHROPIC_API_KEY=sk-ant-...
```

**Local Ollama (default, no API key needed):**
```env
VIDEO_QA_BACKEND=local
VIDEO_LOCAL_LLM_URL=http://127.0.0.1:11434
VIDEO_LOCAL_LLM_MODEL=llama3.2:3b
```

**Remote Ollama** (Ollama running on another machine):
```env
VIDEO_QA_BACKEND=local
VIDEO_LOCAL_LLM_URL=http://192.168.1.50:11434
VIDEO_LOCAL_LLM_MODEL=llama3.2:3b
```

### Embeddings

Used for semantic search over transcript chunks. Requires pgvector in PostgreSQL.

| Backend | Default model | Dimension |
|---|---|---|
| `openai` (default if key set) | `text-embedding-3-small` | 1536 |
| `ollama` | `nomic-embed-text` | 768 |

```env
VIDEO_EMBED_BACKEND=openai          # or: ollama, auto
VIDEO_EMBED_MODEL=text-embedding-3-small
VIDEO_LOCAL_EMBED_MODEL=nomic-embed-text
VIDEO_EMBED_DIM=1536                # must match model output dim
```

### Full LLM config reference

| Variable | Default | Description |
|---|---|---|
| `VIDEO_QA_BACKEND` | `local` | Backend for Q&A: `local`, `openai`, `claude`, `auto` |
| `VIDEO_AI_BACKEND` | `local` | Backend for summaries/notes |
| `VIDEO_LOCAL_LLM_URL` | `http://127.0.0.1:11434` | Ollama endpoint (local or remote) |
| `VIDEO_LOCAL_LLM_MODEL` | `llama3.2:3b` | Ollama model name |
| `VIDEO_LOCAL_LLM_KEEP_ALIVE` | `30m` | Keep model loaded between requests |
| `VIDEO_QA_MODEL` | `gpt-4.1-mini` | OpenAI model for Q&A |
| `VIDEO_AI_MODEL` | `gpt-4.1-mini` | OpenAI model for summaries |
| `VIDEO_CLAUDE_MODEL` | `claude-3-5-sonnet-latest` | Claude model |
| `VIDEO_CLAUDE_RPM` | `5` | Claude requests per minute (rate limit) |
| `VIDEO_QA_QUERY_PLANNER` | `0` | Enable LLM query planner for better retrieval |
| `VIDEO_QA_LLM_RERANK` | `0` | Enable LLM reranking of retrieved chunks |
| `VIDEO_QA_RETRIES` | `1` | Retries on LLM failure |
| `OPENAI_API_KEY` | — | Required for OpenAI backend |
| `ANTHROPIC_API_KEY` | — | Required for Claude backend |
| `HF_TOKEN` | — | Optional HuggingFace token |

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

A browser instance runs on the VPS with an active YouTube session. The system reads cookies directly from the Chrome profile on disk approximately every 5 hours and securely injects the updated cookies into both the API layer and the bot — keeping automation running without manual intervention. No manual export is needed, but **Chrome must be open and logged into YouTube**.

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
