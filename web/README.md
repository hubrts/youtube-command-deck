# YouTube Direct Web UI

This web UI exposes your existing local functionality through HTTP API and browser UI.

## Start

```bash
cd /home/illia/youtube_direct_bot
/home/illia/youtube_direct_bot/.venv/bin/python web_app.py --host 127.0.0.1 --port 8088
```

Open: `http://127.0.0.1:8088`

UI modules are separated in SPA tabs:
- `Transcript Lab`
- `Video Vault` (regular items only)
- `Archive Vault` (LIVE/archive items only)
- `Public Researches`
- `Knowledge Juice`
- `Direct Download`

Frontend implementation is JavaScript (ES modules):
- `web/js/main.js` - SPA entrypoint and page logic
- `web/js/api.js` - HTTP API client helpers
- `web/js/state.js` - shared UI state
- `web/js/utils.js` - formatting and UI helper functions
- `web/app.js` - backward-compatible loader shim for legacy references

## Restart Bot + UI After Changes

```bash
/home/illia/youtube_direct_bot/scripts/restart_bot_ui.sh
```

## Make It Public On Domain

1) Install and start web UI service (user systemd):

```bash
mkdir -p ~/.config/systemd/user
cp /home/illia/youtube_direct_bot/systemd-user/ytdl-web.service ~/.config/systemd/user/

export XDG_RUNTIME_DIR=/run/user/$(id -u)
export DBUS_SESSION_BUS_ADDRESS=unix:path=$XDG_RUNTIME_DIR/bus

systemctl --user daemon-reload
systemctl --user enable --now ytdl-web.service
systemctl --user status ytdl-web.service --no-pager
```

2) Apply nginx reverse-proxy config (root):

```bash
sudo cp /home/illia/youtube_direct_bot/nginx/ytdl-with-web-ui.conf /etc/nginx/sites-available/ytdl
sudo nginx -t
sudo systemctl reload nginx
```

After this, UI should be available at: `https://wetwilly.tech/`

## API Endpoints

- `GET /api/videos`
  - List known videos from index and transcript folder.
- `GET /api/video?video_id=<id>`
  - Return one video detail + transcript preview + saved analysis.
- `GET /api/runtime`
  - Returns runtime flags (including websocket availability/port/path).
- `GET /api/researches`
  - Lists public market-research and Knowledge Juice runs.
- `POST /api/save_transcript`
  - Body: `{"url":"https://www.youtube.com/watch?v=...","force":false}`
  - Saves transcript (captions first, audio transcription fallback).
- `POST /api/analyze`
  - Body: `{"video_id":"<id>","force":false}`
  - Runs local AI analysis with 24h cache behavior.
- `POST /api/ask`
  - Body: `{"video_id":"<id>","question":"..."}`
  - Transcript-grounded Q&A (language-aware).
- `POST /api/clear_history`
  - Body: `{"delete_files":true}`
  - Clears saved index and removes transcript/caption history for a clean state.
- `POST /api/direct_video`
  - Body: `{"url":"https://www.youtube.com/watch?v=..."}`
  - Returns temporary direct video URL from YouTube.
- `POST /api/direct_audio`
  - Body: `{"url":"https://www.youtube.com/watch?v=..."}`
  - Returns temporary direct audio URL from YouTube.
- `POST /api/knowledge_juice`
  - Body: `{"topic":"bakery","private_run":false}`
  - Runs Knowledge Juice Maker (find videos, save transcripts, compare what drives success).
- `POST /api/knowledge_juice/start`
  - Body example:
    `{"topic":"bakery","private_run":false,"max_videos":6,"max_queries":8,"per_query":8,"min_duration_sec":0,"max_duration_sec":0}`
  - Starts an async brewing job and returns `job_id`.
- `GET /api/knowledge_juices`
  - List public Knowledge Juice reports.
- `GET /api/knowledge_juice?run_id=<id>`
  - Get one Knowledge Juice report.
- `GET /api/knowledge_juice/jobs?active_only=1`
  - Lists brewing jobs with live progress snapshots.
- `GET /api/knowledge_juice/job?job_id=<id>`
  - Returns one brewing job state.

## Swagger

- UI: `GET /swagger`
- OpenAPI JSON: `GET /api/openapi.json` (also available at `GET /openapi.json`)
- Auto-generated file: `/home/illia/youtube_direct_bot/web/openapi.auto.json`
- Manual generation command:

```bash
/home/illia/youtube_direct_bot/.venv/bin/python /home/illia/youtube_direct_bot/scripts/generate_swagger.py
```

## Live Brewing (WebSocket)

- Web UI starts an additional websocket server for Knowledge Juice progress.
- Runtime info endpoint: `GET /api/runtime`
- Default websocket URL: `ws://127.0.0.1:8766/ws`

## Notes

- The UI uses the same archive index and transcript files as Telegram bot.
- For analysis/Q&A you can use `VIDEO_AI_BACKEND` and `VIDEO_QA_BACKEND` as: `local`, `openai`, `claude`, or `auto`.
- Claude requires `ANTHROPIC_API_KEY` (or `CLAUDE_API_KEY`).
- If you set backend to `claude`, the app will automatically try local Ollama fallback when Claude is unavailable/rate-limited.
- Built-in Claude throttling is available via `VIDEO_CLAUDE_ENABLE_RATE_LIMIT=1` and `VIDEO_CLAUDE_RPM=5` (free-tier friendly).
