# Architecture Notes

## Refactor Summary

`ytbot_state.py` now acts as a compatibility facade and delegates to modular state-store files under `src/youtube_direct_bot/state_store/`:

- `core.py`: DB bootstrap, migrations, index/chat persistence
- `embeddings.py`: transcript chunk storage + semantic embedding search
- `research.py`: research run/video/facts/topic persistence
- `qa.py`: transcript Q&A history persistence
- `runtime.py`: in-memory runtime state and live-stop controls

`telegram_handlers.py` now imports shared command constants/parsing helpers from `src/youtube_direct_bot/telegram/common.py`.

`web_app.py` now loads OpenAPI schema from `src/youtube_direct_bot/web/openapi.py`.

## Compatibility

Existing imports stay valid (`from ytbot_state import ...`) via facade re-exports.
