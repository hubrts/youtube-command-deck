from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent

BOT_TOKEN = (os.environ.get("YT_BOT_TOKEN") or os.environ.get("BOT_TOKEN") or "").strip()

STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "/var/www/wetwilly.tech/youtube-direct-bot-downloads").strip())
PUBLIC_URL_BASE = os.environ.get("PUBLIC_URL_BASE", "https://wetwilly.tech/youtube-direct-bot-downloads").strip().rstrip("/")

COOKIES_FILE = os.environ.get("COOKIES_FILE", str(BASE_DIR / "cookies.txt")).strip()
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR / "data")).strip()).expanduser()

YTDLP_PROXY = (os.environ.get("YTDLP_PROXY") or "").strip() or None
USE_BROWSER_COOKIES = os.environ.get("USE_BROWSER_COOKIES", "0").strip() == "1"
YT_COOKIES_FROM_BROWSER = (os.environ.get("YT_COOKIES_FROM_BROWSER") or "").strip()
COOKIE_AUTO_REFRESH_ON_START = os.environ.get("COOKIE_AUTO_REFRESH_ON_START", "1").strip() == "1"
COOKIE_MAX_AGE_HOURS = int(os.environ.get("COOKIE_MAX_AGE_HOURS", "6"))

LOCAL_TZ_NAME = (os.environ.get("LOCAL_TZ_NAME") or os.environ.get("LOCAL_TZ") or "America/New_York").strip()
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)
LOCAL_TIME_LABEL = (os.environ.get("LOCAL_TIME_LABEL") or "Local time").strip()
SESSION_SPLIT_HOUR = int(os.environ.get("SESSION_SPLIT_HOUR", "17"))

RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "60"))
MAX_HEIGHT = int(os.environ.get("MAX_HEIGHT", "1080"))
PROGRESS_EDIT_EVERY = 3.0

LIVE_STUCK_TIMEOUT_SEC = int(os.environ.get("LIVE_STUCK_TIMEOUT_SEC", "300"))
LIVE_FROM_START = os.environ.get("LIVE_FROM_START", "1").strip() == "1"

UPCOMING_WAIT_SEC = int(os.environ.get("UPCOMING_WAIT_SEC", "3600"))
UPCOMING_POLL_SEC = int(os.environ.get("UPCOMING_POLL_SEC", "15"))

FULL_REPLAY_RETRY_MINUTES = int(os.environ.get("FULL_REPLAY_RETRY_MINUTES", "360"))
FULL_REPLAY_RETRY_INTERVAL_SEC = int(os.environ.get("FULL_REPLAY_RETRY_INTERVAL_SEC", "60"))
ENABLE_FULL_REPLAY_RETRY = os.environ.get("ENABLE_FULL_REPLAY_RETRY", "0").strip() == "1"
ENABLE_INTERNAL_CLEANUP = os.environ.get("ENABLE_INTERNAL_CLEANUP", "0").strip() == "1"
AUTO_VIDEO_NOTES_FOR_LIVE = os.environ.get("AUTO_VIDEO_NOTES_FOR_LIVE", "1").strip() == "1"

ADMIN_ONLY_START = os.environ.get("ADMIN_ONLY_START", "0").strip() == "1"
ADMIN_CHAT_IDS = set(
    int(x) for x in os.environ.get("ADMIN_CHAT_IDS", "").split(",") if x.strip().isdigit()
)


def _parse_int_set(raw: str) -> set[int]:
    out: set[int] = set()
    for p in (raw or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            pass
    return out


BROADCAST_CHAT_IDS = _parse_int_set(os.environ.get("BROADCAST_CHAT_IDS", ""))
TG_TARGET_CHAT_ID = int((os.environ.get("TG_TARGET_CHAT_ID") or "0").strip() or 0)

CHATS_FILE = DATA_DIR / "known_chats.json"
INDEX_FILE = DATA_DIR / "archive_index.json"
STATE_DB_DSN = (
    os.environ.get("STATE_DB_DSN")
    or os.environ.get("DATABASE_URL")
    or ""
).strip()
STATE_DB_REQUIRE_PGVECTOR = os.environ.get("STATE_DB_REQUIRE_PGVECTOR", "1").strip() == "1"
VIDEO_EMBED_DIM = int((os.environ.get("VIDEO_EMBED_DIM") or "1536").strip() or 1536)


def ensure_runtime_dirs() -> None:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
