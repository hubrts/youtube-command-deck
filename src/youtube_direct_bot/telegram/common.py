from __future__ import annotations

import re

from telegram import KeyboardButton, ReplyKeyboardMarkup

from ytbot_config import (
    ENABLE_FULL_REPLAY_RETRY,
    FULL_REPLAY_RETRY_INTERVAL_SEC,
    FULL_REPLAY_RETRY_MINUTES,
    MAX_HEIGHT,
    RETENTION_DAYS,
    UPCOMING_WAIT_SEC,
)
from ytbot_utils import extract_first_youtube_url, extract_youtube_id

BTN_SAVE = "📥 Save Video"
BTN_DIRECT = "🔗 Direct Link"
BTN_AUDIO = "🎵 Save Audio"
BTN_ASK = "❓ Ask Video"
BTN_RESEARCH = "🧭 Research"
BTN_KNOWLEDGE = "🧃 Knowledge Juice"
BTN_RESEARCH_LIST = "📜 Researches"
BTN_ARCHIVE = "📚 Archive"
BTN_STATUS = "📊 Status"
BTN_RECENT = "🕐 Recent Searches"
BTN_HELP = "ℹ️ Help"

YT_ID_INLINE_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")

PENDING_MODE_KEY = "pending_mode"
MODE_SAVE = "save_url"
MODE_DIRECT = "direct_url"
MODE_AUDIO = "audio_url"
MODE_ASK_URL = "ask_url"
MODE_ASK_QUESTION = "ask_question"
MODE_RESEARCH_GOAL = "research_goal"
MODE_KNOWLEDGE_GOAL = "knowledge_goal"

LAST_NOTES_CTX_KEY = "last_notes_context"


def _step_mode_prompt(title: str, instruction: str, example: str = "") -> str:
    lines = [
        f"🧭 {title}",
        "Step 2/2",
        instruction,
    ]
    if example:
        lines.append(f"Example: {example}")
    return "\n".join(lines)


def _build_help_text() -> str:
    lines = [
        "🎥 YouTube Bot Command Center",
        "",
        "Quick Start",
        "1) Tap a keyboard button",
        "2) Send a YouTube URL (or goal/topic)",
        "3) Receive links/results directly in chat",
        "",
        "Core Modes",
        f"• {BTN_SAVE} - Save video (up to {MAX_HEIGHT}p) with public link",
        f"• {BTN_DIRECT} - Temporary direct CDN link (no server storage)",
        f"• {BTN_AUDIO} - Save MP3 audio",
        f"• {BTN_ASK} - Build transcript context and ask questions",
        f"• {BTN_RESEARCH} - Run market research from your business goal",
        f"• {BTN_KNOWLEDGE} - Topic success blueprint (use --private to keep private)",
        f"• {BTN_RESEARCH_LIST} - Browse public reports",
        f"• {BTN_ARCHIVE} - Open saved LIVE archive",
        f"• {BTN_STATUS} - Show active LIVE recordings",
        "",
        "Useful Commands",
        "• /ask, /ask_video, /analyze, /save_transcript",
        "• /research, /researches, /research_view",
        "• /juice, /juice_jobs, /juice_job, /knowledge_juices, /knowledge_juice",
        "• /videos, /video, /live_start, /live_stop, /direct_audio",
        "",
        "LIVE Notes",
        "• Handles LIVE and upcoming links with one active recording per stream",
        "• Adds archive entries and 📝 Notes for LIVE items",
        f"• Upcoming wait window: {int(UPCOMING_WAIT_SEC / 60)} minutes",
    ]
    if ENABLE_FULL_REPLAY_RETRY:
        lines.append(
            f"• Full replay retries: every {FULL_REPLAY_RETRY_INTERVAL_SEC}s for up to {FULL_REPLAY_RETRY_MINUTES} minutes"
        )
    lines.extend(
        [
            "",
            "Retention",
            f"• Saved LIVE files are auto-deleted after {RETENTION_DAYS} days",
        ]
    )
    return "\n".join(lines)


HELP_TEXT = _build_help_text()


def _resolve_video_ref(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    url = extract_first_youtube_url(raw)
    if url:
        return (extract_youtube_id(url) or "").strip(), url
    if YT_ID_INLINE_RE.match(raw):
        return raw, ""
    return "", ""


def _parse_force_flag(args: list[str]) -> tuple[bool, list[str]]:
    force = False
    out: list[str] = []
    for x in args:
        v = str(x or "").strip()
        if not v:
            continue
        if v.lower() in ("--force", "-f", "force"):
            force = True
            continue
        out.append(v)
    return force, out


def _parse_bool_value(raw: str, default: bool) -> bool:
    txt = str(raw or "").strip().lower()
    if txt in ("1", "true", "yes", "on", "y"):
        return True
    if txt in ("0", "false", "no", "off", "n"):
        return False
    return default


def _parse_research_goal_and_privacy(text: str, private_hint: bool = False) -> tuple[str, bool]:
    raw = re.sub(r"\s+", " ", (text or "").strip())
    if not raw:
        return "", private_hint
    is_private = bool(private_hint)
    low = raw.lower()
    if low.startswith("--private "):
        is_private = True
        raw = raw[len("--private ") :].strip()
    elif low == "--private":
        is_private = True
        raw = ""
    elif low.startswith("private:"):
        is_private = True
        raw = raw.split(":", 1)[1].strip()
    elif low.startswith("public:"):
        is_private = False
        raw = raw.split(":", 1)[1].strip()
    return raw, is_private


def _parse_juice_start_args(args: list[str]) -> tuple[str, bool, dict]:
    private_run = False
    config: dict = {}
    topic_parts: list[str] = []

    int_keys = {
        "--max-videos": "max_videos",
        "--max-queries": "max_queries",
        "--per-query": "per_query",
        "--min-duration": "min_duration_sec",
        "--max-duration": "max_duration_sec",
    }

    i = 0
    while i < len(args):
        token = str(args[i] or "").strip()
        low = token.lower()
        if not token:
            i += 1
            continue

        if low in ("--private", "-p", "private"):
            private_run = True
            i += 1
            continue
        if low in ("--public", "public"):
            private_run = False
            i += 1
            continue

        if low in ("--captions-only", "--fast"):
            config["captions_only"] = True
            i += 1
            continue
        if low in ("--no-captions-only", "--captions-off", "--slow"):
            config["captions_only"] = False
            i += 1
            continue

        if low.startswith("--captions-only="):
            config["captions_only"] = _parse_bool_value(token.split("=", 1)[1], True)
            i += 1
            continue
        if low.startswith("--fast="):
            config["captions_only"] = _parse_bool_value(token.split("=", 1)[1], True)
            i += 1
            continue

        if low in int_keys:
            if i + 1 < len(args):
                try:
                    config[int_keys[low]] = int(str(args[i + 1] or "").strip())
                except Exception:
                    pass
                i += 2
                continue
            i += 1
            continue

        matched_int = False
        for key, cfg_key in int_keys.items():
            if low.startswith(f"{key}="):
                try:
                    config[cfg_key] = int(token.split("=", 1)[1].strip())
                except Exception:
                    pass
                matched_int = True
                break
        if matched_int:
            i += 1
            continue

        topic_parts.append(token)
        i += 1

    topic_raw = " ".join(topic_parts).strip()
    topic, parsed_private = _parse_research_goal_and_privacy(topic_raw, private_hint=private_run)
    return topic, parsed_private, config


def _main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_SAVE), KeyboardButton(BTN_DIRECT)],
            [KeyboardButton(BTN_AUDIO), KeyboardButton(BTN_ASK)],
            [KeyboardButton(BTN_RESEARCH), KeyboardButton(BTN_KNOWLEDGE)],
            [KeyboardButton(BTN_RESEARCH_LIST), KeyboardButton(BTN_RECENT)],
            [KeyboardButton(BTN_ARCHIVE), KeyboardButton(BTN_STATUS)],
            [KeyboardButton(BTN_HELP)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        one_time_keyboard=False,
    )
