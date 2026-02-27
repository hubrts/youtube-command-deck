from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ytbot_config import LOCAL_TIME_LABEL, LOCAL_TZ, PUBLIC_URL_BASE, SESSION_SPLIT_HOUR, STORAGE_DIR
from ytbot_state import load_index

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
YOUTUBE_URL_RE = re.compile(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s<>()]+", re.IGNORECASE)
SERVICE_SLOT_1 = "slot_1"
SERVICE_SLOT_2 = "slot_2"
SERVICE_LABELS = {
    SERVICE_SLOT_1: "Session 1",
    SERVICE_SLOT_2: "Session 2",
}


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s or "").strip()


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def tg_stamp() -> str:
    return now_local().strftime("%Y-%m-%d %I:%M:%S %p")


def now_local_str() -> str:
    return tg_stamp()


def with_tg_time(text: str) -> str:
    stamp = tg_stamp()
    if LOCAL_TIME_LABEL:
        return f"🕒 {stamp} ({LOCAL_TIME_LABEL})\n{text}"
    return f"🕒 {stamp}\n{text}"


def sanitize_filename(name: str, max_len: int = 140) -> str:
    name = (name or "").strip()
    name = name.replace(".", "_")
    name = re.sub(r"[^\w\s\-\(\)\[\],'’«»А-Яа-яЁёІіЇїЄє]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].strip()
    return name or "video"


def make_saved_partial_filename(title: str, video_id: str) -> str:
    safe_title = sanitize_filename(title)
    return f"{safe_title} [{video_id}] (partial).mp4"


def make_saved_full_filename(title: str, video_id: str) -> str:
    safe_title = sanitize_filename(title)
    return f"{safe_title} [{video_id}] (full).mp4"


def is_youtube_url(text: str) -> bool:
    t = (text or "").strip()
    return "youtube.com" in t or "youtu.be" in t


def extract_first_youtube_url(text: str) -> Optional[str]:
    m = YOUTUBE_URL_RE.search(text or "")
    if not m:
        return None
    return m.group(0).rstrip(".,;:!?)]}>'\"")


def extract_youtube_id(url: str) -> Optional[str]:
    u = (url or "").strip()

    m = re.search(r"youtu\.be/([A-Za-z0-9_\-]{6,})", u)
    if m:
        return m.group(1)

    m = re.search(r"[?&]v=([A-Za-z0-9_\-]{6,})", u)
    if m:
        return m.group(1)

    m = re.search(r"youtube\.com/live/([A-Za-z0-9_\-]{6,})", u)
    if m:
        return m.group(1)

    m = re.search(r"youtube\.com/shorts/([A-Za-z0-9_\-]{6,})", u)
    if m:
        return m.group(1)

    return None


def build_public_url(filename: str) -> str:
    return f"{PUBLIC_URL_BASE}/{quote(filename)}"


def ensure_public_filename(video_id: str, filename: str) -> str:
    safe_video_id = re.sub(r"[^A-Za-z0-9_-]+", "", (video_id or "").strip())
    if not safe_video_id:
        return filename

    ext = (Path(filename).suffix or ".mp4").lower()
    alias_name = f"{safe_video_id}{ext}"

    src = STORAGE_DIR / filename
    dst = STORAGE_DIR / alias_name
    if src.name == dst.name:
        return src.name
    if not src.exists():
        return filename

    try:
        if dst.exists():
            return dst.name
        try:
            os.symlink(src.name, dst)
        except Exception:
            os.link(src, dst)
        return dst.name
    except Exception:
        return filename


def classify_service_by_start(start_local: datetime) -> Tuple[str, str]:
    if start_local.hour >= SESSION_SPLIT_HOUR:
        return (SERVICE_SLOT_2, SERVICE_LABELS[SERVICE_SLOT_2])
    return (SERVICE_SLOT_1, SERVICE_LABELS[SERVICE_SLOT_1])


def normalize_service_key_label(
    service_key: str,
    service_label: str,
    *,
    started_local: Optional[datetime] = None,
) -> Tuple[str, str]:
    key_raw = (service_key or "").strip().lower()
    label_raw = (service_label or "").strip().lower()

    key_aliases = {
        "morning": SERVICE_SLOT_1,
        "ранкове": SERVICE_SLOT_1,
        "evening": SERVICE_SLOT_2,
        "вечірнє": SERVICE_SLOT_2,
        "slot1": SERVICE_SLOT_1,
        "slot_1": SERVICE_SLOT_1,
        "session1": SERVICE_SLOT_1,
        "session_1": SERVICE_SLOT_1,
        "slot2": SERVICE_SLOT_2,
        "slot_2": SERVICE_SLOT_2,
        "session2": SERVICE_SLOT_2,
        "session_2": SERVICE_SLOT_2,
    }
    key = key_aliases.get(key_raw, "")
    if not key:
        if "morning" in label_raw or "ранков" in label_raw or "session 1" in label_raw:
            key = SERVICE_SLOT_1
        elif "evening" in label_raw or "вечір" in label_raw or "session 2" in label_raw:
            key = SERVICE_SLOT_2

    if not key and started_local is not None:
        key, _ = classify_service_by_start(started_local)

    if not key:
        return "", ""
    return key, SERVICE_LABELS[key]


def safe_dt_from_ts(ts: Optional[int]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        return None


def pick_live_start(info: dict) -> Optional[datetime]:
    for k in ("live_start_timestamp", "release_timestamp", "timestamp"):
        dt = safe_dt_from_ts(info.get(k))
        if dt:
            return dt
    return None


def live_status(info: dict) -> str:
    return (info.get("live_status") or "").lower().strip()


def is_live_like(info: dict) -> bool:
    ls = live_status(info)
    if ls in ("is_live", "live", "is_upcoming", "was_live", "post_live"):
        return True
    if info.get("is_live") is True:
        return True
    return False


def is_upcoming(info: dict) -> bool:
    return live_status(info) == "is_upcoming"


def looks_like_live_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return "youtube.com/live/" in u


def looks_like_vps_block(err_low: str) -> bool:
    low = (err_low or "").lower()
    return ("confirm you're not a bot" in low) or ("confirm you’re not a bot" in low)


def looks_like_private_unavailable(err_low: str) -> bool:
    low = (err_low or "").lower()
    return ("video unavailable" in low and "private" in low) or ("this video is private" in low)


def fmt_local_time(dt: datetime) -> str:
    return dt.astimezone(LOCAL_TZ).strftime("%I:%M %p")


def build_archive_maps() -> Tuple[List[str], Dict[Tuple[str, str], List[dict]]]:
    index = load_index()
    items_by_date_service: Dict[Tuple[str, str], List[dict]] = {}
    dates = set()

    for vid, rec in index.items():
        try:
            if rec.get("status") not in ("saved", "partial", "recording"):
                continue
            rec = dict(rec)
            started_local = (rec.get("started_local") or "").strip()
            started_utc = (rec.get("started_utc") or "").strip()
            date_key = (rec.get("date_key") or "").strip()
            service_key_raw = (rec.get("service_key") or "").strip()
            service_label_raw = (rec.get("service_label") or "").strip()
            service_key = service_key_raw
            service_label = service_label_raw

            # Normalize old/broken records where date_key/service_key were swapped or localized.
            if started_local:
                try:
                    dt = datetime.fromisoformat(started_local)
                except Exception:
                    dt = None
                if dt:
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_key):
                        date_key = dt.strftime("%Y-%m-%d")
                    sk, sl = normalize_service_key_label(service_key_raw, service_label_raw, started_local=dt)
                    if sk:
                        service_key, service_label = sk, sl
            elif started_utc:
                try:
                    dt_utc = datetime.fromisoformat(started_utc)
                except Exception:
                    dt_utc = None
                if dt_utc:
                    dt = dt_utc.astimezone(LOCAL_TZ)
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_key):
                        date_key = dt.strftime("%Y-%m-%d")
                    sk, sl = normalize_service_key_label(service_key_raw, service_label_raw, started_local=dt)
                    if sk:
                        service_key, service_label = sk, sl
            else:
                sk, sl = normalize_service_key_label(service_key_raw, service_label_raw)
                if sk:
                    service_key, service_label = sk, sl

            rec["date_key"] = date_key
            rec["service_key"] = service_key
            if service_label:
                rec["service_label"] = service_label
            if not date_key or not service_key:
                continue
            rec["video_id"] = vid
            # Old records may store stale Unicode URLs. Rebuild a stable URL from file names when possible.
            base_name = (
                rec.get("full_filename")
                or rec.get("filename")
                or ""
            ).strip()
            if base_name:
                public_name = ensure_public_filename(vid, base_name)
                rec["public_url"] = build_public_url(public_name)
            elif rec.get("full_public_url"):
                rec["public_url"] = rec.get("full_public_url")
            dates.add(date_key)
            items_by_date_service.setdefault((date_key, service_key), []).append(rec)
        except Exception:
            continue

    return sorted(list(dates), reverse=True), items_by_date_service


def make_dates_keyboard(dates: List[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=d, callback_data=f"date:{d}")] for d in dates[:30]]
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("No archive yet", callback_data="noop")]])


def make_service_keyboard(date_key: str, items_by_date_service: Dict[Tuple[str, str], List[dict]]) -> InlineKeyboardMarkup:
    rows = []
    for service_key, label in ((SERVICE_SLOT_1, "1️⃣ Session 1"), (SERVICE_SLOT_2, "2️⃣ Session 2")):
        if (date_key, service_key) in items_by_date_service:
            rows.append([InlineKeyboardButton(label, callback_data=f"arch:{date_key}:{service_key}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="back_dates")])
    return InlineKeyboardMarkup(rows)


def make_items_keyboard(date_key: str, items: List[dict], back_callback: str = "back_dates") -> InlineKeyboardMarkup:
    rows = []
    for rec in items[:10]:
        title = rec.get("title", "Live")
        status = rec.get("status", "")
        video_id = (rec.get("video_id") or "").strip()
        btn_core = f"{title[:30]}… ({status})" if len(title) > 33 else f"{title} ({status})"
        btn = btn_core
        row = [InlineKeyboardButton(btn, url=rec.get("public_url", PUBLIC_URL_BASE))]
        if video_id and status in ("saved", "partial"):
            row.append(InlineKeyboardButton("📝 Notes", callback_data=f"note:{video_id}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=back_callback)])
    return InlineKeyboardMarkup(rows)


def newest_part_for_video(video_id: str) -> Optional[str]:
    try:
        candidates = []
        for fn in os.listdir(STORAGE_DIR):
            if video_id in fn and fn.endswith(".part"):
                candidates.append(str(STORAGE_DIR / fn))
        if not candidates:
            return None
        return max(candidates, key=lambda p: os.path.getmtime(p))
    except Exception:
        return None


def any_existing_file_for_video(video_id: str) -> Optional[str]:
    try:
        candidates = []
        for fn in os.listdir(STORAGE_DIR):
            if video_id in fn and (fn.endswith(".mp4") or fn.endswith(".webm") or fn.endswith(".mkv")):
                candidates.append(str(STORAGE_DIR / fn))
        if not candidates:
            return None
        return max(candidates, key=lambda p: os.path.getmtime(p))
    except Exception:
        return None
