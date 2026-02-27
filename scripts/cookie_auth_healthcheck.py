#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from cookie_manager import strict_cookie_errors
from ytbot_config import BOT_TOKEN, BROADCAST_CHAT_IDS, COOKIES_FILE, DATA_DIR, YTDLP_PROXY
from ytbot_state import load_known_chats


HEALTH_STATE_FILE = DATA_DIR / "cookie_health_state.json"
ALERT_REPEAT_HOURS = int((os.environ.get("COOKIE_HEALTH_ALERT_REPEAT_HOURS") or "6").strip())
MAX_COOKIE_AGE_HOURS = int((os.environ.get("COOKIE_MAX_AGE_HOURS") or "6").strip())


def _now_ts() -> int:
    return int(time.time())


def _load_state() -> dict:
    try:
        return json.loads(HEALTH_STATE_FILE.read_text("utf-8"))
    except Exception:
        return {"status": "unknown", "last_alert_ts": 0}


def _save_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(HEALTH_STATE_FILE) + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(HEALTH_STATE_FILE)


def _probe_youtube_auth(cookies_file: str) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--ignore-no-formats-error",
        "--cookies",
        cookies_file,
        "--skip-download",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    ]
    if YTDLP_PROXY:
        cmd[3:3] = ["--proxy", YTDLP_PROXY]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if p.returncode == 0:
        return True, "yt-dlp probe ok"
    msg = (p.stderr or p.stdout or "yt-dlp probe failed").strip()
    return False, msg[-1200:]


def _target_chats() -> Set[int]:
    return set(load_known_chats()) | set(BROADCAST_CHAT_IDS)


def _send_telegram(text: str) -> None:
    if not BOT_TOKEN:
        print("healthcheck: BOT_TOKEN missing; cannot send Telegram alert")
        return
    targets = _target_chats()
    if not targets:
        print("healthcheck: no known/broadcast chats; cannot send Telegram alert")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in sorted(targets):
        try:
            payload = urlencode({"chat_id": str(chat_id), "text": text, "disable_web_page_preview": "true"}).encode("utf-8")
            req = Request(url, data=payload, method="POST")
            with urlopen(req, timeout=20):
                pass
        except Exception as e:
            print(f"healthcheck: alert send failed for chat {chat_id}: {e}")


def main() -> int:
    cookies_path = Path(COOKIES_FILE)
    reasons: list[str] = strict_cookie_errors(
        cookies_path, max_age_hours=MAX_COOKIE_AGE_HOURS
    )

    if not reasons:
        ok, probe_msg = _probe_youtube_auth(str(cookies_path))
        if not ok:
            reasons.append(probe_msg)

    failed = len(reasons) > 0
    now_ts = _now_ts()
    state = _load_state()
    prev_status = state.get("status", "unknown")
    last_alert_ts = int(state.get("last_alert_ts", 0) or 0)
    repeat_sec = max(1, ALERT_REPEAT_HOURS) * 3600

    should_alert_fail = failed and (
        prev_status != "failed" or (now_ts - last_alert_ts) >= repeat_sec
    )
    should_alert_recovered = (not failed) and prev_status == "failed"

    if should_alert_fail:
        body = "\n".join(f"- {r}" for r in reasons[:6])
        _send_telegram("ALERT: cookie/auth healthcheck failed\n" + body)
        state["last_alert_ts"] = now_ts
    elif should_alert_recovered:
        _send_telegram("RECOVERED: cookie/auth healthcheck is healthy again")
        state["last_alert_ts"] = now_ts

    state["status"] = "failed" if failed else "ok"
    state["last_run_ts"] = now_ts
    state["last_error"] = "\n".join(reasons[:6]) if failed else ""
    _save_state(state)

    if failed:
        print("healthcheck failed:")
        for r in reasons:
            print(f"- {r}")
        return 1

    print("healthcheck ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
