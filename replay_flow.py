from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional

from ytbot_config import FULL_REPLAY_RETRY_INTERVAL_SEC, FULL_REPLAY_RETRY_MINUTES, STORAGE_DIR
from ytbot_state import STATE, load_index, save_index
from ytbot_utils import (
    any_existing_file_for_video,
    build_public_url,
    ensure_public_filename,
    is_live_like,
    looks_like_private_unavailable,
    looks_like_vps_block,
    make_saved_full_filename,
    now_local_str,
    strip_ansi,
    with_tg_time,
)
from ytbot_ytdlp import yt_info, ytdlp_download_with_progress


async def schedule_full_replay_attempt(
    app,
    *,
    url: str,
    video_id: str,
    title: str,
    started_by_chat_id: int,
    date_key: Optional[str] = None,
    service_label: Optional[str] = None,
):
    async with STATE.replay_tasks_lock:
        if video_id in STATE.replay_tasks:
            return
        STATE.replay_tasks.add(video_id)

    async def _runner():
        try:
            await try_download_full_replay(
                app,
                url=url,
                video_id=video_id,
                title=title,
                started_by_chat_id=started_by_chat_id,
                date_key=date_key,
                service_label=service_label,
            )
        finally:
            async with STATE.replay_tasks_lock:
                STATE.replay_tasks.discard(video_id)

    app.create_task(_runner())


async def try_download_full_replay(
    app,
    *,
    url: str,
    video_id: str,
    title: str,
    started_by_chat_id: int,
    date_key: Optional[str],
    service_label: Optional[str],
):
    await asyncio.sleep(10)

    deadline = time.time() + (FULL_REPLAY_RETRY_MINUTES * 60)
    last_private = False

    while time.time() <= deadline:
        try:
            info = await asyncio.to_thread(yt_info, url)
        except Exception as e:
            emsg = strip_ansi(str(e))
            low = emsg.lower()

            if looks_like_private_unavailable(low) or ("private" in low and "unavailable" in low):
                last_private = True
                await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
                continue

            if looks_like_vps_block(low) or "no video formats found" in low:
                try:
                    await app.bot.send_message(
                        chat_id=started_by_chat_id,
                        text=with_tg_time(
                            "⚠️ Could not download FULL replay because YouTube is blocking this server.\n"
                            "Your LIVE part is kept.\n"
                            "Fix: residential proxy/VPN + fresh cookies."
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
                return

            await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
            continue

        if is_live_like(info):
            await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
            continue

        full_name = make_saved_full_filename(title, video_id)
        full_out_template = str(STORAGE_DIR / full_name).replace(".mp4", ".%(ext)s")

        async def progress_cb(kind, pct, speed, eta, raw):
            return

        try:
            final_path = await ytdlp_download_with_progress(
                url,
                video_id,
                full_out_template,
                progress_cb,
                is_live=False,
                extra_args=[],
            )
        except Exception as e:
            reason = strip_ansi(str(e))
            low = reason.lower()

            if looks_like_private_unavailable(low) or "private" in low:
                last_private = True
                await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
                continue

            if looks_like_vps_block(low) or "no video formats found" in low:
                try:
                    await app.bot.send_message(
                        chat_id=started_by_chat_id,
                        text=with_tg_time(
                            "⚠️ Could not download FULL replay because YouTube is blocking this server.\n"
                            "Your LIVE part is kept.\n"
                            "Fix: residential proxy/VPN + fresh cookies."
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
                return

            await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
            continue

        if not final_path:
            final_path = any_existing_file_for_video(video_id)

        if not final_path:
            await asyncio.sleep(FULL_REPLAY_RETRY_INTERVAL_SEC)
            continue

        filename = Path(final_path).name
        public_name = ensure_public_filename(video_id, filename)
        link = build_public_url(public_name)

        idx = load_index()
        rec = idx.get(video_id) or {}

        if date_key and not rec.get("date_key"):
            rec["date_key"] = date_key
        if service_label and not rec.get("service_label"):
            rec["service_label"] = service_label

        rec["full_filename"] = filename
        rec["full_public_url"] = link
        rec["updated_at_local"] = now_local_str()
        rec["full_saved_variant"] = "full"

        if rec.get("status") in (None, "", "failed"):
            rec["status"] = "saved"

        rec["title"] = rec.get("title") or title

        idx[video_id] = rec
        save_index(idx)

        try:
            await app.bot.send_message(
                chat_id=started_by_chat_id,
                text=with_tg_time(
                    "✅ FULL replay saved separately (no merge).\n"
                    f"🎬 {title}\n"
                    + (
                        f"📅 {rec.get('date_key')} - {rec.get('service_label')}\n"
                        if rec.get("date_key") and rec.get("service_label")
                        else ""
                    )
                    + f"🔗 {link}"
                ),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        return

    msg = (
        f"ℹ️ FULL replay is still private/unavailable after {FULL_REPLAY_RETRY_MINUTES} minutes.\n"
        "I kept the recorded part."
        if last_private
        else f"ℹ️ Could not get FULL replay within {FULL_REPLAY_RETRY_MINUTES} minutes.\n"
        "I kept the recorded part."
    )

    try:
        await app.bot.send_message(chat_id=started_by_chat_id, text=with_tg_time(msg), disable_web_page_preview=True)
    except Exception:
        pass
