from __future__ import annotations

import asyncio
import os
import shutil
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Optional

from telegram import Message
from telegram.ext import ContextTypes

from replay_flow import schedule_full_replay_attempt
from video_notes import run_video_notes
from ytbot_config import (
    AUTO_VIDEO_NOTES_FOR_LIVE,
    ENABLE_FULL_REPLAY_RETRY,
    LIVE_FROM_START,
    LOCAL_TZ,
    PROGRESS_EDIT_EVERY,
    RETENTION_DAYS,
    STORAGE_DIR,
    UPCOMING_POLL_SEC,
    UPCOMING_WAIT_SEC,
)
from ytbot_state import (
    ActiveLive,
    ArchiveItem,
    STATE,
    clear_live_stop_request,
    is_live_stop_requested,
    load_index,
    save_index,
)
from ytbot_utils import (
    any_existing_file_for_video,
    build_public_url,
    classify_service_by_start,
    ensure_public_filename,
    extract_youtube_id,
    is_live_like,
    looks_like_live_url,
    is_upcoming,
    looks_like_private_unavailable,
    looks_like_vps_block,
    make_saved_partial_filename,
    newest_part_for_video,
    now_local_str,
    pick_live_start,
    sanitize_filename,
    strip_ansi,
    live_status,
    with_tg_time,
)
from ytbot_ytdlp import yt_info, ytdlp_download_with_progress

BroadcastFn = Callable[[object, str], Awaitable[None]]

def _pick_youtube_live_started_utc(info: dict) -> Optional[datetime]:
    ts = info.get("live_start_timestamp")
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        return None


async def _resolve_live_started_utc(url: str, info: dict) -> datetime:
    started_utc = _pick_youtube_live_started_utc(info)
    if started_utc:
        return started_utc

    try:
        latest_info = await asyncio.to_thread(yt_info, url)
        started_utc = _pick_youtube_live_started_utc(latest_info)
        if started_utc:
            return started_utc
    except Exception:
        pass

    # Fallback only when YouTube did not provide live_start_timestamp.
    return pick_live_start(info) or datetime.now(timezone.utc)


async def wait_for_upcoming_to_start(
    url: str,
    *,
    wait_msg: Message,
    title: str,
) -> Optional[dict]:
    deadline = time.time() + UPCOMING_WAIT_SEC
    last_edit = 0.0

    while time.time() <= deadline:
        try:
            info = await asyncio.to_thread(yt_info, url)
        except Exception:
            await asyncio.sleep(UPCOMING_POLL_SEC)
            continue

        if not is_upcoming(info):
            return info

        now = time.time()
        if now - last_edit >= 10.0:
            last_edit = now
            sched = pick_live_start(info)
            sched_txt = ""
            if sched:
                sched_txt = f"\n🗓 Scheduled (local): {sched.astimezone(LOCAL_TZ).strftime('%Y-%m-%d %I:%M %p')}"
            remaining = int(max(0, deadline - time.time()))
            await wait_msg.edit_text(
                with_tg_time(
                    "⏳ LIVE is planned (upcoming). Waiting for it to start...\n"
                    f"🎬 {title}{sched_txt}\n"
                    f"⏱ Max wait: {int(UPCOMING_WAIT_SEC / 60)} min | Remaining: {int(remaining / 60)} min"
                ),
                disable_web_page_preview=True,
            )

        await asyncio.sleep(UPCOMING_POLL_SEC)

    return None


async def run_download_flow(
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
    wait_msg: Message,
    started_by_chat_id: int,
    *,
    broadcast_fn: Optional[BroadcastFn] = None,
):
    try:
        info = await asyncio.to_thread(yt_info, url)
    except Exception as e:
        emsg = strip_ansi(str(e))
        low = emsg.lower()

        if "will begin in a few moments" in low:
            wait_title = extract_youtube_id(url) or "Live"
            waited_info = await wait_for_upcoming_to_start(url, wait_msg=wait_msg, title=wait_title)
            if waited_info is None:
                await wait_msg.edit_text(
                    with_tg_time(
                        f"⌛️ Timed out. LIVE did not start within {int(UPCOMING_WAIT_SEC / 60)} minutes.\n"
                        f"🎬 {wait_title}"
                    ),
                    disable_web_page_preview=True,
                )
                return
            info = waited_info
        else:
            if looks_like_vps_block(low):
                await wait_msg.edit_text(
                    with_tg_time(
                        "⚠️ YouTube blocked the server request (anti-bot check).\n"
                        "Fix: refresh cookies and usually use a residential proxy/VPN for the server."
                    ),
                    disable_web_page_preview=True,
                )
                return

            if "no video formats found" in low:
                await wait_msg.edit_text(
                    with_tg_time(
                        "⚠️ YouTube did not provide formats to this server.\n"
                        "Fix: route yt-dlp through a residential proxy/VPN and try again."
                    ),
                    disable_web_page_preview=True,
                )
                return

            if looks_like_private_unavailable(low):
                await wait_msg.edit_text(
                    with_tg_time(
                        "🔒 This video is private.\n"
                        "The server needs valid YouTube cookies from an account that can access it."
                    ),
                    disable_web_page_preview=True,
                )
                return

            await wait_msg.edit_text(with_tg_time(f"❌ Could not read video info:\n{emsg[:1200]}"), disable_web_page_preview=True)
            return

    video_id = info.get("id") or extract_youtube_id(url) or "unknown"
    title = info.get("title") or video_id
    channel = info.get("uploader") or info.get("channel") or "Unknown"
    clear_live_stop_request(video_id)

    if is_upcoming(info):
        waited_info = await wait_for_upcoming_to_start(url, wait_msg=wait_msg, title=title)
        if waited_info is None:
            await wait_msg.edit_text(
                with_tg_time(
                    f"⌛️ Timed out. LIVE did not start within {int(UPCOMING_WAIT_SEC / 60)} minutes.\n"
                    f"🎬 {title}"
                ),
                disable_web_page_preview=True,
            )
            return
        info = waited_info

    live = is_live_like(info) or looks_like_live_url(url)
    ls = live_status(info)
    active_live_now = ls in ("is_live", "live", "is_upcoming") or bool(info.get("is_live"))
    archived_live_mode = live and looks_like_live_url(url) and not active_live_now

    if live and not archived_live_mode:
        async with STATE.state_lock:
            if video_id in STATE.active_lives:
                a = STATE.active_lives[video_id]
                mins = int((time.time() - a.started_at) / 60)
                await wait_msg.edit_text(
                    with_tg_time(
                        "🔴 This LIVE is already being recorded.\n"
                        f"🎬 {a.title}\n"
                        f"⏱ Started ~{mins} min ago.\n"
                        "I will not start a second recording."
                    ),
                    disable_web_page_preview=True,
                )
                return

    safe_title = sanitize_filename(title)
    output_template = str(STORAGE_DIR / f"{safe_title} [{video_id}].%(ext)s")

    archive_item: Optional[ArchiveItem] = None
    date_key = None
    service_key = None
    service_label = None

    if live:
        start_utc = await _resolve_live_started_utc(url, info)
        start_local = start_utc.astimezone(LOCAL_TZ)

        date_key = start_local.strftime("%Y-%m-%d")
        service_key, service_label = classify_service_by_start(start_local)

        archive_item = ArchiveItem(
            video_id=video_id,
            url=url,
            title=title,
            channel=channel,
            started_utc=start_utc.isoformat(),
            started_local=start_local.isoformat(),
            date_key=date_key,
            service_key=service_key,
            service_label=service_label,
            filename="",
            public_url="",
            status="recording",
            created_at_local=now_local_str(),
        )

        if archived_live_mode:
            await wait_msg.edit_text(
                with_tg_time(
                    "📼 Saving archived LIVE...\n"
                    f"🎬 {title}\n"
                    f"⏱ Live started (local): {start_local.strftime('%I:%M %p')}\n"
                    f"📂 Session: {service_label}"
                ),
                disable_web_page_preview=True,
            )
            idx = load_index()
            idx[video_id] = asdict(archive_item)
            save_index(idx)
        else:
            await wait_msg.edit_text(
                with_tg_time(
                    "🔴 LIVE recording started!\n"
                    f"🎬 {title}\n"
                    f"⏱ Live started (local): {start_local.strftime('%I:%M %p')}\n"
                    f"📂 Session: {service_label}\n\n"
                    + ("🧲 Trying from start (DVR)...\n" if LIVE_FROM_START else "")
                    + "I will keep recording until the stream ends."
                ),
                disable_web_page_preview=True,
            )

            if broadcast_fn:
                await broadcast_fn(
                    context.application,
                    "🔴 LIVE recording started by someone!\n"
                    f"🎬 {title}\n"
                    f"⏱ Started (local): {start_local.strftime('%I:%M %p')}\n"
                    f"📂 Session: {service_label}\n"
                    f"🔗 https://youtu.be/{video_id}",
                )

            active = ActiveLive(
                video_id=video_id,
                url=url,
                title=title,
                started_local=start_local,
                service_key=service_key,
                service_label=service_label,
                date_key=date_key,
                status_message_chat_id=wait_msg.chat_id,
                status_message_id=wait_msg.message_id,
                started_by_chat_id=started_by_chat_id,
                started_at=time.time(),
            )

            async with STATE.state_lock:
                STATE.active_lives[video_id] = active
                idx = load_index()
                idx[video_id] = asdict(archive_item)
                save_index(idx)
    else:
        await wait_msg.edit_text(with_tg_time(f"⏳ Starting download...\n🎬 {title}"), disable_web_page_preview=True)

    last_edit = 0.0

    async def progress_cb(kind, pct, speed, eta, raw):
        nonlocal last_edit
        now = time.time()
        if now - last_edit < PROGRESS_EDIT_EVERY:
            return
        last_edit = now

        try:
            if kind == "percent":
                text_out = with_tg_time(
                    f"⬇️ Downloading: {pct:.1f}%\n"
                    f"⚡ {speed}\n"
                    f"⏱ ETA: {eta}\n"
                    f"🎬 {title}"
                )
            else:
                prefix = "📼 Saving archived LIVE..." if archived_live_mode else "🔴 Recording LIVE..."
                text_out = with_tg_time(f"{prefix}\n🎬 {title}\n\n{raw[:600]}")

            await context.application.bot.edit_message_text(
                chat_id=wait_msg.chat_id,
                message_id=wait_msg.message_id,
                text=text_out,
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    extra_args = []
    if live and LIVE_FROM_START:
        extra_args.append("--live-from-start")

    try:
        final_path = await ytdlp_download_with_progress(
            url,
            video_id,
            output_template,
            progress_cb,
            is_live=live,
            extra_args=extra_args,
            should_stop_cb=(lambda: is_live_stop_requested(video_id)) if live else None,
        )
    except Exception as e:
        reason = strip_ansi(str(e))

        if live and reason == "LIVE_STOP_REQUESTED":
            part_file = newest_part_for_video(video_id)
            link = ""
            saved_name = ""
            if part_file and os.path.exists(part_file):
                partial_name = make_saved_partial_filename(title, video_id)
                partial_path = str(STORAGE_DIR / partial_name)
                try:
                    shutil.copyfile(part_file, partial_path)
                    saved_name = Path(partial_path).name
                    public_name = ensure_public_filename(video_id, saved_name)
                    link = build_public_url(public_name)
                except Exception:
                    link = ""

            idx = load_index()
            rec = idx.get(video_id) or {}
            rec["status"] = "stopped"
            if saved_name:
                rec["filename"] = saved_name
            if link:
                rec["public_url"] = link
            rec["title"] = rec.get("title") or title
            idx[video_id] = rec
            save_index(idx)

            async with STATE.state_lock:
                STATE.active_lives.pop(video_id, None)
            clear_live_stop_request(video_id)

            await wait_msg.edit_text(
                with_tg_time(
                    "🛑 LIVE recording stopped by user.\n"
                    + (f"🔗 Saved part: {link}\n" if link else "")
                    + f"🗑 Auto-delete after {RETENTION_DAYS} days."
                ),
                disable_web_page_preview=True,
            )
            return

        if live and reason in ("LIVE_STUCK_TIMEOUT", "LIVE_BECAME_PRIVATE"):
            start_utc = await _resolve_live_started_utc(url, info)
            start_local = start_utc.astimezone(LOCAL_TZ)
            date_key = start_local.strftime("%Y-%m-%d")
            service_key, service_label = classify_service_by_start(start_local)

            part_file = newest_part_for_video(video_id)
            link = ""
            saved_name = ""
            notes_local_path = ""

            if part_file and os.path.exists(part_file):
                partial_name = make_saved_partial_filename(title, video_id)
                partial_path = str(STORAGE_DIR / partial_name)

                try:
                    shutil.copyfile(part_file, partial_path)
                    notes_local_path = partial_path
                    saved_name = Path(partial_path).name
                    public_name = ensure_public_filename(video_id, saved_name)
                    link = build_public_url(public_name)
                except Exception:
                    link = ""

                idx = load_index()
                rec = idx.get(video_id) or {}
                rec["started_utc"] = start_utc.isoformat()
                rec["started_local"] = start_local.isoformat()
                rec["date_key"] = date_key
                rec["service_key"] = service_key
                rec["service_label"] = service_label
                rec["status"] = "partial"
                rec["filename"] = saved_name or rec.get("filename", "")
                rec["public_url"] = link or rec.get("public_url", "")
                rec["title"] = rec.get("title") or title
                idx[video_id] = rec
                save_index(idx)

            async with STATE.state_lock:
                STATE.active_lives.pop(video_id, None)

            await wait_msg.edit_text(
                with_tg_time(
                    ("⚠️ Archived LIVE was incomplete/locked.\n" if archived_live_mode else "⚠️ LIVE ended/locked (became private or got stuck).\n")
                    + "I saved the part that was recorded.\n"
                    + (f"🔗 {link}" if link else "✅ Partial saved on server.")
                    + f"\n🗑 Auto-delete after {RETENTION_DAYS} days."
                    + (
                        "\n\n🕵️ I will keep trying to download the FULL replay separately."
                        if (ENABLE_FULL_REPLAY_RETRY and not archived_live_mode)
                        else "\n\nℹ️ Full replay follow-up is disabled to avoid merge/corruption issues."
                    )
                ),
                disable_web_page_preview=True,
            )

            if ENABLE_FULL_REPLAY_RETRY and not archived_live_mode:
                await schedule_full_replay_attempt(
                    context.application,
                    url=url,
                    video_id=video_id,
                    title=title,
                    started_by_chat_id=started_by_chat_id,
                    date_key=date_key,
                    service_label=service_label,
                )
            if AUTO_VIDEO_NOTES_FOR_LIVE:
                context.application.create_task(
                    run_video_notes(
                        context,
                        chat_id=started_by_chat_id,
                        url=url,
                        title_hint=title,
                        video_id=video_id,
                        local_video_path=notes_local_path,
                        note_scope="archived LIVE" if archived_live_mode else "LIVE",
                    )
                )
            return

        low = reason.lower()
        if live:
            async with STATE.state_lock:
                STATE.active_lives.pop(video_id, None)
            idx = load_index()
            rec = idx.get(video_id) or {}
            rec["status"] = "failed"
            idx[video_id] = rec
            save_index(idx)

        if looks_like_vps_block(low) or "no video formats found" in low:
            await wait_msg.edit_text(
                with_tg_time(
                    "❌ Download failed due to YouTube blocking this server.\n"
                    "Fix: use a residential proxy/VPN for yt-dlp on the server."
                ),
                disable_web_page_preview=True,
            )
            return

        if looks_like_private_unavailable(low):
            await wait_msg.edit_text(
                with_tg_time(
                    "🔒 This video is private.\n"
                    "The server needs valid cookies from an account that can access it."
                ),
                disable_web_page_preview=True,
            )
            return

        await wait_msg.edit_text(with_tg_time(f"❌ Download failed:\n{reason[:1200]}"), disable_web_page_preview=True)
        return

    if not final_path:
        final_path = any_existing_file_for_video(video_id)

    if not final_path:
        if live:
            async with STATE.state_lock:
                STATE.active_lives.pop(video_id, None)
            idx = load_index()
            rec = idx.get(video_id) or {}
            rec["status"] = "partial"
            idx[video_id] = rec
            save_index(idx)

        await wait_msg.edit_text(with_tg_time("✅ Finished, but I could not detect the output filename."), disable_web_page_preview=True)
        return

    filename = Path(final_path).name
    public_name = ensure_public_filename(video_id, filename)
    link = build_public_url(public_name)

    if live and archive_item:
        start_utc = await _resolve_live_started_utc(url, info)
        start_local = start_utc.astimezone(LOCAL_TZ)
        date_key = start_local.strftime("%Y-%m-%d")
        service_key, service_label = classify_service_by_start(start_local)
        archive_item.started_utc = start_utc.isoformat()
        archive_item.started_local = start_local.isoformat()
        archive_item.date_key = date_key
        archive_item.service_key = service_key
        archive_item.service_label = service_label

        archive_item.filename = filename
        archive_item.public_url = link
        archive_item.status = "saved"

        idx = load_index()
        idx[video_id] = asdict(archive_item)
        save_index(idx)

        async with STATE.state_lock:
            STATE.active_lives.pop(video_id, None)

        status_line = "✅ Archived LIVE saved!" if archived_live_mode else "✅ LIVE part saved!"
        followup = ""
        if not archived_live_mode:
            followup = (
                "\n\n🕵️ Now I will keep trying to save the FULL replay separately (no merge)."
                if ENABLE_FULL_REPLAY_RETRY
                else "\n\nℹ️ Full replay follow-up is disabled to keep the saved part untouched."
            )
        await wait_msg.edit_text(
            with_tg_time(
                f"{status_line}\n"
                f"🎬 {title}\n"
                f"📅 {archive_item.date_key} - {archive_item.service_label}\n"
                f"🔗 {link}\n"
                f"🗑 Auto-delete after {RETENTION_DAYS} days."
                + followup
            ),
            disable_web_page_preview=True,
        )

        if ENABLE_FULL_REPLAY_RETRY and not archived_live_mode:
            await schedule_full_replay_attempt(
                context.application,
                url=url,
                video_id=video_id,
                title=title,
                started_by_chat_id=started_by_chat_id,
                date_key=archive_item.date_key,
                service_label=archive_item.service_label,
            )
        if AUTO_VIDEO_NOTES_FOR_LIVE:
            context.application.create_task(
                run_video_notes(
                    context,
                    chat_id=started_by_chat_id,
                    url=url,
                    title_hint=title,
                    video_id=video_id,
                    local_video_path=final_path,
                    note_scope="archived LIVE" if archived_live_mode else "LIVE",
                )
            )
        return

    await wait_msg.edit_text(with_tg_time(f"✅ Done!\n📥 Download link:\n{link}"), disable_web_page_preview=True)
