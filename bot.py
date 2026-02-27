#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from maintenance import cleanup_loop
from cookie_manager import assert_cookie_file_strict, ensure_cookies_ready
from telegram_handlers import (
    cb_handler,
    cmd_analyze,
    cmd_ask,
    cmd_ask_video,
    cmd_audio,
    cmd_archive,
    cmd_direct,
    cmd_direct_audio,
    cmd_juice,
    cmd_juice_job,
    cmd_juice_jobs,
    cmd_knowledge_juice,
    cmd_knowledge_juices,
    cmd_live_start,
    cmd_live_stop,
    cmd_recent,
    cmd_research,
    cmd_research_view,
    cmd_researches,
    cmd_save_transcript,
    cmd_start,
    cmd_status,
    cmd_video,
    cmd_videos,
    download_handler,
)
from ytbot_config import (
    BOT_TOKEN,
    COOKIE_AUTO_REFRESH_ON_START,
    COOKIE_MAX_AGE_HOURS,
    COOKIES_FILE,
    ENABLE_INTERNAL_CLEANUP,
    USE_BROWSER_COOKIES,
    YT_COOKIES_FROM_BROWSER,
    BROADCAST_CHAT_IDS,
    ensure_runtime_dirs,
)
from ytbot_state import STATE
from ytbot_utils import with_tg_time

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def post_init(application):
    if ENABLE_INTERNAL_CLEANUP:
        task = asyncio.create_task(cleanup_loop())
        application.bot_data["_cleanup_task"] = task
    warnings = application.bot_data.get("_cookie_startup_warnings") or []
    if warnings:
        text = "⚠️ Cookie check warnings on startup:\n" + "\n".join(f"• {w}" for w in warnings)
        targets = set(STATE.known_chats) | set(BROADCAST_CHAT_IDS)
        for chat_id in list(targets):
            try:
                await application.bot.send_message(chat_id=chat_id, text=with_tg_time(text), disable_web_page_preview=True)
            except Exception:
                pass


async def post_shutdown(application):
    task = application.bot_data.get("_cleanup_task")
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def on_error(update, context):
    logger.exception("Unhandled Telegram error: %s", context.error)


def main():
    if not BOT_TOKEN:
        raise SystemExit("YT_BOT_TOKEN is empty. Set it: export YT_BOT_TOKEN='...'\\n")

    ensure_runtime_dirs()
    cookie_mode = "browser" if USE_BROWSER_COOKIES else "file"
    print(f"Cookie mode: {cookie_mode}; cookies file: {COOKIES_FILE}")
    cookie_warnings = ensure_cookies_ready(
        Path(COOKIES_FILE),
        browser=YT_COOKIES_FROM_BROWSER,
        auto_refresh=COOKIE_AUTO_REFRESH_ON_START,
        max_age_hours=COOKIE_MAX_AGE_HOURS,
        allow_browser_refresh=USE_BROWSER_COOKIES,
    )
    for w in cookie_warnings:
        print(f"WARNING: {w}")
    try:
        assert_cookie_file_strict(Path(COOKIES_FILE))
    except RuntimeError as exc:
        raise SystemExit(f"Cookie validation failed: {exc}")

    if os.path.exists(COOKIES_FILE):
        try:
            st = os.stat(COOKIES_FILE)
            if (st.st_mode & 0o077) != 0:
                print(f"WARNING: cookies file permissions look open. Run: chmod 600 {COOKIES_FILE}")
        except Exception:
            pass
    else:
        print("WARNING: cookies file not found. Private/age-restricted videos may fail without cookies.")

    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=120.0,
        write_timeout=120.0,
        pool_timeout=30.0,
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .concurrent_updates(8)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["_cookie_startup_warnings"] = cookie_warnings

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("archive", cmd_archive))
    app.add_handler(CommandHandler("direct", cmd_direct))
    app.add_handler(CommandHandler("direct_audio", cmd_direct_audio))
    app.add_handler(CommandHandler("audio", cmd_audio))
    app.add_handler(CommandHandler("ask", cmd_ask))
    app.add_handler(CommandHandler("ask_video", cmd_ask_video))
    app.add_handler(CommandHandler("save_transcript", cmd_save_transcript))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("videos", cmd_videos))
    app.add_handler(CommandHandler("video", cmd_video))
    app.add_handler(CommandHandler("live_start", cmd_live_start))
    app.add_handler(CommandHandler("live_stop", cmd_live_stop))
    app.add_handler(CommandHandler("research", cmd_research))
    app.add_handler(CommandHandler("juice", cmd_juice))
    app.add_handler(CommandHandler("knowledge", cmd_juice))
    app.add_handler(CommandHandler("juice_jobs", cmd_juice_jobs))
    app.add_handler(CommandHandler("juice_job", cmd_juice_job))
    app.add_handler(CommandHandler("knowledge_juices", cmd_knowledge_juices))
    app.add_handler(CommandHandler("knowledge_juice", cmd_knowledge_juice))
    app.add_handler(CommandHandler("researches", cmd_researches))
    app.add_handler(CommandHandler("research_view", cmd_research_view))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("recent", cmd_recent))

    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, download_handler))
    app.add_error_handler(on_error)

    print("Bot running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
