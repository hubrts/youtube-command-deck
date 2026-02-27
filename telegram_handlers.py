from __future__ import annotations

import asyncio
import os
from urllib.parse import unquote

from telegram import Update
from telegram.ext import ContextTypes

from download_flow import run_download_flow
from market_research import run_market_research
from video_notes import answer_question_from_transcript, run_video_notes
from ytbot_ytdlp import yt_download_audio_with_path
from ytbot_config import (
    ADMIN_CHAT_IDS,
    ADMIN_ONLY_START,
    BROADCAST_CHAT_IDS,
    STORAGE_DIR,
)
from ytbot_state import STATE, save_known_chats
from ytbot_state import get_public_research_run, load_index, load_public_research_runs, load_recent_searches, save_transcript_qa_entry
from src.youtube_direct_bot.telegram.common import (
    BTN_ARCHIVE,
    BTN_ASK,
    BTN_AUDIO,
    BTN_DIRECT,
    BTN_HELP,
    BTN_KNOWLEDGE,
    BTN_RECENT,
    BTN_RESEARCH,
    BTN_RESEARCH_LIST,
    BTN_SAVE,
    BTN_STATUS,
    HELP_TEXT,
    LAST_NOTES_CTX_KEY,
    MODE_ASK_QUESTION,
    MODE_ASK_URL,
    MODE_AUDIO,
    MODE_DIRECT,
    MODE_KNOWLEDGE_GOAL,
    MODE_RESEARCH_GOAL,
    MODE_SAVE,
    PENDING_MODE_KEY,
    _main_keyboard,
    _parse_force_flag,
    _parse_juice_start_args,
    _parse_research_goal_and_privacy,
    _resolve_video_ref,
    _step_mode_prompt,
)
from ytbot_utils import (
    build_archive_maps,
    extract_first_youtube_url,
    extract_youtube_id,
    fmt_local_time,
    normalize_service_key_label,
    make_dates_keyboard,
    make_items_keyboard,
    with_tg_time,
)


def _web_app_module():
    import web_app

    return web_app


async def track_chat(update: Update) -> None:
    chat = update.effective_chat
    if not chat:
        return
    if chat.id not in STATE.known_chats:
        STATE.known_chats.add(chat.id)
        save_known_chats(STATE.known_chats)


async def broadcast(app, text: str) -> None:
    targets = set(STATE.known_chats) | set(BROADCAST_CHAT_IDS)
    for chat_id in list(targets):
        try:
            await app.bot.send_message(chat_id=chat_id, text=with_tg_time(text), disable_web_page_preview=True)
        except Exception:
            pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    context.chat_data.pop(PENDING_MODE_KEY, None)
    await update.effective_message.reply_text(
        with_tg_time(HELP_TEXT),
        disable_web_page_preview=True,
        reply_markup=_main_keyboard(),
    )


async def cmd_archive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    context.chat_data.pop(PENDING_MODE_KEY, None)
    dates, _map = build_archive_maps()
    await update.effective_message.reply_text(
        with_tg_time("📚 Archive by date:"),
        reply_markup=make_dates_keyboard(dates),
    )


async def cmd_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    text = ""
    if context.args:
        text = " ".join(context.args).strip()
    elif getattr(msg, "text", None):
        parts = (msg.text or "").split(maxsplit=1)
        text = parts[1].strip() if len(parts) > 1 else ""

    url = extract_first_youtube_url(text)
    if not url:
        context.chat_data[PENDING_MODE_KEY] = MODE_DIRECT
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Direct Link Mode",
                    "Send a YouTube URL for direct link mode.",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    context.chat_data.pop(PENDING_MODE_KEY, None)
    await _run_direct_url_mode(msg, url)


async def cmd_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    text = " ".join(context.args).strip() if context.args else ""
    url = extract_first_youtube_url(text)
    if not url:
        context.chat_data[PENDING_MODE_KEY] = MODE_AUDIO
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Audio Mode",
                    "Send a YouTube URL to save audio (MP3).",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    context.chat_data.pop(PENDING_MODE_KEY, None)
    await _run_audio_url_mode(context, msg, url)


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    raw = " ".join(context.args).strip() if context.args else ""
    if not raw:
        ctx = _get_last_notes_context(context)
        if (ctx.get("transcript_path") or "").strip():
            context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
            await msg.reply_text(
                with_tg_time(_step_mode_prompt("Ask Mode", "Send your question about the last analyzed video.")),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
        else:
            context.chat_data[PENDING_MODE_KEY] = MODE_ASK_URL
            await msg.reply_text(
                with_tg_time("No context yet. Send a YouTube URL first, then ask your question."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
        return

    url = extract_first_youtube_url(raw)
    if url:
        ok = await _run_notes_url_mode(context, msg, url)
        if not ok:
            return
        question = raw.replace(url, "").strip()
        if question:
            await _run_ask_question_mode(context, msg, question)
        else:
            context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
        return

    context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
    await _run_ask_question_mode(context, msg, raw)


async def cmd_researches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    items = load_public_research_runs(limit=15)
    if not items:
        await msg.reply_text(
            with_tg_time("📜 No public researches saved yet."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    lines = ["📜 Public Research Archive", "Use /research_view <run_id> to open full report.", ""]
    for item in items:
        topic_tags = [str(t.get("tag") or "") for t in (item.get("topics") or []) if str(t.get("tag") or "").strip()]
        topic_txt = ", ".join(topic_tags[:4]) if topic_tags else "no tags"
        lines.append(
            f"• {item.get('run_id')} | {item.get('goal_text')}\n"
            f"  status={item.get('status')} | topics={topic_txt}"
        )
    await _send_long_chat(context, msg.chat_id, with_tg_time("\n".join(lines)))


async def cmd_recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    context.chat_data.pop(PENDING_MODE_KEY, None)
    msg = update.effective_message
    if not msg:
        return
    items = load_recent_searches(limit=15)
    if not items:
        await msg.reply_text(
            with_tg_time("🕐 No recent searches yet."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    lines = ["🕐 Recent Searches", ""]
    for item in items:
        title = item.get("title") or item.get("video_id") or "Unknown"
        question = item.get("question") or ""
        ts = item.get("asked_at")
        time_str = fmt_local_time(ts) if ts else ""
        vid = item.get("video_id") or ""
        url_line = f"  🔗 https://youtu.be/{vid}" if vid else ""
        lines.append(
            f"• {title}\n"
            f"  ❓ {question}\n"
            f"{url_line}\n"
            f"  🕐 {time_str}"
        )
    await _send_long_chat(context, msg.chat_id, with_tg_time("\n".join(lines)))


async def cmd_research_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    run_id = " ".join(context.args).strip() if context.args else ""
    if not run_id:
        await msg.reply_text(
            with_tg_time("Usage: /research_view <run_id>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    item = get_public_research_run(run_id)
    if not item:
        await msg.reply_text(
            with_tg_time("Research not found or not public."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    report = str(item.get("report_text") or "").strip()
    if not report:
        report = f"Goal: {item.get('goal_text')}\nStatus: {item.get('status')}\n(No report text saved.)"
    await _send_long_chat(context, msg.chat_id, with_tg_time(report))


async def cmd_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    try:
        items = await asyncio.to_thread(lambda: _web_app_module()._build_video_list())
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load videos:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    if not items:
        await msg.reply_text(
            with_tg_time("No saved videos yet."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    lines = [
        "🎬 Saved Videos",
        "Use /video <video_id> for details, /analyze <video_id> to run analysis.",
        "",
    ]
    for row in items[:40]:
        vid = str(row.get("video_id") or "")
        title = str(row.get("title") or vid or "Video")
        source = str(row.get("transcript_source") or "unknown")
        has_analysis = "yes" if bool(row.get("has_analysis")) else "no"
        module = "archive" if bool(row.get("is_archive")) else "video"
        lines.append(
            f"• {vid} | {title}\n"
            f"  transcript={source} | analysis={has_analysis} | module={module}"
        )
    await _send_long_chat(context, msg.chat_id, with_tg_time("\n".join(lines)))


async def cmd_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    target = " ".join(context.args or []).strip()
    video_id, src_url = _resolve_video_ref(target)
    if not video_id:
        last = _get_last_notes_context(context)
        video_id = str(last.get("video_id") or "").strip()
    if not video_id:
        await msg.reply_text(
            with_tg_time("Usage: /video <video_id|youtube_url>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    try:
        item = await asyncio.to_thread(_web_app_module()._video_detail, video_id)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load video detail:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    transcript_exists = bool(item.get("transcript_exists"))
    analysis_text = str(item.get("analysis_text") or "").strip()
    transcript_path = str(item.get("transcript_path") or "").strip()
    title = str(item.get("title") or video_id)
    src = str(item.get("youtube_url") or src_url or "").strip()
    if transcript_exists and transcript_path:
        _set_last_notes_context(
            context,
            url=src,
            title=title,
            transcript_path=transcript_path,
            video_id=video_id,
        )

    summary = (
        f"🎬 {title}\n"
        f"🆔 {video_id}\n"
        f"📄 Transcript: {'yes' if transcript_exists else 'no'}\n"
        f"🧠 Analysis: {'yes' if analysis_text else 'no'}\n"
        f"🧾 Source: {str(item.get('transcript_source') or 'unknown')}\n"
        f"🧠 LLM: {str(item.get('analysis_llm_detail') or item.get('analysis_llm_mode') or 'unknown')}\n"
        f"🔗 {src or 'n/a'}"
    )
    await msg.reply_text(with_tg_time(summary), disable_web_page_preview=True, reply_markup=_main_keyboard())

    if analysis_text:
        await _send_long_chat(context, msg.chat_id, analysis_text)


async def cmd_save_transcript(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    force, rest = _parse_force_flag(list(context.args or []))
    text = " ".join(rest).strip()
    url = extract_first_youtube_url(text)
    if not url:
        await msg.reply_text(
            with_tg_time("Usage: /save_transcript <youtube_url> [--force]"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    wait_msg = await msg.reply_text(with_tg_time("📝 Saving transcript..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._save_transcript_from_url, url, force)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not save transcript:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return

    video_id = str(result.get("video_id") or extract_youtube_id(url) or "")
    title = str(result.get("title") or video_id or "Video")
    transcript_path = str(result.get("transcript_path") or "").strip()
    if transcript_path and video_id:
        _set_last_notes_context(
            context,
            url=url,
            title=title,
            transcript_path=transcript_path,
            video_id=video_id,
        )

    await wait_msg.edit_text(
        with_tg_time(
            "✅ Transcript saved.\n"
            f"🎬 {title}\n"
            f"🆔 {video_id}\n"
            f"📄 Source: {str(result.get('source') or 'unknown')}\n"
            f"♻️ Cached: {'yes' if bool(result.get('cached')) else 'no'}\n"
            f"📁 {transcript_path or 'n/a'}"
        ),
        disable_web_page_preview=True,
    )


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    force, rest = _parse_force_flag(list(context.args or []))
    target = " ".join(rest).strip()
    video_id, _url = _resolve_video_ref(target)
    if not video_id:
        last = _get_last_notes_context(context)
        video_id = str(last.get("video_id") or "").strip()
    if not video_id:
        await msg.reply_text(
            with_tg_time("Usage: /analyze <video_id|youtube_url> [--force]"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    wait_msg = await msg.reply_text(with_tg_time("🧠 Running analysis..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._run_analysis, video_id, force, True)
        detail = await asyncio.to_thread(_web_app_module()._video_detail, video_id)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Analysis failed:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return

    transcript_path = str(detail.get("transcript_path") or "").strip()
    if transcript_path:
        _set_last_notes_context(
            context,
            url=str(detail.get("youtube_url") or ""),
            title=str(detail.get("title") or video_id),
            transcript_path=transcript_path,
            video_id=video_id,
        )

    mode = "cached" if bool(result.get("cached")) else "fresh"
    if bool(result.get("cached")):
        mode = f"cached ({int(result.get('cache_age_sec') or 0)}s old)"
    llm = str(result.get("llm_backend_detail") or result.get("llm_backend") or "unknown")
    await wait_msg.edit_text(
        with_tg_time(
            f"✅ Analysis ready for {video_id}\n"
            f"⚙️ Mode: {mode}\n"
            f"🗣 Lang: {str(result.get('lang') or 'unknown')}\n"
            f"🧠 LLM: {llm}"
        ),
        disable_web_page_preview=True,
    )

    analysis_text = str(result.get("analysis") or "").strip()
    if analysis_text:
        await _send_long_chat(context, msg.chat_id, analysis_text)


async def cmd_ask_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    args = list(context.args or [])
    last = _get_last_notes_context(context)

    video_id = ""
    question = ""
    if args:
        first = str(args[0] or "").strip()
        vref, _ = _resolve_video_ref(first)
        if vref:
            video_id = vref
            question = " ".join(args[1:]).strip()
        else:
            video_id = str(last.get("video_id") or "").strip()
            question = " ".join(args).strip()

    if not video_id:
        video_id = str(last.get("video_id") or "").strip()
    if not video_id or not question:
        await msg.reply_text(
            with_tg_time("Usage: /ask_video <video_id|youtube_url> <question>\nOr set context with /video <video_id> first."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    wait_msg = await msg.reply_text(with_tg_time("❓ Asking selected video transcript..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._run_qa, video_id, question)
        detail = await asyncio.to_thread(_web_app_module()._video_detail, video_id)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Ask failed:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return

    transcript_path = str(detail.get("transcript_path") or "").strip()
    if transcript_path:
        _set_last_notes_context(
            context,
            url=str(detail.get("youtube_url") or ""),
            title=str(detail.get("title") or video_id),
            transcript_path=transcript_path,
            video_id=video_id,
        )

    llm = str(result.get("llm_backend_detail") or result.get("llm_backend") or "unknown")
    await wait_msg.edit_text(
        with_tg_time(f"✅ Answer ready for {video_id}\n🧠 LLM: {llm}"),
        disable_web_page_preview=True,
    )
    answer = str(result.get("answer") or "").strip()
    if answer:
        await _send_long_chat(context, msg.chat_id, answer)


async def cmd_live_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    url = extract_first_youtube_url(" ".join(context.args or []).strip())
    if not url:
        await msg.reply_text(
            with_tg_time("Usage: /live_start <youtube_live_url>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    wait_msg = await msg.reply_text(with_tg_time("🔴 Starting live save..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._start_live_recording, url, 8.0)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Live start failed:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return
    await wait_msg.edit_text(
        with_tg_time(
            "✅ Live start requested.\n"
            f"🆔 {str(result.get('video_id') or 'unknown')}\n"
            f"📍 Status: {str(result.get('startup_status') or result.get('status') or 'requested')}\n"
            f"ℹ️ {str(result.get('startup_message') or 'watch /status for updates')}"
        ),
        disable_web_page_preview=True,
    )


async def cmd_live_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    arg = " ".join(context.args or []).strip()
    video_id, _ = _resolve_video_ref(arg)
    if not video_id and len(STATE.active_lives) == 1:
        video_id = next(iter(STATE.active_lives.keys()))
    if not video_id:
        await msg.reply_text(
            with_tg_time("Usage: /live_stop <video_id|youtube_url>\nTip: if only one active live exists, argument is optional."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    try:
        result = await asyncio.to_thread(_web_app_module()._stop_live_recording, video_id)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Live stop failed:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    status = str(result.get("status") or "")
    if status == "already_finished":
        text = f"✅ Live recording already finished for {video_id}."
    else:
        text = f"🛑 Stop requested for {video_id}."
    await msg.reply_text(with_tg_time(text), disable_web_page_preview=True, reply_markup=_main_keyboard())


async def cmd_direct_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return
    url = extract_first_youtube_url(" ".join(context.args or []))
    if not url:
        await msg.reply_text(
            with_tg_time("Usage: /direct_audio <youtube_url>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    wait_msg = await msg.reply_text(with_tg_time("🔎 Building direct audio link..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._run_direct_audio, url)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not create direct audio link:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return

    await wait_msg.edit_text(
        with_tg_time(
            "✅ Direct audio URL from YouTube:\n"
            f"🎬 {str(result.get('title') or result.get('video_id') or 'Video')}\n"
            "⚠️ Link is temporary and may expire.\n\n"
            f"{str(result.get('download_url') or '')}"
        ),
        disable_web_page_preview=True,
    )


async def cmd_research(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    args = list(context.args or [])
    private_hint = False
    if args and args[0].strip().lower() in ("--private", "-p", "private"):
        private_hint = True
        args = args[1:]
    goal, is_private = _parse_research_goal_and_privacy(" ".join(args).strip(), private_hint=private_hint)
    if not goal:
        context.chat_data[PENDING_MODE_KEY] = MODE_RESEARCH_GOAL
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Research Mode",
                    "Send your business goal. Prefix with private: to avoid saving this run.",
                    "I want to open a bakery and learn what successful owners did.",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    context.chat_data.pop(PENDING_MODE_KEY, None)
    context.application.create_task(
        run_market_research(
            context,
            chat_id=msg.chat_id,
            goal_text=goal,
            persist=(not is_private),
        )
    )


async def cmd_juice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    topic, is_private, config = _parse_juice_start_args(list(context.args or []))
    if not topic:
        context.chat_data[PENDING_MODE_KEY] = MODE_KNOWLEDGE_GOAL
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Knowledge Juice Mode",
                    "Send a topic for Knowledge Juice Maker. Options: --private --max-videos 8 --max-queries 10 --per-query 8 --min-duration 0 --max-duration 0 --fast",
                    "bakery",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    context.chat_data.pop(PENDING_MODE_KEY, None)
    wait_msg = await msg.reply_text(with_tg_time("🧃 Starting brew job..."), disable_web_page_preview=True)
    try:
        job = await asyncio.to_thread(_web_app_module()._start_knowledge_juice_job, topic, is_private, config)
    except Exception as exc:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not start brew job:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
        )
        return

    cfg = job.get("config") if isinstance(job.get("config"), dict) else {}
    await wait_msg.edit_text(
        with_tg_time(
            "✅ Knowledge Brew started.\n"
            f"🆔 Job: {str(job.get('job_id') or '')}\n"
            f"📌 Topic: {str(job.get('topic') or topic)}\n"
            f"🔒 Private: {'yes' if bool(job.get('private_run')) else 'no'}\n"
            f"⚙️ max_videos={int(cfg.get('max_videos') or 0)} | max_queries={int(cfg.get('max_queries') or 0)} | per_query={int(cfg.get('per_query') or 0)}\n"
            "Use /juice_jobs active and /juice_job <job_id> to watch progress."
        ),
        disable_web_page_preview=True,
    )


async def cmd_juice_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    active_only = False
    if context.args:
        flag = str(context.args[0] or "").strip().lower()
        if flag in ("1", "true", "yes", "active", "running", "queued"):
            active_only = True
        elif flag in ("0", "false", "no", "all"):
            active_only = False

    try:
        items = await asyncio.to_thread(_web_app_module()._list_brew_jobs, active_only=active_only)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load brew jobs:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    if not items:
        await msg.reply_text(
            with_tg_time("No brew jobs found."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    lines = [
        f"🧃 Knowledge Brew Jobs ({'active only' if active_only else 'all'})",
        "Use /juice_job <job_id> for details.",
        "",
    ]
    for item in items[:40]:
        progress = item.get("progress") if isinstance(item.get("progress"), dict) else {}
        ratio = float(progress.get("ratio") or 0.0)
        percent = max(0, min(100, int(round(ratio * 100))))
        step = int(progress.get("step") or 0)
        total_steps = int(progress.get("total_steps") or 0)
        detail = str(item.get("progress_detail") or "").strip()
        detail_txt = f"\n  {detail}" if detail else ""
        run_id = str(item.get("run_id") or "").strip()
        run_txt = f" | run={run_id}" if run_id else ""
        lines.append(
            f"• {str(item.get('job_id') or '')} | {str(item.get('topic') or 'topic')}\n"
            f"  status={str(item.get('status') or 'unknown')} | {step}/{total_steps} | {percent}%{run_txt}"
            f"{detail_txt}"
        )
    await _send_long_chat(context, msg.chat_id, with_tg_time("\n".join(lines)))


async def cmd_juice_job(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    job_id = " ".join(context.args or []).strip()
    if not job_id:
        await msg.reply_text(
            with_tg_time("Usage: /juice_job <job_id>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    try:
        items = await asyncio.to_thread(_web_app_module()._list_brew_jobs, active_only=False)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load brew job:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    item = next((x for x in items if str(x.get("job_id") or "").strip() == job_id), None)
    if not item:
        await msg.reply_text(
            with_tg_time("Brew job not found."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    progress = item.get("progress") if isinstance(item.get("progress"), dict) else {}
    cfg = item.get("config") if isinstance(item.get("config"), dict) else {}
    ratio = float(progress.get("ratio") or 0.0)
    percent = max(0, min(100, int(round(ratio * 100))))
    detail = str(item.get("progress_detail") or "").strip()
    llm = str(item.get("llm_backend") or "unknown")
    step = int(progress.get("step") or 0)
    total_steps = int(progress.get("total_steps") or 0)
    summary = (
        "🧃 Knowledge Brew Job\n"
        f"🆔 {job_id}\n"
        f"📌 Topic: {str(item.get('topic') or '')}\n"
        f"📍 Status: {str(item.get('status') or 'unknown')} ({step}/{total_steps}, {percent}%)\n"
        f"🔒 Private: {'yes' if bool(item.get('private_run')) else 'no'}\n"
        f"🧠 LLM: {llm}\n"
        f"⚙️ max_videos={int(cfg.get('max_videos') or 0)} | max_queries={int(cfg.get('max_queries') or 0)} | per_query={int(cfg.get('per_query') or 0)}\n"
        f"📼 Candidates: {int(item.get('total_candidates') or 0)} | Reviewed: {int(item.get('current_index') or 0)}/{int(item.get('total_videos') or 0)}\n"
        f"🔗 Run: {str(item.get('run_id') or 'n/a')}"
    )
    if detail:
        summary += f"\nℹ️ {detail}"
    error = str(item.get("error") or "").strip()
    if error:
        summary += f"\n❌ {error}"
    await msg.reply_text(with_tg_time(summary), disable_web_page_preview=True, reply_markup=_main_keyboard())

    report = str(item.get("report_text") or "").strip()
    if report:
        await _send_long_chat(context, msg.chat_id, report)


async def cmd_knowledge_juices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    try:
        items = await asyncio.to_thread(_web_app_module()._knowledge_juice_list)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load knowledge runs:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    if not items:
        await msg.reply_text(
            with_tg_time("No saved knowledge runs yet."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    lines = ["🧃 Saved Knowledge Runs", "Use /knowledge_juice <run_id> for details.", ""]
    for item in items[:40]:
        run_id = str(item.get("run_id") or "")
        goal = str(item.get("goal_text") or item.get("topic") or "topic")
        status = str(item.get("status") or "unknown")
        started = str(item.get("started_at") or item.get("created_at") or "")
        lines.append(f"• {run_id} | {goal}\n  status={status} | started={started or 'n/a'}")
    await _send_long_chat(context, msg.chat_id, with_tg_time("\n".join(lines)))


async def cmd_knowledge_juice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    msg = update.effective_message
    if not msg:
        return

    run_id = " ".join(context.args or []).strip()
    if not run_id:
        await msg.reply_text(
            with_tg_time("Usage: /knowledge_juice <run_id>"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    try:
        item = await asyncio.to_thread(_web_app_module()._knowledge_juice_detail, run_id)
    except Exception as exc:
        await msg.reply_text(
            with_tg_time(f"❌ Could not load knowledge run:\n{type(exc).__name__}: {exc}"),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    if not item:
        await msg.reply_text(
            with_tg_time("Knowledge run not found."),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    summary = (
        "🧃 Knowledge Run\n"
        f"🆔 {str(item.get('run_id') or run_id)}\n"
        f"📌 Topic: {str(item.get('goal_text') or item.get('topic') or '')}\n"
        f"📍 Status: {str(item.get('status') or 'unknown')}\n"
        f"🕒 Started: {str(item.get('started_at') or item.get('created_at') or 'n/a')}"
    )
    await msg.reply_text(with_tg_time(summary), disable_web_page_preview=True, reply_markup=_main_keyboard())

    report = str(item.get("report_text") or "").strip()
    if report:
        await _send_long_chat(context, msg.chat_id, report)


async def _run_direct_url_mode(msg, url: str):
    wait_msg = await msg.reply_text(with_tg_time("🔎 Building direct YouTube link..."), disable_web_page_preview=True)
    try:
        result = await asyncio.to_thread(_web_app_module()._run_direct_video, url)
    except Exception as e:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not create direct link:\n{str(e)[:1200]}"),
            disable_web_page_preview=True,
        )
        return

    title = str(result.get("title") or result.get("video_id") or "Video")
    direct_url = str(result.get("download_url") or "").strip()
    save_started = bool(result.get("save_started"))
    public_url = str(result.get("public_url") or "").strip()
    video_id = str(result.get("video_id") or "").strip()

    if not direct_url and save_started:
        extra = f"\n🆔 {video_id}" if video_id else ""
        await wait_msg.edit_text(
            with_tg_time(
                "⚠️ Direct CDN link was blocked by YouTube.\n"
                f"🎬 {title}{extra}\n"
                "✅ Server save was started automatically.\n"
                "Use /videos to check when the saved link appears."
            ),
            disable_web_page_preview=True,
        )
        return

    saved_note = ""
    if save_started and public_url:
        saved_note = f"\n💾 Saved copy: {public_url}"
    await wait_msg.edit_text(
        with_tg_time(
            "✅ Direct download URL from YouTube:\n"
            f"🎬 {title}\n"
            "⚠️ Link is temporary and may expire.\n\n"
            f"{direct_url}{saved_note}"
        ),
        disable_web_page_preview=True,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    context.chat_data.pop(PENDING_MODE_KEY, None)
    msg = update.effective_message

    if not STATE.active_lives:
        await msg.reply_text(with_tg_time("✅ No live recording is running right now."), reply_markup=_main_keyboard())
        return

    lines = ["🔴 Active LIVE recordings:"]
    for vid, a in STATE.active_lives.items():
        service_text = f"\n  • Session: {a.service_label}" if (a.service_label or "").strip() else ""
        lines.append(
            f"- {a.title}\n"
            f"  • Started (local): {fmt_local_time(a.started_local)}"
            f"{service_text}\n"
            f"  • https://youtu.be/{vid}"
        )
    await msg.reply_text(with_tg_time("\n".join(lines)), disable_web_page_preview=True, reply_markup=_main_keyboard())


async def _run_save_url_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, msg, url: str) -> None:
    if ADMIN_ONLY_START and update.effective_chat and update.effective_chat.id not in ADMIN_CHAT_IDS:
        await msg.reply_text(with_tg_time("🚫 Only admin can start downloads."))
        return

    wait_msg = await msg.reply_text(with_tg_time("🔎 Checking video info..."), disable_web_page_preview=True)
    started_by_chat_id = update.effective_chat.id if update.effective_chat else 0
    context.application.create_task(
        run_download_flow(
            context,
            url,
            wait_msg,
            started_by_chat_id,
            broadcast_fn=broadcast,
        )
    )


def _set_last_notes_context(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    url: str,
    title: str,
    transcript_path: str,
    video_id: str = "",
) -> None:
    context.chat_data[LAST_NOTES_CTX_KEY] = {
        "url": (url or "").strip(),
        "title": (title or "").strip(),
        "transcript_path": (transcript_path or "").strip(),
        "video_id": (video_id or "").strip(),
    }


def _get_last_notes_context(context: ContextTypes.DEFAULT_TYPE) -> dict:
    raw = context.chat_data.get(LAST_NOTES_CTX_KEY) or {}
    if not isinstance(raw, dict):
        return {}
    return raw


async def _run_audio_url_mode(context: ContextTypes.DEFAULT_TYPE, msg, url: str) -> None:
    wait_msg = await msg.reply_text(with_tg_time("🎵 Building audio file..."), disable_web_page_preview=True)
    try:
        local_path, audio_url, title = await asyncio.to_thread(yt_download_audio_with_path, url)
    except Exception as e:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not save audio:\n{str(e)[:1200]}"),
            disable_web_page_preview=True,
        )
        return
    try:
        await context.application.bot.delete_message(chat_id=msg.chat_id, message_id=wait_msg.message_id)
    except Exception:
        pass

    try:
        with open(local_path, "rb") as fh:
            await context.application.bot.send_audio(
                chat_id=msg.chat_id,
                audio=fh,
                title=(title or "").strip()[:64] or "Audio",
                caption=with_tg_time(
                    "✅ Audio ready.\n"
                    f"🎬 {title}\n"
                    "🧹 Temporary audio file removed from server after upload."
                ),
            )
        cleanup_paths = {local_path}
        try:
            public_name = unquote((audio_url or "").rsplit("/", 1)[-1].split("?", 1)[0])
            if public_name:
                cleanup_paths.add(str(STORAGE_DIR / public_name))
        except Exception:
            pass
        for p in cleanup_paths:
            try:
                os.remove(p)
            except Exception:
                pass
    except Exception as e:
        size_txt = ""
        try:
            size_txt = f"\n📦 File size: {os.path.getsize(local_path)} bytes"
        except Exception:
            pass
        await msg.reply_text(
            with_tg_time(
                "⚠️ Could not upload audio directly to Telegram (often file-size/API limit).\n"
                f"🎬 {title}\n"
                f"🎵 {audio_url}"
                f"{size_txt}\n\n"
                f"Details: {str(e)[:600]}"
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )


async def _run_notes_url_mode(context: ContextTypes.DEFAULT_TYPE, msg, url: str) -> bool:
    video_id = extract_youtube_id(url) or ""
    result = await run_video_notes(
        context,
        chat_id=msg.chat_id,
        url=url,
        title_hint=video_id or "Video",
        video_id=video_id,
        local_video_path="",
        note_scope="video",
    )
    if not result:
        return False
    _set_last_notes_context(
        context,
        url=result.get("source_url") or url,
        title=result.get("title") or (video_id or "Video"),
        transcript_path=result.get("transcript_path") or "",
        video_id=result.get("video_id") or video_id,
    )
    return True


async def _run_ask_question_mode(context: ContextTypes.DEFAULT_TYPE, msg, question: str) -> None:
    ctx = _get_last_notes_context(context)
    transcript_path = (ctx.get("transcript_path") or "").strip()
    if not transcript_path:
        await msg.reply_text(
            with_tg_time(
                f"⚠️ No transcript context yet.\n"
                f"Tap {BTN_ASK} and send a URL first."
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    wait_msg = await msg.reply_text(with_tg_time("❓ Thinking over transcript..."), disable_web_page_preview=True)
    try:
        loop = asyncio.get_running_loop()
        progress_q: asyncio.Queue[tuple[int, int | None, bool]] = asyncio.Queue()

        def _progress(chars: int, tokens: int | None, done: bool) -> None:
            try:
                loop.call_soon_threadsafe(progress_q.put_nowait, (chars, tokens, done))
            except Exception:
                pass

        task = asyncio.create_task(
            asyncio.to_thread(
                answer_question_from_transcript,
                question=question,
                transcript_path=transcript_path,
                title_hint=(ctx.get("title") or "").strip(),
                progress_cb=_progress,
            )
        )

        last_chars = -1
        last_edit = 0.0
        last_heartbeat = 0.0
        started_at = loop.time()
        while True:
            if task.done() and progress_q.empty():
                break
            try:
                chars, tokens, done = await asyncio.wait_for(progress_q.get(), timeout=0.8)
            except asyncio.TimeoutError:
                now = loop.time()
                if (now - last_heartbeat) >= 6.0:
                    elapsed = int(now - started_at)
                    try:
                        await wait_msg.edit_text(
                            with_tg_time(
                                "❓ Thinking over transcript...\n"
                                f"⏱ Elapsed: {elapsed}s\n"
                                "🧠 Waiting for generation chunks..."
                            ),
                            disable_web_page_preview=True,
                        )
                        last_heartbeat = now
                    except Exception:
                        pass
                continue

            now = loop.time()
            if not done and chars == last_chars:
                continue
            if not done and (now - last_edit) < 2.5:
                continue

            token_txt = f"{tokens} tokens" if isinstance(tokens, int) and tokens > 0 else f"~{max(1, chars // 4)} tokens"
            try:
                await wait_msg.edit_text(
                    with_tg_time(
                        "❓ Thinking over transcript...\n"
                        f"🧠 Generated: {chars} chars ({token_txt})"
                    ),
                    disable_web_page_preview=True,
                )
                last_chars = chars
                last_edit = now
            except Exception:
                pass

        answer = await task
    except Exception as e:
        await wait_msg.edit_text(
            with_tg_time(f"❌ Could not answer from transcript:\n{str(e)[:1200]}"),
            disable_web_page_preview=True,
        )
        return
    try:
        save_transcript_qa_entry(
            video_id=(ctx.get("video_id") or "").strip(),
            transcript_path=transcript_path,
            question=(question or "").strip(),
            answer=(answer or "").strip(),
            source="bot",
            chat_id=int(msg.chat_id) if getattr(msg, "chat_id", None) is not None else None,
            lang="",
            extra={
                "title": (ctx.get("title") or "").strip(),
                "url": (ctx.get("url") or "").strip(),
            },
        )
    except Exception:
        pass

    await wait_msg.edit_text(answer, disable_web_page_preview=True)


async def _send_long_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    max_len = 3900
    if len(text) <= max_len:
        await context.application.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
        return

    cur = []
    cur_len = 0
    for line in text.splitlines():
        if cur_len + len(line) + 1 > max_len and cur:
            await context.application.bot.send_message(
                chat_id=chat_id,
                text="\n".join(cur),
                disable_web_page_preview=True,
            )
            cur = [line]
            cur_len = len(line) + 1
        else:
            cur.append(line)
            cur_len += len(line) + 1

    if cur:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text="\n".join(cur),
            disable_web_page_preview=True,
        )


async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = q.data or ""
    if data == "noop":
        return

    dates, items_map = build_archive_maps()

    if data == "back_dates":
        await q.edit_message_text(with_tg_time("📚 Archive by date:"), reply_markup=make_dates_keyboard(dates))
        return

    if data.startswith("date:"):
        date_key = data.split(":", 1)[1]
        all_items = [item for (dkey, _skey), items in items_map.items() if dkey == date_key for item in items]
        all_items.sort(key=lambda r: (r.get("service_key", ""), r.get("title", "")))
        await q.edit_message_text(
            with_tg_time(f"📅 {date_key}\n\nSelect a saved live:"),
            reply_markup=make_items_keyboard(date_key, all_items, back_callback="back_dates"),
        )
        return

    # Backward compatibility for older inline keyboards.
    if data.startswith("arch:"):
        _, date_key, service_key = data.split(":", 2)
        normalized_key, normalized_label = normalize_service_key_label(service_key, "")
        key_for_lookup = normalized_key or service_key
        items = items_map.get((date_key, key_for_lookup), [])
        label = normalized_label or "Session"
        await q.edit_message_text(
            with_tg_time(f"{label}\n📅 {date_key}\n\nSelect a saved live:"),
            reply_markup=make_items_keyboard(date_key, items, back_callback=f"date:{date_key}"),
        )
        return

    if data.startswith("note:"):
        video_id = data.split(":", 1)[1].strip()
        idx = load_index()
        rec = idx.get(video_id) or {}
        if not rec:
            await q.message.reply_text(with_tg_time("⚠️ Archive item not found."))
            return

        existing = (rec.get("video_notes") or "").strip()
        transcript_path = (rec.get("video_transcript_path") or "").strip()
        if existing:
            if transcript_path:
                _set_last_notes_context(
                    context,
                    url=(rec.get("url") or "").strip(),
                    title=(rec.get("title") or video_id).strip(),
                    transcript_path=transcript_path,
                    video_id=video_id,
                )
            await _send_long_chat(context, q.message.chat_id, existing)
            return

        url = (rec.get("url") or "").strip()
        title = (rec.get("title") or video_id).strip()
        if not url:
            await q.message.reply_text(with_tg_time("⚠️ Archive URL is missing for this item."))
            return
        local_name = (rec.get("full_filename") or rec.get("filename") or "").strip()
        local_path = ""
        if local_name:
            p = STORAGE_DIR / local_name
            if p.exists():
                local_path = str(p)

        context.application.create_task(
            run_video_notes(
                context,
                chat_id=q.message.chat_id,
                url=url,
                title_hint=title,
                video_id=video_id,
                local_video_path=local_path,
                note_scope="archived LIVE",
            )
        )
        await q.message.reply_text(with_tg_time("🎙 Building notes for this archive item..."))


async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_chat(update)

    msg = update.effective_message
    if not msg or not getattr(msg, "text", None):
        return

    text = (msg.text or "").strip()
    pending_mode = context.chat_data.get(PENDING_MODE_KEY, "")

    if text == BTN_HELP:
        await cmd_start(update, context)
        return
    if text == BTN_ARCHIVE:
        await cmd_archive(update, context)
        return
    if text == BTN_STATUS:
        await cmd_status(update, context)
        return
    if text == BTN_DIRECT:
        context.chat_data[PENDING_MODE_KEY] = MODE_DIRECT
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Direct Link Mode",
                    "Send a YouTube URL for direct link mode.",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    if text == BTN_AUDIO:
        context.chat_data[PENDING_MODE_KEY] = MODE_AUDIO
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Audio Mode",
                    "Send a YouTube URL to save audio (MP3).",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    if text == BTN_ASK:
        ctx = _get_last_notes_context(context)
        if (ctx.get("transcript_path") or "").strip():
            context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
            await msg.reply_text(
                with_tg_time(_step_mode_prompt("Ask Mode", "Send your question about the last analyzed video.")),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        context.chat_data[PENDING_MODE_KEY] = MODE_ASK_URL
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Ask Mode",
                    "Send a YouTube URL first. I will prepare transcript context, then you can ask questions.",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    if text == BTN_RESEARCH:
        context.chat_data[PENDING_MODE_KEY] = MODE_RESEARCH_GOAL
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Research Mode",
                    "Send your business goal. Prefix with private: to avoid saving this run.",
                    "I want to become a car mechanic and build a profitable shop.",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    if text == BTN_KNOWLEDGE:
        context.chat_data[PENDING_MODE_KEY] = MODE_KNOWLEDGE_GOAL
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Knowledge Juice Mode",
                    "Send a topic for Knowledge Juice Maker. Prefix with private: to avoid saving this run.",
                    "bakery",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return
    if text == BTN_RESEARCH_LIST:
        await cmd_researches(update, context)
        return
    if text == BTN_RECENT:
        await cmd_recent(update, context)
        return
    if text == BTN_SAVE:
        context.chat_data[PENDING_MODE_KEY] = MODE_SAVE
        await msg.reply_text(
            with_tg_time(
                _step_mode_prompt(
                    "Save Mode",
                    "Send a YouTube URL to save and get a public link.",
                    "https://www.youtube.com/watch?v=...",
                )
            ),
            disable_web_page_preview=True,
            reply_markup=_main_keyboard(),
        )
        return

    if pending_mode == MODE_RESEARCH_GOAL:
        context.chat_data.pop(PENDING_MODE_KEY, None)
        goal, is_private = _parse_research_goal_and_privacy(text, private_hint=False)
        if not goal:
            await msg.reply_text(
                with_tg_time("Please send a non-empty research goal."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        context.application.create_task(
            run_market_research(
                context,
                chat_id=msg.chat_id,
                goal_text=goal,
                persist=(not is_private),
            )
        )
        return
    if pending_mode == MODE_KNOWLEDGE_GOAL:
        context.chat_data.pop(PENDING_MODE_KEY, None)
        topic, is_private, config = _parse_juice_start_args(text.split())
        if not topic:
            await msg.reply_text(
                with_tg_time("Please send a non-empty topic."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        wait_msg = await msg.reply_text(with_tg_time("🧃 Starting brew job..."), disable_web_page_preview=True)
        try:
            job = await asyncio.to_thread(_web_app_module()._start_knowledge_juice_job, topic, is_private, config)
        except Exception as exc:
            await wait_msg.edit_text(
                with_tg_time(f"❌ Could not start brew job:\n{type(exc).__name__}: {exc}"),
                disable_web_page_preview=True,
            )
            return
        await wait_msg.edit_text(
            with_tg_time(
                "✅ Knowledge Brew started.\n"
                f"🆔 Job: {str(job.get('job_id') or '')}\n"
                f"📌 Topic: {str(job.get('topic') or topic)}\n"
                "Use /juice_jobs active and /juice_job <job_id> to watch progress."
            ),
            disable_web_page_preview=True,
        )
        return

    url = extract_first_youtube_url(text)
    if not url:
        if pending_mode == MODE_ASK_QUESTION:
            await _run_ask_question_mode(context, msg, text)
            return
        if pending_mode == MODE_DIRECT:
            await msg.reply_text(
                with_tg_time("Please send a valid YouTube URL for direct mode."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        if pending_mode == MODE_AUDIO:
            await msg.reply_text(
                with_tg_time("Please send a valid YouTube URL for audio mode."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        if pending_mode == MODE_ASK_URL:
            await msg.reply_text(
                with_tg_time("Please send a valid YouTube URL to prepare Q&A context."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        if pending_mode == MODE_SAVE:
            await msg.reply_text(
                with_tg_time("Please send a valid YouTube URL for save mode."),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
            return
        if STATE.active_lives:
            await cmd_status(update, context)
        else:
            await msg.reply_text(
                with_tg_time("Use the keyboard: choose a mode first, then send a YouTube URL.\n\n" + HELP_TEXT),
                disable_web_page_preview=True,
                reply_markup=_main_keyboard(),
            )
        return

    context.chat_data.pop(PENDING_MODE_KEY, None)

    if pending_mode == MODE_DIRECT:
        await _run_direct_url_mode(msg, url)
        return
    if pending_mode == MODE_AUDIO:
        await _run_audio_url_mode(context, msg, url)
        return
    if pending_mode == MODE_ASK_URL:
        ok = await _run_notes_url_mode(context, msg, url)
        if not ok:
            return
        context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
        return
    if pending_mode == MODE_ASK_QUESTION:
        ok = await _run_notes_url_mode(context, msg, url)
        if not ok:
            return
        context.chat_data[PENDING_MODE_KEY] = MODE_ASK_QUESTION
        return

    await _run_direct_url_mode(msg, url)


async def start_download_from_external(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    url: str,
    source_label: str = "External",
) -> None:
    parsed_url = extract_first_youtube_url(url or "")
    if not parsed_url:
        return

    if ADMIN_ONLY_START and chat_id not in ADMIN_CHAT_IDS:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=with_tg_time("🚫 Only admin can start downloads."),
        )
        return

    if chat_id not in STATE.known_chats:
        STATE.known_chats.add(chat_id)
        save_known_chats(STATE.known_chats)

    wait_msg = await context.application.bot.send_message(
        chat_id=chat_id,
        text=with_tg_time(f"🔎 Checking video info...\nSource: {source_label}"),
        disable_web_page_preview=True,
    )

    context.application.create_task(
        run_download_flow(
            context,
            parsed_url,
            wait_msg,
            chat_id,
            broadcast_fn=broadcast,
        )
    )
