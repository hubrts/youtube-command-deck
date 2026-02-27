from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from telegram.ext import ContextTypes

from video_notes import (
    _anthropic_chat,
    _download_audio,
    _download_youtube_caption_segments,
    _ollama_chat,
    _openai_chat,
    _save_full_transcript,
    _segments_to_transcript_text,
    _transcribe_segments,
    _try_parse_json_object,
    _yt_dlp_base_cmd,
)
from ytbot_state import (
    create_research_run,
    finalize_research_run,
    load_related_public_topics,
    load_research_video_facts,
    load_research_videos,
    save_research_topics,
    save_research_video_fact,
    save_research_video_transcript,
    save_research_videos,
)
from ytbot_utils import extract_youtube_id, with_tg_time

NO_CAPTION_MAX_DURATION_SEC = 10 * 60


def _parse_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw)
    except Exception:
        return default


def build_knowledge_juice_goal(topic_text: str) -> str:
    topic = re.sub(r"\s+", " ", (topic_text or "").strip())
    if not topic:
        return ""
    return (
        f"I want to become successful in {topic}. "
        "Find popular YouTube videos where real owners/operators explain how they started and grew. "
        "Save transcripts, compare similarities and differences, and give practical next steps."
    )


def _llm_json_with_backend(system_prompt: str, user_prompt: str, *, timeout_sec: int = 120) -> Tuple[dict, str]:
    backend = (os.getenv("VIDEO_QA_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    openai_model = (os.getenv("VIDEO_QA_MODEL") or os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    claude_model = (
        os.getenv("VIDEO_QA_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()

    attempts: List[Tuple[str, str]] = []
    if backend in ("local", "ollama"):
        attempts.append(("local", local_model))
    elif backend == "openai":
        attempts.append(("openai", openai_model))
    elif backend in ("claude", "anthropic"):
        attempts.append(("claude", claude_model))
        attempts.append(("local", local_model))
    else:
        attempts.append(("local", local_model))
        attempts.append(("claude", claude_model))
        attempts.append(("openai", openai_model))

    for provider, model in attempts:
        try:
            text = ""
            if provider == "local":
                text = _ollama_chat(
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.1,
                    timeout_sec=timeout_sec,
                    format_json=True,
                )
            elif provider == "claude":
                text = _anthropic_chat(
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.1,
                    timeout_sec=timeout_sec,
                    max_tokens=1600,
                )
            else:
                text = _openai_chat(
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.1,
                    timeout_sec=timeout_sec,
                )
            obj = _try_parse_json_object(text)
            if obj:
                return obj, provider
        except Exception:
            continue
    return {}, "unknown"


def _llm_json(system_prompt: str, user_prompt: str, *, timeout_sec: int = 120) -> dict:
    payload, _provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=timeout_sec)
    return payload


def _parse_goal_intent(goal_text: str, llm_backend_cb: Optional[Callable[[str], None]] = None) -> dict:
    system_prompt = (
        "Extract structured research intent for a business-learning request. "
        "Return JSON with keys: domain, objective, target_region, target_language, audience, success_signals. "
        "success_signals must be a short list."
    )
    user_prompt = f"Request: {goal_text}"
    payload, provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=60)
    if llm_backend_cb:
        llm_backend_cb(provider)
    return {
        "domain": str(payload.get("domain") or "").strip(),
        "objective": str(payload.get("objective") or goal_text).strip(),
        "target_region": str(payload.get("target_region") or "").strip(),
        "target_language": str(payload.get("target_language") or "").strip(),
        "audience": str(payload.get("audience") or "").strip(),
        "success_signals": payload.get("success_signals") if isinstance(payload.get("success_signals"), list) else [],
    }


def _generate_queries(
    goal_text: str,
    intent: dict,
    max_queries: int,
    llm_backend_cb: Optional[Callable[[str], None]] = None,
) -> List[str]:
    system_prompt = (
        "Generate high-quality YouTube search queries for finding owner success stories and practical business lessons. "
        "Return JSON with key queries (list of strings). Keep queries diverse and concise."
    )
    user_prompt = (
        f"Goal: {goal_text}\n"
        f"Intent: {json.dumps(intent, ensure_ascii=False)}\n"
        f"Max queries: {max_queries}"
    )
    payload, provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=60)
    if llm_backend_cb:
        llm_backend_cb(provider)
    queries: List[str] = []
    raw = payload.get("queries")
    if isinstance(raw, list):
        for item in raw:
            q = str(item or "").strip()
            if q:
                queries.append(q)

    # Generic fallback, still domain-agnostic.
    if not queries:
        base = re.sub(r"\s+", " ", (goal_text or "").strip())
        queries = [
            f"{base} success story",
            f"{base} owner interview",
            f"{base} how I started",
            f"{base} business case study",
            f"{base} mistakes and lessons",
            f"{base} from zero to profitable",
        ]
    out: List[str] = []
    seen = set()
    for q in queries:
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
        if len(out) >= max_queries:
            break
    return out


def _parse_upload_date(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if re.fullmatch(r"\d{8}", raw):
        try:
            return datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _video_popularity_score(meta: dict) -> float:
    views = int(meta.get("view_count") or 0)
    followers = int(meta.get("channel_follower_count") or 0)
    duration = int(meta.get("duration") or 0)
    upload_dt = _parse_upload_date(str(meta.get("upload_date") or meta.get("release_date") or ""))

    view_term = min(1.0, math.log1p(max(0, views)) / 16.0)
    follower_term = min(1.0, math.log1p(max(0, followers)) / 16.0)
    duration_term = 0.0
    if duration > 0:
        duration_term = min(1.0, max(0.0, (duration - 180) / 1800.0))
    recency_term = 0.5
    if upload_dt:
        days = max(0.0, (datetime.now(timezone.utc) - upload_dt).total_seconds() / 86400.0)
        recency_term = max(0.1, min(1.0, 1.0 / (1.0 + days / 180.0)))

    return float(0.55 * view_term + 0.15 * follower_term + 0.20 * recency_term + 0.10 * duration_term)


def _search_youtube_videos(query: str, max_results: int) -> List[dict]:
    base_cmd = [x for x in _yt_dlp_base_cmd() if x != "--no-playlist"]
    cmd = [
        *base_cmd,
        "--dump-single-json",
        f"ytsearch{max(1, max_results)}:{query}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return []

    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        return []
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return []
    out: List[dict] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        vid = str(item.get("id") or "").strip()
        if not vid:
            vid = extract_youtube_id(str(item.get("url") or item.get("webpage_url") or "")) or ""
        if not vid:
            continue
        url = str(item.get("webpage_url") or "").strip() or f"https://www.youtube.com/watch?v={vid}"
        out.append(
            {
                "video_id": vid,
                "url": url,
                "title": str(item.get("title") or "").strip(),
                "channel": str(item.get("channel") or item.get("uploader") or "").strip(),
                "view_count": int(item.get("view_count") or 0),
                "published_utc": str(item.get("upload_date") or item.get("release_date") or "").strip(),
                "duration_sec": int(item.get("duration") or 0),
                "thumbnail_url": str(
                    item.get("thumbnail")
                    or ((item.get("thumbnails") or [{}])[0].get("url") if isinstance(item.get("thumbnails"), list) else "")
                    or ""
                ).strip(),
                "meta": item,
            }
        )
    return out


def _caption_state_from_meta(meta: dict) -> Optional[bool]:
    if not isinstance(meta, dict):
        return None
    for key in ("subtitles", "automatic_captions", "requested_subtitles"):
        if key not in meta:
            continue
        value = meta.get(key)
        if isinstance(value, dict):
            return any(bool(v) for v in value.values())
        if isinstance(value, list):
            return len(value) > 0
        return bool(value)
    return None


def _probe_has_captions(url: str) -> Optional[bool]:
    src = str(url or "").strip()
    if not src:
        return None
    base_cmd = [x for x in _yt_dlp_base_cmd() if x != "--no-playlist"]
    cmd = [
        *base_cmd,
        "--dump-single-json",
        src,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        return None
    return _caption_state_from_meta(payload if isinstance(payload, dict) else {})


def _has_captions(item: dict, cache: Dict[str, bool]) -> bool:
    vid = str(item.get("video_id") or "").strip()
    if vid and vid in cache:
        return bool(cache.get(vid))

    meta_state = _caption_state_from_meta(item.get("meta") if isinstance(item, dict) else {})
    if meta_state is None:
        meta_state = _probe_has_captions(str(item.get("url") or ""))
    value = bool(meta_state) if meta_state is not None else False

    if vid:
        cache[vid] = value
    return value


def _collect_candidate_videos_with_stats(
    queries: List[str],
    per_query: int,
    max_total: int,
    *,
    min_duration_sec: int = 0,
    max_duration_sec: int = 0,
    captions_only: bool = False,
) -> Tuple[List[dict], dict]:
    merged: Dict[str, dict] = {}
    caption_cache: Dict[str, bool] = {}
    effective_no_caption_max_sec = NO_CAPTION_MAX_DURATION_SEC
    if max_duration_sec > 0:
        effective_no_caption_max_sec = min(effective_no_caption_max_sec, int(max_duration_sec))
    stats = {
        "query_count": len([q for q in queries if str(q or "").strip()]),
        "seen_total": 0,
        "eligible_total": 0,
        "with_captions": 0,
        "without_captions": 0,
        "caption_override_kept": 0,
        "filtered_too_short": 0,
        "filtered_no_caption_too_long": 0,
        "filtered_without_captions": 0,
        "captions_only": bool(captions_only),
        "no_caption_max_duration_sec": int(effective_no_caption_max_sec),
        "query_stats": [],
    }
    for q in queries:
        query_text = str(q or "").strip()
        query_rows = _search_youtube_videos(query_text, per_query)
        q_stats = {
            "query": query_text,
            "returned": len(query_rows),
            "eligible": 0,
            "unique_added": 0,
            "with_captions": 0,
            "without_captions": 0,
            "caption_override_kept": 0,
            "filtered_too_short": 0,
            "filtered_no_caption_too_long": 0,
            "filtered_without_captions": 0,
        }
        for item in query_rows:
            stats["seen_total"] += 1
            dur = int(item.get("duration_sec") or 0)
            too_short_if_no_captions = min_duration_sec > 0 and dur > 0 and dur < min_duration_sec
            too_long_if_no_captions = effective_no_caption_max_sec > 0 and dur > 0 and dur > effective_no_caption_max_sec
            needs_caption_override = too_short_if_no_captions or too_long_if_no_captions

            has_captions = _has_captions(item, caption_cache) if (needs_caption_override or captions_only) else bool(
                _caption_state_from_meta(item.get("meta") if isinstance(item, dict) else {})
            )
            item["has_captions"] = has_captions
            if has_captions:
                stats["with_captions"] += 1
                q_stats["with_captions"] += 1
            else:
                stats["without_captions"] += 1
                q_stats["without_captions"] += 1

            if captions_only and not has_captions:
                stats["filtered_without_captions"] += 1
                q_stats["filtered_without_captions"] += 1
                continue

            if needs_caption_override and not has_captions:
                if too_short_if_no_captions:
                    stats["filtered_too_short"] += 1
                    q_stats["filtered_too_short"] += 1
                if too_long_if_no_captions:
                    stats["filtered_no_caption_too_long"] += 1
                    q_stats["filtered_no_caption_too_long"] += 1
                continue

            if needs_caption_override and has_captions:
                stats["caption_override_kept"] += 1
                q_stats["caption_override_kept"] += 1

            stats["eligible_total"] += 1
            q_stats["eligible"] += 1
            vid = item["video_id"]
            item["popularity_score"] = _video_popularity_score(item["meta"])
            if vid not in merged:
                q_stats["unique_added"] += 1
            if vid not in merged or float(item["popularity_score"]) > float(merged[vid].get("popularity_score") or 0.0):
                merged[vid] = item
        stats["query_stats"].append(q_stats)

    ranked = sorted(merged.values(), key=lambda x: float(x.get("popularity_score") or 0.0), reverse=True)
    out: List[dict] = []
    for idx, item in enumerate(ranked[:max_total], start=1):
        rec = dict(item)
        rec["rank"] = idx
        out.append(rec)
    return out, stats


def _collect_candidate_videos(
    queries: List[str],
    per_query: int,
    max_total: int,
    *,
    min_duration_sec: int = 0,
    max_duration_sec: int = 0,
    captions_only: bool = False,
) -> List[dict]:
    out, _stats = _collect_candidate_videos_with_stats(
        queries,
        per_query,
        max_total,
        min_duration_sec=min_duration_sec,
        max_duration_sec=max_duration_sec,
        captions_only=captions_only,
    )
    return out


def _video_preview(item: dict) -> dict:
    return {
        "video_id": str(item.get("video_id") or ""),
        "url": str(item.get("url") or ""),
        "title": str(item.get("title") or ""),
        "channel": str(item.get("channel") or ""),
        "view_count": int(item.get("view_count") or 0),
        "published_utc": str(item.get("published_utc") or ""),
        "duration_sec": int(item.get("duration_sec") or 0),
        "has_captions": bool(item.get("has_captions")),
        "thumbnail_url": str(item.get("thumbnail_url") or ""),
        "popularity_score": float(item.get("popularity_score") or 0.0),
        "rank": int(item.get("rank") or 0),
    }


def _extract_business_facts(
    *,
    goal_text: str,
    title: str,
    transcript_text: str,
    llm_backend_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    window = (transcript_text or "")[:22000]
    system_prompt = (
        "You extract business-learning facts from a transcript. "
        "Return JSON only with keys: is_owner_story, confidence, business_model, growth_levers, "
        "marketing_channels, operations, mistakes, key_metrics, differentiators, evidence_quotes. "
        "All list fields should contain short strings."
    )
    user_prompt = (
        f"Research goal: {goal_text}\n"
        f"Video title: {title}\n\n"
        f"Transcript:\n{window}"
    )
    payload, provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=120)
    if llm_backend_cb:
        llm_backend_cb(provider)
    if payload:
        return payload
    return {
        "is_owner_story": "unknown",
        "confidence": 0.0,
        "business_model": "",
        "growth_levers": [],
        "marketing_channels": [],
        "operations": [],
        "mistakes": [],
        "key_metrics": [],
        "differentiators": [],
        "evidence_quotes": [],
    }


def _build_comparison_report(
    goal_text: str,
    videos: List[dict],
    facts_rows: List[dict],
    llm_backend_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[str, dict]:
    payload = []
    facts_by_vid = {str(x.get("video_id") or ""): x for x in facts_rows}
    owner_conf_min = float((os.getenv("RESEARCH_OWNER_CONFIDENCE_MIN") or "0.55").strip())
    owner_story_videos: List[dict] = []
    for item in videos:
        vid = str(item.get("video_id") or "")
        row = facts_by_vid.get(vid) or {}
        is_owner = row.get("is_owner_story")
        conf = float(row.get("confidence") or 0.0)
        if is_owner is True and conf >= owner_conf_min:
            owner_story_videos.append(item)
        payload.append(
            {
                "video_id": vid,
                "title": item.get("title") or "",
                "channel": item.get("channel") or "",
                "view_count": item.get("view_count") or 0,
                "facts": row.get("facts") or {},
                "is_owner_story": is_owner,
                "confidence": conf,
                "business_model": row.get("business_model") or "",
            }
        )

    comparison_payload = payload
    if len(owner_story_videos) >= 2:
        owner_ids = {str(v.get("video_id") or "") for v in owner_story_videos}
        comparison_payload = [x for x in payload if str(x.get("video_id") or "") in owner_ids]

    system_prompt = (
        "You compare multiple business success stories. "
        "Return JSON with keys: similarities, differences, recommendations. "
        "Each value should be a list of concise bullets."
    )
    user_prompt = (
        f"Goal: {goal_text}\n\n"
        f"Analyzed videos and extracted facts:\n{json.dumps(comparison_payload, ensure_ascii=False)[:42000]}"
    )
    summary, provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=120)
    if llm_backend_cb:
        llm_backend_cb(provider)
    similarities = summary.get("similarities") if isinstance(summary.get("similarities"), list) else []
    differences = summary.get("differences") if isinstance(summary.get("differences"), list) else []
    recommendations = summary.get("recommendations") if isinstance(summary.get("recommendations"), list) else []

    lines = [
        "📊 Business Research Report",
        f"🎯 Goal: {goal_text}",
        f"🎥 Videos analyzed: {len(videos)}",
        f"👤 Owner-story matches: {len(owner_story_videos)}",
        "",
        "Top videos:",
    ]
    for item in videos[:10]:
        lines.append(
            f"• {item.get('title') or item.get('video_id')} "
            f"({item.get('channel') or 'Unknown'}, views: {int(item.get('view_count') or 0)})"
        )

    lines.append("")
    lines.append("✅ Similarities")
    if similarities:
        for x in similarities[:8]:
            lines.append(f"• {str(x)}")
    else:
        lines.append("• Not enough consistent overlap extracted yet.")

    lines.append("")
    lines.append("🧩 Differences")
    if differences:
        for x in differences[:8]:
            lines.append(f"• {str(x)}")
    else:
        lines.append("• Not enough strong contrasts extracted yet.")

    lines.append("")
    lines.append("🛠 Recommended next actions")
    if recommendations:
        for x in recommendations[:8]:
            lines.append(f"• {str(x)}")
    else:
        lines.append("• Collect more interviews and compare again.")

    return "\n".join(lines).strip(), {
        "similarities": similarities,
        "differences": differences,
        "recommendations": recommendations,
        "owner_story_matches": len(owner_story_videos),
        "compared_video_count": len(comparison_payload),
    }


def _extract_research_topics(
    goal_text: str,
    intent: dict,
    facts_rows: List[dict],
    llm_backend_cb: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    system_prompt = (
        "Extract concise topic tags for cross-domain business learning. "
        "Return JSON: {\"topics\":[{\"tag\":\"...\",\"weight\":0.0-1.0}]} with 5-12 tags."
    )
    user_payload = {
        "goal_text": goal_text,
        "intent": intent,
        "facts": [
            {
                "video_id": row.get("video_id"),
                "is_owner_story": row.get("is_owner_story"),
                "confidence": row.get("confidence"),
                "business_model": row.get("business_model"),
                "facts": row.get("facts"),
            }
            for row in facts_rows
        ],
    }
    user_prompt = f"Data:\n{json.dumps(user_payload, ensure_ascii=False)[:32000]}"
    payload, provider = _llm_json_with_backend(system_prompt, user_prompt, timeout_sec=90)
    if llm_backend_cb:
        llm_backend_cb(provider)
    raw = payload.get("topics")
    out: List[dict] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                tag = str(item.get("tag") or item.get("topic") or "").strip().lower()
                try:
                    weight = float(item.get("weight") or 0.5)
                except Exception:
                    weight = 0.5
                if tag:
                    out.append({"tag": re.sub(r"\s+", " ", tag)[:120], "weight": max(0.0, min(1.0, weight))})
            else:
                tag = str(item or "").strip().lower()
                if tag:
                    out.append({"tag": re.sub(r"\s+", " ", tag)[:120], "weight": 0.5})
    dedup: List[dict] = []
    seen = set()
    for item in out:
        tag = item["tag"]
        if tag in seen:
            continue
        seen.add(tag)
        dedup.append(item)
    if dedup:
        return dedup[:12]

    # Fallback tags from intent/facts if model fails.
    fallback = []
    intent_domain = str(intent.get("domain") or "").strip().lower()
    if intent_domain:
        fallback.append({"tag": intent_domain, "weight": 0.8})
    for row in facts_rows:
        bm = str(row.get("business_model") or "").strip().lower()
        if bm:
            fallback.append({"tag": bm, "weight": 0.6})
    return fallback[:8]


async def _send_long_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    max_len = 3900
    if len(text) <= max_len:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=with_tg_time(text),
            disable_web_page_preview=True,
        )
        return

    parts: List[str] = []
    cur: List[str] = []
    used = 0
    for line in text.splitlines():
        if used + len(line) + 1 > max_len and cur:
            parts.append("\n".join(cur))
            cur = [line]
            used = len(line) + 1
        else:
            cur.append(line)
            used += len(line) + 1
    if cur:
        parts.append("\n".join(cur))

    for part in parts:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=with_tg_time(part),
            disable_web_page_preview=True,
        )


def _search_summary_text(video_stats: dict, *, max_queries: int = 4) -> str:
    stats = video_stats if isinstance(video_stats, dict) else {}
    total_queries = int(stats.get("query_count") or 0)
    seen_total = int(stats.get("seen_total") or 0)
    eligible_total = int(stats.get("eligible_total") or 0)
    summary = f"Searched {total_queries} queries and got {seen_total} results; {eligible_total} passed filters."
    if bool(stats.get("captions_only")):
        removed_no_captions = int(stats.get("filtered_without_captions") or 0)
        summary += f" Fast mode removed {removed_no_captions} items without captions."
    rows = stats.get("query_stats")
    if isinstance(rows, list) and rows:
        chunks: List[str] = []
        for row in rows[: max(1, int(max_queries))]:
            if not isinstance(row, dict):
                continue
            q = re.sub(r"\s+", " ", str(row.get("query") or "").strip())
            if len(q) > 42:
                q = q[:39].rstrip() + "..."
            chunks.append(f"\"{q}\"→{int(row.get('returned') or 0)}")
        if chunks:
            summary += " Per query: " + ", ".join(chunks) + "."
    return summary


async def run_market_research(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    goal_text: str,
    persist: bool = True,
    status_title: str = "🧭 Research",
    run_kind: str = "research",
    on_report: Callable[[str, str], Awaitable[None] | None] | None = None,
    on_progress: Callable[[dict], Awaitable[None] | None] | None = None,
    per_query_override: Optional[int] = None,
    max_queries_override: Optional[int] = None,
    max_videos_override: Optional[int] = None,
    min_duration_sec: int = 0,
    max_duration_sec: int = 0,
    captions_only: bool = False,
) -> str:
    goal = re.sub(r"\s+", " ", (goal_text or "").strip())
    if not goal:
        raise RuntimeError("research goal is empty")

    per_query = max(3, int(per_query_override or _parse_int_env("RESEARCH_RESULTS_PER_QUERY", 8)))
    max_queries = max(3, int(max_queries_override or _parse_int_env("RESEARCH_MAX_QUERIES", 8)))
    max_videos = max(2, int(max_videos_override or _parse_int_env("RESEARCH_MAX_VIDEOS", 6)))
    min_duration_sec = max(0, int(min_duration_sec or 0))
    max_duration_sec = max(0, int(max_duration_sec or 0))
    captions_only = bool(captions_only)
    no_caption_max_duration_sec = NO_CAPTION_MAX_DURATION_SEC
    if max_duration_sec > 0:
        no_caption_max_duration_sec = min(no_caption_max_duration_sec, max_duration_sec)
    current_llm_backend = "unknown"

    def _mark_llm_backend(raw_provider: str) -> None:
        nonlocal current_llm_backend
        provider = str(raw_provider or "").strip().lower()
        if provider in ("local", "claude", "openai"):
            current_llm_backend = provider

    async def _emit_progress(event_type: str, **payload: Any) -> None:
        if not on_progress:
            return
        event = {
            "event_type": event_type,
            "run_kind": str(run_kind or "research"),
            "status_title": status_title,
            "llm_backend": current_llm_backend,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        try:
            maybe = on_progress(event)
            if asyncio.iscoroutine(maybe):
                await maybe
        except Exception:
            pass

    status = await context.application.bot.send_message(
        chat_id=chat_id,
        text=with_tg_time(f"{status_title}\nStep 1/5: Understanding your goal"),
        disable_web_page_preview=True,
    )

    run_id = ""
    last_video_stats: dict = {}
    try:
        await _emit_progress(
            "started",
            goal_text=goal,
            config={
                "per_query": per_query,
                "max_queries": max_queries,
                "max_videos": max_videos,
                "min_duration_sec": min_duration_sec,
                "max_duration_sec": max_duration_sec,
                "no_caption_max_duration_sec": int(no_caption_max_duration_sec),
                "captions_only": captions_only,
            },
            detail="Understanding your goal and preparing settings.",
            progress={"step": 1, "total_steps": 5, "ratio": 0.05},
        )
        intent = await asyncio.to_thread(_parse_goal_intent, goal, _mark_llm_backend)
        intent = dict(intent or {})
        intent["run_kind"] = str(run_kind or "research").strip() or "research"
        if persist:
            run_id = create_research_run(chat_id=chat_id, goal_text=goal, intent=intent, is_public=True)

        await context.application.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status.message_id,
            text=with_tg_time(f"{status_title}\nStep 2/5: Generating search queries"),
            disable_web_page_preview=True,
        )
        queries = await asyncio.to_thread(_generate_queries, goal, intent, max_queries, _mark_llm_backend)
        await _emit_progress(
            "queries_ready",
            queries=queries,
            detail=f"Generated {len(queries)} search queries.",
            progress={"step": 2, "total_steps": 5, "ratio": 0.2},
        )

        await context.application.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status.message_id,
            text=with_tg_time(f"{status_title}\nStep 3/5: Finding relevant YouTube videos"),
            disable_web_page_preview=True,
        )
        videos, video_stats = await asyncio.to_thread(
            _collect_candidate_videos_with_stats,
            queries,
            per_query,
            max_videos,
            min_duration_sec=min_duration_sec,
            max_duration_sec=max_duration_sec,
            captions_only=captions_only,
        )
        last_video_stats = video_stats if isinstance(video_stats, dict) else {}
        if not videos:
            err_text = "No candidate videos found. Try a broader goal."
            seen_total = int(video_stats.get("seen_total") or 0)
            eligible_total = int(video_stats.get("eligible_total") or 0)
            filtered_no_caption_too_long = int(video_stats.get("filtered_no_caption_too_long") or 0)
            filtered_too_short = int(video_stats.get("filtered_too_short") or 0)
            with_captions = int(video_stats.get("with_captions") or 0)
            filtered_without_captions = int(video_stats.get("filtered_without_captions") or 0)
            no_caption_limit = int(video_stats.get("no_caption_max_duration_sec") or NO_CAPTION_MAX_DURATION_SEC)
            if captions_only and seen_total > 0 and eligible_total == 0 and filtered_without_captions > 0:
                err_text = "I've found videos, but none had captions/transcripts for fast mode."
            elif (
                seen_total > 0
                and eligible_total == 0
                and filtered_no_caption_too_long > 0
                and with_captions == 0
            ):
                err_text = (
                    f"I've found videos, but the no-caption limit is {int(no_caption_limit/60)} minutes max each "
                    f"and these were longer."
                )
            elif (
                min_duration_sec > 0
                and seen_total > 0
                and eligible_total == 0
                and filtered_too_short > 0
            ):
                err_text = "I've found those videos but they're shorter than your minimum duration setting."
            elif seen_total == 0:
                err_text = "Search returned no videos for the generated queries."

            err_text = f"{err_text} {_search_summary_text(video_stats)}"
            finalize_research_run(
                run_id=run_id,
                status="failed",
                report_text=f"Research failed: {err_text}",
                summary={"queries": queries, "video_stats": video_stats},
            )
            raise RuntimeError(err_text)
        if persist and run_id:
            save_research_videos(run_id, videos)
        await _emit_progress(
            "candidates_ready",
            total_candidates=len(videos),
            videos=[_video_preview(v) for v in videos],
            search_stats=video_stats,
            query_stats=video_stats.get("query_stats") if isinstance(video_stats, dict) else [],
            detail=_search_summary_text(video_stats),
            progress={"step": 3, "total_steps": 5, "ratio": 0.35},
        )

        await context.application.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status.message_id,
            text=with_tg_time(
                f"{status_title}\n"
                f"Step 4/5: Extracting transcripts and facts (0/{len(videos)})"
            ),
            disable_web_page_preview=True,
        )

        processed: List[dict] = []
        facts_memory: List[dict] = []
        for i, item in enumerate(videos, start=1):
            url = str(item.get("url") or "").strip()
            vid = str(item.get("video_id") or "").strip()
            title = str(item.get("title") or vid).strip()
            await _emit_progress(
                "processing_video",
                current_index=i,
                total_videos=len(videos),
                video=_video_preview(item),
                detail=f"Video {i}/{len(videos)}: downloading transcript for {title}",
                progress={
                    "step": 4,
                    "total_steps": 5,
                    "ratio": 0.35 + (0.45 * (i - 1) / max(1, len(videos))),
                },
            )

            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(
                    f"{status_title}\n"
                    f"Step 4/5: Processing video {i}/{len(videos)}\n"
                    f"🎬 {title}"
                ),
                disable_web_page_preview=True,
            )

            temp_dir = Path(tempfile.mkdtemp(prefix="research_"))
            transcript_source = ""
            transcript_text = ""
            try:
                try:
                    segments, cap_title, _cap = await asyncio.to_thread(
                        _download_youtube_caption_segments, url, temp_dir, title
                    )
                    transcript_text = _segments_to_transcript_text(segments)
                    transcript_source = "youtube captions"
                    title = cap_title or title
                except Exception:
                    transcript_text = ""

                if not transcript_text and not captions_only:
                    try:
                        audio_path, dl_title = await asyncio.to_thread(_download_audio, url, temp_dir)
                        segments = await asyncio.to_thread(_transcribe_segments, audio_path)
                        transcript_text = _segments_to_transcript_text(segments)
                        transcript_source = "audio transcription"
                        title = dl_title or title
                    except Exception:
                        transcript_text = ""

                if not transcript_text.strip():
                    continue

                transcript_path = ""
                if persist and run_id:
                    transcript_path = await asyncio.to_thread(_save_full_transcript, vid, title, transcript_text)
                    save_research_video_transcript(
                        run_id=run_id,
                        video_id=vid,
                        transcript_path=transcript_path,
                        transcript_source=transcript_source,
                        transcript_chars=len(transcript_text),
                    )

                facts = await asyncio.to_thread(
                    _extract_business_facts,
                    goal_text=goal,
                    title=title,
                    transcript_text=transcript_text,
                    llm_backend_cb=_mark_llm_backend,
                )
                if persist and run_id:
                    save_research_video_fact(run_id=run_id, video_id=vid, facts=facts)
                processed.append(
                    {
                        **item,
                        "title": title,
                        "transcript_path": transcript_path,
                        "transcript_source": transcript_source,
                        "transcript_chars": len(transcript_text),
                    }
                )
                facts_memory.append(
                    {
                        "video_id": vid,
                        "is_owner_story": facts.get("is_owner_story"),
                        "confidence": facts.get("confidence"),
                        "business_model": facts.get("business_model"),
                        "facts": facts,
                    }
                )
                await _emit_progress(
                    "video_processed",
                    current_index=i,
                    total_videos=len(videos),
                    video=_video_preview(
                        {
                            **item,
                            "title": title,
                            "transcript_source": transcript_source,
                            "transcript_chars": len(transcript_text),
                        }
                    ),
                    detail=f"Video {i}/{len(videos)} analyzed ({transcript_source or 'transcript'}).",
                    progress={
                        "step": 4,
                        "total_steps": 5,
                        "ratio": 0.35 + (0.45 * i / max(1, len(videos))),
                    },
                )
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        await context.application.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status.message_id,
            text=with_tg_time(f"{status_title}\nStep 5/5: Comparing patterns across videos"),
            disable_web_page_preview=True,
        )
        await _emit_progress(
            "comparing",
            compared_video_count=len(processed),
            detail=f"Comparing insights across {len(processed)} videos.",
            progress={"step": 5, "total_steps": 5, "ratio": 0.9},
        )

        saved_videos = load_research_videos(run_id) if persist and run_id else processed
        saved_facts = load_research_video_facts(run_id) if persist and run_id else facts_memory
        topics = await asyncio.to_thread(_extract_research_topics, goal, intent, saved_facts, _mark_llm_backend)
        related = load_related_public_topics([str(t.get("tag") or "") for t in topics], exclude_run_id=run_id, limit=10)

        report, summary = await asyncio.to_thread(
            _build_comparison_report,
            goal,
            saved_videos,
            saved_facts,
            _mark_llm_backend,
        )
        if related:
            report += "\n\n🔎 Related Areas You May Explore\n"
            for item in related[:8]:
                report += f"\n• {item.get('tag')} (seen in {int(item.get('run_count') or 0)} public researches)"
        if persist and run_id:
            report += f"\n\n🌐 Public research ID: {run_id}\nUse /research_view {run_id} in bot or open it in Web UI."
        else:
            report += "\n\n🔒 Private mode: this research was not saved."
        if persist and run_id:
            save_research_topics(run_id, topics)
            finalize_research_run(
                run_id=run_id,
                status="completed",
                report_text=report,
                summary={
                    "queries": queries,
                    "intent": intent,
                    "topics": topics,
                    "related_areas": related,
                    "comparison": summary,
                    "video_count": len(saved_videos),
                },
            )
        if on_report:
            try:
                maybe = on_report(report, run_id)
                if asyncio.iscoroutine(maybe):
                    await maybe
            except Exception:
                pass
        await _emit_progress(
            "completed",
            run_id=run_id,
            is_public=bool(run_id),
            report_text=report,
            summary=summary,
            detail=f"Completed with {len(saved_videos)} analyzed videos.",
            progress={"step": 5, "total_steps": 5, "ratio": 1.0},
        )

        try:
            await context.application.bot.delete_message(chat_id=chat_id, message_id=status.message_id)
        except Exception:
            pass
        await _send_long_message(context, chat_id, report)
        return run_id
    except Exception as exc:
        if persist and run_id:
            try:
                finalize_research_run(
                    run_id=run_id,
                    status="failed",
                    report_text=f"Research failed: {str(exc)}",
                    summary={},
                )
            except Exception:
                pass
        try:
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"❌ Research failed:\n{str(exc)[:1200]}"),
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        await _emit_progress(
            "failed",
            error=str(exc),
            run_id=run_id,
            is_public=bool(run_id),
            search_stats=last_video_stats,
            query_stats=(last_video_stats.get("query_stats") if isinstance(last_video_stats, dict) else []),
            detail=str(exc),
            progress={"step": 5, "total_steps": 5, "ratio": 1.0},
        )
        return run_id


async def run_knowledge_juice(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    topic_text: str,
    persist: bool = True,
    on_report: Callable[[str, str], Awaitable[None] | None] | None = None,
    on_progress: Callable[[dict], Awaitable[None] | None] | None = None,
    per_query_override: Optional[int] = None,
    max_queries_override: Optional[int] = None,
    max_videos_override: Optional[int] = None,
    min_duration_sec: int = 0,
    max_duration_sec: int = 0,
    captions_only: bool = False,
) -> str:
    topic = re.sub(r"\s+", " ", (topic_text or "").strip())
    if not topic:
        raise RuntimeError("knowledge topic is empty")
    goal = build_knowledge_juice_goal(topic)
    return await run_market_research(
        context,
        chat_id=chat_id,
        goal_text=goal,
        persist=persist,
        status_title="🧃 Knowledge Juice",
        run_kind="knowledge_juice",
        on_report=on_report,
        on_progress=on_progress,
        per_query_override=per_query_override,
        max_queries_override=max_queries_override,
        max_videos_override=max_videos_override,
        min_duration_sec=min_duration_sec,
        max_duration_sec=max_duration_sec,
        captions_only=captions_only,
    )
