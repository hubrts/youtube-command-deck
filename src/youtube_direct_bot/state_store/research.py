from __future__ import annotations

import json
import re
import uuid
from typing import Dict, List, Optional, Tuple

from .core import _connect, ensure_db_ready


def create_research_run(*, chat_id: int, goal_text: str, intent: dict, is_public: bool = True) -> str:
    ensure_db_ready()
    run_id = uuid.uuid4().hex
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_runs
                (run_id, chat_id, goal_text, is_public, intent_json, status, report_text, summary_json, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, 'running', '', '{}'::jsonb, NOW(), NOW())
                """,
                (
                    run_id,
                    int(chat_id),
                    (goal_text or "").strip(),
                    bool(is_public),
                    json.dumps(intent or {}, ensure_ascii=False),
                ),
            )
        conn.commit()
    return run_id


def save_research_videos(run_id: str, videos: List[dict]) -> None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM research_videos WHERE run_id = %s", (rid,))
            if videos:
                cur.executemany(
                    """
                    INSERT INTO research_videos
                    (run_id, video_id, rank, url, title, channel, view_count, published_utc, popularity_score,
                     transcript_path, transcript_source, transcript_chars, meta_json, created_at, updated_at)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, '', '', 0, %s::jsonb, NOW(), NOW())
                    """,
                    [
                        (
                            rid,
                            str(v.get("video_id") or "").strip(),
                            int(v.get("rank") or 0),
                            str(v.get("url") or "").strip(),
                            str(v.get("title") or "").strip(),
                            str(v.get("channel") or "").strip(),
                            int(v.get("view_count") or 0),
                            str(v.get("published_utc") or "").strip(),
                            float(v.get("popularity_score") or 0.0),
                            json.dumps(v.get("meta") or {}, ensure_ascii=False),
                        )
                        for v in videos
                        if str(v.get("video_id") or "").strip()
                    ],
                )
        conn.commit()


def save_research_video_transcript(
    *,
    run_id: str,
    video_id: str,
    transcript_path: str,
    transcript_source: str,
    transcript_chars: int,
) -> None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    vid = (video_id or "").strip()
    if not rid or not vid:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research_videos
                SET transcript_path = %s,
                    transcript_source = %s,
                    transcript_chars = %s,
                    updated_at = NOW()
                WHERE run_id = %s AND video_id = %s
                """,
                (
                    (transcript_path or "").strip(),
                    (transcript_source or "").strip(),
                    max(0, int(transcript_chars or 0)),
                    rid,
                    vid,
                ),
            )
        conn.commit()


def save_research_video_fact(*, run_id: str, video_id: str, facts: dict) -> None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    vid = (video_id or "").strip()
    if not rid or not vid:
        return

    story_raw = facts.get("is_owner_story")
    is_owner_story: Optional[bool] = None
    if isinstance(story_raw, bool):
        is_owner_story = story_raw
    elif isinstance(story_raw, (int, float)):
        is_owner_story = bool(story_raw)
    elif isinstance(story_raw, str):
        val = story_raw.strip().lower()
        if val in ("true", "yes", "1", "owner_story", "owner", "y"):
            is_owner_story = True
        elif val in ("false", "no", "0", "n"):
            is_owner_story = False

    try:
        confidence = float(facts.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    business_model = str(facts.get("business_model") or "").strip()[:300]

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_video_facts
                (run_id, video_id, is_owner_story, confidence, business_model, facts_json, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
                ON CONFLICT (run_id, video_id) DO UPDATE
                SET is_owner_story = EXCLUDED.is_owner_story,
                    confidence = EXCLUDED.confidence,
                    business_model = EXCLUDED.business_model,
                    facts_json = EXCLUDED.facts_json,
                    updated_at = NOW()
                """,
                (
                    rid,
                    vid,
                    is_owner_story,
                    confidence,
                    business_model,
                    json.dumps(facts or {}, ensure_ascii=False),
                ),
            )
        conn.commit()


def load_research_videos(run_id: str) -> List[dict]:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return []

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT video_id, rank, url, title, channel, view_count, published_utc,
                       popularity_score, transcript_path, transcript_source, transcript_chars, meta_json
                FROM research_videos
                WHERE run_id = %s
                ORDER BY rank ASC, popularity_score DESC
                """,
                (rid,),
            )
            rows = cur.fetchall()

    out: List[dict] = []
    for row in rows:
        meta = row[11]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        out.append(
            {
                "video_id": str(row[0] or ""),
                "rank": int(row[1] or 0),
                "url": str(row[2] or ""),
                "title": str(row[3] or ""),
                "channel": str(row[4] or ""),
                "view_count": int(row[5] or 0),
                "published_utc": str(row[6] or ""),
                "popularity_score": float(row[7] or 0.0),
                "transcript_path": str(row[8] or ""),
                "transcript_source": str(row[9] or ""),
                "transcript_chars": int(row[10] or 0),
                "meta": meta,
            }
        )
    return out


def load_research_video_facts(run_id: str) -> List[dict]:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return []

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT video_id, is_owner_story, confidence, business_model, facts_json
                FROM research_video_facts
                WHERE run_id = %s
                ORDER BY video_id ASC
                """,
                (rid,),
            )
            rows = cur.fetchall()

    out: List[dict] = []
    for vid, is_owner_story, confidence, business_model, raw in rows:
        facts = raw
        if isinstance(facts, str):
            try:
                facts = json.loads(facts)
            except Exception:
                facts = {}
        if not isinstance(facts, dict):
            facts = {}
        out.append(
            {
                "video_id": str(vid or ""),
                "is_owner_story": None if is_owner_story is None else bool(is_owner_story),
                "confidence": float(confidence or 0.0),
                "business_model": str(business_model or ""),
                "facts": facts,
            }
        )
    return out


def finalize_research_run(
    *,
    run_id: str,
    status: str,
    report_text: str,
    summary: Optional[dict] = None,
) -> None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research_runs
                SET status = %s,
                    report_text = %s,
                    summary_json = %s::jsonb,
                    updated_at = NOW()
                WHERE run_id = %s
                """,
                (
                    (status or "completed").strip(),
                    (report_text or "").strip(),
                    json.dumps(summary or {}, ensure_ascii=False),
                    rid,
                ),
            )
        conn.commit()


def save_research_topics(run_id: str, topics: List[dict | str]) -> None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return

    normalized: List[Tuple[str, float]] = []
    seen = set()
    for item in topics or []:
        if isinstance(item, dict):
            tag = str(item.get("tag") or item.get("topic") or "").strip().lower()
            try:
                weight = float(item.get("weight") or 1.0)
            except Exception:
                weight = 1.0
        else:
            tag = str(item or "").strip().lower()
            weight = 1.0

        tag = re.sub(r"\s+", " ", tag)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized.append((tag[:120], max(0.0, min(1.0, weight))))

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM research_run_topics WHERE run_id = %s", (rid,))
            if normalized:
                cur.executemany(
                    """
                    INSERT INTO research_run_topics (run_id, topic_tag, weight, created_at)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    [(rid, tag, weight) for tag, weight in normalized],
                )
        conn.commit()


def load_public_research_runs(limit: int = 50) -> List[dict]:
    ensure_db_ready()
    lim = max(1, int(limit))

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, chat_id, goal_text, status, report_text, summary_json, intent_json, created_at, updated_at
                FROM research_runs
                WHERE is_public = TRUE
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT run_id, topic_tag, weight
                FROM research_run_topics
                WHERE run_id IN (
                    SELECT run_id FROM research_runs WHERE is_public = TRUE ORDER BY created_at DESC LIMIT %s
                )
                ORDER BY weight DESC, topic_tag ASC
                """,
                (lim,),
            )
            topic_rows = cur.fetchall()

    topics_by_run: Dict[str, List[dict]] = {}
    for run_id, topic_tag, weight in topic_rows:
        rid = str(run_id or "")
        topics_by_run.setdefault(rid, []).append(
            {"tag": str(topic_tag or ""), "weight": float(weight or 0.0)}
        )

    out: List[dict] = []
    for run_id, chat_id, goal_text, status, report_text, summary_json, intent_json, created_at, updated_at in rows:
        summary = summary_json
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except Exception:
                summary = {}
        if not isinstance(summary, dict):
            summary = {}

        intent = intent_json
        if isinstance(intent, str):
            try:
                intent = json.loads(intent)
            except Exception:
                intent = {}
        if not isinstance(intent, dict):
            intent = {}

        rid = str(run_id or "")
        created_iso = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or "")
        updated_iso = updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or "")
        out.append(
            {
                "run_id": rid,
                "chat_id": int(chat_id or 0),
                "goal_text": str(goal_text or ""),
                "status": str(status or ""),
                "report_excerpt": str(report_text or "")[:700],
                "summary": summary,
                "intent": intent,
                "topics": topics_by_run.get(rid, [])[:10],
                "created_at": created_iso,
                "updated_at": updated_iso,
            }
        )
    return out


def get_public_research_run(run_id: str) -> dict | None:
    ensure_db_ready()
    rid = (run_id or "").strip()
    if not rid:
        return None

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, chat_id, goal_text, status, report_text, summary_json, intent_json, created_at, updated_at
                FROM research_runs
                WHERE run_id = %s AND is_public = TRUE
                """,
                (rid,),
            )
            row = cur.fetchone()
            if not row:
                return None

            cur.execute(
                """
                SELECT topic_tag, weight
                FROM research_run_topics
                WHERE run_id = %s
                ORDER BY weight DESC, topic_tag ASC
                """,
                (rid,),
            )
            topic_rows = cur.fetchall()

            cur.execute(
                """
                SELECT video_id, rank, url, title, channel, view_count, published_utc, popularity_score,
                       transcript_path, transcript_source, transcript_chars
                FROM research_videos
                WHERE run_id = %s
                ORDER BY rank ASC, popularity_score DESC
                """,
                (rid,),
            )
            video_rows = cur.fetchall()

    run_id_val, chat_id, goal_text, status, report_text, summary_json, intent_json, created_at, updated_at = row
    summary = summary_json
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}
    if not isinstance(summary, dict):
        summary = {}

    intent = intent_json
    if isinstance(intent, str):
        try:
            intent = json.loads(intent)
        except Exception:
            intent = {}
    if not isinstance(intent, dict):
        intent = {}

    return {
        "run_id": str(run_id_val or ""),
        "chat_id": int(chat_id or 0),
        "goal_text": str(goal_text or ""),
        "status": str(status or ""),
        "report_text": str(report_text or ""),
        "summary": summary,
        "intent": intent,
        "topics": [
            {"tag": str(tag or ""), "weight": float(weight or 0.0)}
            for tag, weight in topic_rows
        ],
        "videos": [
            {
                "video_id": str(v[0] or ""),
                "rank": int(v[1] or 0),
                "url": str(v[2] or ""),
                "title": str(v[3] or ""),
                "channel": str(v[4] or ""),
                "view_count": int(v[5] or 0),
                "published_utc": str(v[6] or ""),
                "popularity_score": float(v[7] or 0.0),
                "transcript_path": str(v[8] or ""),
                "transcript_source": str(v[9] or ""),
                "transcript_chars": int(v[10] or 0),
            }
            for v in video_rows
        ],
        "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
        "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or ""),
    }


def load_related_public_topics(
    base_topics: List[str],
    *,
    exclude_run_id: str = "",
    limit: int = 12,
) -> List[dict]:
    ensure_db_ready()
    tags = [
        re.sub(r"\s+", " ", str(t or "").strip().lower())
        for t in (base_topics or [])
        if str(t or "").strip()
    ]
    tags = list(dict.fromkeys(tags))
    if not tags:
        return []

    lim = max(1, int(limit))
    excl = (exclude_run_id or "").strip()

    with _connect() as conn:
        with conn.cursor() as cur:
            if excl:
                cur.execute(
                    """
                    WITH matched_runs AS (
                        SELECT DISTINCT rt.run_id
                        FROM research_run_topics rt
                        JOIN research_runs rr ON rr.run_id = rt.run_id
                        WHERE rr.is_public = TRUE
                          AND rr.run_id <> %s
                          AND rt.topic_tag = ANY(%s)
                    )
                    SELECT rt.topic_tag,
                           COUNT(DISTINCT rt.run_id) AS run_count,
                           MAX(rt.weight) AS max_weight
                    FROM research_run_topics rt
                    JOIN matched_runs mr ON mr.run_id = rt.run_id
                    WHERE rt.topic_tag <> ALL(%s)
                    GROUP BY rt.topic_tag
                    ORDER BY run_count DESC, max_weight DESC, rt.topic_tag ASC
                    LIMIT %s
                    """,
                    (excl, tags, tags, lim),
                )
            else:
                cur.execute(
                    """
                    WITH matched_runs AS (
                        SELECT DISTINCT rt.run_id
                        FROM research_run_topics rt
                        JOIN research_runs rr ON rr.run_id = rt.run_id
                        WHERE rr.is_public = TRUE
                          AND rt.topic_tag = ANY(%s)
                    )
                    SELECT rt.topic_tag,
                           COUNT(DISTINCT rt.run_id) AS run_count,
                           MAX(rt.weight) AS max_weight
                    FROM research_run_topics rt
                    JOIN matched_runs mr ON mr.run_id = rt.run_id
                    WHERE rt.topic_tag <> ALL(%s)
                    GROUP BY rt.topic_tag
                    ORDER BY run_count DESC, max_weight DESC, rt.topic_tag ASC
                    LIMIT %s
                    """,
                    (tags, tags, lim),
                )
            rows = cur.fetchall()

    return [
        {
            "tag": str(tag or ""),
            "run_count": int(run_count or 0),
            "max_weight": float(max_weight or 0.0),
        }
        for tag, run_count, max_weight in rows
    ]
