from __future__ import annotations

import json
from typing import List, Optional

from .core import _connect, ensure_db_ready


def save_transcript_qa_entry(
    *,
    video_id: str,
    transcript_path: str,
    question: str,
    answer: str,
    source: str = "bot",
    chat_id: Optional[int] = None,
    lang: str = "",
    extra: Optional[dict] = None,
) -> None:
    ensure_db_ready()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transcript_qa_history
                (video_id, transcript_path, question, answer, source, chat_id, lang, extra_json, asked_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                """,
                (
                    str(video_id or "").strip(),
                    str(transcript_path or "").strip(),
                    str(question or "").strip(),
                    str(answer or "").strip(),
                    str(source or "bot").strip() or "bot",
                    int(chat_id) if chat_id is not None else None,
                    str(lang or "").strip(),
                    json.dumps(extra or {}, ensure_ascii=False),
                ),
            )
        conn.commit()


def load_recent_searches(limit: int = 15) -> List[dict]:
    ensure_db_ready()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT video_id, question, source, chat_id, extra_json, asked_at
                    FROM transcript_qa_history
                    ORDER BY asked_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        result = []
        for row in rows:
            extra = row[4] if isinstance(row[4], dict) else {}
            result.append(
                {
                    "video_id": row[0] or "",
                    "question": row[1] or "",
                    "source": row[2] or "",
                    "chat_id": row[3],
                    "title": extra.get("title", ""),
                    "url": extra.get("url", ""),
                    "asked_at": row[5],
                }
            )
        return result
    except Exception:
        return []
