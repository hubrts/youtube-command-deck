from __future__ import annotations

import json
from typing import List, Tuple

from ytbot_config import STATE_DB_REQUIRE_PGVECTOR

from .core import _connect, _vector_literal, ensure_db_ready


def save_transcript_chunks(
    *,
    video_id: str,
    content_hash: str,
    chunks: List[dict],
) -> None:
    if not STATE_DB_REQUIRE_PGVECTOR:
        return
    vid = (video_id or "").strip()
    if not vid:
        return

    ensure_db_ready()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transcript_chunks WHERE video_id = %s", (vid,))
            if chunks:
                cur.executemany(
                    """
                    INSERT INTO transcript_chunks (video_id, chunk_idx, content_hash, chunk_json, updated_at)
                    VALUES (%s, %s, %s, %s::jsonb, NOW())
                    """,
                    [
                        (
                            vid,
                            int(chunk.get("idx") or i),
                            content_hash or "",
                            json.dumps(chunk, ensure_ascii=False),
                        )
                        for i, chunk in enumerate(chunks)
                    ],
                )
        conn.commit()


def load_transcript_chunks(video_id: str) -> List[dict]:
    if not STATE_DB_REQUIRE_PGVECTOR:
        return []
    vid = (video_id or "").strip()
    if not vid:
        return []

    ensure_db_ready()
    out: List[dict] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chunk_json
                FROM transcript_chunks
                WHERE video_id = %s
                ORDER BY chunk_idx ASC
                """,
                (vid,),
            )
            rows = cur.fetchall()

    for row in rows:
        raw = row[0]
        if isinstance(raw, dict):
            out.append(raw)
        elif isinstance(raw, str):
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                pass
    return out


def get_transcript_embedding_meta(video_id: str, model: str) -> Tuple[str, int]:
    if not STATE_DB_REQUIRE_PGVECTOR:
        return "", 0
    vid = (video_id or "").strip()
    model_name = (model or "").strip()
    if not vid or not model_name:
        return "", 0

    ensure_db_ready()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(content_hash), ''), COUNT(*)
                FROM transcript_chunk_embeddings
                WHERE video_id = %s AND model = %s
                """,
                (vid, model_name),
            )
            row = cur.fetchone() or ("", 0)
    return str(row[0] or ""), int(row[1] or 0)


def save_transcript_chunk_embeddings(
    *,
    video_id: str,
    model: str,
    content_hash: str,
    vectors: List[Tuple[int, List[float]]],
) -> None:
    if not STATE_DB_REQUIRE_PGVECTOR:
        return
    vid = (video_id or "").strip()
    model_name = (model or "").strip()
    if not vid or not model_name:
        return

    ensure_db_ready()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transcript_chunk_embeddings WHERE video_id = %s AND model = %s",
                (vid, model_name),
            )
            if vectors:
                cur.executemany(
                    """
                    INSERT INTO transcript_chunk_embeddings
                    (video_id, chunk_idx, model, content_hash, embedding, updated_at)
                    VALUES (%s, %s, %s, %s, %s::vector, NOW())
                    """,
                    [
                        (
                            vid,
                            int(idx),
                            model_name,
                            content_hash or "",
                            _vector_literal(vec),
                        )
                        for idx, vec in vectors
                        if vec
                    ],
                )
        conn.commit()


def search_transcript_chunks_semantic(
    *,
    video_id: str,
    model: str,
    query_vector: List[float],
    limit: int = 12,
) -> List[Tuple[int, float]]:
    if not STATE_DB_REQUIRE_PGVECTOR:
        return []
    vid = (video_id or "").strip()
    model_name = (model or "").strip()
    if not vid or not model_name or not query_vector:
        return []

    ensure_db_ready()
    lim = max(1, int(limit))
    vector_lit = _vector_literal(query_vector)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chunk_idx, (1 - (embedding <=> %s::vector)) AS similarity
                FROM transcript_chunk_embeddings
                WHERE video_id = %s AND model = %s
                ORDER BY embedding <=> %s::vector ASC
                LIMIT %s
                """,
                (vector_lit, vid, model_name, vector_lit, lim),
            )
            rows = cur.fetchall()

    out: List[Tuple[int, float]] = []
    for idx, sim in rows:
        try:
            out.append((int(idx), float(sim)))
        except Exception:
            continue
    return out
