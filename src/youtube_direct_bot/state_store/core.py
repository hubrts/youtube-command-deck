from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, List, Set

from ytbot_config import (
    CHATS_FILE,
    INDEX_FILE,
    STATE_DB_DSN,
    STATE_DB_REQUIRE_PGVECTOR,
    VIDEO_EMBED_DIM,
)

try:
    import psycopg
except Exception:
    psycopg = None


MIGRATION_FLAG_KEY = "json_migrated_v1"
_INIT_LOCK = threading.Lock()
_DB_READY = False


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return default


def _require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError(
            "PostgreSQL backend requires psycopg. Install it: pip install 'psycopg[binary]'"
        )


def _connect():
    _require_psycopg()
    if not STATE_DB_DSN:
        raise RuntimeError(
            "STATE_DB_DSN is empty. Set PostgreSQL DSN, for example: "
            "postgresql://user:password@127.0.0.1:5432/youtube_bot"
        )
    return psycopg.connect(STATE_DB_DSN)


def _ensure_schema(conn) -> None:
    if VIDEO_EMBED_DIM <= 0:
        raise RuntimeError("VIDEO_EMBED_DIM must be a positive integer.")

    with conn.cursor() as cur:
        if STATE_DB_REQUIRE_PGVECTOR:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            except Exception as exc:
                raise RuntimeError(
                    "Could not enable pgvector extension. Install pgvector in PostgreSQL "
                    "or set STATE_DB_REQUIRE_PGVECTOR=0."
                ) from exc

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS known_chats (
                chat_id BIGINT PRIMARY KEY,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_index (
                video_id TEXT PRIMARY KEY,
                record JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_meta (
                key TEXT PRIMARY KEY,
                value_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_runs (
                run_id TEXT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                goal_text TEXT NOT NULL DEFAULT '',
                is_public BOOLEAN NOT NULL DEFAULT TRUE,
                intent_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                status TEXT NOT NULL DEFAULT 'running',
                report_text TEXT NOT NULL DEFAULT '',
                summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_videos (
                run_id TEXT NOT NULL REFERENCES research_runs(run_id) ON DELETE CASCADE,
                video_id TEXT NOT NULL,
                rank INTEGER NOT NULL DEFAULT 0,
                url TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                channel TEXT NOT NULL DEFAULT '',
                view_count BIGINT NOT NULL DEFAULT 0,
                published_utc TEXT NOT NULL DEFAULT '',
                popularity_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                transcript_path TEXT NOT NULL DEFAULT '',
                transcript_source TEXT NOT NULL DEFAULT '',
                transcript_chars INTEGER NOT NULL DEFAULT 0,
                meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (run_id, video_id)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_videos_run_rank
            ON research_videos(run_id, rank)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_video_facts (
                run_id TEXT NOT NULL REFERENCES research_runs(run_id) ON DELETE CASCADE,
                video_id TEXT NOT NULL,
                is_owner_story BOOLEAN,
                confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                business_model TEXT NOT NULL DEFAULT '',
                facts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (run_id, video_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_qa_history (
                id BIGSERIAL PRIMARY KEY,
                video_id TEXT NOT NULL DEFAULT '',
                transcript_path TEXT NOT NULL DEFAULT '',
                question TEXT NOT NULL DEFAULT '',
                answer TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'bot',
                chat_id BIGINT,
                lang TEXT NOT NULL DEFAULT '',
                extra_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                asked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transcript_qa_video_time
            ON transcript_qa_history(video_id, asked_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transcript_qa_source_time
            ON transcript_qa_history(source, asked_at DESC)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_run_topics (
                run_id TEXT NOT NULL REFERENCES research_runs(run_id) ON DELETE CASCADE,
                topic_tag TEXT NOT NULL,
                weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (run_id, topic_tag)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_topics_tag
            ON research_run_topics(topic_tag)
            """
        )
        cur.execute("ALTER TABLE research_runs ADD COLUMN IF NOT EXISTS is_public BOOLEAN NOT NULL DEFAULT TRUE")
        cur.execute("ALTER TABLE research_video_facts ADD COLUMN IF NOT EXISTS is_owner_story BOOLEAN")
        cur.execute("ALTER TABLE research_video_facts ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE research_video_facts ADD COLUMN IF NOT EXISTS business_model TEXT NOT NULL DEFAULT ''")

        if STATE_DB_REQUIRE_PGVECTOR:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS video_embeddings (
                    video_id TEXT PRIMARY KEY REFERENCES archive_index(video_id) ON DELETE CASCADE,
                    embedding VECTOR({int(VIDEO_EMBED_DIM)}) NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    content_hash TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transcript_chunks (
                    video_id TEXT NOT NULL REFERENCES archive_index(video_id) ON DELETE CASCADE,
                    chunk_idx INTEGER NOT NULL,
                    content_hash TEXT NOT NULL DEFAULT '',
                    chunk_json JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (video_id, chunk_idx)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS transcript_chunk_embeddings (
                    video_id TEXT NOT NULL REFERENCES archive_index(video_id) ON DELETE CASCADE,
                    chunk_idx INTEGER NOT NULL,
                    model TEXT NOT NULL,
                    content_hash TEXT NOT NULL DEFAULT '',
                    embedding VECTOR({int(VIDEO_EMBED_DIM)}) NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (video_id, chunk_idx, model)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_video_model
                ON transcript_chunk_embeddings(video_id, model)
                """
            )


def _migrate_json_if_needed(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM bot_meta WHERE key = %s", (MIGRATION_FLAG_KEY,))
        if cur.fetchone():
            return

    chats = _load_json(CHATS_FILE, [])
    index = _load_json(INDEX_FILE, {})

    normalized_chats: Set[int] = set()
    for value in chats:
        try:
            normalized_chats.add(int(value))
        except Exception:
            pass

    normalized_index: Dict[str, dict] = {}
    if isinstance(index, dict):
        for raw_video_id, record in index.items():
            video_id = str(raw_video_id or "").strip()
            if not video_id:
                continue
            normalized_index[video_id] = record if isinstance(record, dict) else {}

    with conn.cursor() as cur:
        if normalized_chats:
            cur.executemany(
                """
                INSERT INTO known_chats (chat_id)
                VALUES (%s)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                [(chat_id,) for chat_id in sorted(normalized_chats)],
            )

        if normalized_index:
            cur.executemany(
                """
                INSERT INTO archive_index (video_id, record, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (video_id) DO UPDATE
                SET record = EXCLUDED.record,
                    updated_at = NOW()
                """,
                [
                    (video_id, json.dumps(record, ensure_ascii=False))
                    for video_id, record in normalized_index.items()
                ],
            )

        cur.execute(
            """
            INSERT INTO bot_meta (key, value_json, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value_json = EXCLUDED.value_json,
                updated_at = NOW()
            """,
            (
                MIGRATION_FLAG_KEY,
                json.dumps(
                    {
                        "known_chats_imported": len(normalized_chats),
                        "archive_items_imported": len(normalized_index),
                    }
                ),
            ),
        )


def ensure_db_ready() -> None:
    global _DB_READY
    if _DB_READY:
        return
    with _INIT_LOCK:
        if _DB_READY:
            return
        with _connect() as conn:
            _ensure_schema(conn)
            _migrate_json_if_needed(conn)
            conn.commit()
        _DB_READY = True


def load_known_chats() -> Set[int]:
    ensure_db_ready()
    out: Set[int] = set()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM known_chats")
            for row in cur.fetchall():
                try:
                    out.add(int(row[0]))
                except Exception:
                    pass
    return out


def save_known_chats(chats: Set[int]) -> None:
    ensure_db_ready()
    normalized_set: Set[int] = set()
    for value in chats:
        try:
            normalized_set.add(int(value))
        except Exception:
            pass
    normalized = sorted(normalized_set)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM known_chats")
            if normalized:
                cur.executemany(
                    "INSERT INTO known_chats (chat_id) VALUES (%s)",
                    [(chat_id,) for chat_id in normalized],
                )
        conn.commit()


def load_index() -> Dict[str, dict]:
    ensure_db_ready()
    out: Dict[str, dict] = {}
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT video_id, record FROM archive_index")
            for video_id, record in cur.fetchall():
                key = str(video_id or "").strip()
                if not key:
                    continue
                if isinstance(record, dict):
                    out[key] = record
                    continue
                if isinstance(record, str):
                    try:
                        parsed = json.loads(record)
                        out[key] = parsed if isinstance(parsed, dict) else {}
                    except Exception:
                        out[key] = {}
                    continue
                out[key] = {}
    return out


def save_index(index: Dict[str, dict]) -> None:
    ensure_db_ready()
    normalized: Dict[str, dict] = {}
    for raw_video_id, record in (index or {}).items():
        video_id = str(raw_video_id or "").strip()
        if not video_id:
            continue
        normalized[video_id] = record if isinstance(record, dict) else {}

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM archive_index")
            if normalized:
                cur.executemany(
                    """
                    INSERT INTO archive_index (video_id, record, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    """,
                    [
                        (video_id, json.dumps(record, ensure_ascii=False))
                        for video_id, record in normalized.items()
                    ],
                )
        conn.commit()


def _vector_literal(vector: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vector) + "]"
