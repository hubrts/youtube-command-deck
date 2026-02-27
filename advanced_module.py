from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest


def _iter_suite_cases(suite: unittest.TestSuite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _iter_suite_cases(item)
        else:
            yield item


def normalize_component(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"web", "ui", "frontend", "website"}:
        return "web"
    if raw in {"tg", "telegram", "bot", "chatbot"}:
        return "tg"
    return "all"


def component_pattern(component: str) -> str:
    selected = normalize_component(component)
    if selected == "web":
        return "test_web*.py"
    if selected == "tg":
        return "test_tg*.py"
    return "test_*.py"


def discover_test_ids(test_root: Path, pattern: str = "test_*.py") -> list[str]:
    root = Path(test_root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        return []
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(root), pattern=str(pattern or "test_*.py"))
    out = []
    for case in _iter_suite_cases(suite):
        try:
            out.append(str(case.id()))
        except Exception:
            continue
    return out


def test_case_label(test_id: str) -> str:
    raw = str(test_id or "").strip()
    if not raw:
        return "test"
    parts = raw.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return raw


def build_test_case_rows(test_ids: list[str], *, max_items: int = 400) -> list[dict]:
    rows = []
    for idx, test_id in enumerate(list(test_ids or [])[: max(1, int(max_items))]):
        tid = str(test_id or "").strip()
        if not tid:
            continue
        rows.append(
            {
                "test_id": tid,
                "label": test_case_label(tid),
                "index": idx + 1,
                "status": "pending",
            }
        )
    return rows


def update_test_case_status(rows: list[dict], test_id: str, status: str) -> bool:
    tid = str(test_id or "").strip()
    if not tid:
        return False
    st = str(status or "").strip().lower() or "pending"
    for row in rows or []:
        if str(row.get("test_id") or "").strip() != tid:
            continue
        if str(row.get("status") or "").strip().lower() == st:
            return False
        row["status"] = st
        return True
    return False


def summarize_metrics(
    *,
    total: int,
    completed: int,
    passed: int,
    failed: int,
    errors: int,
    skipped: int,
    duration_sec: float,
) -> dict:
    total_n = max(0, int(total or 0))
    completed_n = max(0, int(completed or 0))
    passed_n = max(0, int(passed or 0))
    failed_n = max(0, int(failed or 0))
    error_n = max(0, int(errors or 0))
    skipped_n = max(0, int(skipped or 0))
    duration = max(0.0, float(duration_sec or 0.0))

    progress_pct = round((completed_n / total_n) * 100.0, 2) if total_n > 0 else 0.0
    pass_rate_pct = round((passed_n / completed_n) * 100.0, 2) if completed_n > 0 else 0.0
    failure_rate_pct = round(((failed_n + error_n) / completed_n) * 100.0, 2) if completed_n > 0 else 0.0
    tests_per_sec = round((completed_n / duration), 3) if duration > 0 else 0.0
    avg_test_ms = round((duration * 1000.0 / completed_n), 2) if completed_n > 0 else 0.0
    remaining = max(0, total_n - completed_n)

    return {
        "total": total_n,
        "completed": completed_n,
        "remaining": remaining,
        "passed": passed_n,
        "failed": failed_n,
        "errors": error_n,
        "skipped": skipped_n,
        "duration_sec": round(duration, 3),
        "progress_pct": progress_pct,
        "pass_rate_pct": pass_rate_pct,
        "failure_rate_pct": failure_rate_pct,
        "tests_per_sec": tests_per_sec,
        "avg_test_ms": avg_test_ms,
    }


def technology_stack() -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    ui_part = [
        {"name": "Python 3", "details": "web_app.py HTTP API + orchestration"},
        {"name": "ThreadingHTTPServer", "details": "built-in HTTP server for SPA and API"},
        {"name": "JavaScript ES Modules", "details": "single-page UI in web/js/*.js"},
        {"name": "HTML5 + CSS3", "details": "responsive multi-module UI"},
        {"name": "WebSocket (optional)", "details": "live brewing updates via websockets package"},
        {"name": "OpenAPI + Swagger", "details": "schema served from /api/openapi.json and /swagger"},
        {"name": "Component Test Runner", "details": "unittest discovery + live progress via /api/component_tests/*"},
    ]
    be_side = [
        {"name": "python-telegram-bot", "details": "command routing and chat interactions"},
        {"name": "asyncio", "details": "non-blocking bot flows and background tasks"},
        {"name": "yt-dlp pipeline", "details": "direct links, downloads, transcript/audio intake"},
        {"name": "Transcript Maker (YouTube captions)", "details": "yt-dlp caption extraction, VTT parsing, English-preferred selection"},
        {"name": "Transcript Maker (audio STT)", "details": "faster-whisper local transcription fallback on CPU"},
        {"name": "Transcript Analyzer", "details": "video_notes.py runs analysis, Q&A, and citation checks on saved transcripts"},
        {"name": "Local LLM (Ollama)", "details": "VIDEO_LOCAL_LLM_URL + VIDEO_LOCAL_LLM_MODEL for analysis/Q&A"},
        {"name": "Remote LLM (OpenAI)", "details": "Chat Completions + embeddings (gpt-4.1-mini, text-embedding-3-small defaults)"},
        {"name": "Remote LLM (Anthropic Claude)", "details": "Messages API with fallback/rate-limit controls"},
        {"name": "Knowledge Juice Analyzer", "details": "market_research.py compares videos, extracts facts, and builds topic reports"},
        {"name": "Embeddings + Semantic Retrieval", "details": "OpenAI/Ollama embeddings + transcript chunk ranking"},
        {"name": "PostgreSQL + pgvector (optional)", "details": "state_store persists index/research/QA and vector search tables"},
        {"name": "Shared local state", "details": "index/transcript/research files reused by web and TG"},
    ]
    return {
        "generated_at": now_iso,
        "ui_part": ui_part,
        "be_side": be_side,
        # Backward compatibility for existing clients/tests.
        "web": list(ui_part),
        "tg_chatbot": list(be_side),
    }
