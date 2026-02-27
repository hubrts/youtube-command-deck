#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import mimetypes
import os
import re
import shutil
import tempfile
import threading
import traceback
import unittest
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import perf_counter, time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from advanced_module import (
    build_test_case_rows,
    component_pattern,
    discover_test_ids,
    normalize_component,
    summarize_metrics,
    technology_stack,
    update_test_case_status,
)
from market_research import run_knowledge_juice as run_knowledge_juice_bot
from download_flow import run_download_flow
from video_notes import (
    _analysis_output_language_for_text,
    _analysis_ttl_seconds,
    _analyze_transcript_with_ai_with_progress,
    _download_audio,
    _download_youtube_caption_segments,
    _get_cached_ai_analysis,
    _save_caption_source,
    _save_full_transcript,
    _segments_to_transcript_text,
    _transcribe_segments,
    answer_question_from_transcript,
)
from ytbot_config import DATA_DIR, RETENTION_DAYS, STORAGE_DIR
from ytbot_state import (
    STATE,
    get_public_research_run,
    load_index,
    load_public_research_runs,
    request_live_stop,
    save_index,
    save_transcript_qa_entry,
)
from ytbot_ytdlp import yt_direct_audio_url, yt_direct_download_url, yt_info
from ytbot_utils import build_public_url, extract_youtube_id, now_local_str
from src.youtube_direct_bot.web.openapi import load_openapi_spec

try:
    import websockets
except Exception:
    websockets = None

try:
    from video_notes import _estimate_local_analysis_parts as _estimate_local_analysis_parts
except Exception:
    def _estimate_local_analysis_parts(transcript: str) -> int:
        _ = str(transcript or "")
        return 1

WEB_DIR = Path(__file__).resolve().parent / "web"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts"
CAPTIONS_DIR = DATA_DIR / "captions"
NOTES_EXPORTS_DIR = DATA_DIR / "notes_exports"
YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")
DEFAULT_BREW_WS_PORT = int((os.getenv("WEB_BREW_WS_PORT") or "8766").strip() or 8766)
DEFAULT_NO_CAPTION_MAX_DURATION_SEC = 10 * 60
QA_CACHE_LIMIT = 40

_RUNTIME: dict = {
    "ws_enabled": False,
    "ws_host": "127.0.0.1",
    "ws_port": DEFAULT_BREW_WS_PORT,
    "ws_path": "/ws",
}

_BREW_JOBS_LOCK = threading.Lock()
_BREW_JOBS: dict[str, dict] = {}
_COMPONENT_TEST_JOBS_LOCK = threading.Lock()
_COMPONENT_TEST_JOBS: dict[str, dict] = {}
_COMPONENT_TEST_LOG_LIMIT = 220
_COMPONENT_TEST_JOB_LIMIT = 24
_COMPONENT_TEST_CASE_LIMIT = 400
COMPONENT_TESTS_DIR = Path(__file__).resolve().parent / "tests"
_ANALYZE_PROGRESS_LOCK = threading.Lock()
_ANALYZE_PROGRESS: dict[str, dict] = {}
_ASK_PROGRESS_LOCK = threading.Lock()
_ASK_PROGRESS: dict[str, dict] = {}
_NOTES_TASK_LOCK = threading.Lock()
_NOTES_TASK_ACTIVE: set[str] = set()
_PUBLIC_FILE_INDEX_LOCK = threading.Lock()
_PUBLIC_FILE_INDEX = {"built_at": 0.0, "by_video": {}}
_DIRECT_SAVE_LOCK = threading.Lock()
_DIRECT_SAVE_ACTIVE: dict[str, dict] = {}


def _json_dumps(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clip_list(items, limit: int = 30):
    if not isinstance(items, list):
        return []
    return items[: max(1, int(limit))]


def _set_analyze_progress(video_id: str, **changes) -> dict:
    vid = str(video_id or "").strip()
    if not vid:
        return {}
    with _ANALYZE_PROGRESS_LOCK:
        prev = _ANALYZE_PROGRESS.get(vid) or {}
        merged = {**prev, **changes}
        merged["video_id"] = vid
        merged["updated_at"] = _utc_now_iso()
        _ANALYZE_PROGRESS[vid] = merged
        return dict(merged)


def _get_analyze_progress(video_id: str) -> dict:
    vid = str(video_id or "").strip()
    if not vid:
        return {}
    with _ANALYZE_PROGRESS_LOCK:
        row = _ANALYZE_PROGRESS.get(vid)
        return dict(row) if isinstance(row, dict) else {}


def _set_ask_progress(video_id: str, **changes) -> dict:
    vid = str(video_id or "").strip()
    if not vid:
        return {}
    with _ASK_PROGRESS_LOCK:
        prev = _ASK_PROGRESS.get(vid) or {}
        merged = {**prev, **changes}
        merged["video_id"] = vid
        merged["updated_at"] = _utc_now_iso()
        _ASK_PROGRESS[vid] = merged
        return dict(merged)


def _get_ask_progress(video_id: str) -> dict:
    vid = str(video_id or "").strip()
    if not vid:
        return {}
    with _ASK_PROGRESS_LOCK:
        row = _ASK_PROGRESS.get(vid)
        return dict(row) if isinstance(row, dict) else {}


def _notes_task_key(video_id: str, task: str) -> str:
    vid = str(video_id or "").strip()
    kind = str(task or "").strip().lower()
    return f"{kind}:{vid}" if vid and kind else ""


def _try_start_notes_task(video_id: str, task: str) -> bool:
    key = _notes_task_key(video_id, task)
    if not key:
        return False
    with _NOTES_TASK_LOCK:
        if key in _NOTES_TASK_ACTIVE:
            return False
        _NOTES_TASK_ACTIVE.add(key)
    return True


def _finish_notes_task(video_id: str, task: str) -> None:
    key = _notes_task_key(video_id, task)
    if not key:
        return
    with _NOTES_TASK_LOCK:
        _NOTES_TASK_ACTIVE.discard(key)


def _is_notes_task_running(video_id: str, task: str) -> bool:
    key = _notes_task_key(video_id, task)
    if not key:
        return False
    with _NOTES_TASK_LOCK:
        return key in _NOTES_TASK_ACTIVE


def _notes_progress(video_id: str) -> dict:
    vid = str(video_id or "").strip()
    if not vid:
        return {"video_id": "", "busy_task": "", "ask": {"in_progress": False}, "analyze": {"in_progress": False}}

    ask = _get_ask_progress(vid)
    analyze = _get_analyze_progress(vid)
    ask_running = _is_notes_task_running(vid, "ask")
    analyze_running = _is_notes_task_running(vid, "analyze")

    if ask_running:
        ask.setdefault("video_id", vid)
        ask["status"] = "running"
        ask["done"] = False
        ask.setdefault("message", "Asking transcript...")
    if analyze_running:
        analyze.setdefault("video_id", vid)
        analyze["status"] = "running"
        analyze["done"] = False
        analyze.setdefault("message", "Running analysis...")

    ask["in_progress"] = bool(ask_running)
    analyze["in_progress"] = bool(analyze_running)
    busy_task = "ask" if ask_running else ("analyze" if analyze_running else "")

    return {
        "video_id": vid,
        "busy_task": busy_task,
        "ask": ask,
        "analyze": analyze,
    }


def _public_file_index_by_video(*, ttl_sec: float = 20.0) -> dict[str, str]:
    now = time()
    with _PUBLIC_FILE_INDEX_LOCK:
        built_at = float(_PUBLIC_FILE_INDEX.get("built_at") or 0.0)
        by_video = _PUBLIC_FILE_INDEX.get("by_video")
        if (
            isinstance(by_video, dict)
            and by_video
            and (now - built_at) <= max(1.0, float(ttl_sec))
        ):
            return dict(by_video)

        resolved: dict[str, tuple[float, str]] = {}
        try:
            storage = Path(STORAGE_DIR).expanduser().resolve()
            if storage.exists() and storage.is_dir():
                for p in storage.iterdir():
                    if not p.is_file():
                        continue
                    name = str(p.name or "")
                    low = name.lower()
                    if low.endswith((".part", ".tmp", ".temp", ".ytdl", ".aria2")):
                        continue
                    vid = ""
                    m = re.search(r"\[([A-Za-z0-9_-]{6,20})\]", name)
                    if m:
                        vid = str(m.group(1) or "").strip()
                    else:
                        stem = str(Path(name).stem or "").strip()
                        if YT_ID_RE.match(stem):
                            vid = stem
                    if not vid:
                        continue
                    try:
                        mtime = float(p.stat().st_mtime)
                    except Exception:
                        mtime = 0.0
                    prev = resolved.get(vid)
                    if (not prev) or (mtime >= prev[0]):
                        resolved[vid] = (mtime, name)
        except Exception:
            resolved = {}

        plain_map = {vid: val[1] for vid, val in resolved.items()}
        _PUBLIC_FILE_INDEX["built_at"] = now
        _PUBLIC_FILE_INDEX["by_video"] = plain_map
        return dict(plain_map)


def _active_direct_save() -> dict:
    with _DIRECT_SAVE_LOCK:
        row = _DIRECT_SAVE_ACTIVE.get("job")
        if not isinstance(row, dict):
            return {}
        return dict(row)


def _job_snapshot(job: dict) -> dict:
    if not isinstance(job, dict):
        return {}
    return {
        "job_id": str(job.get("job_id") or ""),
        "topic": str(job.get("topic") or ""),
        "status": str(job.get("status") or ""),
        "private_run": bool(job.get("private_run")),
        "is_public": bool(job.get("is_public")),
        "created_at": str(job.get("created_at") or ""),
        "updated_at": str(job.get("updated_at") or ""),
        "run_id": str(job.get("run_id") or ""),
        "last_event_type": str(job.get("last_event_type") or ""),
        "progress_detail": str(job.get("progress_detail") or ""),
        "llm_backend": str(job.get("llm_backend") or ""),
        "progress": job.get("progress") or {},
        "config": job.get("config") or {},
        "total_candidates": int(job.get("total_candidates") or 0),
        "total_videos": int(job.get("total_videos") or 0),
        "current_index": int(job.get("current_index") or 0),
        "current_video": job.get("current_video") or {},
        "candidate_videos": _clip_list(job.get("candidate_videos") or [], limit=24),
        "reviewed_videos": _clip_list(job.get("reviewed_videos") or [], limit=36),
        "search_stats": job.get("search_stats") if isinstance(job.get("search_stats"), dict) else {},
        "query_stats": _clip_list(job.get("query_stats") or [], limit=20),
        "report_text": str(job.get("report_text") or ""),
        "error": str(job.get("error") or ""),
    }


def _list_brew_jobs(*, active_only: bool = False) -> list[dict]:
    with _BREW_JOBS_LOCK:
        jobs = list(_BREW_JOBS.values())
    if active_only:
        jobs = [j for j in jobs if str(j.get("status") or "") in ("queued", "running")]
    jobs.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return [_job_snapshot(j) for j in jobs]


def _component_label(component: str) -> str:
    comp = normalize_component(component)
    if comp == "web":
        return "UI part"
    if comp == "tg":
        return "BE side"
    return "UI + BE"


def _component_job_snapshot(job: dict) -> dict:
    if not isinstance(job, dict):
        return {}
    _refresh_component_job_metrics(job, now_perf=perf_counter())
    return {
        "job_id": str(job.get("job_id") or ""),
        "component": str(job.get("component") or "all"),
        "component_label": _component_label(str(job.get("component") or "all")),
        "status": str(job.get("status") or ""),
        "created_at": str(job.get("created_at") or ""),
        "updated_at": str(job.get("updated_at") or ""),
        "started_at": str(job.get("started_at") or ""),
        "finished_at": str(job.get("finished_at") or ""),
        "pattern": str(job.get("pattern") or ""),
        "current_test": str(job.get("current_test") or ""),
        "summary": str(job.get("summary") or ""),
        "error": str(job.get("error") or ""),
        "metrics": dict(job.get("metrics") or {}),
        "log_tail": _clip_list(list(job.get("log_tail") or []), limit=_COMPONENT_TEST_LOG_LIMIT),
        "test_cases": _clip_list(list(job.get("test_cases") or []), limit=_COMPONENT_TEST_CASE_LIMIT),
    }


def _list_component_test_jobs(*, active_only: bool = False) -> list[dict]:
    with _COMPONENT_TEST_JOBS_LOCK:
        jobs = list(_COMPONENT_TEST_JOBS.values())
    if active_only:
        jobs = [j for j in jobs if str(j.get("status") or "") in ("queued", "running")]
    jobs.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return [_component_job_snapshot(j) for j in jobs]


def _refresh_component_job_metrics(job: dict, *, now_perf: float | None = None) -> None:
    if not isinstance(job, dict):
        return
    started_perf = float(job.get("started_perf") or 0.0)
    finished_perf = float(job.get("finished_perf") or 0.0)
    duration = 0.0
    if finished_perf > 0:
        duration = max(0.0, finished_perf - started_perf)
    elif started_perf > 0:
        duration = max(0.0, float((now_perf or perf_counter())) - started_perf)

    metrics = summarize_metrics(
        total=int(job.get("total_tests") or 0),
        completed=int(job.get("completed_tests") or 0),
        passed=int(job.get("passed_tests") or 0),
        failed=int(job.get("failed_tests") or 0),
        errors=int(job.get("error_tests") or 0),
        skipped=int(job.get("skipped_tests") or 0),
        duration_sec=duration,
    )
    job["metrics"] = metrics


def _trim_component_test_jobs() -> None:
    with _COMPONENT_TEST_JOBS_LOCK:
        if len(_COMPONENT_TEST_JOBS) <= _COMPONENT_TEST_JOB_LIMIT:
            return
        all_jobs = list(_COMPONENT_TEST_JOBS.values())
        all_jobs.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
        keep_ids = {str(j.get("job_id") or "") for j in all_jobs[:_COMPONENT_TEST_JOB_LIMIT]}
        for jid in list(_COMPONENT_TEST_JOBS.keys()):
            if jid not in keep_ids:
                _COMPONENT_TEST_JOBS.pop(jid, None)


def _append_component_test_log(job_id: str, line: str) -> None:
    text = str(line or "").rstrip()
    if not text:
        return
    snap = None
    with _COMPONENT_TEST_JOBS_LOCK:
        job = _COMPONENT_TEST_JOBS.get(job_id)
        if not job:
            return
        logs = list(job.get("log_tail") or [])
        logs.append(text[:700])
        if len(logs) > _COMPONENT_TEST_LOG_LIMIT:
            logs = logs[-_COMPONENT_TEST_LOG_LIMIT :]
        job["log_tail"] = logs
        job["updated_at"] = _utc_now_iso()
        snap = _component_job_snapshot(job)
    if snap:
        _WS_HUB.broadcast({"type": "component_job_update", "job": snap})


def _update_component_test_job(job_id: str, **changes) -> dict | None:
    with _COMPONENT_TEST_JOBS_LOCK:
        job = _COMPONENT_TEST_JOBS.get(job_id)
        if not job:
            return None
        job.update(changes)
        _refresh_component_job_metrics(job)
        job["updated_at"] = _utc_now_iso()
        snap = _component_job_snapshot(job)
    _WS_HUB.broadcast({"type": "component_job_update", "job": snap})
    return snap


def _find_active_component_job(component: str) -> dict | None:
    comp = normalize_component(component)
    with _COMPONENT_TEST_JOBS_LOCK:
        jobs = list(_COMPONENT_TEST_JOBS.values())
    jobs.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    for job in jobs:
        if str(job.get("status") or "") not in ("queued", "running"):
            continue
        if normalize_component(str(job.get("component") or "")) == comp:
            return _component_job_snapshot(job)
    return None


class _LineCaptureStream:
    def __init__(self, line_cb) -> None:
        self._line_cb = line_cb
        self._buf = ""

    def write(self, data: str) -> int:
        txt = str(data or "")
        if not txt:
            return 0
        self._buf += txt
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            clean = line.rstrip("\r")
            if clean.strip():
                self._line_cb(clean)
        return len(txt)

    def flush(self) -> None:
        if self._buf.strip():
            self._line_cb(self._buf.rstrip("\r"))
        self._buf = ""

    def isatty(self) -> bool:
        return False


def _start_component_tests_job(component: str) -> dict:
    selected = normalize_component(component)
    active = _find_active_component_job(selected)
    if active:
        return active

    test_pattern = component_pattern(selected)
    test_ids = discover_test_ids(COMPONENT_TESTS_DIR, test_pattern)
    if not test_ids:
        raise RuntimeError(
            f"No tests discovered for component '{_component_label(selected)}' using pattern '{test_pattern}'."
        )

    job_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    job = {
        "job_id": job_id,
        "component": selected,
        "pattern": test_pattern,
        "status": "queued",
        "created_at": created_at,
        "updated_at": created_at,
        "started_at": "",
        "finished_at": "",
        "started_perf": 0.0,
        "finished_perf": 0.0,
        "current_test": "",
        "summary": "",
        "error": "",
        "total_tests": int(len(test_ids)),
        "completed_tests": 0,
        "passed_tests": 0,
        "failed_tests": 0,
        "error_tests": 0,
        "skipped_tests": 0,
        "metrics": summarize_metrics(
            total=len(test_ids),
            completed=0,
            passed=0,
            failed=0,
            errors=0,
            skipped=0,
            duration_sec=0.0,
        ),
        "log_tail": [],
        "test_cases": build_test_case_rows(test_ids, max_items=_COMPONENT_TEST_CASE_LIMIT),
    }
    with _COMPONENT_TEST_JOBS_LOCK:
        _COMPONENT_TEST_JOBS[job_id] = job
    _trim_component_test_jobs()
    _WS_HUB.broadcast({"type": "component_job_created", "job": _component_job_snapshot(job)})

    def _runner() -> None:
        stream = _LineCaptureStream(lambda line: _append_component_test_log(job_id, line))

        def _mark_case_started(test_id: str) -> None:
            snap = None
            with _COMPONENT_TEST_JOBS_LOCK:
                j = _COMPONENT_TEST_JOBS.get(job_id)
                if j:
                    rows = list(j.get("test_cases") or [])
                    update_test_case_status(rows, test_id, "running")
                    j["test_cases"] = rows
                    j["current_test"] = test_id
                    _refresh_component_job_metrics(j)
                    j["updated_at"] = _utc_now_iso()
                    snap = _component_job_snapshot(j)
            if snap:
                _WS_HUB.broadcast({"type": "component_job_update", "job": snap})
            _append_component_test_log(job_id, f"RUN {test_id}")

        def _mark_case_done(test_id: str, status: str) -> None:
            snap = None
            with _COMPONENT_TEST_JOBS_LOCK:
                j = _COMPONENT_TEST_JOBS.get(job_id)
                if not j:
                    return
                rows = list(j.get("test_cases") or [])
                update_test_case_status(rows, test_id, status)
                j["test_cases"] = rows
                j["completed_tests"] = int(j.get("completed_tests") or 0) + 1
                st = str(status or "").lower()
                if st == "passed":
                    j["passed_tests"] = int(j.get("passed_tests") or 0) + 1
                elif st == "failed":
                    j["failed_tests"] = int(j.get("failed_tests") or 0) + 1
                elif st == "error":
                    j["error_tests"] = int(j.get("error_tests") or 0) + 1
                elif st == "skipped":
                    j["skipped_tests"] = int(j.get("skipped_tests") or 0) + 1
                j["current_test"] = ""
                _refresh_component_job_metrics(j)
                j["updated_at"] = _utc_now_iso()
                snap = _component_job_snapshot(j)
            if snap:
                _WS_HUB.broadcast({"type": "component_job_update", "job": snap})
            _append_component_test_log(job_id, f"{status.upper():<7} {test_id}")

        def _result_class_factory():
            class _ProgressResult(unittest.TextTestResult):
                def startTest(self, test):
                    super().startTest(test)
                    _mark_case_started(str(test.id()))

                def addSuccess(self, test):
                    super().addSuccess(test)
                    _mark_case_done(str(test.id()), "passed")

                def addFailure(self, test, err):
                    super().addFailure(test, err)
                    _mark_case_done(str(test.id()), "failed")

                def addError(self, test, err):
                    super().addError(test, err)
                    _mark_case_done(str(test.id()), "error")

                def addSkip(self, test, reason):
                    super().addSkip(test, reason)
                    _mark_case_done(str(test.id()), "skipped")

                def addExpectedFailure(self, test, err):
                    super().addExpectedFailure(test, err)
                    _mark_case_done(str(test.id()), "failed")

                def addUnexpectedSuccess(self, test):
                    super().addUnexpectedSuccess(test)
                    _mark_case_done(str(test.id()), "failed")

            return _ProgressResult

        try:
            started_iso = _utc_now_iso()
            started_perf = perf_counter()
            _update_component_test_job(
                job_id,
                status="running",
                started_at=started_iso,
                started_perf=started_perf,
                summary=f"Running {len(test_ids)} tests for {_component_label(selected)}",
            )
            loader = unittest.TestLoader()
            suite = loader.discover(start_dir=str(COMPONENT_TESTS_DIR), pattern=test_pattern)
            runner = unittest.TextTestRunner(stream=stream, verbosity=2, resultclass=_result_class_factory())
            result = runner.run(suite)
            finished_perf = perf_counter()
            status = "completed" if result.wasSuccessful() else "failed"
            summary = (
                f"Done: passed={int(job.get('passed_tests') or 0)}, "
                f"failed={int(job.get('failed_tests') or 0)}, "
                f"errors={int(job.get('error_tests') or 0)}, "
                f"skipped={int(job.get('skipped_tests') or 0)}"
            )
            _update_component_test_job(
                job_id,
                status=status,
                finished_at=_utc_now_iso(),
                finished_perf=finished_perf,
                current_test="",
                summary=summary,
            )
        except Exception as exc:
            _append_component_test_log(job_id, f"FATAL {type(exc).__name__}: {exc}")
            _update_component_test_job(
                job_id,
                status="failed",
                finished_at=_utc_now_iso(),
                finished_perf=perf_counter(),
                current_test="",
                error=f"{type(exc).__name__}: {exc}",
                summary="Component test run crashed before completion.",
            )
        finally:
            stream.flush()

    threading.Thread(target=_runner, daemon=True, name=f"component-tests-{job_id[:8]}").start()
    return _component_job_snapshot(job)


class _WebSocketHub:
    def __init__(self) -> None:
        self.loop = None
        self.thread = None
        self.clients = set()
        self.host = ""
        self.port = 0
        self.path = "/ws"
        self.enabled = False

    async def _handler(self, websocket):
        path = getattr(websocket, "path", "") or ""
        if path != self.path:
            try:
                await websocket.close(code=1008, reason="Unsupported path")
            except Exception:
                pass
            return
        self.clients.add(websocket)
        try:
            hello = {
                "type": "hello",
                "runtime": {"ws_port": self.port, "ws_path": self.path},
                "active_jobs": _list_brew_jobs(active_only=True),
                "active_component_jobs": _list_component_test_jobs(active_only=True),
            }
            await websocket.send(json.dumps(hello, ensure_ascii=False))
            async for _msg in websocket:
                pass
        finally:
            self.clients.discard(websocket)

    async def _run_server(self) -> None:
        server = await websockets.serve(self._handler, self.host, self.port, ping_interval=20, ping_timeout=20)
        self.enabled = True
        await server.wait_closed()

    def start(self, host: str, port: int) -> None:
        if websockets is None:
            self.enabled = False
            return
        self.host = host
        self.port = int(port)
        if self.thread and self.thread.is_alive():
            return

        def _target():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            try:
                self.loop.run_until_complete(self._run_server())
            except Exception:
                self.enabled = False
            finally:
                try:
                    self.loop.close()
                except Exception:
                    pass

        self.thread = threading.Thread(target=_target, daemon=True, name="brew-ws-server")
        self.thread.start()

    async def _broadcast_async(self, payload: dict) -> None:
        if not self.clients:
            return
        data = json.dumps(payload, ensure_ascii=False)
        bad = []
        for ws in list(self.clients):
            try:
                await ws.send(data)
            except Exception:
                bad.append(ws)
        for ws in bad:
            self.clients.discard(ws)

    def broadcast(self, payload: dict) -> None:
        if not self.enabled or self.loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast_async(payload), self.loop)
        except Exception:
            pass


_WS_HUB = _WebSocketHub()


def _openapi_spec() -> dict:
    return load_openapi_spec(WEB_DIR)


def _safe_video_id(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    return value if YT_ID_RE.match(value) else ""


def _as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _load_index() -> dict:
    try:
        idx = load_index()
        return idx if isinstance(idx, dict) else {}
    except Exception:
        return {}


def _resolve_transcript_path(video_id: str, record: dict) -> Path:
    by_record = str(record.get("video_transcript_path") or "").strip()
    if by_record:
        return Path(by_record).expanduser()
    return TRANSCRIPTS_DIR / f"{video_id}.txt"


def _extract_title_from_transcript(path: Path, fallback: str = "Video") -> str:
    try:
        for line in path.read_text("utf-8", errors="ignore").splitlines()[:20]:
            txt = line.strip()
            if txt.lower().startswith("title:"):
                candidate = txt.split(":", 1)[1].strip()
                if candidate:
                    return candidate
    except Exception:
        pass
    return fallback


def _is_video_id_like(value: str) -> bool:
    return bool(YT_ID_RE.match((value or "").strip()))


def _resolve_video_title(video_id: str, rec: dict, transcript_path: Path, *, allow_remote: bool = False) -> str:
    rec_title = str(rec.get("video_title") or rec.get("title") or "").strip()
    if rec_title and not _is_video_id_like(rec_title):
        return rec_title

    if transcript_path.exists() and transcript_path.is_file():
        transcript_title = _extract_title_from_transcript(transcript_path, "").strip()
        if transcript_title and not _is_video_id_like(transcript_title):
            return transcript_title

    if allow_remote:
        try:
            info = yt_info(f"https://www.youtube.com/watch?v={video_id}")
            fetched = str((info or {}).get("title") or "").strip()
            if fetched and not _is_video_id_like(fetched):
                return fetched
        except Exception:
            pass

    return rec_title or video_id or "Video"


def _slugify_text(value: str, max_len: int = 52) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "item"
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    if not text:
        return "item"
    return text[: max(8, int(max_len))]


def _short_hash(value: str) -> str:
    raw = str(value or "").encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()[:12]


def _transcript_stamp(path: Path) -> str:
    try:
        st = path.stat()
        return f"{int(st.st_mtime_ns)}:{int(st.st_size)}"
    except Exception:
        return "0:0"


def _question_cache_key(question: str) -> str:
    return re.sub(r"\s+", " ", str(question or "").strip().lower())


def _qa_cached_answer(rec: dict, question: str, transcript_stamp: str) -> dict:
    if not isinstance(rec, dict):
        return {}
    q_key = _question_cache_key(question)
    rows = rec.get("video_qa_cache")
    if not isinstance(rows, list):
        return {}
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        if str(row.get("question_key") or "") != q_key:
            continue
        if str(row.get("transcript_stamp") or "") != str(transcript_stamp or ""):
            continue
        answer = str(row.get("answer") or "").strip()
        if answer:
            return dict(row)
    return {}


def _save_qa_cache_entry(
    rec: dict,
    *,
    question: str,
    transcript_stamp: str,
    answer: str,
    llm_backend: str,
    llm_backend_detail: str,
) -> None:
    if not isinstance(rec, dict):
        return
    entry = {
        "question_key": _question_cache_key(question),
        "question_text": str(question or "").strip(),
        "transcript_stamp": str(transcript_stamp or ""),
        "answer": str(answer or "").strip(),
        "llm_backend": str(llm_backend or "").strip(),
        "llm_backend_detail": str(llm_backend_detail or "").strip(),
        "saved_at": _utc_now_iso(),
    }
    rows = rec.get("video_qa_cache")
    if not isinstance(rows, list):
        rows = []
    filtered = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if (
            str(row.get("question_key") or "") == entry["question_key"]
            and str(row.get("transcript_stamp") or "") == entry["transcript_stamp"]
        ):
            continue
        filtered.append(row)
    filtered.append(entry)
    rec["video_qa_cache"] = filtered[-QA_CACHE_LIMIT:]


def _save_markdown_note(
    *,
    note_kind: str,
    video_id: str,
    title: str,
    transcript_path: str,
    youtube_url: str,
    question: str = "",
    answer: str = "",
    analysis: str = "",
    cached: bool = False,
) -> str:
    kind = str(note_kind or "note").strip().lower() or "note"
    vid = _safe_video_id(video_id) or "video"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _slugify_text(question or title or vid)
    digest = _short_hash(f"{vid}|{kind}|{question}|{analysis}|{answer}|{stamp}")
    try:
        NOTES_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return ""
    out_path = NOTES_EXPORTS_DIR / f"{kind}_{stamp}_{vid}_{slug}_{digest}.md"
    lines = [
        f"# {kind.upper()}",
        "",
        f"- video_id: {vid}",
        f"- title: {str(title or '').strip()}",
        f"- youtube_url: {str(youtube_url or '').strip()}",
        f"- transcript_path: {str(transcript_path or '').strip()}",
        f"- cached: {'yes' if cached else 'no'}",
        f"- created_at: {_utc_now_iso()}",
        "",
    ]
    if question:
        lines.extend(
            [
                "## Question",
                "",
                str(question or "").strip(),
                "",
            ]
        )
    if answer:
        lines.extend(
            [
                "## Answer",
                "",
                str(answer or "").strip(),
                "",
            ]
        )
    if analysis:
        lines.extend(
            [
                "## Analysis",
                "",
                str(analysis or "").strip(),
                "",
            ]
        )
    try:
        out_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        return str(out_path)
    except Exception:
        return ""


def _is_archive_record(rec: dict) -> bool:
    if not isinstance(rec, dict):
        return False
    if str(rec.get("date_key") or "").strip():
        return True
    if str(rec.get("service_key") or "").strip():
        return True
    if str(rec.get("service_label") or "").strip():
        return True
    if str(rec.get("filename") or rec.get("full_filename") or "").strip():
        return True
    return False


def _resolve_record_public_url(_video_id: str, rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""

    for key in ("public_url", "full_public_url"):
        value = str(rec.get(key) or "").strip()
        if value:
            return value

    public_filename = str(rec.get("public_filename") or "").strip()
    if public_filename:
        base = Path(public_filename).name
        if base:
            return build_public_url(base)

    fallback_name = str(rec.get("filename") or rec.get("full_filename") or "").strip()
    if fallback_name:
        base = Path(fallback_name).name
        if base:
            return build_public_url(base)

    vid = _safe_video_id(str(_video_id or ""))
    if vid:
        indexed = _public_file_index_by_video()
        base = str(indexed.get(vid) or "").strip()
        if base:
            return build_public_url(base)

    return ""


def _build_video_list() -> list[dict]:
    idx = _load_index()
    items: list[dict] = []
    seen_ids: set[str] = set()
    for video_id, rec in idx.items():
        vid = _safe_video_id(str(video_id))
        if not vid:
            continue
        seen_ids.add(vid)
        rec = rec if isinstance(rec, dict) else {}
        path = _resolve_transcript_path(vid, rec)
        has_transcript = path.exists() and path.is_file() and path.stat().st_size > 0
        transcript_updated_at_epoch = int(path.stat().st_mtime) if has_transcript else 0
        transcript_source = str(rec.get("video_transcript_source") or "").strip() or ("file" if has_transcript else "")
        transcript_chars = int(rec.get("video_transcript_chars") or 0)
        if transcript_chars <= 0 and has_transcript:
            transcript_chars = int(path.stat().st_size)
        title = _resolve_video_title(vid, rec, path, allow_remote=False)
        is_archive = _is_archive_record(rec)
        raw_archive_status = str(rec.get("status") or "")
        is_recording_status = bool(is_archive and raw_archive_status.lower() == "recording")
        is_live_active = bool(
            is_recording_status and vid in STATE.active_lives
        )
        can_stop_live = bool(is_live_active or is_recording_status)
        archive_status_effective = (
            "ended" if (is_recording_status and not is_live_active) else raw_archive_status
        )
        public_url = _resolve_record_public_url(vid, rec)
        thumb = str(rec.get("thumbnail_url") or "").strip() or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
        analysis_text = str(rec.get("video_ai_analysis") or "")
        analysis_llm_backend = _extract_llm_backend_label(analysis_text)
        analysis_llm_detail = _extract_llm_backend_detail(analysis_text)
        analysis_llm_mode = "local" if analysis_llm_backend in {"local", "local_fallback"} else (
            "remote" if analysis_llm_backend in {"claude", "openai"} else "unknown"
        )
        items.append(
            {
                "video_id": vid,
                "title": title,
                "channel": str(rec.get("channel") or rec.get("uploader") or ""),
                "transcript_path": str(path),
                "has_transcript": has_transcript,
                "transcript_updated_at_epoch": transcript_updated_at_epoch,
                "transcript_source": transcript_source,
                "transcript_chars": transcript_chars,
                "has_analysis": bool(str(rec.get("video_ai_analysis") or "").strip()),
                "analysis_llm_backend": analysis_llm_backend,
                "analysis_llm_detail": analysis_llm_detail,
                "analysis_llm_mode": analysis_llm_mode,
                "analysis_saved_at_epoch": int(rec.get("video_ai_analysis_saved_at_epoch") or 0),
                "updated_local": str(rec.get("video_notes_updated_at_local") or ""),
                "module": "archive" if is_archive else "video_vault",
                "is_archive": is_archive,
                "archive_status": raw_archive_status,
                "archive_status_effective": archive_status_effective,
                "can_stop_live": can_stop_live,
                "is_live_active": bool(is_live_active),
                "archive_date_key": str(rec.get("date_key") or ""),
                "archive_service_key": str(rec.get("service_key") or ""),
                "archive_service_label": str(rec.get("service_label") or ""),
                "archive_started_local": str(rec.get("started_local") or ""),
                "source_url": str(rec.get("url") or ""),
                "public_url": public_url,
                "youtube_url": f"https://www.youtube.com/watch?v={vid}",
                "thumbnail_url": thumb,
            }
        )

    for path in sorted(TRANSCRIPTS_DIR.glob("*.txt")):
        vid = _safe_video_id(path.stem)
        if not vid or vid in seen_ids:
            continue
        title = _resolve_video_title(vid, {}, path, allow_remote=False)
        items.append(
            {
                "video_id": vid,
                "title": title,
                "channel": "",
                "transcript_path": str(path),
                "has_transcript": True,
                "transcript_updated_at_epoch": int(path.stat().st_mtime),
                "transcript_source": "file",
                "transcript_chars": int(path.stat().st_size),
                "has_analysis": False,
                "analysis_llm_backend": "unknown",
                "analysis_llm_detail": "unknown",
                "analysis_llm_mode": "unknown",
                "analysis_saved_at_epoch": 0,
                "updated_local": "",
                "module": "video_vault",
                "is_archive": False,
                "archive_status": "",
                "archive_status_effective": "",
                "can_stop_live": False,
                "is_live_active": False,
                "archive_date_key": "",
                "archive_service_key": "",
                "archive_service_label": "",
                "archive_started_local": "",
                "source_url": "",
                "public_url": "",
                "youtube_url": f"https://www.youtube.com/watch?v={vid}",
                "thumbnail_url": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            }
        )

    items.sort(
        key=lambda x: (
            x.get("analysis_saved_at_epoch") or 0,
            x.get("transcript_updated_at_epoch") or 0,
            x.get("updated_local") or "",
            x.get("video_id") or "",
        ),
        reverse=True,
    )
    return items


def _video_detail(video_id: str) -> dict:
    idx = _load_index()
    rec = idx.get(video_id)
    if not isinstance(rec, dict):
        rec = {}
    path = _resolve_transcript_path(video_id, rec)
    exists = path.exists() and path.is_file() and path.stat().st_size > 0
    transcript_updated_at_epoch = int(path.stat().st_mtime) if exists else 0
    transcript_source = str(rec.get("video_transcript_source") or "").strip() or ("file" if exists else "")
    transcript_chars = int(rec.get("video_transcript_chars") or 0)
    if transcript_chars <= 0 and exists:
        transcript_chars = int(path.stat().st_size)
    title = _resolve_video_title(video_id, rec, path, allow_remote=True)
    if title and not _is_video_id_like(title):
        current = str(rec.get("video_title") or rec.get("title") or "").strip()
        if current != title:
            rec["video_title"] = title
            rec["title"] = title
            idx[video_id] = rec
            save_index(idx)
    transcript_preview = ""
    if exists:
        # Return full transcript so the UI can show the complete saved notes.
        transcript_preview = path.read_text("utf-8", errors="ignore").strip()
    is_archive = _is_archive_record(rec)
    raw_archive_status = str(rec.get("status") or "")
    is_recording_status = bool(is_archive and raw_archive_status.lower() == "recording")
    is_live_active = bool(
        is_recording_status and video_id in STATE.active_lives
    )
    can_stop_live = bool(is_live_active or is_recording_status)
    archive_status_effective = (
        "ended" if (is_recording_status and not is_live_active) else raw_archive_status
    )
    public_url = _resolve_record_public_url(video_id, rec)
    thumb = str(rec.get("thumbnail_url") or "").strip() or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    analysis_text = str(rec.get("video_ai_analysis") or "")
    analysis_llm_backend = _extract_llm_backend_label(analysis_text)
    analysis_llm_detail = _extract_llm_backend_detail(analysis_text)
    analysis_llm_mode = "local" if analysis_llm_backend in {"local", "local_fallback"} else (
        "remote" if analysis_llm_backend in {"claude", "openai"} else "unknown"
    )
    return {
        "video_id": video_id,
        "title": title,
        "channel": str(rec.get("channel") or rec.get("uploader") or ""),
        "transcript_exists": exists,
        "transcript_updated_at_epoch": transcript_updated_at_epoch,
        "transcript_path": str(path),
        "transcript_source": transcript_source,
        "transcript_chars": transcript_chars,
        "analysis_text": analysis_text,
        "analysis_llm_backend": analysis_llm_backend,
        "analysis_llm_detail": analysis_llm_detail,
        "analysis_llm_mode": analysis_llm_mode,
        "analysis_saved_at_epoch": int(rec.get("video_ai_analysis_saved_at_epoch") or 0),
        "analysis_lang": str(rec.get("video_ai_analysis_lang") or ""),
        "transcript_preview": transcript_preview,
        "module": "archive" if is_archive else "video_vault",
        "is_archive": is_archive,
        "archive_status": raw_archive_status,
        "archive_status_effective": archive_status_effective,
        "can_stop_live": can_stop_live,
        "is_live_active": bool(is_live_active),
        "archive_date_key": str(rec.get("date_key") or ""),
        "archive_service_key": str(rec.get("service_key") or ""),
        "archive_service_label": str(rec.get("service_label") or ""),
        "archive_started_local": str(rec.get("started_local") or ""),
        "source_url": str(rec.get("url") or ""),
        "public_url": public_url,
        "youtube_url": f"https://www.youtube.com/watch?v={video_id}",
        "thumbnail_url": thumb,
        "notes_progress": _notes_progress(video_id),
    }


def _research_list() -> list[dict]:
    items = load_public_research_runs(limit=120)
    out: list[dict] = []
    for item in items:
        intent = item.get("intent") if isinstance(item.get("intent"), dict) else {}
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        display_title = str(summary.get("display_title") or "").strip()
        run_kind = str(intent.get("run_kind") or "research").strip() or "research"
        preview_videos_raw = item.get("preview_videos") if isinstance(item.get("preview_videos"), list) else []
        preview_videos: list[dict] = []
        for row in preview_videos_raw[:4]:
            if not isinstance(row, dict):
                continue
            preview_videos.append(
                {
                    "video_id": str(row.get("video_id") or ""),
                    "rank": int(row.get("rank") or 0),
                    "title": str(row.get("title") or ""),
                    "thumbnail_url": str(row.get("thumbnail_url") or ""),
                }
            )
        out.append(
            {
                "run_id": str(item.get("run_id") or ""),
                "goal_text": str(item.get("goal_text") or ""),
                "display_title": display_title,
                "status": str(item.get("status") or ""),
                "run_kind": run_kind,
                "topics": item.get("topics") or [],
                "preview_videos": preview_videos,
                "created_at": str(item.get("created_at") or ""),
                "updated_at": str(item.get("updated_at") or ""),
                "report_excerpt": str(item.get("report_excerpt") or ""),
            }
        )
    return out


def _research_transcript_text(path_text: str, *, max_chars: int) -> tuple[str, bool]:
    path_raw = str(path_text or "").strip()
    if not path_raw:
        return "", False
    try:
        p = Path(path_raw).expanduser().resolve()
    except Exception:
        return "", False
    if not p.exists() or not p.is_file():
        return "", False
    try:
        txt = p.read_text("utf-8", errors="ignore").strip()
    except Exception:
        return "", False
    if not txt:
        return "", False
    if max_chars > 0 and len(txt) > max_chars:
        clipped = txt[:max_chars].rstrip()
        clipped += "\n...[truncated]"
        return clipped, True
    return txt, False


def _research_detail(run_id: str) -> dict | None:
    item = get_public_research_run(run_id)
    if not item:
        return None
    intent = item.get("intent") if isinstance(item.get("intent"), dict) else {}
    summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
    display_title = str(summary.get("display_title") or "").strip()
    run_kind = str(intent.get("run_kind") or "research").strip() or "research"
    try:
        transcript_max_chars = max(0, int((os.getenv("WEB_RESEARCH_TRANSCRIPT_MAX_CHARS") or "0").strip()))
    except Exception:
        transcript_max_chars = 0
    raw_videos = item.get("videos") if isinstance(item.get("videos"), list) else []
    videos: list[dict] = []
    for row in raw_videos:
        if not isinstance(row, dict):
            continue
        video = dict(row)
        transcript_text, transcript_truncated = _research_transcript_text(
            str(video.get("transcript_path") or ""),
            max_chars=transcript_max_chars,
        )
        video["transcript_text"] = transcript_text
        video["transcript_truncated"] = bool(transcript_truncated)
        videos.append(video)
    return {
        "run_id": str(item.get("run_id") or ""),
        "goal_text": str(item.get("goal_text") or ""),
        "display_title": display_title,
        "status": str(item.get("status") or ""),
        "run_kind": run_kind,
        "topics": item.get("topics") or [],
        "summary": summary,
        "report_text": str(item.get("report_text") or ""),
        "videos": videos,
        "created_at": str(item.get("created_at") or ""),
        "updated_at": str(item.get("updated_at") or ""),
    }


def _knowledge_juice_list() -> list[dict]:
    items = _research_list()
    return [x for x in items if str(x.get("run_kind") or "") == "knowledge_juice"]


def _knowledge_juice_detail(run_id: str) -> dict | None:
    item = _research_detail(run_id)
    if not item:
        return None
    if str(item.get("run_kind") or "") != "knowledge_juice":
        return None
    return item


def _extract_llm_backend_label(text: str) -> str:
    raw = str(text or "")
    if not raw.strip():
        return "unknown"
    low = raw.lower()
    if "backend: claude" in low:
        return "claude"
    if "backend: openai" in low:
        return "openai"
    if "backend: local transcript fallback" in low:
        return "local_fallback"
    if "backend: local" in low:
        return "local"
    return "unknown"


def _extract_llm_backend_detail(text: str) -> str:
    raw = str(text or "")
    if not raw.strip():
        return "unknown"

    for line in raw.splitlines():
        ln = str(line or "").strip()
        if not ln:
            continue
        pos = ln.lower().find("backend:")
        if pos < 0:
            continue
        detail = re.sub(r"\s+", " ", ln[pos + len("backend:") :].strip(" \t-:"))
        if detail:
            return detail

    label = _extract_llm_backend_label(raw)
    if label == "claude":
        return "Claude"
    if label == "openai":
        return "OpenAI"
    if label == "local":
        return "local"
    if label == "local_fallback":
        return "local transcript fallback"
    return "unknown"


def _run_analysis(video_id: str, force: bool = False, save: bool = True) -> dict:
    idx = _load_index()
    rec = idx.get(video_id)
    if not isinstance(rec, dict):
        rec = {}

    path = _resolve_transcript_path(video_id, rec)
    if not path.exists():
        raise RuntimeError("Transcript file is missing for this video.")
    transcript = path.read_text("utf-8", errors="ignore").strip()
    if not transcript:
        raise RuntimeError("Transcript file is empty.")

    started_ts = time()
    try:
        estimated_parts = max(1, int(_estimate_local_analysis_parts(transcript)))
    except Exception:
        estimated_parts = 1
    _set_analyze_progress(
        video_id,
        status="running",
        phase="preparing",
        done=False,
        error="",
        message="Preparing analysis...",
        started_at=_utc_now_iso(),
        elapsed_sec=0.0,
        chunk_completed=0,
        chunk_total=estimated_parts,
        generated_chars=0,
        generated_tokens=0,
    )

    lang_code, _lang_label = _analysis_output_language_for_text(transcript)
    ttl_sec = _analysis_ttl_seconds()
    if not force:
        cached, age_sec = _get_cached_ai_analysis(rec, ttl_sec, lang_code)
        if cached:
            analysis_md_path = _save_markdown_note(
                note_kind="analysis",
                video_id=video_id,
                title=_resolve_video_title(video_id, rec, path, allow_remote=False),
                transcript_path=str(path),
                youtube_url=f"https://www.youtube.com/watch?v={video_id}",
                analysis=cached,
                cached=True,
            )
            if save:
                rec["video_transcript_path"] = str(path)
                if not str(rec.get("video_transcript_source") or "").strip():
                    rec["video_transcript_source"] = "file"
                if int(rec.get("video_transcript_chars") or 0) <= 0:
                    rec["video_transcript_chars"] = int(path.stat().st_size)
                rec["video_ai_analysis"] = cached
                rec["video_ai_analysis_lang"] = lang_code
                rec["video_ai_analysis_saved_at_epoch"] = int(time())
                if analysis_md_path:
                    rec["video_ai_analysis_md_path"] = analysis_md_path
                idx[video_id] = rec
                save_index(idx)
            llm_backend = _extract_llm_backend_label(cached)
            llm_detail = _extract_llm_backend_detail(cached)
            _set_analyze_progress(
                video_id,
                status="completed",
                phase="cached",
                done=True,
                error="",
                message=f"Loaded cached analysis ({int(age_sec)}s old).",
                elapsed_sec=round(time() - started_ts, 2),
                chunk_completed=estimated_parts,
                chunk_total=estimated_parts,
                generated_chars=len(cached),
                generated_tokens=max(1, len(cached) // 4),
                llm_backend=llm_backend,
                llm_backend_detail=llm_detail,
            )
            return {
                "analysis": cached,
                "cached": True,
                "cache_age_sec": age_sec,
                "lang": lang_code,
                "llm_backend": llm_backend,
                "llm_backend_detail": llm_detail,
                "chunk_completed": estimated_parts,
                "chunk_total": estimated_parts,
                "analysis_md_path": analysis_md_path,
            }

    title = _resolve_video_title(video_id, rec, path, allow_remote=False)
    try:
        def _progress_cb(chars: int, tokens: int | None, done: bool) -> None:
            safe_chars = max(0, int(chars or 0))
            safe_tokens = max(0, int(tokens or 0))
            _set_analyze_progress(
                video_id,
                status="completed" if done else "running",
                phase="analyzing",
                done=bool(done),
                elapsed_sec=round(time() - started_ts, 2),
                generated_chars=safe_chars,
                generated_tokens=safe_tokens,
                message=(
                    "Analysis completed."
                    if done
                    else f"Generating analysis... {safe_chars} chars"
                ),
            )

        def _chunk_progress_cb(completed: int, total: int) -> None:
            safe_total = max(1, int(total or 0))
            safe_done = max(0, min(safe_total, int(completed or 0)))
            _set_analyze_progress(
                video_id,
                status="running",
                phase="chunking",
                done=False,
                elapsed_sec=round(time() - started_ts, 2),
                chunk_completed=safe_done,
                chunk_total=safe_total,
                message=f"Analyzing transcript parts: {safe_done}/{safe_total}",
            )

        analysis = _analyze_transcript_with_ai_with_progress(
            title,
            transcript,
            _progress_cb,
            _chunk_progress_cb,
        )
        if not analysis.strip():
            raise RuntimeError("LLM returned empty analysis.")
    except Exception as exc:
        _set_analyze_progress(
            video_id,
            status="failed",
            phase="failed",
            done=True,
            error=str(exc),
            message=f"Analysis failed: {exc}",
            elapsed_sec=round(time() - started_ts, 2),
        )
        raise

    if save:
        if title and not _is_video_id_like(title):
            rec["video_title"] = title
            rec["title"] = title
        rec["video_transcript_path"] = str(path)
        if not str(rec.get("video_transcript_source") or "").strip():
            rec["video_transcript_source"] = "file"
        if int(rec.get("video_transcript_chars") or 0) <= 0:
            rec["video_transcript_chars"] = int(path.stat().st_size)
        rec["video_ai_analysis"] = analysis
        rec["video_ai_analysis_lang"] = lang_code
        rec["video_ai_analysis_saved_at_epoch"] = int(time())
        idx[video_id] = rec
        save_index(idx)

    llm_backend = _extract_llm_backend_label(analysis)
    llm_detail = _extract_llm_backend_detail(analysis)
    analysis_md_path = _save_markdown_note(
        note_kind="analysis",
        video_id=video_id,
        title=title,
        transcript_path=str(path),
        youtube_url=f"https://www.youtube.com/watch?v={video_id}",
        analysis=analysis,
        cached=False,
    )
    if save and analysis_md_path:
        rec["video_ai_analysis_md_path"] = analysis_md_path
        idx[video_id] = rec
        save_index(idx)
    snap = _get_analyze_progress(video_id)
    chunk_total = max(1, int(snap.get("chunk_total") or estimated_parts or 1))
    chunk_completed = max(0, int(snap.get("chunk_completed") or 0))
    if chunk_completed <= 0 or chunk_completed > chunk_total:
        chunk_completed = chunk_total
    generated_chars = max(int(snap.get("generated_chars") or 0), len(analysis))
    generated_tokens = max(int(snap.get("generated_tokens") or 0), max(1, len(analysis) // 4))
    _set_analyze_progress(
        video_id,
        status="completed",
        phase="done",
        done=True,
        error="",
        message="Analysis completed.",
        elapsed_sec=round(time() - started_ts, 2),
        chunk_completed=chunk_completed,
        chunk_total=chunk_total,
        generated_chars=generated_chars,
        generated_tokens=generated_tokens,
        llm_backend=llm_backend,
        llm_backend_detail=llm_detail,
    )

    return {
        "analysis": analysis,
        "cached": False,
        "cache_age_sec": 0,
        "lang": lang_code,
        "llm_backend": llm_backend,
        "llm_backend_detail": llm_detail,
        "chunk_completed": chunk_completed,
        "chunk_total": chunk_total,
        "analysis_md_path": analysis_md_path,
    }


def _store_analysis_result(
    video_id: str,
    analysis: str,
    *,
    llm_backend: str = "",
    llm_backend_detail: str = "",
) -> dict:
    idx = _load_index()
    rec = idx.get(video_id)
    if not isinstance(rec, dict):
        rec = {}
    path = _resolve_transcript_path(video_id, rec)
    if not path.exists():
        raise RuntimeError("Transcript file is missing for this video.")
    transcript = path.read_text("utf-8", errors="ignore").strip()
    body = str(analysis or "").strip()
    if not body:
        raise RuntimeError("analysis is required")

    lang_code, _lang_label = _analysis_output_language_for_text(transcript or body)
    title = _resolve_video_title(video_id, rec, path, allow_remote=False)
    backend_label = str(llm_backend or "").strip().lower() or _extract_llm_backend_label(body)
    backend_detail = str(llm_backend_detail or "").strip() or _extract_llm_backend_detail(body)
    analysis_md_path = _save_markdown_note(
        note_kind="analysis",
        video_id=video_id,
        title=title,
        transcript_path=str(path),
        youtube_url=f"https://www.youtube.com/watch?v={video_id}",
        analysis=body,
        cached=False,
    )

    if title and not _is_video_id_like(title):
        rec["video_title"] = title
        rec["title"] = title
    rec["video_transcript_path"] = str(path)
    if not str(rec.get("video_transcript_source") or "").strip():
        rec["video_transcript_source"] = "file"
    if int(rec.get("video_transcript_chars") or 0) <= 0:
        rec["video_transcript_chars"] = int(path.stat().st_size)
    rec["video_ai_analysis"] = body
    rec["video_ai_analysis_lang"] = lang_code
    rec["video_ai_analysis_saved_at_epoch"] = int(time())
    if analysis_md_path:
        rec["video_ai_analysis_md_path"] = analysis_md_path
    idx[video_id] = rec
    save_index(idx)
    return {
        "analysis": body,
        "cached": False,
        "cache_age_sec": 0,
        "lang": lang_code,
        "llm_backend": backend_label or "browser",
        "llm_backend_detail": backend_detail or "browser",
        "analysis_md_path": analysis_md_path,
    }


def _run_qa(video_id: str, question: str) -> dict:
    started_ts = time()
    _set_ask_progress(
        video_id,
        status="running",
        phase="preparing",
        done=False,
        error="",
        message="Preparing transcript context...",
        started_at=_utc_now_iso(),
        elapsed_sec=0.0,
        cached=False,
    )
    try:
        idx = _load_index()
        rec = idx.get(video_id)
        if not isinstance(rec, dict):
            rec = {}
        transcript_path = _resolve_transcript_path(video_id, rec)
        if not transcript_path.exists():
            raise RuntimeError("Transcript file is missing for this video.")
        question_text = str(question or "").strip()
        title = _resolve_video_title(video_id, rec, transcript_path, allow_remote=False)
        transcript_stamp = _transcript_stamp(transcript_path)
        cached_row = _qa_cached_answer(rec, question_text, transcript_stamp)
        cached = bool(cached_row)
        if cached:
            answer = str(cached_row.get("answer") or "").strip()
            llm_backend = str(cached_row.get("llm_backend") or "").strip() or _extract_llm_backend_label(answer)
            llm_detail = str(cached_row.get("llm_backend_detail") or "").strip() or _extract_llm_backend_detail(answer)
            _set_ask_progress(
                video_id,
                status="completed",
                phase="cached",
                done=True,
                error="",
                message="Loaded cached answer.",
                elapsed_sec=round(time() - started_ts, 2),
                answer_chars=len(answer),
                cached=True,
                llm_backend=llm_backend,
                llm_backend_detail=llm_detail,
            )
        else:
            _set_ask_progress(
                video_id,
                status="running",
                phase="answering",
                done=False,
                error="",
                message="Generating answer from transcript...",
                elapsed_sec=round(time() - started_ts, 2),
                cached=False,
            )
            answer = answer_question_from_transcript(
                question=question_text,
                transcript_path=str(transcript_path),
                title_hint=title,
                progress_cb=None,
            )
            llm_backend = _extract_llm_backend_label(answer)
            llm_detail = _extract_llm_backend_detail(answer)
            _save_qa_cache_entry(
                rec,
                question=question_text,
                transcript_stamp=transcript_stamp,
                answer=answer,
                llm_backend=llm_backend,
                llm_backend_detail=llm_detail,
            )
            idx[video_id] = rec
            try:
                save_index(idx)
            except Exception:
                pass
            _set_ask_progress(
                video_id,
                status="completed",
                phase="done",
                done=True,
                error="",
                message="Answer ready.",
                elapsed_sec=round(time() - started_ts, 2),
                answer_chars=len(answer),
                cached=False,
                llm_backend=llm_backend,
                llm_backend_detail=llm_detail,
            )
        qa_md_path = _save_markdown_note(
            note_kind="ask",
            video_id=video_id,
            title=title,
            transcript_path=str(transcript_path),
            youtube_url=f"https://www.youtube.com/watch?v={video_id}",
            question=question_text,
            answer=answer,
            cached=cached,
        )
        try:
            save_transcript_qa_entry(
                video_id=video_id,
                transcript_path=str(transcript_path),
                question=question_text,
                answer=(answer or "").strip(),
                source="web",
                chat_id=None,
                lang="",
                extra={
                    "title": title,
                    "youtube_url": f"https://www.youtube.com/watch?v={video_id}",
                    "cached": bool(cached),
                    "qa_md_path": qa_md_path,
                },
            )
        except Exception:
            pass
        return {
            "answer": answer,
            "llm_backend": llm_backend,
            "llm_backend_detail": llm_detail,
            "cached": bool(cached),
            "qa_md_path": qa_md_path,
        }
    except Exception as exc:
        _set_ask_progress(
            video_id,
            status="failed",
            phase="failed",
            done=True,
            error=str(exc),
            message=f"Ask failed: {exc}",
            elapsed_sec=round(time() - started_ts, 2),
            cached=False,
        )
        raise


class _WebNullBot:
    def __init__(self, signal: "_WebRunnerSignal | None" = None) -> None:
        self._mid = 1
        self._signal = signal

    async def send_message(self, *args, **kwargs):
        mid = self._mid
        self._mid += 1
        chat_id = int(kwargs.get("chat_id") or 0)
        return _WebNullMessage(chat_id=chat_id, message_id=mid, signal=self._signal)

    async def edit_message_text(self, *args, **kwargs):
        text = ""
        if "text" in kwargs:
            text = str(kwargs.get("text") or "")
        elif args:
            text = str(args[0] or "")
        if self._signal and text:
            self._signal.observe(text)
        return None

    async def delete_message(self, *args, **kwargs):
        return None


class _WebRunnerSignal:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._event = threading.Event()
        self.status = "pending"
        self.message = ""
        self.error = ""
        self.done = False

    @staticmethod
    def _classify(clean: str) -> str:
        low = clean.lower()
        if "already being recorded" in low:
            return "already_running"
        if "live recording started" in low:
            return "started"
        if "saving archived live" in low:
            return "archived"
        if "live is planned (upcoming)" in low:
            return "upcoming"
        if "timed out. live did not start" in low:
            return "failed"
        if "could not read video info" in low or "download failed" in low:
            return "failed"
        if "❌" in clean or "⚠️" in clean or "🔒" in clean:
            return "failed"
        return "pending"

    def observe(self, text: str) -> None:
        clean = re.sub(r"\s+", " ", str(text or "")).strip()
        if not clean:
            return
        status = self._classify(clean)
        with self._lock:
            self.message = clean[:2400]
            if status != "pending":
                self.status = status
                if status == "failed":
                    self.error = clean[:2400]
                self._event.set()

    def mark_failed(self, err: str) -> None:
        msg = re.sub(r"\s+", " ", str(err or "")).strip()[:2400]
        with self._lock:
            self.status = "failed"
            self.error = msg or "Live runner failed."
            if not self.message:
                self.message = self.error
            self._event.set()

    def mark_done(self) -> None:
        with self._lock:
            self.done = True
            self._event.set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(timeout=max(0.0, float(timeout)))

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "message": self.message,
                "error": self.error,
                "done": self.done,
            }


class _WebNullMessage:
    def __init__(self, chat_id: int, message_id: int, signal: _WebRunnerSignal | None = None) -> None:
        self.chat_id = int(chat_id or 0)
        self.message_id = int(message_id or 0)
        self._signal = signal

    async def edit_text(self, *args, **kwargs):
        text = ""
        if "text" in kwargs:
            text = str(kwargs.get("text") or "")
        elif args:
            text = str(args[0] or "")
        clean = re.sub(r"\s+", " ", text).strip()
        if clean:
            print(
                f"WEB-RUNNER[{self.chat_id}:{self.message_id}] {clean[:1200]}",
                flush=True,
            )
            if self._signal:
                self._signal.observe(clean)
        return None


class _WebNullApp:
    def __init__(self, bot: _WebNullBot | None = None, signal: _WebRunnerSignal | None = None) -> None:
        self.bot = bot or _WebNullBot(signal=signal)

    def create_task(self, coro):
        try:
            return asyncio.create_task(coro)
        except Exception:
            return None


def _run_knowledge_juice(topic: str, private_run: bool) -> dict:
    topic_text = re.sub(r"\s+", " ", (topic or "").strip())
    if not topic_text:
        raise RuntimeError("topic is required")

    captured: dict[str, str] = {"report_text": ""}

    async def _capture_report(report_text: str, _run_id: str):
        captured["report_text"] = str(report_text or "")

    async def _runner():
        ctx = SimpleNamespace(application=_WebNullApp())
        run_id = await run_knowledge_juice_bot(
            ctx,
            chat_id=0,
            topic_text=topic_text,
            persist=(not private_run),
            on_report=_capture_report,
        )
        item = get_public_research_run(run_id) if run_id else None
        report_text = str((item or {}).get("report_text") or "").strip() or captured["report_text"]
        status = str((item or {}).get("status") or "completed")
        goal_text = str((item or {}).get("goal_text") or "")
        if run_id and status == "failed":
            raise RuntimeError(report_text or "Knowledge Juice failed.")
        if not run_id and not report_text.strip():
            raise RuntimeError("Knowledge Juice failed. Try a broader topic.")
        return {
            "run_id": run_id,
            "status": status,
            "is_public": bool(run_id),
            "topic": topic_text,
            "goal_text": goal_text,
            "report_text": report_text,
        }

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_runner())
    finally:
        loop.close()


def _start_live_recording(url: str, startup_wait_sec: float = 8.0) -> dict:
    src_url = str(url or "").strip()
    if not src_url:
        raise RuntimeError("url is required")
    video_id = _safe_video_id(extract_youtube_id(src_url) or "")

    live_job_id = uuid.uuid4().hex
    signal = _WebRunnerSignal()

    def _runner() -> None:
        async def _run_async():
            app = _WebNullApp(signal=signal)
            ctx = SimpleNamespace(application=app)
            wait_msg = _WebNullMessage(chat_id=0, message_id=1, signal=signal)
            await run_download_flow(
                ctx,
                src_url,
                wait_msg,
                started_by_chat_id=0,
                broadcast_fn=None,
            )

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run_async())
        except Exception as e:
            print(f"web live runner failed for {src_url}: {e}", flush=True)
            traceback.print_exc()
            signal.mark_failed(str(e))
        finally:
            signal.mark_done()
            try:
                loop.close()
            except Exception:
                pass

    threading.Thread(target=_runner, daemon=True, name=f"live-start-{live_job_id[:8]}").start()
    wait_deadline = time() + max(2.0, float(startup_wait_sec or 0.0))
    startup_status = "requested"
    startup_message = ""

    while time() < wait_deadline:
        if video_id and video_id in STATE.active_lives:
            startup_status = "started"
            break

        snap = signal.snapshot()
        snap_status = str(snap.get("status") or "")
        if snap_status in {"started", "already_running", "upcoming", "archived", "failed"}:
            startup_status = snap_status
            startup_message = str(snap.get("error") or snap.get("message") or "")
            break

        if bool(snap.get("done")):
            startup_status = "failed"
            startup_message = str(snap.get("error") or snap.get("message") or "Live runner exited before startup.")
            break

        signal.wait(0.25)

    if not startup_message:
        snap = signal.snapshot()
        startup_message = str(snap.get("error") or snap.get("message") or "")

    if startup_status == "requested" and video_id and video_id in STATE.active_lives:
        startup_status = "started"

    return {
        "live_job_id": live_job_id,
        "video_id": video_id,
        "url": src_url,
        "status": startup_status,
        "startup_status": startup_status,
        "startup_message": startup_message,
    }


def _resolve_direct_title(url: str, video_id: str, candidate: str = "") -> str:
    raw = str(candidate or "").strip()
    if raw and not _is_video_id_like(raw):
        return raw

    vid = _safe_video_id(video_id or extract_youtube_id(url) or "")
    if not vid:
        return raw or "Video"

    rec = _load_index().get(vid)
    if not isinstance(rec, dict):
        rec = {}
    path = _resolve_transcript_path(vid, rec)
    resolved = str(_resolve_video_title(vid, rec, path, allow_remote=True) or "").strip()
    if resolved and not _is_video_id_like(resolved):
        return resolved

    return vid


def _start_server_save(url: str) -> dict:
    src_url = str(url or "").strip()
    if not src_url:
        raise RuntimeError("url is required")
    video_id = _safe_video_id(extract_youtube_id(src_url) or "")
    public_url = ""
    if video_id:
        rec = _load_index().get(video_id)
        if isinstance(rec, dict):
            public_url = _resolve_record_public_url(video_id, rec)
    title = _resolve_direct_title(src_url, video_id, "")
    if public_url:
        return {
            "save_job_id": "",
            "video_id": video_id,
            "title": title,
            "url": src_url,
            "public_url": public_url,
            "status": "already_saved",
        }

    active = _active_direct_save()
    active_status = str(active.get("status") or "").strip().lower()
    if active and active_status == "running":
        return {
            "save_job_id": str(active.get("save_job_id") or ""),
            "video_id": str(active.get("video_id") or ""),
            "title": str(active.get("title") or ""),
            "url": str(active.get("url") or ""),
            "public_url": "",
            "status": "busy",
            "busy": True,
            "busy_message": "Another save is already running. Please wait until it finishes.",
        }

    save_job_id = uuid.uuid4().hex
    with _DIRECT_SAVE_LOCK:
        _DIRECT_SAVE_ACTIVE["job"] = {
            "save_job_id": save_job_id,
            "video_id": video_id,
            "title": title,
            "url": src_url,
            "status": "running",
            "started_at": _utc_now_iso(),
        }

    def _runner() -> None:
        async def _run_async():
            app = _WebNullApp()
            ctx = SimpleNamespace(application=app)
            wait_msg = _WebNullMessage(chat_id=0, message_id=1)
            await run_download_flow(
                ctx,
                src_url,
                wait_msg,
                started_by_chat_id=0,
                broadcast_fn=None,
            )

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run_async())
        except Exception as e:
            print(f"web save runner failed for {src_url}: {e}", flush=True)
            traceback.print_exc()
        finally:
            with _DIRECT_SAVE_LOCK:
                current = _DIRECT_SAVE_ACTIVE.get("job")
                if isinstance(current, dict) and str(current.get("save_job_id") or "") == save_job_id:
                    _DIRECT_SAVE_ACTIVE.clear()
            try:
                loop.close()
            except Exception:
                pass

    threading.Thread(target=_runner, daemon=True, name=f"direct-save-{save_job_id[:8]}").start()
    return {
        "save_job_id": save_job_id,
        "video_id": video_id,
        "title": title,
        "url": src_url,
        "public_url": public_url,
        "status": "started",
    }


def _stop_live_recording(video_id: str) -> dict:
    vid = _safe_video_id(video_id or "")
    if not vid:
        raise RuntimeError("video_id is required")
    active = STATE.active_lives.get(vid)
    if not active:
        return {
            "video_id": vid,
            "title": "",
            "status": "already_finished",
        }
    if not request_live_stop(vid):
        raise RuntimeError("could not request live stop")
    return {
        "video_id": vid,
        "title": str(getattr(active, "title", "") or ""),
        "status": "stop_requested",
    }


def _normalize_brew_config(raw: dict) -> dict:
    def _to_int(name: str, default: int, min_v: int = 0, max_v: int = 5000) -> int:
        try:
            val = int(raw.get(name) if isinstance(raw, dict) else default)
        except Exception:
            val = default
        return max(min_v, min(max_v, val))

    def _to_bool(name: str, default: bool = False) -> bool:
        if not isinstance(raw, dict):
            return bool(default)
        val = raw.get(name, default)
        if isinstance(val, bool):
            return val
        text = str(val or "").strip().lower()
        return text in {"1", "true", "yes", "on"}

    max_duration_sec = _to_int("max_duration_sec", 0, 0, 6 * 3600)
    no_caption_max = DEFAULT_NO_CAPTION_MAX_DURATION_SEC
    if max_duration_sec > 0:
        no_caption_max = min(no_caption_max, max_duration_sec)

    return {
        "max_videos": _to_int("max_videos", 6, 2, 40),
        "max_queries": _to_int("max_queries", 8, 3, 30),
        "per_query": _to_int("per_query", 8, 3, 30),
        "min_duration_sec": _to_int("min_duration_sec", 0, 0, 6 * 3600),
        "max_duration_sec": max_duration_sec,
        "no_caption_max_duration_sec": int(no_caption_max),
        "captions_only": _to_bool("captions_only", True),
    }


def _update_brew_job(job_id: str, **changes) -> dict | None:
    with _BREW_JOBS_LOCK:
        job = _BREW_JOBS.get(job_id)
        if not job:
            return None
        job.update(changes)
        job["updated_at"] = _utc_now_iso()
        snap = _job_snapshot(job)
    _WS_HUB.broadcast({"type": "juice_job_update", "job": snap})
    return snap


def _append_brew_review(job_id: str, video: dict) -> None:
    with _BREW_JOBS_LOCK:
        job = _BREW_JOBS.get(job_id)
        if not job:
            return
        reviewed = job.get("reviewed_videos")
        if not isinstance(reviewed, list):
            reviewed = []
        reviewed.append(video or {})
        job["reviewed_videos"] = reviewed[-60:]
        job["updated_at"] = _utc_now_iso()
        snap = _job_snapshot(job)
    _WS_HUB.broadcast({"type": "juice_job_update", "job": snap})


def _handle_brew_progress(job_id: str, event: dict) -> None:
    evt = dict(event or {})
    etype = str(evt.get("event_type") or "").strip()
    progress = evt.get("progress") if isinstance(evt.get("progress"), dict) else {}
    stage = str(evt.get("status_title") or "Knowledge Juice").strip()
    patch: dict = {"stage": stage, "last_event_type": etype, "progress": progress}
    detail = str(evt.get("detail") or "").strip()
    if detail:
        patch["progress_detail"] = detail
    llm_backend = str(evt.get("llm_backend") or "").strip().lower()
    if llm_backend:
        patch["llm_backend"] = llm_backend
    if etype == "started":
        patch["status"] = "running"
        cfg = evt.get("config") if isinstance(evt.get("config"), dict) else {}
        if cfg:
            patch["config"] = cfg
        patch["search_stats"] = {}
        patch["query_stats"] = []
    elif etype == "queries_ready":
        patch["queries"] = evt.get("queries") if isinstance(evt.get("queries"), list) else []
    elif etype in ("search_query_started", "search_query_processed"):
        patch["status"] = "running"
        patch["search_stats"] = evt.get("search_stats") if isinstance(evt.get("search_stats"), dict) else {}
        patch["query_stats"] = evt.get("query_stats") if isinstance(evt.get("query_stats"), list) else []
    elif etype == "candidates_ready":
        patch["candidate_videos"] = evt.get("videos") if isinstance(evt.get("videos"), list) else []
        patch["total_candidates"] = int(evt.get("total_candidates") or 0)
        patch["search_stats"] = evt.get("search_stats") if isinstance(evt.get("search_stats"), dict) else {}
        patch["query_stats"] = evt.get("query_stats") if isinstance(evt.get("query_stats"), list) else []
    elif etype == "processing_video":
        patch["status"] = "running"
        patch["current_video"] = evt.get("video") if isinstance(evt.get("video"), dict) else {}
        patch["current_index"] = int(evt.get("current_index") or 0)
        patch["total_videos"] = int(evt.get("total_videos") or 0)
    elif etype == "video_processed":
        patch["current_video"] = evt.get("video") if isinstance(evt.get("video"), dict) else {}
        patch["current_index"] = int(evt.get("current_index") or 0)
        patch["total_videos"] = int(evt.get("total_videos") or 0)
        _append_brew_review(job_id, patch["current_video"])
    elif etype == "comparing":
        patch["status"] = "running"
        patch["current_video"] = {}
    elif etype == "completed":
        patch["status"] = "completed"
        patch["run_id"] = str(evt.get("run_id") or "")
        patch["is_public"] = bool(evt.get("is_public"))
        patch["report_text"] = str(evt.get("report_text") or "")
    elif etype == "failed":
        patch["status"] = "failed"
        patch["error"] = str(evt.get("error") or "Brewing failed.")
        patch["run_id"] = str(evt.get("run_id") or "")
        patch["is_public"] = bool(evt.get("is_public"))
        patch["search_stats"] = evt.get("search_stats") if isinstance(evt.get("search_stats"), dict) else {}
        patch["query_stats"] = evt.get("query_stats") if isinstance(evt.get("query_stats"), list) else []

    _update_brew_job(job_id, **patch)


def _start_knowledge_juice_job(topic: str, private_run: bool, config: dict) -> dict:
    topic_text = re.sub(r"\s+", " ", (topic or "").strip())
    if not topic_text:
        raise RuntimeError("topic is required")

    cfg = _normalize_brew_config(config or {})
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "topic": topic_text,
        "status": "queued",
        "stage": "Queued",
        "private_run": bool(private_run),
        "is_public": False,
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "run_id": "",
        "last_event_type": "",
        "progress_detail": "",
        "llm_backend": "",
        "progress": {"step": 0, "total_steps": 5, "ratio": 0.0},
        "config": cfg,
        "queries": [],
        "total_candidates": 0,
        "total_videos": int(cfg.get("max_videos") or 0),
        "current_index": 0,
        "current_video": {},
        "candidate_videos": [],
        "reviewed_videos": [],
        "search_stats": {},
        "query_stats": [],
        "report_text": "",
        "error": "",
    }
    with _BREW_JOBS_LOCK:
        _BREW_JOBS[job_id] = job
    _WS_HUB.broadcast({"type": "juice_job_created", "job": _job_snapshot(job)})

    def _runner() -> None:
        async def _run_async():
            try:
                _update_brew_job(job_id, status="running", stage="Starting")
                ctx = SimpleNamespace(application=SimpleNamespace(bot=_WebNullBot()))
                run_id = await run_knowledge_juice_bot(
                    ctx,
                    chat_id=0,
                    topic_text=topic_text,
                    persist=(not private_run),
                    on_report=None,
                    on_progress=lambda e: _handle_brew_progress(job_id, e),
                    per_query_override=int(cfg.get("per_query") or 8),
                    max_queries_override=int(cfg.get("max_queries") or 8),
                    max_videos_override=int(cfg.get("max_videos") or 6),
                    min_duration_sec=int(cfg.get("min_duration_sec") or 0),
                    max_duration_sec=int(cfg.get("max_duration_sec") or 0),
                    captions_only=bool(cfg.get("captions_only")),
                )
                if run_id:
                    item = get_public_research_run(run_id)
                    if item:
                        _update_brew_job(
                            job_id,
                            run_id=run_id,
                            is_public=True,
                            report_text=str(item.get("report_text") or ""),
                            status=str(item.get("status") or "completed"),
                        )
                with _BREW_JOBS_LOCK:
                    final = _BREW_JOBS.get(job_id) or {}
                if str(final.get("status") or "") not in ("completed", "failed"):
                    _update_brew_job(job_id, status="completed")
            except Exception as exc:
                _update_brew_job(job_id, status="failed", error=str(exc))

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run_async())
        finally:
            try:
                loop.close()
            except Exception:
                pass

    threading.Thread(target=_runner, daemon=True, name=f"brew-job-{job_id[:8]}").start()
    return _job_snapshot(job)


def _run_direct_video(url: str) -> dict:
    src_url = str(url or "").strip()
    video_id = _safe_video_id(extract_youtube_id(src_url) or "")
    if not video_id:
        raise RuntimeError("Could not extract YouTube video ID from URL.")
    try:
        direct_url, title = yt_direct_download_url(src_url)
        title = _resolve_direct_title(src_url, video_id, title)
    except Exception as e:
        msg = str(e or "")
        low = msg.lower()
        if ("confirm you're not a bot" in low) or ("confirm you’re not a bot" in low):
            # Do not auto-start server save; require explicit user action from UI.
            return {
                "video_id": video_id,
                "title": _resolve_direct_title(src_url, video_id, ""),
                "download_url": "",
                "media_type": "video",
                "temporary": True,
                "save_started": False,
                "save_status": "manual_required",
                "save_busy": False,
                "save_busy_message": "",
                "save_job_id": "",
                "public_url": "",
                "fallback_reason": "youtube_antibot_direct_blocked",
            }
        raise
    return {
        "video_id": video_id,
        "title": title,
        "download_url": direct_url,
        "media_type": "video",
        "temporary": True,
    }


def _run_direct_audio(url: str) -> dict:
    src_url = str(url or "").strip()
    video_id = _safe_video_id(extract_youtube_id(src_url) or "")
    if not video_id:
        raise RuntimeError("Could not extract YouTube video ID from URL.")
    direct_url, title = yt_direct_audio_url(src_url)
    title = _resolve_direct_title(src_url, video_id, title)
    return {
        "video_id": video_id,
        "title": title,
        "download_url": direct_url,
        "media_type": "audio",
        "temporary": True,
    }


def _save_transcript_from_url(url: str, force: bool = False) -> dict:
    src_url = (url or "").strip()
    video_id = _safe_video_id(extract_youtube_id(src_url) or "")
    if not video_id:
        raise RuntimeError("Could not extract YouTube video ID from URL.")

    idx = _load_index()
    rec = idx.get(video_id)
    if not isinstance(rec, dict):
        rec = {}

    existing_path = _resolve_transcript_path(video_id, rec)
    if not force and existing_path.exists() and existing_path.is_file() and existing_path.stat().st_size > 0:
        title = _resolve_video_title(video_id, rec, existing_path, allow_remote=True)
        if title and not _is_video_id_like(title):
            current = str(rec.get("video_title") or rec.get("title") or "").strip()
            if current != title:
                rec["video_title"] = title
                rec["title"] = title
                idx[video_id] = rec
                save_index(idx)
        return {
            "video_id": video_id,
            "title": title,
            "transcript_path": str(existing_path),
            "source": str(rec.get("video_transcript_source") or "cached transcript"),
            "cached": True,
        }

    workdir = Path(tempfile.mkdtemp(prefix="ytweb_transcript_"))
    transcript_source = ""
    title = video_id
    segments = []
    caption_saved_path = ""
    try:
        try:
            segments, title, caption_tmp_path = _download_youtube_caption_segments(src_url, workdir, video_id)
            transcript_source = "youtube captions"
            try:
                caption_saved_path = _save_caption_source(video_id, caption_tmp_path)
            except Exception:
                caption_saved_path = ""
        except Exception:
            segments = []

        if not segments:
            audio_path, title = _download_audio(src_url, workdir)
            segments = _transcribe_segments(audio_path)
            transcript_source = "audio transcription"

        transcript_text = _segments_to_transcript_text(segments)
        if not transcript_text.strip():
            raise RuntimeError("No transcript text was extracted from this video.")

        transcript_path = _save_full_transcript(video_id, title, transcript_text)
        rec["video_title"] = title
        rec["title"] = title
        rec["video_transcript_path"] = transcript_path
        rec["video_transcript_source"] = transcript_source
        rec["video_transcript_chars"] = len(transcript_text)
        rec["video_notes_updated_at_local"] = now_local_str()
        if caption_saved_path:
            rec["video_caption_path"] = caption_saved_path
        idx[video_id] = rec
        save_index(idx)

        return {
            "video_id": video_id,
            "title": title,
            "transcript_path": transcript_path,
            "source": transcript_source,
            "cached": False,
        }
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _clear_history(delete_files: bool = True) -> dict:
    removed_transcripts = 0
    removed_captions = 0
    removed_index_entries = 0

    idx = _load_index()
    if idx:
        removed_index_entries = len(idx)
    save_index({})

    if delete_files:
        for pattern in ("*.txt",):
            for p in TRANSCRIPTS_DIR.glob(pattern):
                if p.is_file():
                    try:
                        p.unlink()
                        removed_transcripts += 1
                    except Exception:
                        pass

        if CAPTIONS_DIR.exists():
            for p in CAPTIONS_DIR.glob("*"):
                if p.is_file():
                    try:
                        p.unlink()
                        removed_captions += 1
                    except Exception:
                        pass

    return {
        "removed_index_entries": removed_index_entries,
        "removed_transcripts": removed_transcripts,
        "removed_captions": removed_captions,
    }


def _friendly_api_error(exc: Exception) -> str:
    raw = str(exc or "").strip()
    low = raw.lower()
    if (
        "rate-limited by youtube" in low
        or (
            "this content isn't available, try again later" in low
            and ("youtube" in low or "yt-dlp" in low)
        )
    ):
        return (
            "YouTube temporarily rate-limited this server session (can last up to about 1 hour). "
            "This is not necessarily a bad video URL; YouTube is blocking requests right now. "
            "Retry later, or rotate cookies/proxy to reduce blocking."
        )
    if "this video is private" in low or "private video" in low:
        return (
            "This video is private/unavailable for the current cookies/session. "
            "Use cookies from an account that can access it."
        )
    if raw:
        return f"{type(exc).__name__}: {raw}"
    return f"{type(exc).__name__}: unknown error"


class AppHandler(BaseHTTPRequestHandler):
    server_version = "YTDirectWeb/1.0"

    def _send_json(self, status: int, payload: dict) -> None:
        body = _json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, file_path: Path) -> None:
        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        ctype, _ = mimetypes.guess_type(str(file_path))
        data = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", (ctype or "application/octet-stream") + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def _body_json(self) -> dict:
        try:
            raw_len = int(self.headers.get("Content-Length", "0"))
        except Exception:
            raw_len = 0
        if raw_len <= 0:
            return {}
        payload = self.rfile.read(raw_len).decode("utf-8", errors="ignore")
        try:
            obj = json.loads(payload)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        try:
            if path == "/api/runtime":
                ws_port = int(_RUNTIME.get("ws_port") or DEFAULT_BREW_WS_PORT)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "runtime": {
                            "ws_enabled": bool(_RUNTIME.get("ws_enabled")),
                            "ws_port": ws_port,
                            "ws_path": str(_RUNTIME.get("ws_path") or "/ws"),
                            "retention_days": int(RETENTION_DAYS),
                        },
                    },
                )
                return

            if path == "/api/advanced/stack":
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "item": technology_stack(),
                    },
                )
                return

            if path == "/api/component_tests/jobs":
                qs = parse_qs(parsed.query)
                active_only = str((qs.get("active_only") or ["0"])[0]).strip() in ("1", "true", "yes")
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "items": _list_component_test_jobs(active_only=active_only)},
                )
                return

            if path == "/api/component_tests/job":
                qs = parse_qs(parsed.query)
                job_id = str((qs.get("job_id") or [""])[0]).strip()
                if not job_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "job_id is required"})
                    return
                with _COMPONENT_TEST_JOBS_LOCK:
                    job = _COMPONENT_TEST_JOBS.get(job_id)
                if not job:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "job not found"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "item": _component_job_snapshot(job)})
                return

            if path in ("/swagger", "/swagger/"):
                self._send_file(WEB_DIR / "swagger.html")
                return

            if path in ("/openapi.json", "/api/openapi.json"):
                self._send_json(HTTPStatus.OK, _openapi_spec())
                return

            if path == "/api/videos":
                self._send_json(HTTPStatus.OK, {"ok": True, "items": _build_video_list()})
                return

            if path == "/api/video":
                qs = parse_qs(parsed.query)
                video_id = _safe_video_id((qs.get("video_id") or [""])[0])
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "item": _video_detail(video_id)})
                return

            if path == "/api/analyze_progress":
                qs = parse_qs(parsed.query)
                video_id = _safe_video_id((qs.get("video_id") or [""])[0])
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                item = _get_analyze_progress(video_id)
                if _is_notes_task_running(video_id, "analyze"):
                    item.setdefault("video_id", video_id)
                    item["status"] = "running"
                    item["done"] = False
                    item.setdefault("message", "Running analysis...")
                item["in_progress"] = bool(_is_notes_task_running(video_id, "analyze"))
                self._send_json(HTTPStatus.OK, {"ok": True, "item": item})
                return

            if path == "/api/researches":
                self._send_json(HTTPStatus.OK, {"ok": True, "items": _research_list()})
                return

            if path == "/api/knowledge_juices":
                self._send_json(HTTPStatus.OK, {"ok": True, "items": _knowledge_juice_list()})
                return

            if path == "/api/knowledge_juice/jobs":
                qs = parse_qs(parsed.query)
                active_only = str((qs.get("active_only") or ["0"])[0]).strip() in ("1", "true", "yes")
                self._send_json(HTTPStatus.OK, {"ok": True, "items": _list_brew_jobs(active_only=active_only)})
                return

            if path == "/api/knowledge_juice/job":
                qs = parse_qs(parsed.query)
                job_id = str((qs.get("job_id") or [""])[0]).strip()
                if not job_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "job_id is required"})
                    return
                with _BREW_JOBS_LOCK:
                    job = _BREW_JOBS.get(job_id)
                if not job:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "job not found"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "item": _job_snapshot(job)})
                return

            if path == "/api/research":
                qs = parse_qs(parsed.query)
                run_id = str((qs.get("run_id") or [""])[0]).strip()
                if not run_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "run_id is required"})
                    return
                item = _research_detail(run_id)
                if not item:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "research not found"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "item": item})
                return

            if path == "/api/knowledge_juice":
                qs = parse_qs(parsed.query)
                run_id = str((qs.get("run_id") or [""])[0]).strip()
                if not run_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "run_id is required"})
                    return
                item = _knowledge_juice_detail(run_id)
                if not item:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "knowledge juice not found"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True, "item": item})
                return

            if path == "/":
                self._send_file(WEB_DIR / "index.html")
                return

            rel = path.lstrip("/")
            full = (WEB_DIR / rel).resolve()
            if WEB_DIR.resolve() not in full.parents and full != WEB_DIR.resolve():
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
            self._send_file(full)
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": f"{type(exc).__name__}: {exc}"},
            )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        started = perf_counter()
        try:
            payload = self._body_json()
            if path == "/api/analyze":
                video_id = _safe_video_id(str(payload.get("video_id") or ""))
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                force = _as_bool(payload.get("force"), default=False)
                save = _as_bool(payload.get("save"), default=True)
                if not _try_start_notes_task(video_id, "analyze"):
                    snap = _get_analyze_progress(video_id)
                    if not isinstance(snap, dict):
                        snap = {}
                    snap.setdefault("video_id", video_id)
                    snap["status"] = "running"
                    snap["done"] = False
                    snap.setdefault("message", "Analysis already running.")
                    snap["in_progress"] = True
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "video_id": video_id,
                            "elapsed_sec": round(perf_counter() - started, 2),
                            "status": "already_running",
                            "in_progress": True,
                            "item": snap,
                            "analysis": "",
                            "cached": False,
                            "cache_age_sec": 0,
                            "lang": "",
                        },
                    )
                    return
                try:
                    result = _run_analysis(video_id, force=force, save=save)
                finally:
                    _finish_notes_task(video_id, "analyze")
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "video_id": video_id,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/analyze_store":
                video_id = _safe_video_id(str(payload.get("video_id") or ""))
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                analysis = str(payload.get("analysis") or "").strip()
                if not analysis:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "analysis is required"})
                    return
                result = _store_analysis_result(
                    video_id,
                    analysis,
                    llm_backend=str(payload.get("llm_backend") or ""),
                    llm_backend_detail=str(payload.get("llm_backend_detail") or ""),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "video_id": video_id,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/ask":
                video_id = _safe_video_id(str(payload.get("video_id") or ""))
                question = str(payload.get("question") or "").strip()
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                if not question:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "question is required"})
                    return
                if not _try_start_notes_task(video_id, "ask"):
                    snap = _get_ask_progress(video_id)
                    if not isinstance(snap, dict):
                        snap = {}
                    snap.setdefault("video_id", video_id)
                    snap["status"] = "running"
                    snap["done"] = False
                    snap.setdefault("message", "Ask already running.")
                    snap["in_progress"] = True
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "video_id": video_id,
                            "elapsed_sec": round(perf_counter() - started, 2),
                            "status": "already_running",
                            "in_progress": True,
                            "item": snap,
                            "answer": "",
                            "cached": False,
                        },
                    )
                    return
                try:
                    result = _run_qa(video_id, question)
                finally:
                    _finish_notes_task(video_id, "ask")
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "video_id": video_id,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/save_transcript":
                url = str(payload.get("url") or "").strip()
                if not url:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "url is required"})
                    return
                force = bool(payload.get("force"))
                result = _save_transcript_from_url(url, force=force)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/clear_history":
                delete_files = bool(payload.get("delete_files", True))
                result = _clear_history(delete_files=delete_files)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/direct_video":
                url = str(payload.get("url") or "").strip()
                if not url:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "url is required"})
                    return
                result = _run_direct_video(url)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/direct_audio":
                url = str(payload.get("url") or "").strip()
                if not url:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "url is required"})
                    return
                result = _run_direct_audio(url)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/direct_save_server":
                url = str(payload.get("url") or "").strip()
                if not url:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "url is required"})
                    return
                result = _start_server_save(url)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/live/start":
                url = str(payload.get("url") or "").strip()
                if not url:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "url is required"})
                    return
                result = _start_live_recording(url)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/live/stop":
                video_id = str(payload.get("video_id") or "").strip()
                if not video_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "video_id is required"})
                    return
                result = _stop_live_recording(video_id)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/knowledge_juice":
                topic = str(payload.get("topic") or payload.get("goal") or "").strip()
                if not topic:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "topic is required"})
                    return
                private_run = bool(payload.get("private_run") or payload.get("private"))
                result = _run_knowledge_juice(topic, private_run)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        **result,
                    },
                )
                return

            if path == "/api/knowledge_juice/start":
                topic = str(payload.get("topic") or payload.get("goal") or "").strip()
                if not topic:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "topic is required"})
                    return
                private_run = bool(payload.get("private_run") or payload.get("private"))
                job = _start_knowledge_juice_job(
                    topic,
                    private_run,
                    {
                        "max_videos": payload.get("max_videos"),
                        "max_queries": payload.get("max_queries"),
                        "per_query": payload.get("per_query"),
                        "min_duration_sec": payload.get("min_duration_sec"),
                        "max_duration_sec": payload.get("max_duration_sec"),
                        "captions_only": payload.get("captions_only"),
                    },
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        "item": job,
                    },
                )
                return

            if path == "/api/component_tests/start":
                component = str(payload.get("component") or "all").strip()
                item = _start_component_tests_job(component)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "elapsed_sec": round(perf_counter() - started, 2),
                        "item": item,
                    },
                )
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Endpoint not found"})
        except Exception as exc:
            traceback.print_exc()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": _friendly_api_error(exc)},
            )

    def log_message(self, format: str, *args) -> None:
        if os.getenv("WEB_APP_QUIET", "0").strip() == "1":
            return
        super().log_message(format, *args)


def main() -> None:
    parser = argparse.ArgumentParser(description="YouTube Direct Bot web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--ws-host", default=os.getenv("WEB_BREW_WS_HOST", "127.0.0.1"))
    parser.add_argument("--ws-port", type=int, default=DEFAULT_BREW_WS_PORT)
    args = parser.parse_args()

    _RUNTIME["ws_host"] = args.ws_host
    _RUNTIME["ws_port"] = int(args.ws_port)
    _RUNTIME["ws_path"] = "/ws"
    _RUNTIME["ws_enabled"] = websockets is not None
    _WS_HUB.start(args.ws_host, int(args.ws_port))

    httpd = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"Web UI running at http://{args.host}:{args.port}")
    if _RUNTIME["ws_enabled"]:
        print(f"Brew WS running at ws://{args.ws_host}:{int(args.ws_port)}/ws")
    else:
        print("Brew WS disabled (websockets package missing or failed to start).")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
