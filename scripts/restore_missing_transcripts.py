#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Set

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEFAULT_ENV_FILE = Path.home() / ".config" / "ytdl-direct-bot.env"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file(DEFAULT_ENV_FILE)


def _safe_video_id(raw: str) -> str:
    value = (raw or "").strip()
    if len(value) != 11:
        return ""
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")
    return value if all(ch in allowed for ch in value) else ""


def _normalize_video_ids(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: Set[str] = set()
    for raw in values or []:
        vid = _safe_video_id(str(raw))
        if not vid or vid in seen:
            continue
        seen.add(vid)
        out.append(vid)
    return out


def _collect_ids(
    index: dict,
    recent_limit: int,
    research_limit: int,
    *,
    explicit_ids: Iterable[str],
    load_recent_searches,
    load_public_research_runs,
    get_public_research_run,
) -> Set[str]:
    explicit = _normalize_video_ids(explicit_ids)
    if explicit:
        return set(explicit)

    ids: Set[str] = set()

    for raw in index.keys():
        vid = _safe_video_id(str(raw))
        if vid:
            ids.add(vid)

    for row in load_recent_searches(limit=max(1, int(recent_limit))):
        vid = _safe_video_id(str((row or {}).get("video_id") or ""))
        if vid:
            ids.add(vid)

    public_runs = load_public_research_runs(limit=max(1, int(research_limit)))
    for run in public_runs:
        run_id = str((run or {}).get("run_id") or "").strip()
        if not run_id:
            continue
        detail = get_public_research_run(run_id)
        if not isinstance(detail, dict):
            continue
        for video in detail.get("videos") or []:
            vid = _safe_video_id(str((video or {}).get("video_id") or ""))
            if vid:
                ids.add(vid)

    return ids


def _transcript_path(index: dict, video_id: str, *, data_dir: Path) -> Path:
    rec = index.get(video_id)
    if isinstance(rec, dict):
        by_record = str(rec.get("video_transcript_path") or "").strip()
        if by_record:
            return Path(by_record).expanduser()
    return data_dir / "transcripts" / f"{video_id}.txt"


def _existing_missing_ids(index: dict, ids: Iterable[str], *, data_dir: Path) -> tuple[list[str], list[str]]:
    existing: list[str] = []
    missing: list[str] = []
    for vid in sorted(set(ids)):
        path = _transcript_path(index, vid, data_dir=data_dir)
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            existing.append(vid)
        else:
            missing.append(vid)
    return existing, missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore missing transcript files for known video IDs."
    )
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be restored.")
    parser.add_argument("--force", action="store_true", help="Force transcript rebuild even if cached by helper.")
    parser.add_argument("--limit", type=int, default=0, help="Max number of missing videos to process (0 = all).")
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=300,
        help="How many recent QA rows to scan for video IDs.",
    )
    parser.add_argument(
        "--research-limit",
        type=int,
        default=50,
        help="How many public research runs to scan for video IDs.",
    )
    parser.add_argument(
        "--video-id",
        action="append",
        default=[],
        help="Specific video ID to restore; can be provided multiple times.",
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="After restore, run AI analysis and save video notes for targeted videos.",
    )
    parser.add_argument(
        "--force-analysis",
        action="store_true",
        help="When used with --analyze, ignore analysis cache and regenerate notes.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help="Optional env file to preload before DB-backed imports.",
    )
    args = parser.parse_args()

    if args.env_file:
        _load_env_file(Path(str(args.env_file)).expanduser())

    try:
        from ytbot_config import DATA_DIR
        from ytbot_state import (
            get_public_research_run,
            load_index,
            load_public_research_runs,
            load_recent_searches,
        )
    except Exception as exc:
        print(f"error: failed to initialize state store: {type(exc).__name__}: {exc}")
        return 2

    index = load_index()
    candidates = _collect_ids(
        index=index,
        recent_limit=args.recent_limit,
        research_limit=args.research_limit,
        explicit_ids=args.video_id,
        load_recent_searches=load_recent_searches,
        load_public_research_runs=load_public_research_runs,
        get_public_research_run=get_public_research_run,
    )
    existing, missing = _existing_missing_ids(index=index, ids=candidates, data_dir=DATA_DIR)

    if args.limit and args.limit > 0:
        missing = missing[: args.limit]

    print(f"known_video_ids={len(candidates)}")
    print(f"existing_transcripts={len(existing)}")
    print(f"missing_transcripts={len(missing)}")

    if not missing:
        print("Nothing to restore.")
        return 0

    for vid in missing:
        print(f"missing: {vid}")

    if args.analyze:
        for vid in sorted(set(existing + missing)):
            print(f"analyze_target: {vid}")

    if args.dry_run:
        print("Dry run complete.")
        return 0

    try:
        from web_app import _run_analysis, _save_transcript_from_url
    except Exception as exc:
        print(f"error: failed to import transcript saver: {type(exc).__name__}: {exc}")
        return 2

    restored = 0
    failed = 0
    restored_ids: Set[str] = set()
    for vid in missing:
        url = f"https://www.youtube.com/watch?v={vid}"
        try:
            result = _save_transcript_from_url(url, force=bool(args.force))
            path = str(result.get("transcript_path") or "").strip()
            source = str(result.get("source") or "").strip()
            print(f"restored: {vid} source={source} path={path}")
            restored += 1
            restored_ids.add(vid)
        except Exception as exc:
            print(f"failed: {vid} error={type(exc).__name__}: {exc}")
            failed += 1

    analysis_failed = 0
    analysis_ok = 0
    if args.analyze:
        analyze_ids = sorted(set(existing) | restored_ids)
        for vid in analyze_ids:
            try:
                result = _run_analysis(vid, force=bool(args.force_analysis), save=True)
                backend = str(result.get("llm_backend") or "").strip() or "unknown"
                print(f"analyzed: {vid} backend={backend}")
                analysis_ok += 1
            except Exception as exc:
                print(f"analysis_failed: {vid} error={type(exc).__name__}: {exc}")
                analysis_failed += 1

    print(f"restore_done restored={restored} failed={failed}")
    if args.analyze:
        print(f"analysis_done analyzed={analysis_ok} failed={analysis_failed}")
    return 0 if (failed == 0 and analysis_failed == 0) else 2


if __name__ == "__main__":
    raise SystemExit(main())
