#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run transcript analysis for all saved videos and persist results."
    )
    parser.add_argument(
        "--backend",
        default="local",
        choices=["local", "claude", "openai", "auto"],
        help="Analysis backend to use (default: local).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run analysis even when cache is valid.",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Analyze only videos that currently have no saved analysis.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap for number of videos to process (0 = all).",
    )
    parser.add_argument(
        "--fallback-backend",
        default="",
        choices=["", "local", "claude", "openai", "auto"],
        help="Optional fallback backend used when primary backend fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    os.environ["VIDEO_AI_BACKEND"] = args.backend
    os.environ.setdefault("VIDEO_USE_AI_ANALYZER", "1")

    try:
        import web_app
    except Exception as exc:
        print(f"Failed to import web_app: {type(exc).__name__}: {exc}")
        return 2

    idx = web_app._load_index()
    if not isinstance(idx, dict) or not idx:
        print("No saved videos found in index.")
        return 0

    targets: list[str] = []
    for raw_video_id, raw_rec in idx.items():
        video_id = web_app._safe_video_id(str(raw_video_id))
        if not video_id:
            continue
        rec = raw_rec if isinstance(raw_rec, dict) else {}
        transcript_path = web_app._resolve_transcript_path(video_id, rec)
        if not transcript_path.exists() or not transcript_path.is_file() or transcript_path.stat().st_size <= 0:
            continue
        if args.only_missing and str(rec.get("video_ai_analysis") or "").strip():
            continue
        targets.append(video_id)

    targets = sorted(set(targets))
    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    if not targets:
        print("No matching videos with saved transcripts.")
        return 0

    ok = 0
    failed = 0
    total = len(targets)
    print(
        f"Starting analysis for {total} video(s) "
        f"(backend={args.backend}, force={args.force}, only_missing={args.only_missing}, "
        f"fallback={args.fallback_backend or 'none'})"
    )

    backend_chain = [args.backend]
    if args.fallback_backend and args.fallback_backend != args.backend:
        backend_chain.append(args.fallback_backend)

    for i, video_id in enumerate(targets, start=1):
        print(f"[{i}/{total}] {video_id} ... ", end="", flush=True)
        done = False
        last_err: Exception | None = None
        for bidx, backend in enumerate(backend_chain):
            os.environ["VIDEO_AI_BACKEND"] = backend
            try:
                result = web_app._run_analysis(video_id, force=args.force, save=True)
                cached = bool(result.get("cached"))
                llm = str(result.get("llm_backend_detail") or result.get("llm_backend") or "unknown")
                mode = f"cached ({int(result.get('cache_age_sec') or 0)}s)" if cached else "fresh"
                if bidx == 0:
                    print(f"OK | {mode} | llm={llm}")
                else:
                    print(f"OK via fallback={backend} | {mode} | llm={llm}")
                ok += 1
                done = True
                break
            except Exception as exc:
                last_err = exc
                if bidx + 1 < len(backend_chain):
                    print(
                        f"primary failed ({type(exc).__name__}: {exc}); retrying {backend_chain[bidx + 1]} ... ",
                        end="",
                        flush=True,
                    )
                    continue
        if not done:
            print(f"FAIL | {type(last_err).__name__}: {last_err}")
            failed += 1

    print(f"Done. success={ok} failed={failed} total={total}")
    if failed and args.backend == "local":
        print(
            "Hint: local backend failures with empty analysis usually mean Ollama is not running or model is missing.\n"
            "Check with:\n"
            "  ollama list\n"
            "  ollama run llama3.2:3b \"hello\"\n"
            "If needed, use fallback in this script, e.g. --fallback-backend claude."
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
