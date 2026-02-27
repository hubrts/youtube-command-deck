from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from ytbot_config import RETENTION_DAYS, STORAGE_DIR
from ytbot_state import load_index, save_index
from ytbot_utils import now_local_str


def cleanup_old_files() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    deleted = 0

    index = load_index()
    to_delete_ids = []

    for vid, rec in index.items():
        try:
            names = []
            for k in ("filename", "full_filename"):
                n = (rec.get(k) or "").strip()
                if n and n not in names:
                    names.append(n)

            if not names:
                continue

            existing = []
            for n in names:
                p = STORAGE_DIR / n
                if p.exists():
                    existing.append(p)

            # If all referenced files are gone, remove stale archive entry.
            if not existing:
                to_delete_ids.append(vid)
                continue

            newest_mtime = max(
                datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) for p in existing
            )
            if newest_mtime < cutoff:
                for p in existing:
                    try:
                        p.unlink(missing_ok=True)
                        deleted += 1
                    except Exception:
                        pass
                to_delete_ids.append(vid)
        except Exception:
            continue

    for vid in to_delete_ids:
        rec = index.get(vid) or {}
        tr = (rec.get("video_transcript_path") or "").strip()
        if tr:
            try:
                from pathlib import Path

                p = Path(tr)
                # Keep cleanup scoped to project data/transcripts directory.
                if "transcripts" in p.parts:
                    p.unlink(missing_ok=True)
            except Exception:
                pass
        index.pop(vid, None)

    save_index(index)
    return deleted


async def cleanup_loop() -> None:
    await asyncio.sleep(30)
    while True:
        try:
            n = cleanup_old_files()
            if n:
                print(f"[{now_local_str()}] Cleanup deleted {n} old file(s)")
        except Exception as e:
            print(f"[{now_local_str()}] Cleanup error: {e}")
        await asyncio.sleep(24 * 3600)
