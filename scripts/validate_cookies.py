#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from cookie_manager import auth_artifact_snapshot, strict_cookie_errors


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate YouTube cookies file integrity.")
    parser.add_argument(
        "--cookies-file",
        default=(os.environ.get("COOKIES_FILE") or str(BASE_DIR / "cookies.txt")).strip(),
        help="Path to Netscape cookies file.",
    )
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=int((os.environ.get("COOKIE_MAX_AGE_HOURS") or "0").strip()),
        help="Maximum allowed cookie file age in hours (0 disables age check).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    cookies_path = Path(str(args.cookies_file)).expanduser()
    max_age_hours = int(args.max_age_hours)
    snapshot = auth_artifact_snapshot(cookies_path) if cookies_path.exists() else {
        "session_artifacts": [],
        "api_artifacts": [],
        "has_session_auth": False,
        "has_api_auth": False,
    }
    reasons = strict_cookie_errors(
        cookies_path, max_age_hours=(max_age_hours if max_age_hours > 0 else None)
    )
    if reasons:
        print("cookie validation failed:")
        for reason in reasons:
            print(f"- {reason}")
        return 1
    print(f"cookie validation ok: {cookies_path}")
    print(
        "auth artifacts: "
        f"session={snapshot.get('session_artifacts') or []} "
        f"api={snapshot.get('api_artifacts') or []}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
