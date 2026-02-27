from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

SESSION_AUTH_ARTIFACTS = {
    "HSID",
    "SID",
    "SSID",
    "__Secure-1PSID",
    "__Secure-3PSID",
}

API_AUTH_ARTIFACTS = {
    "APISID",
    "SAPISID",
    "__Secure-1PAPISID",
    "__Secure-3PAPISID",
}


def _read_lines(path: Path) -> List[str]:
    try:
        return path.read_text("utf-8", errors="ignore").splitlines()
    except Exception:
        return []


def is_netscape_cookie_file(path: Path) -> bool:
    lines = _read_lines(path)
    if not lines:
        return False
    return lines[0].startswith("# Netscape HTTP Cookie File")


def _iter_cookie_rows(path: Path):
    for line in _read_lines(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 7:
            continue
        yield parts


def has_unexpired_youtube_auth(path: Path) -> bool:
    now = int(time.time())
    for domain, _sub, _p, _secure, expiry, name, _value in _iter_cookie_rows(path):
        if "youtube.com" not in (domain or "").lower():
            continue
        if name not in SESSION_AUTH_ARTIFACTS:
            continue
        try:
            exp = int(expiry)
        except Exception:
            continue
        if exp == 0 or exp > now:
            return True
    return False


def youtube_cookie_names(path: Path, *, only_unexpired: bool = True) -> set[str]:
    now = int(time.time())
    names: set[str] = set()
    for domain, _sub, _p, _secure, expiry, name, _value in _iter_cookie_rows(path):
        if "youtube.com" not in (domain or "").lower():
            continue
        if only_unexpired:
            try:
                exp = int(expiry)
            except Exception:
                continue
            if exp != 0 and exp <= now:
                continue
        names.add(name)
    return names


def auth_artifact_snapshot(path: Path) -> dict:
    present = youtube_cookie_names(path, only_unexpired=True)
    session_present = sorted(present & SESSION_AUTH_ARTIFACTS)
    api_present = sorted(present & API_AUTH_ARTIFACTS)
    return {
        "session_artifacts": session_present,
        "api_artifacts": api_present,
        "has_session_auth": bool(session_present),
        "has_api_auth": bool(api_present),
    }


def cookie_file_too_old(path: Path, max_age_hours: int) -> bool:
    try:
        age_sec = time.time() - path.stat().st_mtime
    except Exception:
        return True
    return age_sec > max(1, max_age_hours) * 3600


def strict_cookie_errors(path: Path, *, max_age_hours: Optional[int] = None) -> List[str]:
    reasons: List[str] = []
    if not path.exists():
        reasons.append(f"cookies file missing: {path}")
        return reasons
    if not is_netscape_cookie_file(path):
        reasons.append(f"cookies file is not Netscape format: {path}")
        return reasons

    snapshot = auth_artifact_snapshot(path)
    if not snapshot.get("has_session_auth"):
        reasons.append(
            "broken cookies: no unexpired YouTube session auth artifacts found "
            f"(need one of: {', '.join(sorted(SESSION_AUTH_ARTIFACTS))})"
        )

    if max_age_hours is not None and cookie_file_too_old(path, max_age_hours):
        reasons.append(f"cookies file older than {max_age_hours}h")

    return reasons


def assert_cookie_file_strict(path: Path, *, max_age_hours: Optional[int] = None) -> None:
    reasons = strict_cookie_errors(path, max_age_hours=max_age_hours)
    if reasons:
        raise RuntimeError("; ".join(reasons))


def refresh_cookies_from_browser(path: Path, browser: str, timeout_sec: int = 45) -> Tuple[bool, str]:
    if not browser:
        return False, "YT_COOKIES_FROM_BROWSER is empty"

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    cmd = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--ignore-no-formats-error",
        "--cookies-from-browser",
        browser,
        "--cookies",
        str(tmp_path),
        "--skip-download",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False, f"refresh timed out after {timeout_sec}s"
    if p.returncode != 0:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        msg = (p.stderr or p.stdout or "refresh failed").strip()
        return False, msg[-1000:]

    errors = strict_cookie_errors(tmp_path)
    if errors:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False, "; ".join(errors)

    try:
        os.chmod(tmp_path, 0o600)
    except Exception:
        pass

    tmp_path.replace(path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass

    return True, "cookies refreshed from browser"


def ensure_cookies_ready(
    path: Path,
    *,
    browser: str,
    auto_refresh: bool,
    max_age_hours: int,
    allow_browser_refresh: bool = True,
) -> List[str]:
    warnings: List[str] = []

    invalid_or_missing = (not path.exists()) or (not is_netscape_cookie_file(path))
    expired_or_stale = False if invalid_or_missing else (
        (not has_unexpired_youtube_auth(path)) or cookie_file_too_old(path, max_age_hours)
    )

    if (invalid_or_missing or expired_or_stale) and auto_refresh and allow_browser_refresh:
        ok, msg = refresh_cookies_from_browser(path, browser)
        if not ok:
            warnings.append(f"Cookie auto-refresh failed: {msg}")

    warnings.extend(strict_cookie_errors(path, max_age_hours=max_age_hours))

    return warnings
