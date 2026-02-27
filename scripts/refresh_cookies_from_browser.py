#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from cookie_manager import strict_cookie_errors

class BrowserSession:
    def __init__(self, proc: Optional[subprocess.Popen], profile_dir: str | None):
        self.proc = proc
        self.profile_dir = profile_dir

    def close(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=8)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        if self.profile_dir:
            shutil.rmtree(self.profile_dir, ignore_errors=True)


def _browser_base_name(browser: str) -> str:
    raw = (browser or "").strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    return raw


def _resolve_browser_cmd(browser: str) -> list[str]:
    b = _browser_base_name(browser).lower()
    if b in ("chrome", "google-chrome", "google chrome"):
        for cmd in ("google-chrome", "google-chrome-stable", "chromium-browser", "chromium"):
            if shutil.which(cmd):
                return [cmd]
    if b in ("chromium", "chromium-browser"):
        for cmd in ("chromium-browser", "chromium", "google-chrome"):
            if shutil.which(cmd):
                return [cmd]
    if shutil.which(b):
        return [b]
    return []


def _touch_youtube_session(browser: str) -> tuple[bool, str]:
    do_touch = (os.environ.get("COOKIE_TOUCH_YOUTUBE_BEFORE_EXPORT") or "1").strip() == "1"
    if not do_touch:
        return True, "browser touch disabled"

    require_open = (os.environ.get("COOKIE_REQUIRE_OPEN_YOUTUBE") or "1").strip() == "1"
    wait_sec = max(1, int((os.environ.get("COOKIE_TOUCH_WAIT_SEC") or "2").strip()))
    display = (os.environ.get("DISPLAY") or "").strip()
    if not display:
        msg = "DISPLAY is not set; cannot verify existing YouTube tab"
        if require_open:
            return False, msg
        return True, msg
    if not shutil.which("xdotool"):
        msg = "xdotool not found; cannot verify existing YouTube tab"
        if require_open:
            return False, msg
        return True, msg

    window_name = (os.environ.get("COOKIE_TOUCH_WINDOW_NAME") or "YouTube").strip()
    try:
        search = subprocess.run(
            ["xdotool", "search", "--onlyvisible", "--name", window_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if search.returncode != 0 or not (search.stdout or "").strip():
            open_flag = (os.environ.get("COOKIE_TOUCH_OPEN_NEW_WINDOW_IF_MISSING") or "0").strip()
            msg = (
                "no existing visible YouTube window found; "
                "refresh skipped without opening a new window"
            )
            if open_flag == "1":
                msg += " (COOKIE_TOUCH_OPEN_NEW_WINDOW_IF_MISSING is ignored)"
            if require_open:
                return False, msg
            return True, msg
        first_window = (search.stdout or "").splitlines()[0].strip()
        subprocess.run(
            ["xdotool", "windowactivate", "--sync", first_window, "key", "ctrl+r"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        time.sleep(wait_sec)
        return True, "existing YouTube window refreshed"
    except Exception:
        return False, "failed to refresh existing YouTube window"


def _resolve_cookie_targets(base: Path, primary: Path) -> list[Path]:
    targets: list[Path] = []
    seen: set[str] = set()

    # Optional explicit mirror target:
    # COOKIE_MIRROR_TO_SIBLING=1 COOKIE_MIRROR_SIBLING_PATH=/abs/path/to/cookies.txt
    if (os.environ.get("COOKIE_MIRROR_TO_SIBLING") or "0").strip() == "1":
        sibling_raw = (os.environ.get("COOKIE_MIRROR_SIBLING_PATH") or "").strip()
        if sibling_raw:
            sibling_cookie = Path(sibling_raw).expanduser()
            if not sibling_cookie.is_absolute():
                sibling_cookie = (base / sibling_cookie).resolve()
            key = str(sibling_cookie.resolve()) if sibling_cookie.exists() else str(sibling_cookie)
            if sibling_cookie != primary and key not in seen:
                seen.add(key)
                targets.append(sibling_cookie)

    # Optional extra targets: comma-separated absolute or relative paths.
    raw_extra = (os.environ.get("COOKIE_MIRROR_FILES") or "").strip()
    if raw_extra:
        for part in raw_extra.split(","):
            ptxt = part.strip()
            if not ptxt:
                continue
            p = Path(ptxt).expanduser()
            if not p.is_absolute():
                p = (base / p).resolve()
            key = str(p)
            if p != primary and key not in seen:
                seen.add(key)
                targets.append(p)

    return targets


def main() -> int:
    base = BASE_DIR
    cookies_file = Path(os.environ.get("COOKIES_FILE", str(base / "cookies.txt")))
    browser = (os.environ.get("YT_COOKIES_FROM_BROWSER") or "chrome").strip()
    timeout_sec = int((os.environ.get("COOKIE_REFRESH_TIMEOUT_SEC") or "90").strip())

    cookies_file.parent.mkdir(parents=True, exist_ok=True)
    ok_touch, touch_msg = _touch_youtube_session(browser)
    if not ok_touch:
        print(f"cookie refresh failed: {touch_msg}")
        return 2
    if touch_msg:
        print(f"cookie refresh note: {touch_msg}")

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{cookies_file.name}.",
        suffix=".tmp",
        dir=str(cookies_file.parent),
    )
    os.close(fd)
    tmp_cookie_path = Path(tmp_name)
    try:
        tmp_cookie_path.unlink(missing_ok=True)
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
        str(tmp_cookie_path),
        "--skip-download",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    ]

    try:
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            print(
                "cookie refresh timed out; browser/keyring is likely not available in this session"
            )
            return 124
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "cookie refresh failed").strip()
            print(f"cookie refresh failed: {msg[-1000:]}")
            return p.returncode

        errors = strict_cookie_errors(tmp_cookie_path)
        if errors:
            print(f"cookie refresh failed: {'; '.join(errors)}")
            return 3

        try:
            os.chmod(tmp_cookie_path, 0o600)
        except Exception:
            pass
        tmp_cookie_path.replace(cookies_file)
    finally:
        try:
            tmp_cookie_path.unlink(missing_ok=True)
        except Exception:
            pass

    try:
        os.chmod(cookies_file, 0o600)
    except Exception:
        pass

    print(f"cookie refresh ok: {cookies_file}")

    for dst in _resolve_cookie_targets(base, cookies_file):
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(cookies_file, dst)
            try:
                os.chmod(dst, 0o600)
            except Exception:
                pass
            print(f"cookie mirror ok: {dst}")
        except Exception as e:
            print(f"cookie mirror failed: {dst}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
