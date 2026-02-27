from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Optional

from cookie_manager import strict_cookie_errors
from ytbot_config import (
    COOKIES_FILE,
    LIVE_STUCK_TIMEOUT_SEC,
    MAX_HEIGHT,
    STORAGE_DIR,
    USE_BROWSER_COOKIES,
    YTDLP_PROXY,
    YT_COOKIES_FROM_BROWSER,
)
from ytbot_utils import build_public_url, ensure_public_filename, extract_youtube_id, newest_part_for_video, sanitize_filename, strip_ansi


def _is_antibot_error(text: str) -> bool:
    low = (text or "").lower()
    return "confirm you're not a bot" in low or "confirm you’re not a bot" in low


def _is_retryable_youtube_access_error(text: str) -> bool:
    low = (text or "").lower()
    return (
        _is_antibot_error(low)
        or "no video formats found" in low
        or "challenge solving failed" in low
    )


def _apply_proxy(cmd: list[str]) -> list[str]:
    if YTDLP_PROXY:
        return [*cmd[:3], "--proxy", YTDLP_PROXY, *cmd[3:]]
    return cmd


def _cookie_source() -> tuple[str, str]:
    browser = (YT_COOKIES_FROM_BROWSER or "").strip()
    if USE_BROWSER_COOKIES and browser:
        return ("browser", browser)
    return ("file", COOKIES_FILE)


def _cookie_args() -> list[str]:
    source, value = _cookie_source()
    if source == "browser":
        return ["--cookies-from-browser", value]
    return ["--cookies", value]


def _assert_cookies_ready_for_ytdlp() -> None:
    source, value = _cookie_source()
    if source == "browser":
        return
    reasons = strict_cookie_errors(Path(value))
    if reasons:
        raise RuntimeError("Broken cookies: " + "; ".join(reasons))


def yt_info(url: str) -> dict:
    import subprocess

    _assert_cookies_ready_for_ytdlp()
    variants = [
        [],
        ["--extractor-args", "youtube:player_client=android,ios,web"],
        ["--extractor-args", "youtube:player_client=tv_embedded,web_safari"],
    ]

    last_err = ""
    for idx, variant in enumerate(variants, start=1):
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist",
            "--no-warnings",
            *_cookie_args(),
            "--js-runtimes", "node",
            "--remote-components", "ejs:github",
            *variant,
            "-J",
            url,
        ]
        cmd = _apply_proxy(cmd)

        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 0:
            return json.loads(p.stdout)

        err = strip_ansi((p.stderr or p.stdout or "").strip())
        last_err = err[-1500:] or "yt-dlp info failed"

        # Retry across client profiles for common YouTube anti-bot/challenge failures.
        if not _is_retryable_youtube_access_error(last_err):
            break

        if idx < len(variants):
            time.sleep(2.0 * idx)

    raise RuntimeError(last_err)


def yt_direct_download_url(url: str) -> tuple[str, str]:
    import subprocess

    _assert_cookies_ready_for_ytdlp()
    variants = [
        [],
        ["--extractor-args", "youtube:player_client=android,ios,web"],
        ["--extractor-args", "youtube:player_client=tv_embedded,web_safari"],
    ]

    fmt = f"best[ext=mp4][height<={MAX_HEIGHT}]/best[height<={MAX_HEIGHT}]/best"
    last_err = ""
    for idx, variant in enumerate(variants, start=1):
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist",
            "--no-warnings",
            *_cookie_args(),
            "--js-runtimes", "node",
            "--remote-components", "ejs:github",
            *variant,
            "--print", "%(title)s",
            "-g",
            "-f", fmt,
            url,
        ]
        cmd = _apply_proxy(cmd)

        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 0:
            lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
            if len(lines) >= 2:
                title = lines[0]
                direct_url = lines[-1]
                return direct_url, title
            last_err = "yt-dlp returned empty direct URL"
        else:
            err = strip_ansi((p.stderr or p.stdout or "").strip())
            last_err = err[-1500:] or "yt-dlp direct URL failed"

        if not _is_retryable_youtube_access_error(last_err):
            break
        if idx < len(variants):
            time.sleep(2.0 * idx)

    raise RuntimeError(last_err)


def yt_direct_audio_url(url: str) -> tuple[str, str]:
    import subprocess

    _assert_cookies_ready_for_ytdlp()
    variants = [
        [],
        ["--extractor-args", "youtube:player_client=android,ios,web"],
        ["--extractor-args", "youtube:player_client=tv_embedded,web_safari"],
    ]

    last_err = ""
    for idx, variant in enumerate(variants, start=1):
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist",
            "--no-warnings",
            *_cookie_args(),
            "--js-runtimes", "node",
            "--remote-components", "ejs:github",
            *variant,
            "--print", "%(title)s",
            "-g",
            "-f", "bestaudio/best",
            url,
        ]
        cmd = _apply_proxy(cmd)

        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 0:
            lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
            if len(lines) >= 2:
                title = lines[0]
                direct_url = lines[-1]
                return direct_url, title
            last_err = "yt-dlp returned empty direct audio URL"
        else:
            err = strip_ansi((p.stderr or p.stdout or "").strip())
            last_err = err[-1500:] or "yt-dlp direct audio URL failed"

        if not _is_retryable_youtube_access_error(last_err):
            break
        if idx < len(variants):
            time.sleep(2.0 * idx)

    raise RuntimeError(last_err)


def _any_existing_audio_for_video(video_id: str) -> Optional[str]:
    if not video_id:
        return None
    exts = (".mp3", ".m4a", ".aac", ".opus", ".wav", ".flac")
    try:
        candidates = []
        for fn in os.listdir(STORAGE_DIR):
            if video_id in fn and fn.lower().endswith(exts):
                candidates.append(str(STORAGE_DIR / fn))
        if not candidates:
            return None
        return max(candidates, key=lambda p: os.path.getmtime(p))
    except Exception:
        return None


def yt_download_audio_with_path(url: str) -> tuple[str, str, str]:
    import subprocess

    _assert_cookies_ready_for_ytdlp()
    info = yt_info(url)
    title = (info.get("title") or "audio").strip()
    video_id = (info.get("id") or extract_youtube_id(url) or "").strip()
    safe_title = sanitize_filename(title)
    output_template = str(STORAGE_DIR / f"{safe_title} [{video_id}].%(ext)s")

    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist",
        "--no-warnings",
        *_cookie_args(),
        "--js-runtimes", "node",
        "--remote-components", "ejs:github",
        "-x",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "-o", output_template,
        url,
    ]
    cmd = _apply_proxy(cmd)

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        err = strip_ansi((p.stderr or p.stdout or "").strip())
        raise RuntimeError(err[-1500:] or "yt-dlp audio download failed")

    final_path = _any_existing_audio_for_video(video_id)
    if not final_path:
        raise RuntimeError("Audio download finished, but output file was not found.")

    filename = os.path.basename(final_path)
    public_name = ensure_public_filename(video_id, filename)
    return final_path, build_public_url(public_name), title


def yt_download_audio_public_url(url: str) -> tuple[str, str]:
    _path, public_url, title = yt_download_audio_with_path(url)
    return public_url, title


def human_bytes(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            if u == "B":
                return f"{int(f)} {u}"
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} TB"


def get_part_stats(video_id: str) -> tuple[str | None, int | None, float | None]:
    part = newest_part_for_video(video_id)
    if not part:
        return None, None, None
    try:
        st = os.stat(part)
        return part, st.st_size, st.st_mtime
    except Exception:
        return part, None, None


async def ytdlp_download_with_progress(
    url: str,
    video_id: str,
    output_template: str,
    progress_cb,
    *,
    is_live: bool,
    extra_args: Optional[List[str]] = None,
    should_stop_cb=None,
):
    _assert_cookies_ready_for_ytdlp()
    extra_args = extra_args or []
    fmt = f"bv*[height<={MAX_HEIGHT}]+ba/b[height<={MAX_HEIGHT}]/b"
    variants = [
        [],
        ["--extractor-args", "youtube:player_client=android,ios,web"],
        ["--extractor-args", "youtube:player_client=tv_embedded,web_safari"],
    ]

    percent_re = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
    eta_re = re.compile(r"ETA\s+(\d+:\d+)")
    speed_re = re.compile(r"at\s+([0-9.]+[KMG]iB/s)")

    async def _run_once(extractor_variant: List[str]) -> Optional[str]:
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-playlist",
            "--no-warnings",
            *_cookie_args(),
            "--js-runtimes", "node",
            "--remote-components", "ejs:github",
            *extractor_variant,
            "--newline",
            "--print", "after_move:filepath",
            "-f", fmt,
            "--merge-output-format", "mp4",
            *extra_args,
            "-o", output_template,
            url,
        ]
        cmd2 = _apply_proxy(cmd)
        p = await asyncio.create_subprocess_exec(
            *cmd2,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        final_path: Optional[str] = None
        saw_private_error = False
        recent_lines: List[str] = []
        last_file_change_ts = time.time()
        last_size = -1
        last_rate_check_ts = time.time()
        last_rate_check_size = -1
        last_live_report_ts = 0.0
        live_report_every = 10.0

        async def update_live_file_activity() -> tuple[int | None, float | None]:
            nonlocal last_file_change_ts, last_size
            nonlocal last_rate_check_ts, last_rate_check_size

            _part, size, mtime = get_part_stats(video_id)
            if size is None:
                return None, None

            now = time.time()

            if size != last_size:
                last_size = size
                last_file_change_ts = now
            elif mtime is not None and (now - mtime) < 2.5:
                last_file_change_ts = now

            rate = None
            if last_rate_check_size >= 0:
                dt = now - last_rate_check_ts
                if dt > 0.5:
                    rate = (size - last_rate_check_size) / dt
            last_rate_check_ts = now
            last_rate_check_size = size

            return size, rate

        while True:
            if callable(should_stop_cb):
                try:
                    if bool(should_stop_cb()):
                        try:
                            p.terminate()
                        except Exception:
                            pass
                        raise RuntimeError("LIVE_STOP_REQUESTED")
                except RuntimeError:
                    raise
                except Exception:
                    pass

            if is_live:
                size, rate = await update_live_file_activity()

                if (time.time() - last_file_change_ts) > LIVE_STUCK_TIMEOUT_SEC:
                    try:
                        p.terminate()
                    except Exception:
                        pass
                    raise RuntimeError("LIVE_STUCK_TIMEOUT")

                now = time.time()
                if size is not None and (now - last_live_report_ts) >= live_report_every:
                    last_live_report_ts = now
                    if size == 0:
                        await progress_cb(
                            kind="live_stats",
                            pct=None,
                            speed=None,
                            eta=None,
                            raw="⌛ Connected. Waiting for first LIVE chunk...",
                        )
                        continue

                    rate_txt = "?"
                    if rate is not None:
                        rate_txt = f"{human_bytes(int(max(rate, 0)))} /s"

                    await progress_cb(
                        kind="live_stats",
                        pct=None,
                        speed=None,
                        eta=None,
                        raw=f"📦 File: {human_bytes(size)}\n⚡ Growth: {rate_txt}",
                    )

            try:
                line = await asyncio.wait_for(p.stdout.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if not line:
                break

            s = strip_ansi(line.decode("utf-8", errors="ignore")).strip()
            if not s:
                continue
            recent_lines.append(s)
            if len(recent_lines) > 40:
                recent_lines.pop(0)

            if "video unavailable" in s.lower() and "private" in s.lower():
                saw_private_error = True

            if s.startswith(str(STORAGE_DIR) + os.sep) and (
                s.endswith(".mp4") or s.endswith(".webm") or s.endswith(".mkv")
            ):
                final_path = s

            m = percent_re.search(s)
            if m:
                pct = float(m.group(1))
                eta = eta_re.search(s).group(1) if eta_re.search(s) else "?"
                spd = speed_re.search(s).group(1) if speed_re.search(s) else "?"
                await progress_cb(kind="percent", pct=pct, speed=spd, eta=eta, raw=s)
            else:
                await progress_cb(kind="line", pct=None, speed=None, eta=None, raw=s)

        rc = await p.wait()
        if rc != 0:
            if saw_private_error and is_live:
                raise RuntimeError("LIVE_BECAME_PRIVATE")
            err_line = ""
            for ln in reversed(recent_lines):
                low = ln.lower()
                if low.startswith("error:"):
                    err_line = ln
                    break
                if "no video formats found" in low or "confirm you" in low or "challenge solving failed" in low:
                    err_line = ln
                    break
            raise RuntimeError(err_line or "YTDLP_FAILED")

        return final_path

    for idx, variant in enumerate(variants, start=1):
        try:
            return await _run_once(variant)
        except RuntimeError as e:
            reason = str(e or "")
            if reason in ("LIVE_STOP_REQUESTED", "LIVE_STUCK_TIMEOUT", "LIVE_BECAME_PRIVATE"):
                raise

            low = reason.lower()
            retryable = (
                "no video formats found" in low
                or _is_antibot_error(low)
                or "challenge solving failed" in low
                or reason == "YTDLP_FAILED"
            )
            if idx < len(variants) and retryable:
                try:
                    await progress_cb(
                        kind="line",
                        pct=None,
                        speed=None,
                        eta=None,
                        raw=f"Retrying with alternate YouTube client profile ({idx + 1}/{len(variants)})...",
                    )
                except Exception:
                    pass
                await asyncio.sleep(1.2 * idx)
                continue
            raise

    raise RuntimeError("YTDLP_FAILED")
