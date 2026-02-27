from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from telegram.ext import ContextTypes

from cookie_manager import strict_cookie_errors
from ytbot_config import (
    COOKIES_FILE,
    DATA_DIR,
    USE_BROWSER_COOKIES,
    VIDEO_EMBED_DIM,
    YTDLP_PROXY,
    YT_COOKIES_FROM_BROWSER,
)
from ytbot_state import (
    get_transcript_embedding_meta,
    load_index,
    load_transcript_chunks,
    save_index,
    save_transcript_chunk_embeddings,
    save_transcript_chunks,
    search_transcript_chunks_semantic,
)
from ytbot_utils import extract_youtube_id, now_local_str, with_tg_time

QA_STOPWORDS = {
    "what",
    "with",
    "this",
    "that",
    "about",
    "video",
    "відео",
    "they",
    "them",
    "their",
    "theirs",
    "doing",
    "does",
    "did",
    "done",
    "are",
    "were",
    "have",
    "has",
    "had",
    "there",
}

LANG_LABELS = {
    "uk": "Ukrainian",
    "en": "English",
}

_CLAUDE_RATE_LOCK = threading.Lock()
_CLAUDE_REQUEST_TIMES: List[float] = []


def _normalize_lang_code(raw: str, default: str = "uk") -> str:
    val = (raw or "").strip().lower()
    if val in LANG_LABELS:
        return val
    if val in ("ua", "ukr", "ukrainian"):
        return "uk"
    if val in ("en", "eng", "english"):
        return "en"
    return default


def _env_bool(name: str, default: str = "0") -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw)
    except Exception:
        return int(default)


_YTDLP_TITLE_TIMEOUT_SEC = max(10, _env_int("VIDEO_YTDLP_TITLE_TIMEOUT_SEC", 40))
_YTDLP_TIMEOUT_SEC = max(30, _env_int("VIDEO_YTDLP_TIMEOUT_SEC", 90))


def _claude_wait_for_slot() -> None:
    if not _env_bool("VIDEO_CLAUDE_ENABLE_RATE_LIMIT", "1"):
        return
    rpm = max(1, min(120, _env_int("VIDEO_CLAUDE_RPM", 5)))
    window_sec = 60.0
    while True:
        now = time.time()
        wait_sec = 0.0
        with _CLAUDE_RATE_LOCK:
            while _CLAUDE_REQUEST_TIMES and (now - _CLAUDE_REQUEST_TIMES[0]) >= window_sec:
                _CLAUDE_REQUEST_TIMES.pop(0)
            if len(_CLAUDE_REQUEST_TIMES) < rpm:
                _CLAUDE_REQUEST_TIMES.append(now)
                return
            wait_sec = max(0.05, window_sec - (now - _CLAUDE_REQUEST_TIMES[0]) + 0.01)
        time.sleep(wait_sec)


def _ai_output_language() -> Tuple[str, str]:
    raw = (os.getenv("VIDEO_AI_OUTPUT_LANG") or "auto").strip().lower()
    if raw in ("auto", "detect"):
        return "auto", "Auto"
    if raw in ("en", "eng", "english"):
        return "en", "English"
    return "uk", "Ukrainian"


def _analysis_output_language_for_text(transcript: str) -> Tuple[str, str]:
    lang_code, lang_label = _ai_output_language()
    if lang_code != "auto":
        return lang_code, lang_label
    sample = "\n".join(_transcript_body_lines(transcript)[:400]) or (transcript or "")
    detected = _normalize_lang_code(_detect_text_language(sample), default="en")
    return detected, f"{LANG_LABELS.get(detected, 'English')} (auto)"


def _default_ai_analysis_prompt(lang_code: str) -> str:
    if lang_code == "en":
        return (
            "You analyze video transcripts and return concise, useful notes in English. "
            "Output sections exactly: "
            "1) Short video idea, "
            "2) Key points (5-10 bullets), "
            "3) Practical takeaways / what to do next, "
            "4) Uncertain points / risks (if any, with timestamps). "
            "If uncertain, say it is uncertain."
        )
    return (
        "Ти аналізуєш транскрипти відео і повертаєш короткі, корисні нотатки українською. "
        "Поверни рівно такі розділи: "
        "1) Коротка ідея відео, "
        "2) Ключові тези (5-10 пунктів), "
        "3) Практичні висновки/що робити далі, "
        "4) Невизначені моменти/ризики (якщо є, з таймкодами). "
        "Якщо не впевнений, так і напиши."
    )


def _ai_language_directive(lang_code: str) -> str:
    if lang_code == "en":
        return "Respond only in English."
    return "Відповідай тільки українською."


def _detect_text_language(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return _normalize_lang_code(os.getenv("VIDEO_QA_DEFAULT_LANG") or "uk")
    low = t.lower()
    if re.search(r"[іїєґ]", low):
        return "uk"
    cyr = len(re.findall(r"[а-яёіїєґ]", low))
    lat = len(re.findall(r"[a-z]", low))
    if lat > cyr:
        return "en"
    if cyr > 0:
        return _normalize_lang_code(os.getenv("VIDEO_QA_CYRILLIC_DEFAULT_LANG") or "uk")
    return _normalize_lang_code(os.getenv("VIDEO_QA_DEFAULT_LANG") or "uk")


def _extract_translate_target_lang(question: str) -> str:
    low = (question or "").strip().lower()
    if not low:
        return ""
    en_hits = (
        "англійською",
        "english",
        "in english",
    )
    uk_hits = (
        "українською",
        "ukrainian",
        "in ukrainian",
    )
    for h in en_hits:
        if h in low:
            return "en"
    for h in uk_hits:
        if h in low:
            return "uk"
    return ""


def _is_translation_request(question: str) -> bool:
    low = (question or "").strip().lower()
    if not low:
        return False
    triggers = (
        "translate",
        "переклади",
        "translation",
        "переклад",
    )
    return any(x in low for x in triggers)


def _qa_target_language(question: str) -> Tuple[str, bool]:
    target = _extract_translate_target_lang(question)
    if target:
        return target, True
    return _normalize_lang_code(_detect_text_language(question)), _is_translation_request(question)


def _qa_unreliable_text(lang: str) -> str:
    if lang == "en":
        return "I cannot answer this reliably from the saved transcript context."
    return "Я не можу надійно відповісти за збереженим контекстом транскрипту."


def _qa_unavailable_text(lang: str, reason: str) -> str:
    if lang == "en":
        return f"AI answer unavailable right now ({reason}). Try again shortly."
    return f"AI-відповідь зараз недоступна ({reason}). Спробуйте трохи пізніше."


def _provider_caption(provider: str, model: str) -> str:
    p = (provider or "").strip().lower()
    m = (model or "").strip()
    if p == "claude":
        return f"☁️ Backend: Claude ({m})"
    if p == "openai":
        return f"☁️ Backend: OpenAI ({m})"
    if p == "local":
        return f"🖥️ Backend: local ({m})"
    return "🧩 Backend: unknown"


def _extract_quoted_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    patterns = (
        r'"([^"]{2,500})"',
        r"“([^”]{2,500})”",
        r"'([^']{2,500})'",
    )
    for pat in patterns:
        m = re.search(pat, t)
        if m:
            return m.group(1).strip()
    return ""


def _extract_translation_source_text(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return ""
    quoted = _extract_quoted_text(q)
    if quoted:
        return quoted
    if ":" in q:
        tail = q.split(":", 1)[1].strip()
        if len(tail) >= 2:
            return tail
    pat = re.compile(
        r"(?is)^(?:translate|переклади|translation|переклад)\b.*?(?:to|на|in)?\s*"
        r"(?:english|англійською|ukrainian|українською)?\s*"
        r"(?:text|текст)?\s*[-–—]?\s*(.+)$"
    )
    m = pat.match(q)
    if m:
        tail = (m.group(1) or "").strip()
        if len(tail) >= 2:
            return tail
    return ""


def _ensure_output_language(text: str, target_lang: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    if target_lang not in LANG_LABELS:
        return t
    if _detect_text_language(t) == target_lang:
        return t
    return _translate_text_for_output(t, target_lang)


def _post_json(url: str, payload: dict, timeout_sec: int = 120) -> dict:
    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw)
    return obj if isinstance(obj, dict) else {}


def _ollama_keep_alive() -> str:
    # Keep local model loaded between requests to avoid cold starts on VPS.
    raw = str(os.getenv("VIDEO_LOCAL_LLM_KEEP_ALIVE") or "30m").strip()
    return raw or "30m"


def _ollama_chat(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    timeout_sec: int = 120,
    format_json: bool = False,
    base_url: str = "",
    progress_cb: Optional[Callable[[int, Optional[int], bool], None]] = None,
) -> str:
    base = (base_url or os.getenv("VIDEO_LOCAL_LLM_URL") or "http://127.0.0.1:11434").strip()
    api_url = f"{base.rstrip('/')}/api/chat"
    keep_alive = _ollama_keep_alive()
    if progress_cb is None:
        payload: dict = {
            "model": model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "options": {"temperature": temperature},
            "keep_alive": keep_alive,
        }
        if format_json:
            payload["format"] = "json"

        obj = _post_json(api_url, payload, timeout_sec=timeout_sec)
        message = obj.get("message") or {}
        text = str(message.get("content") or "").strip()
        if text:
            return text
        err = str(obj.get("error") or "").strip()
        if err:
            raise RuntimeError(f"ollama_error: {err}")
        raise RuntimeError("ollama_empty_response")

    payload = {
        "model": model,
        "stream": True,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": temperature},
        "keep_alive": keep_alive,
    }
    if format_json:
        payload["format"] = "json"

    req = Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    chunks: List[str] = []
    chars = 0
    last_emit = 0.0
    with urlopen(req, timeout=timeout_sec) as resp:
        for raw in resp:
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            err = str(obj.get("error") or "").strip()
            if err:
                raise RuntimeError(f"ollama_error: {err}")

            message = obj.get("message") or {}
            piece = str(message.get("content") or "")
            if piece:
                chunks.append(piece)
                chars += len(piece)
                now = time.time()
                if now - last_emit >= 2.0:
                    progress_cb(chars, max(1, chars // 4), False)
                    last_emit = now

            if obj.get("done"):
                eval_count = obj.get("eval_count")
                token_count: Optional[int] = None
                if isinstance(eval_count, (int, float)):
                    token_count = int(eval_count)
                progress_cb(chars, token_count if token_count and token_count > 0 else max(1, chars // 4), True)
                break

    text = "".join(chunks).strip()
    if text:
        return text
    raise RuntimeError("ollama_empty_response")


def _openai_chat(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    timeout_sec: int = 120,
) -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("missing_openai_api_key")

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
    }

    req = Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw)
    return (
        obj.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )


def _anthropic_chat(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    timeout_sec: int = 120,
    max_tokens: int = 1200,
) -> str:
    api_key = (
        os.getenv("ANTHROPIC_API_KEY")
        or os.getenv("CLAUDE_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("missing_anthropic_api_key")

    api_url = (
        os.getenv("VIDEO_ANTHROPIC_URL")
        or os.getenv("ANTHROPIC_API_URL")
        or "https://api.anthropic.com/v1/messages"
    ).strip()
    body = {
        "model": model,
        "max_tokens": max(64, int(max_tokens)),
        "temperature": float(temperature),
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    req = Request(
        api_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    _claude_wait_for_slot()
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        detail = ""
        try:
            payload = json.loads(exc.read().decode("utf-8", errors="ignore") or "{}")
            detail = str(((payload.get("error") or {}).get("message") or "")).strip()
        except Exception:
            detail = ""
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"claude_http_{exc.code}{suffix}") from exc
    obj = json.loads(raw or "{}")
    pieces: List[str] = []
    for block in (obj.get("content") or []):
        if not isinstance(block, dict):
            continue
        if str(block.get("type") or "").strip() == "text":
            txt = str(block.get("text") or "")
            if txt:
                pieces.append(txt)
    text = "".join(pieces).strip()
    if text:
        return text
    err = str(((obj.get("error") or {}).get("message") or "")).strip()
    if err:
        raise RuntimeError(f"claude_error: {err}")
    raise RuntimeError("claude_empty_response")


def _chat_with_provider(
    *,
    provider: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    timeout_sec: int,
    format_json: bool = False,
    progress_cb: Optional[Callable[[int, Optional[int], bool], None]] = None,
) -> str:
    if provider == "local":
        return _ollama_chat(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            timeout_sec=timeout_sec,
            format_json=format_json,
            progress_cb=progress_cb,
        )
    if provider == "openai":
        return _openai_chat(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            timeout_sec=timeout_sec,
        )
    if provider == "claude":
        return _anthropic_chat(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            timeout_sec=timeout_sec,
            max_tokens=1600 if format_json else 1200,
        )
    raise RuntimeError(f"unsupported_provider:{provider}")


def _translate_text_for_output(text: str, target_lang: str) -> str:
    src = (text or "").strip()
    if not src:
        return ""
    lang = (target_lang or "").strip().lower()
    if lang not in LANG_LABELS:
        return src

    backend = (os.getenv("VIDEO_QA_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    openai_model = (os.getenv("VIDEO_QA_MODEL") or os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    claude_model = (
        os.getenv("VIDEO_QA_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()
    timeout_sec = int((os.getenv("VIDEO_QA_TIMEOUT_SEC") or "180").strip())

    system_prompt = (
        "You are a professional translator. "
        f"Translate to {LANG_LABELS[lang]}. "
        "Preserve meaning and keep it concise. Return only translated text."
    )
    user_prompt = f"Text:\n{src}"
    attempts: List[Tuple[str, str]] = []
    if backend in ("local", "ollama"):
        attempts = [("local", local_model)]
    elif backend == "openai":
        attempts = [("openai", openai_model)]
    elif backend in ("claude", "anthropic"):
        attempts = [("claude", claude_model), ("local", local_model)]
    else:
        attempts = [("local", local_model), ("claude", claude_model), ("openai", openai_model)]

    for provider, model in attempts:
        try:
            out = _chat_with_provider(
                provider=provider,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
                timeout_sec=timeout_sec,
            )
            out = (out or "").strip()
            if out:
                return out
        except Exception:
            continue
    return src


def _free_strict_mode_active() -> bool:
    # Enabled by default for free/no-HF operation.
    raw = (os.getenv("VIDEO_FREE_STRICT_MODE") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _yt_dlp_base_cmd() -> list[str]:
    reasons = strict_cookie_errors(Path(COOKIES_FILE))
    if reasons:
        raise RuntimeError("Broken cookies: " + "; ".join(reasons))
    cmd = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--no-playlist",
        "--no-warnings",
        "--cookies",
        COOKIES_FILE,
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
    ]
    if YTDLP_PROXY:
        cmd += ["--proxy", YTDLP_PROXY]
    browser = YT_COOKIES_FROM_BROWSER if USE_BROWSER_COOKIES else ""
    if browser:
        cmd += ["--cookies-from-browser", browser]
    return cmd


def _download_audio(url: str, workdir: Path) -> Tuple[str, str]:
    title_cmd = [*_yt_dlp_base_cmd(), "--print", "title", url]
    title = "Live"
    try:
        title_proc = subprocess.run(
            title_cmd,
            capture_output=True,
            text=True,
            timeout=_YTDLP_TITLE_TIMEOUT_SEC,
        )
        title_lines = (title_proc.stdout or "").strip().splitlines()
        if title_lines:
            title = title_lines[-1]
    except subprocess.TimeoutExpired:
        title = "Live"

    out_template = str(workdir / "audio.%(ext)s")
    client_variants = [
        ["--extractor-args", "youtube:player_client=android,ios,web"],
        ["--extractor-args", "youtube:player_client=tv_embedded,web_safari"],
        [],
    ]
    mode_variants = [
        ["-x", "--audio-format", "m4a", "--audio-quality", "0"],
        ["-x", "--audio-format", "mp3", "--audio-quality", "0"],
        ["-f", "bestaudio[ext=m4a]/bestaudio", "--remux-video", "m4a"],
        ["-f", "bestaudio"],
    ]

    last_err = ""
    for client_variant in client_variants:
        for mode_variant in mode_variants:
            for old in workdir.glob("audio.*"):
                try:
                    old.unlink()
                except Exception:
                    pass

            dl_cmd = [
                *_yt_dlp_base_cmd(),
                *client_variant,
                *mode_variant,
                "-o",
                out_template,
                url,
            ]

            try:
                proc = subprocess.run(
                    dl_cmd,
                    capture_output=True,
                    text=True,
                    timeout=_YTDLP_TIMEOUT_SEC,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"Audio download timed out after {_YTDLP_TIMEOUT_SEC}s.")
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "download failed").strip()
                last_err = err[-1200:]
                continue

            files = [
                p for p in sorted(workdir.glob("audio.*")) if p.is_file() and p.suffix != ".part"
            ]
            if not files:
                last_err = "Audio extraction finished, but no audio file was found."
                continue

            best = max(files, key=lambda p: p.stat().st_size if p.exists() else 0)
            if not best.exists() or best.stat().st_size <= 0:
                last_err = "Audio download produced an empty file."
                continue

            return str(best), title

    raise RuntimeError(last_err or "Audio download failed.")


def _parse_vtt_ts(raw: str) -> float:
    t = (raw or "").strip()
    if not t:
        return 0.0
    t = t.replace(",", ".")
    parts = t.split(":")
    try:
        if len(parts) == 3:
            hh = int(parts[0])
            mm = int(parts[1])
            ss = float(parts[2])
            return hh * 3600 + mm * 60 + ss
        if len(parts) == 2:
            mm = int(parts[0])
            ss = float(parts[1])
            return mm * 60 + ss
    except Exception:
        return 0.0
    return 0.0


def _parse_vtt_segments(path: Path) -> List[Dict[str, object]]:
    lines = path.read_text("utf-8", errors="ignore").splitlines()
    out: List[Dict[str, object]] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "-->" not in line:
            i += 1
            continue

        try:
            left, right = line.split("-->", 1)
        except ValueError:
            i += 1
            continue

        start = _parse_vtt_ts(left.strip().split(" ")[0])
        end = _parse_vtt_ts(right.strip().split(" ")[0])
        i += 1

        text_lines: List[str] = []
        while i < len(lines):
            cur = lines[i].strip()
            if not cur:
                break
            text_lines.append(cur)
            i += 1

        text = " ".join(text_lines).strip()
        text = re.sub(r"<[^>]+>", "", text).strip()
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            out.append(
                {
                    "start": max(0.0, start),
                    "end": max(start, end),
                    "text": text,
                }
            )
        i += 1
    return out


def _caption_lang_from_name(name: str) -> str:
    raw = str(name or "").strip().lower()
    m = re.search(r"\.([a-z0-9_-]+)\.vtt$", raw)
    if not m:
        return ""
    return m.group(1).strip().lower()


def _is_english_caption_lang(lang: str) -> bool:
    value = str(lang or "").strip().lower()
    return value == "en" or value.startswith("en-") or value.startswith("en_")


def _pick_caption_file(candidates: List[Path]) -> Path:
    if not candidates:
        raise RuntimeError("No caption files found.")

    pref_raw = (os.getenv("VIDEO_SUB_LANG_PREFER") or "en,en-us,en-gb").strip()
    prefs = [x.strip().lower() for x in pref_raw.split(",") if x.strip()]

    english_candidates = [p for p in candidates if _is_english_caption_lang(_caption_lang_from_name(p.name))]
    if not english_candidates:
        raise RuntimeError("No English YouTube captions available for this video.")

    scored: List[Tuple[int, float, int, int, Path]] = []
    for p in english_candidates:
        lang = _caption_lang_from_name(p.name)
        score = 100
        for idx, pref in enumerate(prefs):
            if lang == pref or lang.startswith(pref):
                score = idx
                break
        try:
            segs = _parse_vtt_segments(p)
            coverage_sec = float(max((float(x.get("end") or 0.0) for x in segs), default=0.0))
        except Exception:
            coverage_sec = 0.0
        try:
            size_bytes = int(p.stat().st_size)
        except Exception:
            size_bytes = 0
        scored.append((score, -coverage_sec, -size_bytes, len(p.name), p))

    scored.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    return scored[0][4]


def _download_youtube_caption_segments(
    url: str,
    workdir: Path,
    title_hint: str = "",
) -> Tuple[List[Dict[str, object]], str, str]:
    out_template = str(workdir / "%(id)s.%(ext)s")
    sub_langs = (os.getenv("VIDEO_SUB_LANGS") or "en.*,en,-live_chat").strip()
    cmd = [
        *_yt_dlp_base_cmd(),
        "--skip-download",
        "--write-subs",
        "--write-auto-subs",
        "--sub-format",
        "vtt",
        "--sub-langs",
        sub_langs,
        "--print",
        "title",
        "-o",
        out_template,
        url,
    ]

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_YTDLP_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Caption download timed out after {_YTDLP_TIMEOUT_SEC}s.") from exc
    if p.returncode != 0:
        err = (p.stderr or p.stdout or "caption download failed").strip()
        raise RuntimeError(err[-1200:])

    title_lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    title = title_lines[0] if title_lines else (title_hint or "Video")

    vtt_files = sorted(workdir.glob("*.vtt"))
    if not vtt_files:
        raise RuntimeError("No YouTube captions available for this video.")

    chosen = _pick_caption_file(vtt_files)
    segments = _parse_vtt_segments(chosen)
    if not segments:
        raise RuntimeError("Caption file exists, but no transcript text was parsed.")

    return segments, title, str(chosen)


def _extract_audio_from_local(video_path: str, workdir: Path) -> Tuple[str, str]:
    src = Path(video_path)
    if not src.exists():
        raise RuntimeError(f"local video file not found: {video_path}")

    out_audio = workdir / "audio_local.m4a"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src),
        "-vn",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        str(out_audio),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0 or not out_audio.exists():
        err = (p.stderr or p.stdout or "ffmpeg extract failed").strip()
        raise RuntimeError(err[-1200:])

    return str(out_audio), src.stem


def _transcribe_segments(audio_path: str) -> List[Dict[str, object]]:
    from faster_whisper import WhisperModel

    model_name = os.getenv("VIDEO_WHISPER_MODEL", "base").strip() or "base"
    model = WhisperModel(model_name, device="cpu", compute_type="int8")
    prompt = (
        os.getenv("VIDEO_TRANSCRIBE_PROMPT")
        or "Це відео може містити українську та англійську мови."
    ).strip()

    segments, _info = model.transcribe(
        audio_path,
        vad_filter=True,
        beam_size=5,
        initial_prompt=prompt,
    )

    out: List[Dict[str, object]] = []
    for seg in segments:
        text = (seg.text or "").strip()
        if not text:
            continue
        out.append({"start": float(seg.start), "end": float(seg.end), "text": text})
    return out


def _load_diarization_labels(audio_path: str) -> List[Tuple[float, float, str]]:
    token = (os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN") or "").strip()
    if not token:
        return []

    pipeline = None
    try:
        from pyannote.audio import Pipeline

        # Free HF default (community model). Requires accepting model conditions and a free read token.
        model_id = (os.getenv("VIDEO_DIAR_MODEL") or "pyannote/speaker-diarization-community-1").strip()
        pipeline = Pipeline.from_pretrained(model_id, use_auth_token=token)
        min_speakers = int((os.getenv("VIDEO_MIN_SPEAKERS") or "2").strip())
        max_speakers = int((os.getenv("VIDEO_MAX_SPEAKERS") or "3").strip())
        if min_speakers > max_speakers:
            min_speakers, max_speakers = max_speakers, min_speakers
        diarization = pipeline(audio_path, min_speakers=min_speakers, max_speakers=max_speakers)
    except Exception:
        # Backward-compatible fallback.
        try:
            from pyannote.audio import Pipeline
            pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization-3.1", use_auth_token=token)
            min_speakers = int((os.getenv("VIDEO_MIN_SPEAKERS") or "2").strip())
            max_speakers = int((os.getenv("VIDEO_MAX_SPEAKERS") or "3").strip())
            if min_speakers > max_speakers:
                min_speakers, max_speakers = max_speakers, min_speakers
            diarization = pipeline(audio_path, min_speakers=min_speakers, max_speakers=max_speakers)
        except Exception:
            pass
        if pipeline is None:
            return []
        try:
            diarization = pipeline(audio_path)
        except Exception:
            return []

    labels: List[Tuple[float, float, str]] = []
    for turn, _track, label in diarization.itertracks(yield_label=True):
        labels.append((float(turn.start), float(turn.end), str(label)))
    return labels


def _speaker_for_ts(ts: float, diar: List[Tuple[float, float, str]]) -> str:
    for start, end, label in diar:
        if start <= ts <= end:
            return label
    return "SPEAKER_00"


def _build_bullets(segments: List[Dict[str, object]], diar: List[Tuple[float, float, str]]) -> OrderedDict[str, List[str]]:
    min_minutes = float((os.getenv("VIDEO_MIN_PRIMARY_SPEAKER_MINUTES") or "10").strip())
    min_seconds = max(60.0, min_minutes * 60.0)
    min_sentence_chars = int((os.getenv("VIDEO_MIN_SENTENCE_CHARS") or "20").strip())
    max_bullets = int((os.getenv("VIDEO_MAX_BULLETS_PER_SPEAKER") or "12").strip())
    grouped: OrderedDict[str, List[str]] = OrderedDict()
    durations: Dict[str, float] = {}

    for seg in segments:
        start = float(seg["start"])
        end = float(seg["end"])
        speaker = _speaker_for_ts((start + end) / 2.0, diar)
        txt = str(seg["text"]).strip()
        if len(txt) < 8:
            continue
        grouped.setdefault(speaker, []).append(txt)
        durations[speaker] = durations.get(speaker, 0.0) + max(0.0, end - start)

    scored: List[Tuple[str, List[str], int]] = []
    for speaker, texts in grouped.items():
        if durations.get(speaker, 0.0) < min_seconds:
            continue
        merged = " ".join(texts)
        sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", merged) if len(s.strip()) >= min_sentence_chars]
        if not sents:
            sents = [t for t in texts if len(t) >= min_sentence_chars]

        bullets = sents[:max_bullets]
        if bullets:
            scored.append((speaker, bullets, sum(len(x) for x in texts)))

    # Fallback: if no one reached threshold, keep the longest speaker so notes are not empty.
    if not scored and grouped:
        top_speaker = max(durations.items(), key=lambda x: x[1])[0]
        texts = grouped.get(top_speaker, [])
        merged = " ".join(texts)
        sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", merged) if len(s.strip()) >= min_sentence_chars]
        if not sents:
            sents = [t for t in texts if len(t) >= min_sentence_chars]
        bullets = sents[:max_bullets]
        if bullets:
            scored.append((top_speaker, bullets, sum(len(x) for x in texts)))

    scored.sort(key=lambda x: x[2], reverse=True)
    result: OrderedDict[str, List[str]] = OrderedDict()
    for speaker, bullets, _ in scored[:3]:
        result[speaker] = bullets

    return result


def _extract_references(segments: List[Dict[str, object]]) -> List[str]:
    # Generic chapter:verse-style reference matcher (best effort).
    ref_re = re.compile(
        r"\b(?:[1-3]\s*)?[A-Za-zА-Яа-яІіЇїЄєЁё\.\-]{2,25}\s+\d{1,3}:\d{1,3}(?:-\d{1,3})?\b"
    )
    out: List[str] = []
    seen = set()
    for seg in segments:
        text = str(seg.get("text") or "")
        for m in ref_re.findall(text):
            ref = re.sub(r"\s+", " ", m.strip())
            key = ref.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(ref)
    return out


def _segments_to_transcript_text(segments: List[Dict[str, object]]) -> str:
    lines: List[str] = []
    for seg in segments:
        start = float(seg.get("start") or 0.0)
        text = str(seg.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"[{_fmt_ts(start)}] {text}")
    return "\n".join(lines).strip()


def _save_full_transcript(video_id: str, title: str, transcript: str) -> str:
    tr_dir = DATA_DIR / "transcripts"
    tr_dir.mkdir(parents=True, exist_ok=True)
    safe_vid = re.sub(r"[^A-Za-z0-9_-]+", "", (video_id or "").strip()) or "unknown"
    out = tr_dir / f"{safe_vid}.txt"
    header = f"Title: {title}\nVideo ID: {video_id}\nGenerated: {now_local_str()}\n\n"
    out.write_text(header + transcript + "\n", encoding="utf-8")
    return str(out)


def _save_caption_source(video_id: str, caption_path: str) -> str:
    src = Path(caption_path).expanduser()
    if not src.exists():
        raise RuntimeError(f"Caption file not found: {src}")
    cap_dir = DATA_DIR / "captions"
    cap_dir.mkdir(parents=True, exist_ok=True)
    safe_vid = re.sub(r"[^A-Za-z0-9_-]+", "", (video_id or "").strip()) or "unknown"
    out = cap_dir / f"{safe_vid}.vtt"
    shutil.copyfile(src, out)
    return str(out)


def _transcript_body_lines(transcript: str) -> List[str]:
    out: List[str] = []
    for line in (transcript or "").splitlines():
        ln = line.strip()
        if not ln:
            continue
        if ln.startswith("Title:") or ln.startswith("Video ID:") or ln.startswith("Generated:"):
            continue
        out.append(ln)
    return out


def _question_keywords(question: str) -> List[str]:
    words = [
        w.lower()
        for w in re.findall(r"[A-Za-zА-Яа-яІіЇїЄєЁё0-9]{3,}", question or "")
    ]

    def _variants(w: str) -> List[str]:
        out = {w}
        # Simple English normalization so "titles" can match "title".
        if re.fullmatch(r"[a-z0-9]{3,}", w):
            if w.endswith("ies") and len(w) > 4:
                out.add(w[:-3] + "y")
            if w.endswith("es") and len(w) > 4:
                out.add(w[:-2])
            if w.endswith("s") and len(w) > 3:
                out.add(w[:-1])
            if w.endswith("ing") and len(w) > 5:
                out.add(w[:-3])
            if w.endswith("ed") and len(w) > 4:
                out.add(w[:-2])
        # Light Cyrillic stemming for broad keyword matching.
        if re.search(r"[а-яёіїєґ]", w):
            suffixes = (
                "ами",
                "ями",
                "ові",
                "ев",
                "ов",
                "ый",
                "ий",
                "ій",
                "ая",
                "ое",
                "ые",
                "их",
                "ых",
                "ом",
                "ем",
                "ам",
                "ям",
                "ах",
                "ях",
                "ів",
                "ев",
                "ов",
                "ей",
                "у",
                "ю",
                "а",
                "я",
                "и",
                "ы",
                "е",
                "о",
                "й",
            )
            for sfx in suffixes:
                if w.endswith(sfx) and len(w) - len(sfx) >= 3:
                    out.add(w[: -len(sfx)])
        return [x for x in out if len(x) >= 3]

    out: List[str] = []
    seen = set()
    for w in words:
        if w in QA_STOPWORDS:
            continue
        for v in _variants(w):
            if v in QA_STOPWORDS or v in seen:
                continue
            seen.add(v)
            out.append(v)
    return out


def _extract_title_from_saved_transcript(transcript: str, fallback: str) -> str:
    for line in (transcript or "").splitlines():
        ln = line.strip()
        if ln.lower().startswith("title:"):
            t = ln.split(":", 1)[1].strip()
            if t:
                return t
    return fallback


def _norm_text_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _verify_evidence_lines(evidence: List[str], transcript: str, *, limit: int = 3) -> List[str]:
    body_lines = _transcript_body_lines(transcript)
    if not body_lines:
        return []

    matched: List[str] = []
    seen = set()
    norm_lines = [_norm_text_for_match(re.sub(r"^\[\d{1,4}:[0-5]\d\]\s*", "", ln)) for ln in body_lines]
    original_lines = [re.sub(r"^\[\d{1,4}:[0-5]\d\]\s*", "", ln).strip() for ln in body_lines]

    for raw in evidence or []:
        ev = _norm_text_for_match(re.sub(r"^\[\d{1,4}:[0-5]\d\]\s*", "", str(raw)))
        if len(ev) < 8:
            continue
        for i, ln_norm in enumerate(norm_lines):
            if ev in ln_norm or ln_norm in ev:
                candidate = original_lines[i]
                key = _norm_text_for_match(candidate)
                if key in seen:
                    break
                seen.add(key)
                matched.append(candidate)
                break
        if len(matched) >= limit:
            break

    return matched


def _try_parse_json_object(raw: str) -> dict:
    txt = (raw or "").strip()
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    m = re.search(r"\{.*\}", txt, flags=re.S)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _compact_answer(text: str, max_chars: int = 220) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return ""
    if len(t) <= max_chars:
        return t
    m = re.search(r"(.{40,220}?[.!?])(?:\s|$)", t)
    if m:
        return m.group(1).strip()
    return t[: max_chars - 3].rstrip() + "..."


def _segments_from_transcript_text(transcript: str) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for line in (transcript or "").splitlines():
        ln = line.strip()
        if not ln:
            continue
        m = re.match(r"^\[(\d{1,4}):([0-5]\d)\]\s+(.+)$", ln)
        if not m:
            continue
        mm = int(m.group(1))
        ss = int(m.group(2))
        text = m.group(3).strip()
        if not text:
            continue
        start = float(mm * 60 + ss)
        out.append({"start": start, "end": start + 10.0, "text": text})

    if out:
        return out

    # Fallback for transcript files without [mm:ss] lines.
    t = 0.0
    for ln in _transcript_body_lines(transcript)[:1200]:
        out.append({"start": t, "end": t + 10.0, "text": ln})
        t += 10.0
    return out


def _sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _build_transcript_chunks(transcript: str) -> List[dict]:
    per_chunk = max(4, int((os.getenv("VIDEO_QA_CHUNK_LINES") or "8").strip()))
    overlap = max(0, int((os.getenv("VIDEO_QA_CHUNK_OVERLAP") or "2").strip()))
    stride = max(1, per_chunk - overlap)

    segments = _segments_from_transcript_text(transcript)
    if not segments:
        return []

    chunks: List[dict] = []
    idx = 0
    for start in range(0, len(segments), stride):
        window = segments[start : start + per_chunk]
        if not window:
            continue
        first_ts = float(window[0].get("start") or 0.0)
        last_ts = float(window[-1].get("end") or first_ts)
        lines = []
        for seg in window:
            ts = _fmt_ts(float(seg.get("start") or 0.0))
            text = str(seg.get("text") or "").strip()
            if text:
                lines.append(f"[{ts}] {text}")
        body = "\n".join(lines).strip()
        if not body:
            continue
        chunks.append(
            {
                "idx": idx,
                "start_ts": first_ts,
                "end_ts": max(first_ts, last_ts),
                "text": body,
            }
        )
        idx += 1
    return chunks


def _openai_embeddings(texts: List[str], model: str, timeout_sec: int) -> List[List[float]]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("missing_openai_api_key")
    if not texts:
        return []
    req = Request(
        "https://api.openai.com/v1/embeddings",
        data=json.dumps({"model": model, "input": texts}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    payload = json.loads(raw)
    data = payload.get("data") or []
    out: List[List[float]] = []
    for row in data:
        emb = row.get("embedding") if isinstance(row, dict) else None
        if isinstance(emb, list):
            out.append([float(x) for x in emb])
    return out


def _ollama_embeddings(texts: List[str], model: str, timeout_sec: int) -> List[List[float]]:
    if not texts:
        return []
    base = (os.getenv("VIDEO_LOCAL_LLM_URL") or "http://127.0.0.1:11434").strip()
    url = f"{base.rstrip('/')}/api/embeddings"
    out: List[List[float]] = []
    for txt in texts:
        req = Request(
            url,
            data=json.dumps({"model": model, "prompt": txt}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        payload = json.loads(raw)
        emb = payload.get("embedding")
        if not isinstance(emb, list):
            raise RuntimeError("ollama_embedding_missing")
        out.append([float(x) for x in emb])
    return out


def _embed_texts(texts: List[str], for_query: bool = False) -> Tuple[str, List[List[float]]]:
    backend = (os.getenv("VIDEO_EMBED_BACKEND") or "auto").strip().lower()
    openai_model = (os.getenv("VIDEO_EMBED_MODEL") or "text-embedding-3-small").strip()
    ollama_model = (os.getenv("VIDEO_LOCAL_EMBED_MODEL") or "nomic-embed-text").strip()
    timeout_sec = int((os.getenv("VIDEO_EMBED_TIMEOUT_SEC") or "90").strip())

    attempts: List[Tuple[str, str]] = []
    if backend == "openai":
        attempts = [("openai", openai_model)]
    elif backend in ("local", "ollama"):
        attempts = [("ollama", ollama_model)]
    else:
        if (os.getenv("OPENAI_API_KEY") or "").strip():
            attempts.append(("openai", openai_model))
        attempts.append(("ollama", ollama_model))

    last_error = ""
    for provider, model in attempts:
        try:
            vectors = (
                _openai_embeddings(texts, model, timeout_sec)
                if provider == "openai"
                else _ollama_embeddings(texts, model, timeout_sec)
            )
            vectors = [v for v in vectors if isinstance(v, list) and len(v) > 0]
            if not vectors:
                last_error = f"{provider}_empty_vectors"
                continue
            expected_dim = int(VIDEO_EMBED_DIM)
            if all(len(v) == expected_dim for v in vectors):
                return f"{provider}:{model}", vectors
            # Keep compatibility even if runtime dim env differs from provider output.
            if for_query and vectors and len(vectors[0]) > 0:
                return f"{provider}:{model}", vectors
            last_error = f"{provider}_dim_mismatch"
        except Exception as exc:
            last_error = f"{provider}_embed_error:{str(exc)[:180]}"

    raise RuntimeError(last_error or "embedding_failed")


def _plan_query_for_retrieval(question: str, target_lang: str) -> dict:
    planner_enabled = _env_bool("VIDEO_QA_QUERY_PLANNER", "0")
    if not planner_enabled:
        return {"keywords": [], "focus": "any", "expanded_question": ""}

    backend = (os.getenv("VIDEO_QA_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    openai_model = (os.getenv("VIDEO_QA_MODEL") or os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    claude_model = (
        os.getenv("VIDEO_QA_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()
    timeout_sec = int((os.getenv("VIDEO_QA_PLANNER_TIMEOUT_SEC") or "45").strip())
    system_prompt = (
        "You extract retrieval intent from a user question about a transcript. "
        "Return only JSON with keys: focus, keywords, expanded_question. "
        "focus must be one of: beginning, middle, ending, any. "
        "keywords must be a short list (<=8) of retrieval terms."
    )
    system_prompt = f"{system_prompt}\n{_ai_language_directive(target_lang)}"
    user_prompt = f"Question: {question}"
    try:
        text = ""
        attempts: List[Tuple[str, str]] = []
        if backend in ("local", "ollama"):
            attempts = [("local", local_model)]
        elif backend == "openai":
            attempts = [("openai", openai_model)]
        elif backend in ("claude", "anthropic"):
            attempts = [("claude", claude_model), ("local", local_model)]
        else:
            attempts = [("local", local_model), ("claude", claude_model), ("openai", openai_model)]
        for provider, model in attempts:
            try:
                text = _chat_with_provider(
                    provider=provider,
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.0,
                    timeout_sec=timeout_sec,
                    format_json=True,
                )
                if (text or "").strip():
                    break
            except Exception:
                continue
        payload = _try_parse_json_object(text)
        focus = str(payload.get("focus") or "any").strip().lower()
        if focus not in ("beginning", "middle", "ending", "any"):
            focus = "any"
        keywords_raw = payload.get("keywords")
        keywords: List[str] = []
        if isinstance(keywords_raw, list):
            for item in keywords_raw:
                val = str(item or "").strip()
                if val:
                    keywords.append(val)
        expanded = str(payload.get("expanded_question") or "").strip()
        return {
            "keywords": keywords[:8],
            "focus": focus,
            "expanded_question": expanded[:300],
        }
    except Exception:
        return {"keywords": [], "focus": "any", "expanded_question": ""}


def _lexical_chunk_scores(chunks: List[dict], question: str, planner: dict) -> Dict[int, float]:
    q_words = _question_keywords(question)
    planner_words = _question_keywords(" ".join(planner.get("keywords") or []))
    all_words = list(dict.fromkeys(q_words + planner_words))
    q_low = (question or "").strip().lower()
    scores: Dict[int, float] = {}

    for chunk in chunks:
        idx = int(chunk.get("idx") or 0)
        text = str(chunk.get("text") or "")
        low = text.lower()
        if not low:
            continue
        score = 0.0
        for word in all_words:
            if word in low:
                score += 1.0 + min(1.5, 0.2 * low.count(word))
        if q_low and q_low in low:
            score += 3.0
        scores[idx] = score
    return scores


def _chunk_focus_boost(chunk_idx: int, total: int, focus: str) -> float:
    if total <= 1:
        return 0.0
    pos = float(chunk_idx) / float(max(1, total - 1))
    if focus == "ending":
        return 0.25 * pos
    if focus == "beginning":
        return 0.25 * (1.0 - pos)
    if focus == "middle":
        return 0.20 * (1.0 - abs(pos - 0.5) * 2.0)
    return 0.0


def _semantic_chunk_scores(video_id: str, transcript: str, chunks: List[dict], query_text: str) -> Dict[int, float]:
    vid = (video_id or "").strip()
    if not vid or not chunks:
        return {}
    chunk_payload = [
        {
            "idx": int(ch.get("idx") or i),
            "start_ts": float(ch.get("start_ts") or 0.0),
            "end_ts": float(ch.get("end_ts") or 0.0),
            "text": str(ch.get("text") or ""),
        }
        for i, ch in enumerate(chunks)
    ]
    content_hash = _sha256_text(json.dumps(chunk_payload, ensure_ascii=False, sort_keys=True))
    chunk_texts = [str(ch.get("text") or "") for ch in chunk_payload]
    if not chunk_texts:
        return {}

    try:
        model_name, query_vecs = _embed_texts([query_text], for_query=True)
    except Exception:
        return {}
    if not query_vecs:
        return {}
    query_vec = query_vecs[0]
    stored_hash, stored_count = ("", 0)
    try:
        stored_hash, stored_count = get_transcript_embedding_meta(vid, model_name)
    except Exception:
        stored_hash, stored_count = ("", 0)

    # Ensure chunk JSON is present for later reuse/debugging.
    try:
        existing_chunks = load_transcript_chunks(vid)
        if len(existing_chunks) != len(chunk_payload):
            save_transcript_chunks(video_id=vid, content_hash=content_hash, chunks=chunk_payload)
    except Exception:
        pass

    needs_rebuild = stored_hash != content_hash or stored_count < len(chunk_payload)
    if needs_rebuild:
        try:
            model_name_for_chunks, chunk_vectors = _embed_texts(chunk_texts, for_query=False)
            if model_name_for_chunks == model_name and len(chunk_vectors) == len(chunk_payload):
                save_transcript_chunk_embeddings(
                    video_id=vid,
                    model=model_name,
                    content_hash=content_hash,
                    vectors=[(i, vec) for i, vec in enumerate(chunk_vectors)],
                )
            else:
                return {}
        except Exception:
            return {}

    try:
        hits = search_transcript_chunks_semantic(
            video_id=vid,
            model=model_name,
            query_vector=query_vec,
            limit=max(12, min(40, len(chunk_payload))),
        )
    except Exception:
        return {}

    scores: Dict[int, float] = {}
    for idx, similarity in hits:
        scores[int(idx)] = _clip01((float(similarity) + 1.0) / 2.0)
    return scores


def _rerank_chunk_ids_with_llm(
    question: str,
    chunks: List[dict],
    candidate_ids: List[int],
    target_lang: str,
) -> List[int]:
    if not candidate_ids or not _env_bool("VIDEO_QA_LLM_RERANK", "0"):
        return candidate_ids

    backend = (os.getenv("VIDEO_QA_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    openai_model = (os.getenv("VIDEO_QA_MODEL") or os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    claude_model = (
        os.getenv("VIDEO_QA_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()
    timeout_sec = int((os.getenv("VIDEO_QA_RERANK_TIMEOUT_SEC") or "45").strip())
    max_items = min(10, len(candidate_ids))
    ids = candidate_ids[:max_items]
    snippets: List[str] = []
    by_idx = {int(ch.get("idx") or i): ch for i, ch in enumerate(chunks)}
    for idx in ids:
        chunk = by_idx.get(idx) or {}
        start_ts = _fmt_ts(float(chunk.get("start_ts") or 0.0))
        body = str(chunk.get("text") or "")
        body = re.sub(r"\s+", " ", body).strip()
        snippets.append(f"{idx}: [{start_ts}] {body[:260]}")

    system_prompt = (
        "Rank transcript snippets by how directly they answer the user question. "
        "Return JSON only: {\"ordered_ids\":[...]} using only provided IDs."
    )
    system_prompt = f"{system_prompt}\n{_ai_language_directive(target_lang)}"
    user_prompt = (
        f"Question: {question}\n\n"
        "Snippets:\n"
        + "\n".join(snippets)
    )
    try:
        text = ""
        attempts: List[Tuple[str, str]] = []
        if backend in ("local", "ollama"):
            attempts = [("local", local_model)]
        elif backend == "openai":
            attempts = [("openai", openai_model)]
        elif backend in ("claude", "anthropic"):
            attempts = [("claude", claude_model), ("local", local_model)]
        else:
            attempts = [("local", local_model), ("claude", claude_model), ("openai", openai_model)]
        for provider, model in attempts:
            try:
                text = _chat_with_provider(
                    provider=provider,
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.0,
                    timeout_sec=timeout_sec,
                    format_json=True,
                )
                if (text or "").strip():
                    break
            except Exception:
                continue
        payload = _try_parse_json_object(text)
        ordered = payload.get("ordered_ids")
        if not isinstance(ordered, list):
            return candidate_ids
        valid = [int(x) for x in ordered if int(x) in ids]
        if not valid:
            return candidate_ids
        seen = set(valid)
        tail = [x for x in candidate_ids if x not in seen]
        return valid + tail
    except Exception:
        return candidate_ids


def _read_transcript(path: str) -> str:
    p = Path(path).expanduser()
    if not p.exists():
        raise RuntimeError(f"Transcript file not found: {p}")
    txt = p.read_text("utf-8", errors="ignore").strip()
    if not txt:
        raise RuntimeError("Transcript file is empty.")
    return txt


def _analyze_transcript_with_ai(title: str, transcript: str) -> str:
    return _analyze_transcript_with_ai_with_progress(title, transcript, None)


def _estimate_local_analysis_parts(transcript: str) -> int:
    text = str(transcript or "")
    if not text:
        return 1
    max_chars = int((os.getenv("VIDEO_AI_MAX_CHARS") or "24000").strip())
    used = text[:max_chars]
    trigger_chars = max(4000, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_TRIGGER_CHARS") or "12000").strip()))
    if len(used) < trigger_chars:
        return 1
    chunk_chars = max(2500, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_CHARS") or "7000").strip()))
    overlap_chars = max(0, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_OVERLAP_CHARS") or "400").strip()))
    max_chunks = max(1, int((os.getenv("VIDEO_AI_LOCAL_MAX_CHUNKS") or "8").strip()))
    chunks = _split_text_windows(used, chunk_chars, overlap_chars)[:max_chunks]
    return max(1, len(chunks))


def _split_text_windows(text: str, window_chars: int, overlap_chars: int) -> List[str]:
    src = str(text or "").strip()
    if not src:
        return []
    win = max(1200, int(window_chars or 0))
    overlap = max(0, min(int(overlap_chars or 0), win // 3))
    out: List[str] = []
    start = 0
    n = len(src)
    while start < n:
        end = min(n, start + win)
        if end < n:
            min_cut = start + int(win * 0.55)
            cut = src.rfind("\n", min_cut, end)
            if cut < 0:
                cut = src.rfind(" ", min_cut, end)
            if cut > start:
                end = cut
        chunk = src[start:end].strip()
        if chunk:
            out.append(chunk)
        if end >= n:
            break
        start = max(end - overlap, start + 1)
    return out


def _analyze_local_transcript_chunked(
    *,
    title: str,
    transcript: str,
    truncated: bool,
    lang_code: str,
    system_prompt: str,
    model: str,
    timeout_sec: int,
    progress_cb: Optional[Callable[[int, Optional[int], bool], None]],
    chunk_progress_cb: Optional[Callable[[int, int], None]],
) -> Tuple[str, int]:
    trigger_chars = max(4000, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_TRIGGER_CHARS") or "12000").strip()))
    if len(transcript or "") < trigger_chars:
        return "", 0

    chunk_chars = max(2500, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_CHARS") or "7000").strip()))
    overlap_chars = max(0, int((os.getenv("VIDEO_AI_LOCAL_CHUNK_OVERLAP_CHARS") or "400").strip()))
    max_chunks = max(1, int((os.getenv("VIDEO_AI_LOCAL_MAX_CHUNKS") or "8").strip()))
    synth_max_chars = max(8000, int((os.getenv("VIDEO_AI_LOCAL_SYNTH_MAX_CHARS") or "22000").strip()))

    chunks = _split_text_windows(transcript, chunk_chars, overlap_chars)[:max_chunks]
    if len(chunks) <= 1:
        return "", 0
    if chunk_progress_cb:
        chunk_progress_cb(0, len(chunks))

    notes: List[str] = []
    generated_chars = 0
    for idx, chunk in enumerate(chunks, start=1):
        if lang_code == "en":
            part_user_prompt = (
                f"Title: {title}\n"
                f"Transcript part {idx}/{len(chunks)} {'(source transcript was truncated)' if truncated else ''}:\n\n"
                f"{chunk}\n\n"
                "Task: summarize ONLY this part with concrete facts, practical actions, and uncertainties."
            )
        else:
            part_user_prompt = (
                f"Назва: {title}\n"
                f"Частина транскрипту {idx}/{len(chunks)} {'(вхідний транскрипт був обрізаний)' if truncated else ''}:\n\n"
                f"{chunk}\n\n"
                "Завдання: підсумуй ТІЛЬКИ цю частину з фактами, практичними діями та невизначеностями."
            )
        try:
            part_text = _chat_with_provider(
                provider="local",
                model=model,
                system_prompt=system_prompt,
                user_prompt=part_user_prompt,
                temperature=0.2,
                timeout_sec=timeout_sec,
                progress_cb=None,
            )
        except Exception:
            part_text = ""
        part_text = str(part_text or "").strip()
        if not part_text:
            continue
        notes.append(part_text)
        generated_chars += len(part_text)
        if progress_cb:
            progress_cb(generated_chars, max(1, generated_chars // 4), False)
        if chunk_progress_cb:
            chunk_progress_cb(idx, len(chunks))

    if not notes:
        return "", len(chunks)
    if len(notes) == 1:
        return notes[0], len(chunks)

    joined = "\n\n".join(f"PART {i + 1}/{len(notes)}:\n{txt}" for i, txt in enumerate(notes))
    joined = joined[:synth_max_chars]
    if lang_code == "en":
        synth_user_prompt = (
            f"Title: {title}\n"
            "Below are analyses from multiple transcript parts. Merge them into one final coherent analysis.\n\n"
            f"{joined}"
        )
    else:
        synth_user_prompt = (
            f"Назва: {title}\n"
            "Нижче аналізи з кількох частин транскрипту. Об'єднай їх у фінальний узгоджений аналіз.\n\n"
            f"{joined}"
        )
    try:
        final_text = _chat_with_provider(
            provider="local",
            model=model,
            system_prompt=system_prompt,
            user_prompt=synth_user_prompt,
            temperature=0.2,
            timeout_sec=timeout_sec,
            progress_cb=None,
        )
    except Exception:
        final_text = ""
    final_text = str(final_text or "").strip()
    if final_text:
        if progress_cb:
            done_chars = generated_chars + len(final_text)
            progress_cb(done_chars, max(1, done_chars // 4), False)
        return final_text, len(chunks)

    fallback_text = "\n\n".join(f"Part {i + 1}/{len(notes)}\n{txt}" for i, txt in enumerate(notes))
    return fallback_text.strip(), len(chunks)


def _analyze_transcript_with_ai_with_progress(
    title: str,
    transcript: str,
    progress_cb: Optional[Callable[[int, Optional[int], bool], None]],
    chunk_progress_cb: Optional[Callable[[int, int], None]] = None,
) -> str:
    enabled = _env_bool("VIDEO_USE_AI_ANALYZER", "1")
    if not enabled:
        return ""

    backend = (os.getenv("VIDEO_AI_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    openai_model = (os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    claude_model = (
        os.getenv("VIDEO_AI_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()
    lang_code, lang_label = _analysis_output_language_for_text(transcript)
    max_chars = int((os.getenv("VIDEO_AI_MAX_CHARS") or "24000").strip())
    timeout_sec = int((os.getenv("VIDEO_AI_TIMEOUT_SEC") or "240").strip())
    used = transcript[:max_chars]
    truncated = len(transcript) > len(used)

    system_prompt = (
        os.getenv("VIDEO_AI_PROMPT")
        or _default_ai_analysis_prompt(lang_code)
    ).strip()
    system_prompt = f"{system_prompt}\n{_ai_language_directive(lang_code)}"

    if lang_code == "en":
        user_prompt = (
            f"Title: {title}\n"
            f"Transcript {'(truncated to character limit)' if truncated else ''}:\n\n"
            f"{used}"
        )
    else:
        user_prompt = (
            f"Назва: {title}\n"
            f"Транскрипт {'(обрізаний до ліміту символів)' if truncated else ''}:\n\n"
            f"{used}"
        )

    try:
        txt = ""
        used_provider = ""
        used_model = ""
        used_chunk_parts = 0
        attempts: List[Tuple[str, str]] = []
        if backend in ("local", "ollama"):
            attempts = [("local", local_model)]
        elif backend == "openai":
            attempts = [("openai", openai_model)]
        elif backend in ("claude", "anthropic"):
            attempts = [("claude", claude_model), ("local", local_model)]
        elif backend == "auto":
            attempts = [("local", local_model), ("claude", claude_model), ("openai", openai_model)]
        else:
            attempts = [("local", local_model)]
        for provider, model in attempts:
            try:
                chunk_parts = 0
                if provider == "local":
                    chunked_text, chunk_parts = _analyze_local_transcript_chunked(
                        title=title,
                        transcript=used,
                        truncated=truncated,
                        lang_code=lang_code,
                        system_prompt=system_prompt,
                        model=model,
                        timeout_sec=timeout_sec,
                        progress_cb=progress_cb,
                        chunk_progress_cb=chunk_progress_cb,
                    )
                    if chunked_text.strip():
                        txt = chunked_text
                        used_provider = provider
                        used_model = model
                        used_chunk_parts = chunk_parts
                        break
                txt = _chat_with_provider(
                    provider=provider,
                    model=model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.2,
                    timeout_sec=timeout_sec,
                    progress_cb=progress_cb if provider == "local" else None,
                )
                if (txt or "").strip():
                    used_provider = provider
                    used_model = model
                    used_chunk_parts = chunk_parts
                    break
            except Exception:
                continue

        if not txt:
            return ""
        prefix = "🧠 AI Video Analysis\n"
        if used_provider == "local":
            prefix += f"🖥️ Backend: local ({local_model})\n"
        elif used_provider == "claude":
            prefix += f"☁️ Backend: Claude ({used_model})\n"
        else:
            prefix += f"☁️ Backend: OpenAI ({openai_model})\n"
        prefix += f"🗣 Output language: {lang_label}\n"
        if used_provider == "local" and used_chunk_parts > 1:
            prefix += f"ℹ️ Local chunked analysis: {used_chunk_parts} parts.\n"
        if truncated:
            prefix += "ℹ️ Analysis used a truncated transcript window due to size limits.\n"
        return with_tg_time(prefix + txt)
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, RuntimeError, ValueError):
        # Keep transcript flow working even if LLM is unavailable.
        return ""


async def _run_ai_analysis_with_progress(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
    total_steps: int,
    title: str,
    transcript: str,
) -> str:
    loop = asyncio.get_running_loop()
    progress_q: asyncio.Queue[Tuple[int, Optional[int], bool]] = asyncio.Queue()

    def _progress(chars: int, tokens: Optional[int], done: bool) -> None:
        try:
            loop.call_soon_threadsafe(progress_q.put_nowait, (chars, tokens, done))
        except Exception:
            pass

    task = asyncio.create_task(
        asyncio.to_thread(
            _analyze_transcript_with_ai_with_progress,
            title,
            transcript,
            _progress,
        )
    )

    last_chars = -1
    last_edit = 0.0
    last_heartbeat = 0.0
    started_at = loop.time()
    while True:
        if task.done() and progress_q.empty():
            break
        try:
            chars, tokens, done = await asyncio.wait_for(progress_q.get(), timeout=0.8)
        except asyncio.TimeoutError:
            now = loop.time()
            if (now - last_heartbeat) >= 6.0:
                elapsed = int(now - started_at)
                try:
                    await context.application.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=with_tg_time(
                            "🎙 Ask Video\n"
                            f"Step 4/{total_steps}: Running LLM analysis\n"
                            f"⏱ Elapsed: {elapsed}s\n"
                            "🧠 Waiting for generation chunks..."
                        ),
                        disable_web_page_preview=True,
                    )
                    last_heartbeat = now
                except Exception:
                    pass
            continue

        now = loop.time()
        if not done and chars == last_chars:
            continue
        if not done and (now - last_edit) < 2.5:
            continue

        token_txt = f"{tokens} tokens" if isinstance(tokens, int) and tokens > 0 else f"~{max(1, chars // 4)} tokens"
        try:
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=with_tg_time(
                    "🎙 Ask Video\n"
                    f"Step 4/{total_steps}: Running LLM analysis\n"
                    f"🧠 Generated: {chars} chars ({token_txt})"
                ),
                disable_web_page_preview=True,
            )
            last_chars = chars
            last_edit = now
        except Exception:
            pass

    return await task


def _analysis_ttl_seconds() -> int:
    raw = (os.getenv("VIDEO_AI_ANALYSIS_TTL_HOURS") or "24").strip()
    try:
        hours = float(raw)
    except Exception:
        hours = 24.0
    if hours <= 0:
        return 0
    return int(hours * 3600)


def _get_cached_ai_analysis(record: dict, ttl_seconds: int, expected_lang: str) -> Tuple[str, int]:
    if ttl_seconds <= 0:
        return "", 0
    cached = str(record.get("video_ai_analysis") or "").strip()
    if not cached:
        return "", 0
    stored_lang = str(record.get("video_ai_analysis_lang") or "").strip().lower()
    if not stored_lang or stored_lang != expected_lang:
        return "", 0
    try:
        saved_ts = float(record.get("video_ai_analysis_saved_at_epoch") or 0.0)
    except Exception:
        saved_ts = 0.0
    if saved_ts <= 0:
        return "", 0
    age_sec = int(max(0.0, time.time() - saved_ts))
    if age_sec <= ttl_seconds:
        return cached, age_sec
    return "", age_sec


def _fmt_ts(seconds: float) -> str:
    sec = max(0, int(seconds))
    mm = sec // 60
    ss = sec % 60
    return f"{mm:02d}:{ss:02d}"


def _find_potential_concerns(segments: List[Dict[str, object]]) -> List[dict]:
    concerns: List[dict] = []
    for seg in segments:
        text = str(seg.get("text") or "").strip()
        if not text:
            continue
        ts = float(seg.get("start") or 0.0)

        # ASR nonsense heuristic: unusually noisy text.
        letters = sum(ch.isalpha() for ch in text)
        noise_ratio = 0.0 if not text else (1.0 - (letters / max(1, len(text))))
        if len(text) >= 35 and noise_ratio > 0.45:
            concerns.append(
                {
                    "time": _fmt_ts(ts),
                    "label": "Possible transcription noise/nonsense",
                    "snippet": text[:220],
                    "verses": [],
                }
            )

    # Keep concise to avoid flooding Telegram.
    return concerns[:6]


def _format_notes(
    title: str,
    notes: OrderedDict[str, List[str]],
    diar_used: bool,
    references: List[str],
    concerns: List[dict],
) -> str:
    if not notes:
        return with_tg_time("I could not extract enough speech content to build notes.")

    lines = [f"📝 Video Notes & Key Ideas", f"🎬 {title}", ""]
    lines.append(f"👥 Detected main speakers: {len(notes)}")
    lines.append("")
    if not diar_used:
        lines.append("ℹ️ Speaker diarization unavailable; speaker grouping quality may be lower.")
        lines.append("")

    for i, (speaker, bullets) in enumerate(notes.items(), start=1):
        pretty = speaker.replace("SPEAKER_", "Speaker ")
        lines.append(f"{i}. {pretty}")
        for bullet in bullets:
            lines.append(f"• {bullet}")
        lines.append("")

    lines.append("🔖 References Mentioned")
    if references:
        for ref in references[:20]:
            lines.append(f"• {ref}")
    else:
        lines.append("• No explicit references detected in transcript.")
    lines.append("")

    lines.append("🧪 Potential Transcript Issues (Auto-check)")
    if concerns:
        lines.append("⚠️ These are automatic flags; verify in original audio before relying on them.")
        for c in concerns:
            lines.append(f"• [{c['time']}] {c['label']}")
            lines.append(f"  ↳ “{c['snippet']}”")
            if c.get("verses"):
                lines.append(f"  ↳ Suggested verses: {', '.join(c['verses'])}")
    else:
        lines.append("• No major transcript-quality issues detected.")
    lines.append("")

    return with_tg_time("\n".join(lines).strip())


async def _send_long_to_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    max_len = 3900
    if len(text) <= max_len:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=text,
            disable_web_page_preview=True,
        )
        return

    parts = []
    cur = []
    cur_len = 0
    for line in text.splitlines():
        if cur_len + len(line) + 1 > max_len and cur:
            parts.append("\n".join(cur))
            cur = [line]
            cur_len = len(line) + 1
        else:
            cur.append(line)
            cur_len += len(line) + 1
    if cur:
        parts.append("\n".join(cur))

    for part in parts:
        await context.application.bot.send_message(
            chat_id=chat_id,
            text=part,
            disable_web_page_preview=True,
        )


def _fallback_answer_from_transcript(question: str, transcript: str) -> str:
    body_lines = _transcript_body_lines(transcript)
    q_words = _question_keywords(question)

    if not q_words:
        if not body_lines:
            return ""
        for ln in body_lines:
            clean = re.sub(r"^\[\d{1,4}:[0-5]\d\]\s*", "", ln).strip()
            if len(clean) >= 20:
                return _compact_answer(clean)
        return _compact_answer(body_lines[0]) if body_lines else ""

    scored: List[Tuple[int, str]] = []
    for line in body_lines:
        ln = line.strip()
        if not ln:
            continue
        low = ln.lower()
        score = sum(1 for w in q_words if w in low)
        if score > 0:
            scored.append((score, ln))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [ln for _s, ln in scored[:5]]
    if not top:
        return ""

    first = re.sub(r"^\[\d{1,4}:[0-5]\d\]\s*", "", top[0]).strip()
    if first:
        return _compact_answer(first)
    return ""


def _build_qa_context_from_transcript(
    *,
    question: str,
    transcript: str,
    max_chars: int,
    video_id: str = "",
    target_lang: str = "en",
) -> Tuple[str, bool, List[str], dict]:
    chunks = _build_transcript_chunks(transcript)
    if not chunks:
        lines = _transcript_body_lines(transcript)
        txt = "\n".join(lines[:120]).strip()
        return txt, len(txt) < len(transcript), lines[:4], {"focus": "any", "keywords": []}

    planner = _plan_query_for_retrieval(question, target_lang)
    focus = str(planner.get("focus") or "any").strip().lower()
    expanded = str(planner.get("expanded_question") or "").strip()
    query_text = question if not expanded else f"{question}\n{expanded}"

    lexical = _lexical_chunk_scores(chunks, question, planner)
    semantic = _semantic_chunk_scores(video_id, transcript, chunks, query_text)

    max_lex = max(lexical.values()) if lexical else 0.0
    combined: List[Tuple[float, int]] = []
    has_semantic = bool(semantic)
    for chunk in chunks:
        idx = int(chunk.get("idx") or 0)
        lex_norm = 0.0 if max_lex <= 0 else (lexical.get(idx, 0.0) / max_lex)
        sem_score = semantic.get(idx, 0.0)
        base = (0.45 * lex_norm + 0.55 * sem_score) if has_semantic else lex_norm
        score = base + _chunk_focus_boost(idx, len(chunks), focus)
        combined.append((score, idx))

    combined.sort(key=lambda x: x[0], reverse=True)
    top_chunk_count = max(4, int((os.getenv("VIDEO_QA_TOP_CHUNKS") or "6").strip()))
    candidate_ids = [idx for _score, idx in combined[: max(8, top_chunk_count)]]
    if not candidate_ids:
        candidate_ids = [int(chunks[-1].get("idx") or (len(chunks) - 1))]

    candidate_ids = _rerank_chunk_ids_with_llm(question, chunks, candidate_ids, target_lang)
    chosen_primary = candidate_ids[:top_chunk_count]
    picked = set(chosen_primary)
    for idx in list(chosen_primary):
        if idx - 1 >= 0:
            picked.add(idx - 1)
        if idx + 1 < len(chunks):
            picked.add(idx + 1)

    out: List[str] = []
    used = 0
    evidence_lines: List[str] = []
    by_idx = {int(ch.get("idx") or i): ch for i, ch in enumerate(chunks)}
    for idx in sorted(picked):
        chunk = by_idx.get(idx)
        if not chunk:
            continue
        block = str(chunk.get("text") or "").strip()
        if not block:
            continue
        if used + len(block) + 2 > max_chars and out:
            break
        out.append(block)
        used += len(block) + 2
        for line in block.splitlines()[:2]:
            clean = line.strip()
            if clean and clean not in evidence_lines:
                evidence_lines.append(clean)
            if len(evidence_lines) >= 6:
                break
        if len(evidence_lines) >= 6:
            continue

    context_txt = "\n\n".join(out).strip()
    if not context_txt:
        context_txt = transcript[:max_chars]
    truncated = len(context_txt) < len(transcript)
    return context_txt, truncated, evidence_lines, planner


def answer_question_from_transcript(
    *,
    question: str,
    transcript_path: str,
    title_hint: str = "",
    progress_cb: Optional[Callable[[int, Optional[int], bool], None]] = None,
) -> str:
    transcript = _read_transcript(transcript_path)
    target_lang, translate_requested = _qa_target_language(question)
    if translate_requested:
        source_text = _extract_translation_source_text(question)
        if source_text:
            translated = _translate_text_for_output(source_text, target_lang)
            final = translated.strip() or source_text
            return with_tg_time(f"❓ Q&A Answer\n\n{final}")

    backend = (os.getenv("VIDEO_QA_BACKEND") or "local").strip().lower()
    local_model = (os.getenv("VIDEO_LOCAL_LLM_MODEL") or "llama3.2:3b").strip()
    local_fallback_model = (os.getenv("VIDEO_QA_LOCAL_FALLBACK_MODEL") or "").strip()
    openai_model = (os.getenv("VIDEO_QA_MODEL") or os.getenv("VIDEO_AI_MODEL") or "gpt-4.1-mini").strip()
    openai_fallback_model = (os.getenv("VIDEO_QA_FALLBACK_MODEL") or "gpt-4.1-nano").strip()
    claude_model = (
        os.getenv("VIDEO_QA_CLAUDE_MODEL")
        or os.getenv("VIDEO_CLAUDE_MODEL")
        or "claude-3-5-sonnet-latest"
    ).strip()
    claude_fallback_model = (os.getenv("VIDEO_QA_CLAUDE_FALLBACK_MODEL") or "claude-3-5-haiku-latest").strip()
    allow_local_fallback = _env_bool("VIDEO_QA_ALLOW_LOCAL_FALLBACK", "1")
    max_chars = int((os.getenv("VIDEO_QA_MAX_CHARS") or "24000").strip())
    timeout_sec = int((os.getenv("VIDEO_QA_TIMEOUT_SEC") or "180").strip())
    qa_retries = max(1, int((os.getenv("VIDEO_QA_RETRIES") or "1").strip()))
    stem = re.sub(r"[^A-Za-z0-9_-]+", "", Path(transcript_path).stem)
    resolved_video_id = stem if re.fullmatch(r"[A-Za-z0-9_-]{6,20}", stem or "") else ""
    context_txt, truncated, evidence_hints, planner = _build_qa_context_from_transcript(
        question=question,
        transcript=transcript,
        max_chars=max_chars,
        video_id=resolved_video_id,
        target_lang=target_lang,
    )
    last_ai_error = ""
    system_prompt = (
        os.getenv("VIDEO_QA_PROMPT")
        or (
            "You are a strict transcript-grounded assistant. "
            "Use ONLY the provided transcript content. "
            "Do not use outside knowledge. "
            "If evidence is missing or ambiguous, return insufficient. "
            "Return ONLY JSON with keys: status, answer, evidence. "
            "status must be 'answered' or 'insufficient'. "
            "evidence must be a list of short verbatim lines from transcript content. "
            "Make answer short (one sentence, <= 25 words)."
        )
    ).strip()
    system_prompt = (
        f"{system_prompt}\n"
        f"{_ai_language_directive(target_lang)}\n"
        "If the user requests translation, provide the translated answer in the requested language."
    )
    user_prompt = (
        f"Title: {title_hint or 'Video'}\n"
        f"Retrieval focus: {planner.get('focus') or 'any'}\n"
        f"Planner keywords: {', '.join(planner.get('keywords') or [])}\n"
        f"Priority evidence lines: {' | '.join(evidence_hints[:4])}\n\n"
        f"Transcript file content {'(filtered/truncated)' if truncated else ''}:\n{context_txt}\n\n"
        f"Question: {question}\n\n"
        "Return JSON only. Example:\n"
        '{"status":"answered","answer":"...","evidence":["line 1","line 2"]}'
    )

    backends: List[Tuple[str, str]] = []
    if backend in ("local", "ollama"):
        backends.append(("local", local_model))
        if local_fallback_model and local_fallback_model != local_model:
            backends.append(("local", local_fallback_model))
    elif backend == "openai":
        backends.append(("openai", openai_model))
        if openai_fallback_model and openai_fallback_model != openai_model:
            backends.append(("openai", openai_fallback_model))
    elif backend in ("claude", "anthropic"):
        backends.append(("claude", claude_model))
        if claude_fallback_model and claude_fallback_model != claude_model:
            backends.append(("claude", claude_fallback_model))
        if local_model:
            backends.append(("local", local_model))
        if local_fallback_model and local_fallback_model != local_model:
            backends.append(("local", local_fallback_model))
    elif backend == "auto":
        backends.append(("local", local_model))
        if local_fallback_model and local_fallback_model != local_model:
            backends.append(("local", local_fallback_model))
        backends.append(("claude", claude_model))
        if claude_fallback_model and claude_fallback_model != claude_model:
            backends.append(("claude", claude_fallback_model))
        backends.append(("openai", openai_model))
        if openai_fallback_model and openai_fallback_model != openai_model:
            backends.append(("openai", openai_fallback_model))
    else:
        backends.append(("local", local_model))

    saw_insufficient = False
    for current_backend, current_model in backends:
        for attempt in range(qa_retries):
            try:
                text = _chat_with_provider(
                    provider=current_backend,
                    model=current_model,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=0.2,
                    timeout_sec=timeout_sec,
                    format_json=True,
                    progress_cb=progress_cb if current_backend == "local" else None,
                )

                payload = _try_parse_json_object(text)
                if not payload:
                    last_ai_error = f"{current_backend}_invalid_json"
                    if attempt + 1 < qa_retries:
                        time.sleep(1.0 * (attempt + 1))
                        continue
                    break

                status = str(payload.get("status") or "").strip().lower()
                answer = str(payload.get("answer") or "").strip()
                raw_evidence = payload.get("evidence")
                evidence_lines: List[str] = []
                if isinstance(raw_evidence, list):
                    for item in raw_evidence:
                        ln = str(item or "").strip()
                        if ln:
                            evidence_lines.append(ln)
                if not evidence_lines:
                    evidence_lines = list(evidence_hints)
                verified_evidence = _verify_evidence_lines(evidence_lines, transcript, limit=3)

                if status == "answered" and answer and verified_evidence:
                    final_answer = _compact_answer(answer)
                    if final_answer:
                        final_answer = _ensure_output_language(final_answer, target_lang)
                        backend_line = _provider_caption(current_backend, current_model)
                        return with_tg_time(f"❓ Q&A Answer\n{backend_line}\n\n{final_answer}")
                    break
                saw_insufficient = True
                last_ai_error = f"{current_backend}_insufficient"
                if attempt + 1 < qa_retries:
                    time.sleep(0.8 * (attempt + 1))
                    continue
                break
            except HTTPError as e:
                last_ai_error = f"{current_backend}_http_{e.code}"
                if e.code in (429, 500, 502, 503, 504) and attempt + 1 < qa_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                break
            except RuntimeError as e:
                last_ai_error = str(e) or f"{current_backend}_runtime_error"
                if attempt + 1 < qa_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                break
            except (URLError, TimeoutError, OSError, json.JSONDecodeError):
                last_ai_error = f"{current_backend}_network_or_parse_error"
                if attempt + 1 < qa_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                break

    if not allow_local_fallback:
        reason = last_ai_error or ("insufficient" if saw_insufficient else "ai_unavailable")
        return with_tg_time(
            "❓ Q&A Answer\n\n"
            f"{_qa_unavailable_text(target_lang, reason)}"
        )

    # Local transcript fallback when model output is insufficient/unavailable.
    fallback = _fallback_answer_from_transcript(question, context_txt or transcript)
    if fallback:
        fallback = _ensure_output_language(fallback, target_lang)
        return with_tg_time(f"❓ Q&A Answer\n🧩 Backend: local transcript fallback\n\n{fallback}")
    return with_tg_time(f"❓ Q&A Answer\n\n{_qa_unreliable_text(target_lang)}")


async def run_video_notes(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    url: str,
    title_hint: str,
    video_id: str = "",
    local_video_path: str = "",
    note_scope: str = "LIVE",
) -> Dict[str, str] | None:
    resolved_video_id = (video_id or extract_youtube_id(url) or "").strip()
    auto_analyze = _env_bool("VIDEO_AUTO_ANALYZE_ON_SAVE", "1") and _env_bool("VIDEO_USE_AI_ANALYZER", "1")
    total_steps = 4 if auto_analyze else 3
    source_line = "Preparing transcript source"
    status = await context.application.bot.send_message(
        chat_id=chat_id,
        text=with_tg_time(f"🎙 Ask Video for {note_scope}: {title_hint}\nStep 1/{total_steps}: {source_line}"),
        disable_web_page_preview=True,
    )

    temp_dir = Path(tempfile.mkdtemp(prefix="video_notes_"))
    try:
        use_yt_captions = (os.getenv("VIDEO_USE_YT_CAPTIONS") or "1").strip() == "1"
        audio_path = ""
        title = title_hint
        transcript_source = "audio transcription"
        caption_tmp_path = ""
        caption_saved_path = ""
        transcript_text = ""
        segments: List[Dict[str, object]] = []

        cached_transcript_path = ""
        if resolved_video_id:
            p = DATA_DIR / "transcripts" / f"{resolved_video_id}.txt"
            if p.exists() and p.is_file() and p.stat().st_size > 0:
                cached_transcript_path = str(p)

        if cached_transcript_path:
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"🎙 Ask Video\nStep 1/{total_steps}: Reusing saved transcript"),
                disable_web_page_preview=True,
            )
            try:
                transcript_text = await asyncio.to_thread(_read_transcript, cached_transcript_path)
                title = _extract_title_from_saved_transcript(transcript_text, title)
                segments = _segments_from_transcript_text(transcript_text)
                transcript_source = "cached transcript"
            except Exception:
                transcript_text = ""
                segments = []

        if not segments and use_yt_captions and url:
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"🎙 Ask Video\nStep 1/{total_steps}: Trying YouTube captions/transcript"),
                disable_web_page_preview=True,
            )
            try:
                segments, title, caption_tmp_path = await asyncio.to_thread(
                    _download_youtube_caption_segments,
                    url,
                    temp_dir,
                    title_hint,
                )
                transcript_source = "youtube captions"
            except Exception:
                segments = []
                caption_tmp_path = ""

        if not segments:
            source_line = "Extracting audio from saved file" if local_video_path else "Downloading audio"
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"🎙 Ask Video\nStep 1/{total_steps}: {source_line}"),
                disable_web_page_preview=True,
            )
            if local_video_path:
                try:
                    audio_path, title = await asyncio.to_thread(_extract_audio_from_local, local_video_path, temp_dir)
                except Exception:
                    await context.application.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status.message_id,
                        text=with_tg_time(f"🎙 Ask Video\nStep 1/{total_steps}: Local extract failed, falling back to URL audio download"),
                        disable_web_page_preview=True,
                    )

            if not audio_path:
                audio_path, title = await asyncio.to_thread(_download_audio, url, temp_dir)

            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"🎙 Ask Video\nStep 2/{total_steps}: Transcribing speech"),
                disable_web_page_preview=True,
            )
            segments = await asyncio.to_thread(_transcribe_segments, audio_path)

        if transcript_source == "youtube captions":
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"🎙 Ask Video\nStep 2/{total_steps}: Using YouTube captions transcript"),
                disable_web_page_preview=True,
            )
            if caption_tmp_path:
                try:
                    caption_saved_path = await asyncio.to_thread(
                        _save_caption_source,
                        resolved_video_id or "unknown",
                        caption_tmp_path,
                    )
                except Exception:
                    caption_saved_path = ""
        if not transcript_text:
            transcript_text = _segments_to_transcript_text(segments)
        transcript_path = cached_transcript_path
        if not transcript_path:
            transcript_path = await asyncio.to_thread(
                _save_full_transcript,
                resolved_video_id or "unknown",
                title,
                transcript_text,
            )

        await context.application.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status.message_id,
            text=with_tg_time(f"🎙 Ask Video\nStep 3/{total_steps}: Saving transcript context"),
            disable_web_page_preview=True,
        )
        text = (
            "✅ Transcript saved for this video.\n"
            f"🎬 {title}\n"
            f"📄 Transcript source: {transcript_source}\n"
            f"📄 Transcript saved on server: {transcript_path}\n"
            "❓ Now send your question about this video."
        )
        if caption_saved_path:
            text += f"\n📄 Caption file saved on server: {caption_saved_path}"

        analysis_text = ""
        analysis_from_cache = False
        idx: Dict[str, dict] = {}
        rec: dict = {}
        if resolved_video_id:
            try:
                idx = load_index()
                rec = idx.get(resolved_video_id) or {}
            except Exception:
                idx = {}
                rec = {}
        if auto_analyze:
            ttl_sec = _analysis_ttl_seconds()
            analysis_lang_code, _analysis_lang_label = _analysis_output_language_for_text(transcript_text)
            cached_text = ""
            cached_age_sec = 0
            if rec:
                cached_text, cached_age_sec = _get_cached_ai_analysis(rec, ttl_sec, analysis_lang_code)

            if cached_text:
                analysis_text = cached_text
                analysis_from_cache = True
                age_mins = max(1, int(cached_age_sec / 60))
                await context.application.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status.message_id,
                    text=with_tg_time(
                        "🎙 Ask Video\n"
                        f"Step 4/{total_steps}: Reusing cached LLM analysis\n"
                        f"♻️ Cache age: {age_mins} min (TTL: {int(ttl_sec / 3600)}h)"
                    ),
                    disable_web_page_preview=True,
                )
            else:
                await context.application.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status.message_id,
                    text=with_tg_time(f"🎙 Ask Video\nStep 4/{total_steps}: Running LLM analysis"),
                    disable_web_page_preview=True,
                )
                analysis_text = await _run_ai_analysis_with_progress(
                    context,
                    chat_id=chat_id,
                    message_id=status.message_id,
                    total_steps=total_steps,
                    title=title,
                    transcript=transcript_text,
                )

        if resolved_video_id:
            try:
                if not idx:
                    idx = load_index()
                if not rec:
                    rec = idx.get(resolved_video_id) or {}
                rec["video_notes"] = text
                rec["video_notes_updated_at_local"] = now_local_str()
                rec["video_transcript_path"] = transcript_path
                rec["video_transcript_chars"] = len(transcript_text)
                rec["video_transcript_source"] = transcript_source
                if caption_saved_path:
                    rec["video_caption_path"] = caption_saved_path
                if analysis_text:
                    rec["video_ai_analysis"] = analysis_text
                    rec["video_ai_analysis_lang"] = analysis_lang_code
                    if (not analysis_from_cache) or not rec.get("video_ai_analysis_saved_at_epoch"):
                        rec["video_ai_analysis_saved_at_epoch"] = int(time.time())
                idx[resolved_video_id] = rec
                save_index(idx)
            except Exception:
                pass

        try:
            await context.application.bot.delete_message(chat_id=chat_id, message_id=status.message_id)
        except Exception:
            pass

        await _send_long_to_chat(context, chat_id, text)
        if analysis_text:
            await _send_long_to_chat(context, chat_id, analysis_text)
        return {
            "title": title,
            "transcript_path": transcript_path,
            "video_id": resolved_video_id,
            "source_url": (url or "").strip(),
            "caption_path": caption_saved_path,
            "notes_text": text,
        }
    except Exception as e:
        try:
            await context.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text=with_tg_time(f"❌ Ask Video failed:\n{str(e)[:1200]}"),
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# Backward-compatible alias for older imports.
async def run_video_notes_for_live(*args, **kwargs):
    return await run_video_notes(*args, **kwargs)
