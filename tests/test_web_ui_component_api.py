from __future__ import annotations

import importlib
import io
import json
import sys
import types
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch


def _ensure_telegram_stub() -> None:
    if "telegram" in sys.modules and "telegram.ext" in sys.modules:
        return
    telegram_mod = types.ModuleType("telegram")
    ext_mod = types.ModuleType("telegram.ext")

    class _ContextTypes:
        DEFAULT_TYPE = object

    class _InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class _InlineKeyboardMarkup:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class _Message:
        pass

    ext_mod.ContextTypes = _ContextTypes
    telegram_mod.InlineKeyboardButton = _InlineKeyboardButton
    telegram_mod.InlineKeyboardMarkup = _InlineKeyboardMarkup
    telegram_mod.Message = _Message
    telegram_mod.ext = ext_mod
    sys.modules.setdefault("telegram", telegram_mod)
    sys.modules.setdefault("telegram.ext", ext_mod)


def _install_lightweight_import_stubs() -> None:
    if "download_flow" not in sys.modules:
        download_mod = types.ModuleType("download_flow")

        def _noop_download_flow(*args, **kwargs):
            return {"ok": True}

        download_mod.run_download_flow = _noop_download_flow
        sys.modules["download_flow"] = download_mod

    if "market_research" not in sys.modules:
        market_mod = types.ModuleType("market_research")

        async def _noop_market_research(*args, **kwargs):
            return {"ok": True}

        market_mod.run_knowledge_juice = _noop_market_research
        sys.modules["market_research"] = market_mod

    if "video_notes" not in sys.modules:
        notes_mod = types.ModuleType("video_notes")
        notes_mod._analysis_output_language_for_text = lambda _text: ("en", "English")
        notes_mod._analysis_ttl_seconds = lambda: 3600
        notes_mod._analyze_transcript_with_ai_with_progress = lambda *args, **kwargs: "analysis"
        notes_mod._download_audio = lambda *args, **kwargs: "/tmp/audio.mp3"
        notes_mod._download_youtube_caption_segments = lambda *args, **kwargs: ([], "title", "")
        notes_mod._get_cached_ai_analysis = lambda *args, **kwargs: ""
        notes_mod._save_caption_source = lambda *args, **kwargs: ""
        notes_mod._save_full_transcript = lambda *args, **kwargs: "/tmp/t.txt"
        notes_mod._segments_to_transcript_text = lambda *args, **kwargs: ""
        notes_mod._transcribe_segments = lambda *args, **kwargs: []
        notes_mod.answer_question_from_transcript = lambda *args, **kwargs: ""
        sys.modules["video_notes"] = notes_mod

    if "ytbot_state" not in sys.modules:
        state_mod = types.ModuleType("ytbot_state")
        state_mod.STATE = SimpleNamespace(active_live={}, known_chats=set())
        state_mod.get_public_research_run = lambda *_args, **_kwargs: None
        state_mod.load_index = lambda: {}
        state_mod.load_public_research_runs = lambda: []
        state_mod.request_live_stop = lambda *_args, **_kwargs: True
        state_mod.save_index = lambda *_args, **_kwargs: None
        state_mod.save_transcript_qa_entry = lambda *_args, **_kwargs: None
        sys.modules["ytbot_state"] = state_mod

    if "ytbot_ytdlp" not in sys.modules:
        ytdlp_mod = types.ModuleType("ytbot_ytdlp")
        ytdlp_mod.yt_direct_audio_url = lambda *_args, **_kwargs: ("https://example/audio", "audio")
        ytdlp_mod.yt_direct_download_url = lambda *_args, **_kwargs: ("https://example/video", "video")
        ytdlp_mod.yt_info = lambda *_args, **_kwargs: {"id": "abc12345", "title": "stub"}
        sys.modules["ytbot_ytdlp"] = ytdlp_mod

    if "ytbot_utils" not in sys.modules:
        utils_mod = types.ModuleType("ytbot_utils")
        utils_mod.build_public_url = lambda name: f"https://example/{name}"
        utils_mod.extract_youtube_id = lambda _url: "abc12345"
        utils_mod.now_local_str = lambda: "2026-02-27 00:00:00"
        sys.modules["ytbot_utils"] = utils_mod


class WebUiComponentApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if "web_app" in sys.modules:
            cls.web_app = sys.modules["web_app"]
            return
        _ensure_telegram_stub()
        _install_lightweight_import_stubs()
        cls.web_app = importlib.import_module("web_app")

    def _request_json(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""

        class _HandlerHarness(self.web_app.AppHandler):
            def __init__(self, req_path: str, req_method: str, req_body: bytes):
                self.path = req_path
                self.command = req_method
                self.request_version = "HTTP/1.1"
                self.requestline = f"{req_method} {req_path} HTTP/1.1"
                self.client_address = ("127.0.0.1", 54321)
                self.server = None
                self.rfile = io.BytesIO(req_body)
                self.wfile = io.BytesIO()
                self.headers = {"Content-Length": str(len(req_body))}
                self.status = 0

            def send_response(self, code: int, message=None):
                self.status = int(code)

            def send_header(self, _name: str, _value: str):
                return

            def end_headers(self):
                return

            def send_error(self, code, message=None, explain=None):
                self._send_json(int(code), {"ok": False, "error": str(message or explain or "request failed")})

            def log_message(self, format: str, *args) -> None:
                return

        handler = _HandlerHarness(path, method, body)
        if method.upper() == "GET":
            handler.do_GET()
        else:
            handler.do_POST()
        raw = handler.wfile.getvalue().decode("utf-8", errors="replace").strip()
        if not raw:
            return handler.status, {}
        return handler.status, json.loads(raw)

    def test_get_endpoints_for_ui_tabs(self) -> None:
        with ExitStack() as stack:
            m_stack = stack.enter_context(
                patch.object(
                    self.web_app,
                    "technology_stack",
                    return_value={"generated_at": "2026-02-27T00:00:00+00:00", "web": [{"name": "X"}], "tg_chatbot": [{"name": "Y"}]},
                )
            )
            m_videos = stack.enter_context(
                patch.object(self.web_app, "_build_video_list", return_value=[{"video_id": "abc12345"}])
            )
            m_video = stack.enter_context(
                patch.object(self.web_app, "_video_detail", return_value={"video_id": "abc12345", "title": "Sample"})
            )
            m_researches = stack.enter_context(
                patch.object(self.web_app, "_research_list", return_value=[{"run_id": "r1"}])
            )
            m_research = stack.enter_context(
                patch.object(self.web_app, "_research_detail", return_value={"run_id": "r1", "goal": "bakery"})
            )
            m_juices = stack.enter_context(
                patch.object(self.web_app, "_knowledge_juice_list", return_value=[{"run_id": "k1"}])
            )
            m_juice = stack.enter_context(
                patch.object(self.web_app, "_knowledge_juice_detail", return_value={"run_id": "k1", "topic": "bakery"})
            )
            m_jobs = stack.enter_context(
                patch.object(self.web_app, "_list_brew_jobs", return_value=[{"job_id": "j1"}])
            )
            m_component_jobs = stack.enter_context(
                patch.object(self.web_app, "_list_component_test_jobs", return_value=[{"job_id": "c1"}])
            )

            status, payload = self._request_json("GET", "/api/runtime")
            self.assertEqual(status, 200)
            self.assertTrue(payload["ok"])
            self.assertIn("runtime", payload)

            status, payload = self._request_json("GET", "/api/advanced/stack")
            self.assertEqual(status, 200)
            self.assertEqual(payload["item"]["web"][0]["name"], "X")
            m_stack.assert_called_once()

            status, payload = self._request_json("GET", "/api/videos")
            self.assertEqual(status, 200)
            self.assertEqual(payload["items"][0]["video_id"], "abc12345")
            m_videos.assert_called_once()

            status, payload = self._request_json("GET", "/api/video")
            self.assertEqual(status, 400)
            self.assertIn("video_id is required", payload["error"])

            status, payload = self._request_json("GET", "/api/video?video_id=abc12345")
            self.assertEqual(status, 200)
            self.assertEqual(payload["item"]["title"], "Sample")
            m_video.assert_called_once_with("abc12345")

            status, payload = self._request_json("GET", "/api/researches")
            self.assertEqual(status, 200)
            self.assertEqual(payload["items"][0]["run_id"], "r1")
            m_researches.assert_called_once()

            status, payload = self._request_json("GET", "/api/research?run_id=r1")
            self.assertEqual(status, 200)
            self.assertEqual(payload["item"]["run_id"], "r1")
            m_research.assert_called_once_with("r1")

            status, payload = self._request_json("GET", "/api/knowledge_juices")
            self.assertEqual(status, 200)
            self.assertEqual(payload["items"][0]["run_id"], "k1")
            m_juices.assert_called_once()

            status, payload = self._request_json("GET", "/api/knowledge_juice?run_id=k1")
            self.assertEqual(status, 200)
            self.assertEqual(payload["item"]["run_id"], "k1")
            m_juice.assert_called_once_with("k1")

            status, payload = self._request_json("GET", "/api/knowledge_juice/jobs?active_only=1")
            self.assertEqual(status, 200)
            self.assertEqual(payload["items"][0]["job_id"], "j1")
            m_jobs.assert_called_once_with(active_only=True)

            status, payload = self._request_json("GET", "/api/component_tests/jobs?active_only=1")
            self.assertEqual(status, 200)
            self.assertEqual(payload["items"][0]["job_id"], "c1")
            m_component_jobs.assert_called_once_with(active_only=True)

            status, payload = self._request_json("GET", "/api/component_tests/job?job_id=missing")
            self.assertEqual(status, 404)
            self.assertIn("job not found", payload["error"])

    def test_post_endpoints_validate_required_fields(self) -> None:
        bad_cases = [
            ("/api/analyze", {}, "video_id is required"),
            ("/api/ask", {"video_id": "abc12345"}, "question is required"),
            ("/api/save_transcript", {}, "url is required"),
            ("/api/direct_video", {}, "url is required"),
            ("/api/direct_audio", {}, "url is required"),
            ("/api/direct_save_server", {}, "url is required"),
            ("/api/live/start", {}, "url is required"),
            ("/api/live/stop", {}, "video_id is required"),
            ("/api/knowledge_juice", {}, "topic is required"),
            ("/api/knowledge_juice/start", {}, "topic is required"),
        ]
        for path, payload, expected_error in bad_cases:
            with self.subTest(path=path):
                status, body = self._request_json("POST", path, payload)
                self.assertEqual(status, 400)
                self.assertIn(expected_error, body["error"])

    def test_post_endpoints_dispatch_backend_work(self) -> None:
        with ExitStack() as stack:
            m_analyze = stack.enter_context(
                patch.object(self.web_app, "_run_analysis", return_value={"analysis": "A"})
            )
            m_qa = stack.enter_context(
                patch.object(self.web_app, "_run_qa", return_value={"answer": "B"})
            )
            m_save_tr = stack.enter_context(
                patch.object(self.web_app, "_save_transcript_from_url", return_value={"video_id": "abc12345"})
            )
            m_clear = stack.enter_context(
                patch.object(self.web_app, "_clear_history", return_value={"cleared": True})
            )
            m_direct_video = stack.enter_context(
                patch.object(self.web_app, "_run_direct_video", return_value={"direct_url": "https://example/video.mp4"})
            )
            m_direct_audio = stack.enter_context(
                patch.object(self.web_app, "_run_direct_audio", return_value={"direct_url": "https://example/audio.m4a"})
            )
            m_save_server = stack.enter_context(
                patch.object(self.web_app, "_start_server_save", return_value={"job_id": "save1"})
            )
            m_live_start = stack.enter_context(
                patch.object(self.web_app, "_start_live_recording", return_value={"video_id": "abc12345"})
            )
            m_live_stop = stack.enter_context(
                patch.object(self.web_app, "_stop_live_recording", return_value={"ok": True})
            )
            m_juice = stack.enter_context(
                patch.object(self.web_app, "_run_knowledge_juice", return_value={"run_id": "r1"})
            )
            m_juice_start = stack.enter_context(
                patch.object(self.web_app, "_start_knowledge_juice_job", return_value={"job_id": "j1"})
            )
            m_component = stack.enter_context(
                patch.object(self.web_app, "_start_component_tests_job", return_value={"job_id": "c1"})
            )

            status, _payload = self._request_json(
                "POST",
                "/api/analyze",
                {"video_id": "abc12345", "force": True, "save": False},
            )
            self.assertEqual(status, 200)
            m_analyze.assert_called_once_with("abc12345", force=True, save=False)

            status, _payload = self._request_json(
                "POST",
                "/api/ask",
                {"video_id": "abc12345", "question": "What is the main idea?"},
            )
            self.assertEqual(status, 200)
            m_qa.assert_called_once_with("abc12345", "What is the main idea?")

            status, _payload = self._request_json(
                "POST",
                "/api/save_transcript",
                {"url": "https://www.youtube.com/watch?v=abc12345", "force": True},
            )
            self.assertEqual(status, 200)
            m_save_tr.assert_called_once_with("https://www.youtube.com/watch?v=abc12345", force=True)

            status, _payload = self._request_json(
                "POST",
                "/api/clear_history",
                {"delete_files": False},
            )
            self.assertEqual(status, 200)
            m_clear.assert_called_once_with(delete_files=False)

            status, _payload = self._request_json(
                "POST",
                "/api/direct_video",
                {"url": "https://www.youtube.com/watch?v=abc12345"},
            )
            self.assertEqual(status, 200)
            m_direct_video.assert_called_once_with("https://www.youtube.com/watch?v=abc12345")

            status, _payload = self._request_json(
                "POST",
                "/api/direct_audio",
                {"url": "https://www.youtube.com/watch?v=abc12345"},
            )
            self.assertEqual(status, 200)
            m_direct_audio.assert_called_once_with("https://www.youtube.com/watch?v=abc12345")

            status, _payload = self._request_json(
                "POST",
                "/api/direct_save_server",
                {"url": "https://www.youtube.com/watch?v=abc12345"},
            )
            self.assertEqual(status, 200)
            m_save_server.assert_called_once_with("https://www.youtube.com/watch?v=abc12345")

            status, _payload = self._request_json(
                "POST",
                "/api/live/start",
                {"url": "https://www.youtube.com/watch?v=abc12345"},
            )
            self.assertEqual(status, 200)
            m_live_start.assert_called_once_with("https://www.youtube.com/watch?v=abc12345")

            status, _payload = self._request_json(
                "POST",
                "/api/live/stop",
                {"video_id": "abc12345"},
            )
            self.assertEqual(status, 200)
            m_live_stop.assert_called_once_with("abc12345")

            status, _payload = self._request_json(
                "POST",
                "/api/knowledge_juice",
                {"topic": "bakery", "private_run": True},
            )
            self.assertEqual(status, 200)
            m_juice.assert_called_once_with("bakery", True)

            status, _payload = self._request_json(
                "POST",
                "/api/knowledge_juice/start",
                {
                    "topic": "bakery",
                    "private_run": True,
                    "max_videos": 6,
                    "max_queries": 8,
                    "per_query": 8,
                    "min_duration_sec": 0,
                    "max_duration_sec": 0,
                    "captions_only": True,
                },
            )
            self.assertEqual(status, 200)
            m_juice_start.assert_called_once_with(
                "bakery",
                True,
                {
                    "max_videos": 6,
                    "max_queries": 8,
                    "per_query": 8,
                    "min_duration_sec": 0,
                    "max_duration_sec": 0,
                    "captions_only": True,
                },
            )

            status, _payload = self._request_json(
                "POST",
                "/api/component_tests/start",
                {"component": "web"},
            )
            self.assertEqual(status, 200)
            m_component.assert_called_once_with("web")

    def test_notes_endpoints_return_already_running_state(self) -> None:
        with ExitStack() as stack:
            m_try = stack.enter_context(
                patch.object(self.web_app, "_try_start_notes_task", return_value=False)
            )
            m_analyze_progress = stack.enter_context(
                patch.object(
                    self.web_app,
                    "_get_analyze_progress",
                    return_value={"video_id": "abc12345", "status": "running", "message": "Analysis already running."},
                )
            )
            m_ask_progress = stack.enter_context(
                patch.object(
                    self.web_app,
                    "_get_ask_progress",
                    return_value={"video_id": "abc12345", "status": "running", "message": "Ask already running."},
                )
            )
            m_run_analyze = stack.enter_context(
                patch.object(self.web_app, "_run_analysis")
            )
            m_run_qa = stack.enter_context(
                patch.object(self.web_app, "_run_qa")
            )

            status, payload = self._request_json(
                "POST",
                "/api/analyze",
                {"video_id": "abc12345"},
            )
            self.assertEqual(status, 200)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload.get("status"), "already_running")
            self.assertTrue(payload.get("in_progress"))
            self.assertEqual(payload.get("item", {}).get("status"), "running")
            m_run_analyze.assert_not_called()

            status, payload = self._request_json(
                "POST",
                "/api/ask",
                {"video_id": "abc12345", "question": "What changed?"},
            )
            self.assertEqual(status, 200)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload.get("status"), "already_running")
            self.assertTrue(payload.get("in_progress"))
            self.assertEqual(payload.get("item", {}).get("status"), "running")
            m_run_qa.assert_not_called()

            m_try.assert_any_call("abc12345", "analyze")
            m_try.assert_any_call("abc12345", "ask")
            m_analyze_progress.assert_called_once_with("abc12345")
            m_ask_progress.assert_called_once_with("abc12345")


if __name__ == "__main__":
    unittest.main()
