from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.refresh_cookies_from_browser import BrowserSession


class RefreshCookiesBrowserTests(unittest.TestCase):
    def test_browser_session_close_terminates_process_and_removes_profile_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            profile_dir = Path(td) / "profile"
            profile_dir.mkdir(parents=True, exist_ok=True)
            proc = subprocess.Popen(["sleep", "60"])
            try:
                session = BrowserSession(proc=proc, profile_dir=str(profile_dir))
                session.close()
                self.assertIsNotNone(proc.poll())
                self.assertFalse(profile_dir.exists())
            finally:
                if proc.poll() is None:
                    proc.kill()


if __name__ == "__main__":
    unittest.main()
