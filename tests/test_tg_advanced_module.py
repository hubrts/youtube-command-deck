from __future__ import annotations

import unittest

from advanced_module import component_pattern, normalize_component, technology_stack


class TgAdvancedModuleTests(unittest.TestCase):
    def test_normalize_component_accepts_telegram_aliases(self) -> None:
        self.assertEqual(normalize_component("tg"), "tg")
        self.assertEqual(normalize_component("telegram"), "tg")
        self.assertEqual(normalize_component("chatbot"), "tg")

    def test_component_pattern_for_tg(self) -> None:
        self.assertEqual(component_pattern("tg"), "test_tg*.py")
        self.assertEqual(component_pattern("telegram"), "test_tg*.py")

    def test_technology_stack_contains_tg_entries(self) -> None:
        stack = technology_stack()
        self.assertIn("tg_chatbot", stack)
        tg_items = stack["tg_chatbot"]
        self.assertTrue(isinstance(tg_items, list) and len(tg_items) > 0)
        names = {str(item.get("name") or "") for item in tg_items}
        self.assertIn("python-telegram-bot", names)
        self.assertIn("Local LLM (Ollama)", names)
        self.assertIn("Remote LLM (OpenAI)", names)
        self.assertIn("Remote LLM (Anthropic Claude)", names)
        self.assertIn("Transcript Maker (YouTube captions)", names)
        self.assertIn("Transcript Maker (audio STT)", names)
        self.assertIn("Transcript Analyzer", names)


if __name__ == "__main__":
    unittest.main()
