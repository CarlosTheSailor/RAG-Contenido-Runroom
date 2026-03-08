from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.application.newsletter_linkedin_generator import (
    NewsletterLinkedInGenerator,
    NewsletterLinkedInInput,
    build_newsletter_generation_prompt,
)
from src.config import Settings


def _settings_without_openai() -> Settings:
    return Settings(
        supabase_db_url="postgresql://user:pass@localhost:5432/db",
        openai_api_key=None,
        openai_base_url="https://api.openai.com/v1",
        youtube_api_key=None,
        youtube_api_base_url="https://www.googleapis.com/youtube/v3",
        openai_embedding_model="text-embedding-3-large",
        openai_metadata_model="gpt-4.1-mini",
        openai_newsletter_model="gpt-4.1-mini",
        newsletter_rag_min_score=0.74,
        embedding_dim=1536,
        runroom_sitemap_url="https://www.runroom.com/sitemap.xml",
        auto_match_threshold=0.86,
        auto_match_margin=0.06,
        log_level="INFO",
    )


class NewsletterLinkedInGeneratorTests(unittest.TestCase):
    def test_load_style_examples_auto_loads_txt_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "prompts").mkdir(parents=True, exist_ok=True)
            (base / "examples").mkdir(parents=True, exist_ok=True)
            (base / "prompts" / "base_prompt.txt").write_text("prompt base", encoding="utf-8")
            (base / "examples" / "b.txt").write_text("ejemplo b", encoding="utf-8")
            (base / "examples" / "a.txt").write_text("ejemplo a", encoding="utf-8")
            (base / "examples" / "ignore.md").write_text("no", encoding="utf-8")

            generator = NewsletterLinkedInGenerator(settings=_settings_without_openai(), assets_dir=base)
            examples = generator.load_style_examples()

            self.assertEqual([name for name, _ in examples], ["a.txt", "b.txt"])

    def test_build_prompt_includes_related_content_and_optional_fields(self) -> None:
        prompt = build_newsletter_generation_prompt(
            payload=NewsletterLinkedInInput(
                idea="Idea principal",
                referencias="https://example.com/ref",
                audiencia="Lideres de producto",
                objetivo_secundario="Abrir debate",
            ),
            related_content=[
                {
                    "title": "Episodio X",
                    "url": "https://example.com/x",
                    "content_type": "episode",
                    "score": 0.93,
                    "excerpt": "extracto de prueba",
                }
            ],
            style_examples=[("post_ejemplo1.txt", "Texto ejemplo")],
        )

        self.assertIn("IDEA: Idea principal", prompt)
        self.assertIn("REFERENCIAS: https://example.com/ref", prompt)
        self.assertIn("[episode] Episodio X", prompt)
        self.assertIn("=== EJEMPLO post_ejemplo1.txt ===", prompt)

    def test_generate_uses_fallback_without_openai_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "prompts").mkdir(parents=True, exist_ok=True)
            (base / "examples").mkdir(parents=True, exist_ok=True)
            (base / "prompts" / "base_prompt.txt").write_text("prompt base", encoding="utf-8")
            (base / "examples" / "post_ejemplo1.txt").write_text("Texto ejemplo", encoding="utf-8")

            generator = NewsletterLinkedInGenerator(settings=_settings_without_openai(), assets_dir=base)
            result = generator.generate(
                payload=NewsletterLinkedInInput(idea="Sistema de incentivos"),
                related_content=[
                    {
                        "title": "Caso Y",
                        "url": "https://example.com/y",
                        "content_type": "case_study",
                        "score": 0.88,
                        "matched_chunks": [{"text": "contenido relacionado"}],
                    }
                ],
            )

            self.assertIn("=== ARTICULO | VARIANTE A (Provocacion util) ===", result.output_text)
            self.assertGreaterEqual(len(result.warnings), 1)
            self.assertEqual(result.used_examples, ["post_ejemplo1.txt"])
            self.assertEqual(result.related_content[0]["title"], "Caso Y")


if __name__ == "__main__":
    unittest.main()
