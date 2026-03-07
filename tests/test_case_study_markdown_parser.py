from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.content.case_study_markdown import parse_case_studies_markdown


class CaseStudyMarkdownParserTests(unittest.TestCase):
    def test_parser_handles_heading_variants_and_labels(self) -> None:
        content = """
# Case Studies de Runroom - Documento Completo
**Fecha de extracción:** 16 de febrero de 2026
**Fuente:** https://www.runroom.com/cases

## Case Study #1: Caso Uno
**Cliente:** Cliente Uno
**URL Original:** https://www.runroom.com/cases/caso-uno

# Caso Uno
## Descripción del Proyecto
Texto de descripción.

## Reto
Texto del reto.

## Solución y Procesos
* React
* HubSpot

## Resultados
Resultado principal.

====================================================================================================

## Case Study #2: Caso Dos
**Cliente:** Cliente Dos
**URL:** https://www.runroom.com/cases/caso-dos

# Caso Dos
## El Desafío
Bloque desafío.

## Impacto en el Mercado (Resultados)
Bloque impacto.
""".strip()

        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as f:
            f.write(content)
            path = Path(f.name)

        try:
            docs = parse_case_studies_markdown(path)
        finally:
            path.unlink(missing_ok=True)

        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0].item.title, "Caso Uno")
        self.assertEqual(docs[0].item.metadata.get("client"), "Cliente Uno")
        self.assertEqual(docs[0].item.slug, "caso-uno")

        section_keys = [section.section_key for section in docs[0].sections]
        self.assertIn("description", section_keys)
        self.assertIn("challenge", section_keys)
        self.assertIn("solution", section_keys)
        self.assertIn("results", section_keys)

        section_keys_case2 = [section.section_key for section in docs[1].sections]
        self.assertIn("challenge", section_keys_case2)
        self.assertIn("results", section_keys_case2)

    def test_parser_real_markdown_if_available(self) -> None:
        real_path = Path("/Users/carlos/Downloads/Runroom_Case_Studies_Completo.md")
        if not real_path.exists():
            self.skipTest("Real markdown file not found in /Users/carlos/Downloads")

        docs = parse_case_studies_markdown(real_path)
        self.assertEqual(len(docs), 47)

        first = docs[0]
        self.assertEqual(first.item.content_type, "case_study")
        self.assertTrue(first.item.title)
        self.assertTrue(first.item.url)
        self.assertGreaterEqual(len(first.sections), 3)


if __name__ == "__main__":
    unittest.main()
