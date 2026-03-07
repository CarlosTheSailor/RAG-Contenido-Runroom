from __future__ import annotations

import unittest
from unittest.mock import patch

from src.content.case_study_url import parse_case_study_url


class CaseStudyUrlParserTests(unittest.TestCase):
    def test_parse_case_study_url_with_html_sections(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Caso URL" />
            <meta name="description" content="Resumen breve" />
          </head>
          <body>
            <main>
              <h1>Caso URL</h1>
              <h2>Desafío</h2>
              <p>Necesitaban mejorar el funnel.</p>
              <h2>Solución</h2>
              <p>Diseñamos un nuevo producto digital.</p>
              <h2>Resultados</h2>
              <p>Mejoró la conversión un 20%.</p>
            </main>
          </body>
        </html>
        """

        with patch("src.content.case_study_url.fetch_url_html", return_value=html):
            doc = parse_case_study_url("https://www.runroom.com/cases/caso-url")

        self.assertEqual(doc.item.title, "Caso URL")
        self.assertEqual(doc.item.slug, "caso-url")
        self.assertEqual(doc.item.content_type, "case_study")

        keys = [section.section_key for section in doc.sections]
        self.assertIn("challenge", keys)
        self.assertIn("solution", keys)
        self.assertIn("results", keys)


if __name__ == "__main__":
    unittest.main()
