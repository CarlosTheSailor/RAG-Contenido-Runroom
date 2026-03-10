from __future__ import annotations

import unittest
from unittest.mock import patch

from src.content.runroom_lab_url import parse_runroom_lab_url


class RunroomLabUrlParserTests(unittest.TestCase):
    def test_parse_runroom_lab_url_maps_to_runroom_lab_content_type(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="LAB Heart of Agile" />
            <meta name="description" content="Resumen del LAB" />
          </head>
          <body>
            <main>
              <h1>LAB Heart of Agile</h1>
              <h2>Resultados</h2>
              <p>Aprendimos a simplificar Agile.</p>
            </main>
          </body>
        </html>
        """

        with patch("src.content.case_study_url.fetch_url_html", return_value=html):
            doc = parse_runroom_lab_url("https://info.runroom.com/lab-heart-of-agile")

        self.assertEqual(doc.item.content_type, "runroom_lab")
        self.assertEqual(doc.item.source, "runroom_lab_url")
        self.assertEqual(doc.item.content_key, "runroom_lab:runroom:lab-heart-of-agile")
        self.assertEqual(doc.item.metadata.get("content_type"), "runroom_lab")
        self.assertEqual(doc.item.metadata.get("source"), "runroom_lab_url")
        self.assertEqual(doc.item.metadata.get("original_url"), "https://info.runroom.com/lab-heart-of-agile")

        self.assertGreaterEqual(len(doc.sections), 1)
        self.assertTrue(any(section.section_key == "results" for section in doc.sections))


if __name__ == "__main__":
    unittest.main()
