from __future__ import annotations

import unittest

from src.theme_intel.parsing import parse_themes_json
from src.theme_intel.utils import normalize_tag


class ThemeIntelParsingTests(unittest.TestCase):
    def test_parse_themes_json_from_fenced_payload(self) -> None:
        payload = """
```json
{
  "temas_prioritarios_newsletters": [
    {
      "tema": "IA aplicada a growth",
      "contexto_newsletters": "Varias newsletters priorizan automatizacion de campañas.",
      "keywords": ["ia", "growth", "automatizacion"],
      "datos_cuantitativos_relacionados": [
        {
          "dato": "+20% conversion",
          "fuente": "Growth Weekly",
          "texto_fuente_breve": "subió un 20%",
          "url_referencia": "https://example.com/growth",
          "newsletter_origen": "Growth Weekly"
        }
      ]
    }
  ]
}
```
"""
        parsed = parse_themes_json(payload)

        self.assertEqual(len(parsed.temas), 1)
        self.assertEqual(parsed.temas[0].tema, "IA aplicada a growth")
        self.assertEqual(parsed.temas[0].keywords, ["ia", "growth", "automatizacion"])
        self.assertEqual(parsed.temas[0].datos_cuantitativos_relacionados[0].fuente, "Growth Weekly")

    def test_normalize_tag_removes_accents_and_spaces(self) -> None:
        self.assertEqual(normalize_tag("Experiencia de Cliente"), "experiencia-de-cliente")
        self.assertEqual(normalize_tag("IA & Tech"), "ia-tech")


if __name__ == "__main__":
    unittest.main()
