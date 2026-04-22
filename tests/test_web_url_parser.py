from __future__ import annotations

import unittest
from unittest.mock import patch

from src.content.web_url import parse_runroom_web_url


class RunroomWebUrlParserTests(unittest.TestCase):
    def test_parse_runroom_web_url_filters_footer_boilerplate(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Product Open Space 2026" />
            <meta name="description" content="Resumen del evento." />
          </head>
          <body>
            <main>
              <h1>Product Open Space 2026</h1>
              <p>Servicios Casos Nosotros Academy Realworld</p>
              <p>Introduccion editorial del articulo.</p>
              <h2>Conclusiones</h2>
              <p>Aprendimos como priorizar impacto en la era de la IA.</p>
              <h2>Contacto</h2>
              <p>Completa el formulario y nos pondremos en contacto contigo.</p>
              <h2>Nuestros Servicios</h2>
              <p>Customer Experience Producto Digital Growth Marketing Academy</p>
            </main>
          </body>
        </html>
        """

        with patch("src.content.case_study_url.fetch_url_html", return_value=html):
            doc = parse_runroom_web_url(
                "https://www.runroom.com/realworld/product-open-space-2026",
                content_type="article",
            )

        self.assertEqual(doc.item.content_type, "article")
        self.assertEqual(doc.item.source, "runroom_web_url")
        self.assertEqual(doc.item.content_key, "web:www.runroom.com:realworld:product-open-space-2026")

        payload = "\n".join(section.text for section in doc.sections)
        self.assertIn("Introduccion editorial del articulo.", payload)
        self.assertIn("priorizar impacto en la era de la IA", payload)
        self.assertNotIn("Completa el formulario", payload)
        self.assertNotIn("Customer Experience Producto Digital", payload)

    def test_parse_runroom_web_url_supports_training_type(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Formacion Impact-Driven Growth" />
          </head>
          <body>
            <main>
              <h1>Formacion Impact-Driven Growth</h1>
              <p>Herramienta IA Metodologia Formacion Libro Precios</p>
              <h2>Programa</h2>
              <p>Aprende a aplicar el framework paso a paso.</p>
            </main>
          </body>
        </html>
        """

        with patch("src.content.case_study_url.fetch_url_html", return_value=html):
            doc = parse_runroom_web_url(
                "https://idg.runroom.com/formacion",
                content_type="training",
            )

        self.assertEqual(doc.item.content_type, "training")
        self.assertEqual(doc.item.slug, "formacion")
        self.assertTrue(any(section.section_title == "Programa" for section in doc.sections))
        self.assertNotIn("Herramienta IA Metodologia Formacion Libro Precios", doc.item.raw_text)


if __name__ == "__main__":
    unittest.main()
