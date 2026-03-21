from __future__ import annotations

import base64
import unittest

from src.theme_intel.gmail import _extract_message_body, _to_source_document
from src.theme_intel.utils import clean_newsletter_text, looks_like_html_fallback_text


def _b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")


def _part(mime_type: str, body: str) -> dict[str, object]:
    return {
        "mimeType": mime_type,
        "body": {"data": _b64(body)},
    }


class ThemeIntelGmailExtractionTests(unittest.TestCase):
    def test_prefers_html_when_plain_text_is_html_fallback_placeholder(self) -> None:
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                _part(
                    "text/plain",
                    (
                        "You have received a newsletter from DesignerUp.\n"
                        "However, your email software can't display HTML emails.\n"
                        "You can view the newsletter by clicking here:"
                    ),
                ),
                _part(
                    "text/html",
                    (
                        "<html><body><h1>We are changing course</h1>"
                        "<p>This issue explains the shift.</p>"
                        "<a href='https://designerup.co/news/1'>Read more</a>"
                        "</body></html>"
                    ),
                ),
            ],
        }

        extracted = _extract_message_body(payload)

        self.assertEqual(extracted["mode"], "html_fallback")
        self.assertIn("We are changing course", extracted["raw_text"])
        self.assertNotIn("can't display HTML emails", extracted["raw_text"])

    def test_keeps_real_plain_text_when_available(self) -> None:
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                _part("text/plain", "Plain issue summary.\nThis is the body that should win."),
                _part("text/html", "<html><body><p>HTML mirror</p></body></html>"),
            ],
        }

        extracted = _extract_message_body(payload)

        self.assertEqual(extracted["mode"], "plain")
        self.assertEqual(extracted["raw_text"], "Plain issue summary.\nThis is the body that should win.")

    def test_html_only_messages_are_converted_to_text_and_links_are_preserved(self) -> None:
        payload = {
            "id": "msg-1",
            "threadId": "thr-1",
            "payload": {
                "mimeType": "text/html",
                "headers": [
                    {"name": "Subject", "value": "HTML only"},
                    {"name": "From", "value": "sender@example.com"},
                ],
                "body": {
                    "data": _b64(
                        "<html><body><h1>Hello world</h1><p>Body copy.</p>"
                        "<a href='https://example.com/post'>Open post</a></body></html>"
                    )
                },
            },
        }

        doc = _to_source_document(payload, labels_map={})

        assert doc is not None
        self.assertEqual(doc.metadata["extraction_mode"], "html")
        self.assertIn("Hello world", doc.raw_text)
        self.assertIn("https://example.com/post", doc.links)

    def test_cleaner_removes_html_fallback_lines_but_keeps_editorial_text(self) -> None:
        text = (
            "Has recibido un correo de Gema Gutiérrez Medina.\n"
            "Sin embargo, tu software de correo no puede desplegar correos en formato HTML.\n"
            "Puedes ver este correo aquí:\n"
            "Our world doesn't quite know what to do with multidisciplinary thinkers.\n"
        )

        cleaned = clean_newsletter_text(text)

        self.assertTrue(looks_like_html_fallback_text(text))
        self.assertNotIn("no puede desplegar correos", cleaned.lower())
        self.assertIn("multidisciplinary thinkers", cleaned)


if __name__ == "__main__":
    unittest.main()
