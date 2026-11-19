from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from src.config import Settings
from src.content.ingest import ingest_web_feed, ingest_web_urls
from src.content.web_feed import parse_feed_entries

_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Impacto &amp; Producto</title>
  <link>https://carlosiglesias.info/newsletter/</link>
  <item>
    <title>El equipo era el LLM</title>
    <link>https://carlosiglesias.info/newsletter/el-equipo-era-el-llm/</link>
    <pubDate>Tue, 17 Nov 2026 00:00:00 GMT</pubDate>
  </item>
  <item>
    <title>El libro en el cajon</title>
    <link>https://carlosiglesias.info/newsletter/el-libro-en-el-cajon/</link>
    <pubDate>Tue, 10 Nov 2026 00:00:00 GMT</pubDate>
  </item>
  <item>
    <title>Duplicada</title>
    <link>https://carlosiglesias.info/newsletter/el-libro-en-el-cajon/</link>
  </item>
  <item>
    <title>Otro dominio</title>
    <link>https://evil.example.com/newsletter/trampa/</link>
  </item>
</channel></rss>
"""

_ATOM = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Entrada Atom</title>
    <link rel="alternate" href="https://www.runroom.com/realworld/entrada-atom"/>
    <published>2026-05-04T10:00:00Z</published>
  </entry>
</feed>
"""

_FEED_URL = "https://carlosiglesias.info/newsletter/rss.xml"


def _page(title: str, date: str | None = None) -> str:
    json_ld = ""
    if date:
        json_ld = (
            '<script type="application/ld+json">{"@context":"https://schema.org","@graph":'
            '[{"@type":"BlogPosting","datePublished":"%s"}]}</script>' % date
        )
    return f"<html><head>{json_ld}</head><body><h1>{title}</h1><p>Contenido editorial de {title}.</p></body></html>"


def _settings() -> Settings:
    return Settings.__new__(Settings)


class WebFeedParserTests(unittest.TestCase):
    def test_rss_entries_keep_order_dates_and_drop_foreign_or_duplicate_links(self) -> None:
        entries = parse_feed_entries(_FEED, feed_url=_FEED_URL)

        self.assertEqual(
            [entry.url for entry in entries],
            [
                "https://carlosiglesias.info/newsletter/el-equipo-era-el-llm/",
                "https://carlosiglesias.info/newsletter/el-libro-en-el-cajon/",
            ],
        )
        self.assertEqual(entries[0].title, "El equipo era el LLM")
        self.assertEqual(entries[0].published_at, datetime(2026, 11, 17, tzinfo=timezone.utc))

    def test_atom_entries_are_supported(self) -> None:
        entries = parse_feed_entries(_ATOM, feed_url="https://www.runroom.com/feed.xml")

        self.assertEqual([entry.url for entry in entries], ["https://www.runroom.com/realworld/entrada-atom"])
        self.assertEqual(entries[0].published_at, datetime(2026, 5, 4, 10, 0, tzinfo=timezone.utc))

    def test_malformed_feed_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            parse_feed_entries("<rss><channel>", feed_url=_FEED_URL)


class IngestWebUrlsTests(unittest.TestCase):
    def test_dry_run_reports_each_url_without_touching_database_or_embeddings(self) -> None:
        pages = {
            "https://carlosiglesias.info/newsletter/uno/": _page("Uno", date="2026-11-17"),
            "https://carlosiglesias.info/newsletter/dos/": _page("Dos"),
        }
        fallback = datetime(2026, 1, 6, tzinfo=timezone.utc)

        with patch("src.content.case_study_url.fetch_url_html", side_effect=lambda url: pages[url]), patch(
            "src.content.ingest.ingest_documents"
        ) as ingest_documents:
            summary = ingest_web_urls(
                settings=_settings(),
                schema_path=None,
                urls=list(pages),
                content_type="article",
                published_at_by_url={"https://carlosiglesias.info/newsletter/dos/": fallback},
                dry_run=True,
            )

        ingest_documents.assert_not_called()
        self.assertEqual(summary["documents_total"], 2)
        self.assertEqual(summary["items_upserted"], 2)
        self.assertEqual(summary["failed"], 0)
        first, second = summary["results"]
        self.assertEqual(first["content_key"], "web:carlosiglesias.info:newsletter:uno")
        # The page's own JSON-LD date wins; the feed date only fills the gap.
        self.assertEqual(first["published_at"], "2026-11-17T00:00:00+00:00")
        self.assertEqual(second["published_at"], fallback.isoformat())

    def test_one_failing_url_does_not_abort_the_rest(self) -> None:
        def fetch(url: str) -> str:
            if url.endswith("/rota/"):
                raise TimeoutError("timed out")
            return _page("Buena")

        counts = {"documents_total": 1, "items_upserted": 1, "sections_written": 1, "chunks_written": 2, "dry_run": False}
        with patch("src.content.case_study_url.fetch_url_html", side_effect=fetch), patch(
            "src.content.ingest.ingest_documents", return_value=counts
        ) as ingest_documents:
            summary = ingest_web_urls(
                settings=_settings(),
                schema_path=None,
                urls=[
                    "https://carlosiglesias.info/newsletter/rota/",
                    "https://example.com/no-permitida/",
                    "https://carlosiglesias.info/newsletter/buena/",
                ],
                content_type="article",
            )

        self.assertEqual(ingest_documents.call_count, 1)
        self.assertEqual([item["status"] for item in summary["results"]], ["error", "error", "ok"])
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(summary["items_upserted"], 1)
        self.assertEqual(summary["chunks_written"], 2)
        self.assertIn("TimeoutError", summary["results"][0]["error"])

    def test_feed_ingest_respects_limit_and_passes_feed_dates(self) -> None:
        with patch("src.content.web_feed.fetch_feed_xml", return_value=_FEED), patch(
            "src.content.case_study_url.fetch_url_html", side_effect=lambda url: _page("Sin fecha")
        ):
            summary = ingest_web_feed(
                settings=_settings(),
                schema_path=None,
                feed_url=_FEED_URL,
                content_type="article",
                limit=1,
                dry_run=True,
            )

        self.assertEqual(summary["feed_entries_total"], 2)
        self.assertEqual(summary["documents_total"], 1)
        self.assertEqual(summary["results"][0]["published_at"], "2026-11-17T00:00:00+00:00")

    def test_feed_from_disallowed_host_is_rejected_before_fetching(self) -> None:
        with patch("src.content.web_feed.fetch_feed_xml") as fetch_feed_xml:
            with self.assertRaises(ValueError):
                ingest_web_feed(settings=_settings(), schema_path=None, feed_url="https://example.com/rss.xml")
        fetch_feed_xml.assert_not_called()


if __name__ == "__main__":
    unittest.main()
