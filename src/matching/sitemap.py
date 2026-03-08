from __future__ import annotations

import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

from src.pipeline.models import RunroomArticle

logger = logging.getLogger(__name__)

LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
META_CONTENT_RE = re.compile(r"<meta[^>]+(?:property|name)=['\"](?P<name>[^'\"]+)['\"][^>]*content=['\"](?P<content>[^'\"]+)['\"]", re.IGNORECASE)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
EPISODE_CODE_RE = re.compile(r"\b([er]\d{3})\b", re.IGNORECASE)



def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    xml_text = _fetch_text(sitemap_url)
    if not xml_text.strip():
        return []

    try:
        root = ET.fromstring(xml_text)
        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        loc_nodes = root.findall(".//s:loc", ns)
        urls = [node.text.strip() for node in loc_nodes if node.text]
        if urls:
            return urls
    except ET.ParseError:
        logger.warning("Could not parse sitemap XML with ElementTree, fallback to regex")

    return [m.group(1).strip() for m in LOC_RE.finditer(xml_text)]



def build_runroom_articles(sitemap_url: str, fetch_metadata: bool = True) -> list[RunroomArticle]:
    urls = fetch_sitemap_urls(sitemap_url)
    realworld_urls = [u for u in urls if _is_realworld_article_url(u)]

    articles: list[RunroomArticle] = []
    for url in sorted(set(realworld_urls)):
        slug = _slug_from_url(url)
        if not slug or slug.isdigit():
            continue

        title = slug.replace("-", " ").strip()
        description = ""
        if fetch_metadata:
            page = _fetch_text(url)
            title, description = _extract_page_metadata(page, title)

        lang = "en" if "/en/realworld/" in url else "es"
        code_hint_match = EPISODE_CODE_RE.search(slug)
        code_hint = code_hint_match.group(1).lower() if code_hint_match else None

        articles.append(
            RunroomArticle(
                url=url,
                slug=slug,
                title=title,
                description=description,
                lang=lang,
                episode_code_hint=code_hint,
            )
        )

    return articles



def _fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "rag-contenidos-runroom/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")



def _is_realworld_article_url(url: str) -> bool:
    if not url.startswith("https://www.runroom.com/"):
        return False

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return path.startswith("/realworld/") or path.startswith("/en/realworld/")



def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    parts = path.split("/")
    if len(parts) < 2:
        return ""
    if parts[0] == "en" and len(parts) >= 3:
        return parts[2]
    return parts[1]



def _extract_page_metadata(html: str, fallback_title: str) -> tuple[str, str]:
    meta = {}
    for match in META_CONTENT_RE.finditer(html):
        name = match.group("name").lower().strip()
        content = match.group("content").strip()
        meta[name] = content

    title = meta.get("og:title") or meta.get("twitter:title") or ""
    description = meta.get("og:description") or meta.get("description") or ""

    if not title:
        title_match = TITLE_RE.search(html)
        if title_match:
            title = _strip_html_spaces(title_match.group(1))

    if title.endswith("| Realworld"):
        title = title.replace("| Realworld", "").strip()

    if not title:
        title = fallback_title

    return _strip_html_spaces(title), _strip_html_spaces(description)



def _strip_html_spaces(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()
