from __future__ import annotations

import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

_ATOM_NS = "{http://www.w3.org/2005/Atom}"


@dataclass(frozen=True)
class FeedEntry:
    url: str
    title: str | None = None
    published_at: datetime | None = None


def discover_feed_entries(feed_url: str, timeout: int = 30) -> list[FeedEntry]:
    """Fetch an RSS or Atom feed and return its entries, in feed order.

    Entries that point to a host other than the feed's own are dropped, so a feed
    can only trigger ingestion of pages from the site that publishes it.
    """
    return parse_feed_entries(fetch_feed_xml(feed_url, timeout=timeout), feed_url=feed_url)


def fetch_feed_xml(feed_url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(feed_url, headers={"User-Agent": "runroom-content-layer/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def parse_feed_entries(xml_text: str, feed_url: str) -> list[FeedEntry]:
    try:
        root = ET.fromstring(xml_text.strip())
    except ET.ParseError as exc:
        raise ValueError(f"Feed invalido: no es XML bien formado ({exc}).") from exc

    feed_host = _bare_host(feed_url)
    entries: list[FeedEntry] = []
    seen: set[str] = set()

    for node in _entry_nodes(root):
        url = _entry_url(node)
        if not url or url in seen:
            continue
        if urlparse(url).scheme not in {"http", "https"} or _bare_host(url) != feed_host:
            continue
        seen.add(url)
        entries.append(
            FeedEntry(
                url=url,
                title=_child_text(node, "title") or None,
                published_at=_entry_published_at(node),
            )
        )

    return entries


def _entry_nodes(root: ET.Element) -> list[ET.Element]:
    items = list(root.iter("item"))
    if items:
        return items
    return list(root.iter(f"{_ATOM_NS}entry"))


def _entry_url(node: ET.Element) -> str:
    link = _child_text(node, "link")
    if link:
        return link

    fallback = ""
    for atom_link in node.findall(f"{_ATOM_NS}link"):
        href = (atom_link.get("href") or "").strip()
        if not href:
            continue
        if (atom_link.get("rel") or "alternate") == "alternate":
            return href
        fallback = fallback or href
    return fallback


def _entry_published_at(node: ET.Element) -> datetime | None:
    for tag in ("pubDate", "published", "updated"):
        value = _child_text(node, tag)
        if not value:
            continue
        parsed = _parse_feed_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _parse_feed_datetime(value: str) -> datetime | None:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        parsed = None

    if parsed is None:
        iso_value = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(iso_value)
        except ValueError:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _child_text(node: ET.Element, tag: str) -> str:
    for candidate in (tag, f"{_ATOM_NS}{tag}"):
        child = node.find(candidate)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return ""


def _bare_host(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host
