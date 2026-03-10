from __future__ import annotations

import html
import re
import urllib.request
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from src.pipeline.normalization import normalize_for_match

DEFAULT_RUNROOM_LABS_INDEX_URL = "https://info.runroom.com/runroom-lab-todas-las-ediciones"

_ALLOWED_HOSTS = {
    "runroom.com",
    "www.runroom.com",
    "info.runroom.com",
}

_SUMMARY_KEYWORDS = (
    "lee",
    "aprendizajes",
    "conclusiones",
    "insights",
    "resumen",
    "learnings",
)

_VIDEO_KEYWORDS = (
    "video",
    "youtube",
    "mira",
    "charla",
)

_IMAGE_EXT_RE = re.compile(r"\.(?:png|jpe?g|gif|webp|svg)$", re.IGNORECASE)


class _LabsIndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.groups: list[dict[str, Any]] = []

        self._div_classes: list[str] = []

        self._current_group: dict[str, Any] | None = None
        self._current_group_depth: int | None = None

        self._header_depth: int | None = None
        self._header_parts: list[str] = []

        self._content_depth: int | None = None
        self._current_link_href: str | None = None
        self._current_link_parts: list[str] = []

        self._order_counter = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key.lower(): value or "" for key, value in attrs}

        if tag == "div":
            class_attr = attrs_map.get("class", "")
            classes = set(class_attr.split())
            depth = len(self._div_classes) + 1

            if "accordion_group" in classes:
                self._start_group(depth)

            self._div_classes.append(class_attr)

            if self._current_group is not None:
                if "accordion_header" in classes:
                    self._header_depth = depth
                if "accordion_content" in classes:
                    self._content_depth = depth
            return

        if self._current_group is None:
            return

        if tag == "a" and self._content_depth is not None:
            href = attrs_map.get("href", "").strip()
            if href:
                self._current_link_href = href
                self._current_link_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_group is None:
            return

        if self._header_depth is not None:
            self._header_parts.append(data)

        if self._current_link_href is not None:
            self._current_link_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._current_group is not None and self._current_link_href is not None:
            self._current_group["links"].append(
                {
                    "href": self._current_link_href,
                    "text": _clean_text("".join(self._current_link_parts)),
                    "order": self._order_counter,
                }
            )
            self._order_counter += 1
            self._current_link_href = None
            self._current_link_parts = []
            return

        if tag != "div" or not self._div_classes:
            return

        depth = len(self._div_classes)

        if self._header_depth == depth:
            self._header_depth = None

        if self._content_depth == depth:
            self._content_depth = None

        if self._current_group is not None and self._current_group_depth == depth:
            self._finalize_group()

        self._div_classes.pop()

    def close(self) -> None:
        super().close()
        if self._current_group is not None:
            self._finalize_group()

    def _start_group(self, depth: int) -> None:
        if self._current_group is not None:
            self._finalize_group()

        self._current_group = {
            "header": "",
            "links": [],
        }
        self._current_group_depth = depth
        self._header_depth = None
        self._header_parts = []
        self._content_depth = None
        self._current_link_href = None
        self._current_link_parts = []

    def _finalize_group(self) -> None:
        if self._current_group is None:
            return

        self._current_group["header"] = _clean_text("".join(self._header_parts))
        self.groups.append(self._current_group)

        self._current_group = None
        self._current_group_depth = None
        self._header_depth = None
        self._header_parts = []
        self._content_depth = None
        self._current_link_href = None
        self._current_link_parts = []


def fetch_runroom_labs_index_html(index_url: str, timeout: int = 30) -> str:
    request = urllib.request.Request(index_url, headers={"User-Agent": "runroom-content-layer/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def discover_runroom_lab_urls(index_url: str = DEFAULT_RUNROOM_LABS_INDEX_URL, timeout: int = 30) -> tuple[list[str], dict[str, int]]:
    index_html = fetch_runroom_labs_index_html(index_url=index_url, timeout=timeout)
    return parse_runroom_lab_urls(index_html=index_html, index_url=index_url)


def parse_runroom_lab_urls(index_html: str, index_url: str = DEFAULT_RUNROOM_LABS_INDEX_URL) -> tuple[list[str], dict[str, int]]:
    parser = _LabsIndexParser()
    parser.feed(index_html)
    parser.close()

    groups = parser.groups

    selected_urls: list[str] = []
    seen_urls: set[str] = set()

    groups_with_selected_url = 0
    groups_without_selected_url = 0
    duplicates_removed = 0

    for group in groups:
        # Rule: one summary URL per accordion group.
        best_url = _select_best_summary_url(links=group.get("links") or [], index_url=index_url)
        if best_url is None:
            groups_without_selected_url += 1
            continue

        groups_with_selected_url += 1
        if best_url in seen_urls:
            duplicates_removed += 1
            continue

        seen_urls.add(best_url)
        selected_urls.append(best_url)

    return selected_urls, {
        "groups_total": len(groups),
        "groups_with_selected_url": groups_with_selected_url,
        "groups_without_selected_url": groups_without_selected_url,
        "duplicates_removed": duplicates_removed,
    }


def _select_best_summary_url(links: list[dict[str, Any]], index_url: str) -> str | None:
    best_url: str | None = None
    best_score = -1
    best_order = -1

    for link in links:
        href = str(link.get("href") or "")
        text = str(link.get("text") or "")
        order = int(link.get("order") or 0)

        normalized_url = _normalize_url(href=href, index_url=index_url)
        if not normalized_url:
            continue

        score = _summary_link_score(url=normalized_url, text=text)
        if score < 0:
            continue

        # Tie-breaker: keep the latest link in source order.
        if score > best_score or (score == best_score and order > best_order):
            best_url = normalized_url
            best_score = score
            best_order = order

    return best_url


def _normalize_url(href: str, index_url: str) -> str | None:
    raw = html.unescape(href).strip()
    if not raw:
        return None

    parsed_raw = urlsplit(raw)
    if parsed_raw.scheme.lower() in {"mailto", "tel", "javascript"}:
        return None

    absolute_url = urljoin(index_url, raw)
    parsed = urlsplit(absolute_url)

    host = (parsed.hostname or "").lower()
    if host not in _ALLOWED_HOSTS:
        return None

    normalized_path = _normalize_path(parsed.path)
    if not normalized_path:
        return None

    if normalized_path.startswith("/hubfs/") or normalized_path.startswith("/hs-fs/"):
        return None

    if _IMAGE_EXT_RE.search(normalized_path):
        return None

    if normalized_path == _normalize_path(urlsplit(index_url).path):
        return None

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != "hslang"
    ]

    cleaned_query = urlencode(query_items, doseq=True)

    normalized = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/") or "/",
            cleaned_query,
            "",
        )
    )

    normalized_match = normalize_for_match(normalized)
    if "youtube" in normalized_match or "youtu be" in normalized_match or "linkedin" in normalized_match:
        return None

    return normalized


def _summary_link_score(url: str, text: str) -> int:
    text_norm = normalize_for_match(text)
    path_norm = normalize_for_match(urlsplit(url).path)

    if not text_norm:
        return -1

    if any(keyword in text_norm for keyword in _VIDEO_KEYWORDS):
        return -1

    summary_hits = sum(1 for keyword in _SUMMARY_KEYWORDS if keyword in text_norm)

    is_realworld_path = "/realworld/" in urlsplit(url).path.lower()

    # Accept explicit summary CTA text, or a direct runroom realworld article URL.
    if summary_hits == 0 and not is_realworld_path:
        return -1

    score = summary_hits * 100

    if is_realworld_path:
        score += 30

    if "lab" in path_norm:
        score += 10

    score += min(10, len(text_norm.split()))

    return score


def _clean_text(value: str) -> str:
    cleaned = html.unescape(value)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _normalize_path(path: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/") or "/"
