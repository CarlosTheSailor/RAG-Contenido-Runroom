from __future__ import annotations

import re
from urllib.parse import urlparse

from src.content.case_study_url import parse_case_study_url
from src.content.models import CanonicalDocument, CanonicalSection
from src.pipeline.normalization import estimate_tokens, normalize_text, slugify, strip_accents

_ALLOWED_CONTENT_TYPES = {"article", "training", "other"}

_BOILERPLATE_SECTION_TITLES = {
    "contacto",
    "nuestros servicios",
    "la empresa",
    "siguenos",
    "newsletter",
    "legal",
    "certificaciones",
}

_BOILERPLATE_CONTAINS = (
    "completa el formulario y nos pondremos en contacto",
    "aviso legal",
    "politica de privacidad",
    "politica sgi",
    "politica de cookies",
    "canal de denuncias",
    "todos los derechos reservados",
    "carrer de santa eulalia",
    "barcelona 08012 spain",
    "certificacion ens",
    "certificacion iso",
    "certificacion nis2",
    "share linkedin",
    "share bluesky",
    "instagram linkedin bluesky youtube",
    "runroom ©",
    "runroom sl",
)

_NAV_TOKENS = {
    "servicios",
    "casos",
    "nosotros",
    "academy",
    "realworld",
    "eventos",
    "customer",
    "experience",
    "producto",
    "digital",
    "growth",
    "marketing",
    "herramienta",
    "ia",
    "metodologia",
    "metodología",
    "formacion",
    "formación",
    "libro",
    "precios",
    "iniciar",
    "sesion",
    "sesión",
    "runroom",
}


def parse_runroom_web_url(url: str, content_type: str) -> CanonicalDocument:
    normalized_type = str(content_type or "").strip().lower()
    if normalized_type not in _ALLOWED_CONTENT_TYPES:
        raise ValueError(f"Unsupported web content_type: {content_type}")

    document = parse_case_study_url(url)
    document.sections = _sanitize_web_sections(document.sections)

    if not document.sections and document.item.title.strip():
        fallback_text = normalize_text(document.item.title).strip()
        document.sections = [
            CanonicalSection(
                section_order=0,
                section_key="description",
                section_title="Descripción",
                text=fallback_text,
                token_count=estimate_tokens(fallback_text),
                metadata={"section_key": "description", "section_title": "Descripción"},
                source_locator={"source": "runroom_web_fallback"},
            )
        ]

    slug = document.item.slug or _slug_from_url(url) or slugify(document.item.title)

    document.item.content_key = _content_key_from_url(url)
    document.item.content_type = normalized_type
    document.item.slug = slug
    document.item.source = "runroom_web_url"

    metadata = dict(document.item.metadata or {})
    metadata.update(
        {
            "content_type": normalized_type,
            "source": "runroom_web_url",
            "original_url": url,
        }
    )
    metadata.pop("client", None)
    document.item.metadata = metadata
    document.item.raw_text = "\n\n".join(section.text for section in document.sections if section.text.strip())

    return document


def _content_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path.strip("/") or "home"
    return f"web:{host}:{path.replace('/', ':')}"


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    return path.split("/")[-1]


def _sanitize_web_sections(sections: list[CanonicalSection]) -> list[CanonicalSection]:
    output: list[CanonicalSection] = []
    for section in sections:
        if _is_boilerplate_title(section.section_title):
            continue

        kept_lines: list[str] = []
        for line in section.text.splitlines():
            cleaned_line = normalize_text(line).strip()
            if not cleaned_line:
                continue
            if _is_boilerplate_line(cleaned_line):
                continue
            kept_lines.append(cleaned_line)

        text = normalize_text("\n".join(kept_lines)).strip()
        if not text:
            continue

        output.append(
            CanonicalSection(
                section_order=len(output),
                section_key=section.section_key,
                section_title=section.section_title,
                text=text,
                token_count=estimate_tokens(text),
                metadata=dict(section.metadata or {}),
                source_locator=dict(section.source_locator or {}),
            )
        )
    return output


def _is_boilerplate_title(title: str | None) -> bool:
    normalized = _normalize_filter_text(title or "")
    if not normalized:
        return False
    return normalized in _BOILERPLATE_SECTION_TITLES


def _is_boilerplate_line(line: str) -> bool:
    normalized = _normalize_filter_text(line)
    if not normalized:
        return True
    if any(fragment in normalized for fragment in _BOILERPLATE_CONTAINS):
        return True

    tokens = normalized.split()
    if not tokens:
        return True

    nav_hits = sum(1 for token in tokens if token in _NAV_TOKENS)
    if len(tokens) <= 6 and nav_hits >= max(2, len(tokens) - 1):
        return True
    if len(tokens) >= 8 and nav_hits >= 5 and (nav_hits / len(tokens)) >= 0.55:
        return True
    return False


def _normalize_filter_text(value: str) -> str:
    text = strip_accents(value.lower())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
