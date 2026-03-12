from __future__ import annotations

import re
from urllib.parse import urlparse

from src.content.case_study_url import parse_case_study_url
from src.content.models import CanonicalDocument, CanonicalSection
from src.pipeline.normalization import estimate_tokens, normalize_text, slugify, strip_accents

_RUNROOM_LAB_BOILERPLATE_EXACT = {
    "casos servicios nosotros academy realworld",
}

_RUNROOM_LAB_BOILERPLATE_CONTAINS = (
    "saltar al contenido principal",
    "saltar al pie de pagina",
    "completa el formulario y nos pondremos en contacto",
    "siguenos instagram linkedin bluesky youtube",
    "tienes que inscribirte para reservar tu plaza",
    "consulta los proximos eventos en runroom com lab",
    "uso de imagenes en el evento",
    "el evento es gratuito y las cervezas tambien",
    "compartir en linkedin",
    "compartir en bluesky",
    "accesibilidad hemos de informarte que lamentablemente",
)

_RUNROOM_LAB_NAV_TOKENS = {
    "casos",
    "servicios",
    "nosotros",
    "academy",
    "realworld",
    "instagram",
    "linkedin",
    "bluesky",
    "youtube",
    "formulario",
    "inscribirte",
    "plaza",
    "saltar",
    "contenido",
    "principal",
    "pie",
    "pagina",
    "compartir",
    "accesibilidad",
    "evento",
    "imagenes",
}


def parse_runroom_lab_url(url: str) -> CanonicalDocument:
    document = parse_case_study_url(url)
    document.sections = _sanitize_runroom_lab_sections(document.sections)
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
                source_locator={"source": "runroom_lab_fallback"},
            )
        ]

    slug = document.item.slug or _slug_from_url(url) or slugify(document.item.title)

    document.item.content_key = f"runroom_lab:runroom:{slug}"
    document.item.content_type = "runroom_lab"
    document.item.slug = slug
    document.item.source = "runroom_lab_url"

    metadata = dict(document.item.metadata or {})
    metadata.update(
        {
            "content_type": "runroom_lab",
            "source": "runroom_lab_url",
            "original_url": url,
        }
    )
    document.item.metadata = metadata
    document.item.raw_text = "\n\n".join(section.text for section in document.sections if section.text.strip())

    return document


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    return path.split("/")[-1]


def _sanitize_runroom_lab_sections(sections: list[CanonicalSection]) -> list[CanonicalSection]:
    output: list[CanonicalSection] = []
    for section in sections:
        kept_lines: list[str] = []
        for line in section.text.splitlines():
            cleaned_line = normalize_text(line).strip()
            if not cleaned_line:
                continue
            if _is_runroom_lab_boilerplate_line(cleaned_line):
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


def _is_runroom_lab_boilerplate_line(line: str) -> bool:
    normalized = _normalize_filter_text(line)
    if not normalized:
        return True
    if normalized in _RUNROOM_LAB_BOILERPLATE_EXACT:
        return True
    if any(fragment in normalized for fragment in _RUNROOM_LAB_BOILERPLATE_CONTAINS):
        return True

    tokens = normalized.split()
    if not tokens:
        return True
    nav_hits = sum(1 for token in tokens if token in _RUNROOM_LAB_NAV_TOKENS)
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
