from __future__ import annotations

import re
from typing import Iterable

from src.pipeline.normalization import normalize_for_match, strip_accents

_SECTION_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("description", ("descripcion", "descripción", "titulo del caso", "title of case", "overview")),
    ("challenge", ("reto", "desafio", "desafío", "retos", "origen de una vision", "origen de una visión")),
    ("approach", ("enfoque", "como trabajamos", "cómo trabajamos", "estrategia", "vision 360", "visión 360")),
    ("solution", ("solucion", "solución", "aportacion de valor", "aportación de valor", "cocreando", "arquitectura adaptada")),
    ("process", ("proceso", "procesos", "metodologia", "metodología", "implementacion", "implementación", "delivery", "sprint", "fase", "como lo hicimos", "cómo lo hicimos", "onboard")),
    ("results", ("resultados", "valor entregado", "competencias adquiridas", "resultado", "crecimiento")),
    ("impact", ("impacto", "estado actual", "impacto en el mercado", "impacto en cifras")),
    ("technologies", ("tecnologias", "tecnologías", "herramientas", "stack", "tech")),
    ("areas", ("areas", "áreas", "areas de expertise", "audiencia", "perfil del participante", "a quien nos dirigimos", "a quién nos dirigimos")),
    ("quotes", ("quote", "cita", "testimonio")),
    ("next_steps", ("proximos pasos", "próximos pasos", "siguientes fases", "roadmap")),
]

_BULLET_RE = re.compile(r"^\s*(?:[-*]|\d+[.)])\s+(?P<item>.+?)\s*$")


def canonical_section_key(title: str | None) -> str:
    if not title:
        return "other"

    norm = normalize_for_match(title)
    if not norm:
        return "other"

    for key, patterns in _SECTION_RULES:
        if any(pattern in norm for pattern in patterns):
            return key

    if norm == "cliente":
        return "other"
    if "url" in norm:
        return "other"
    return "other"


def extract_bullet_items(text: str) -> list[str]:
    items: list[str] = []
    for line in text.splitlines():
        m = _BULLET_RE.match(line)
        if m:
            items.append(_clean_inline(m.group("item")))
    return [item for item in items if item]


def normalize_values(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = _clean_inline(value)
        if not cleaned:
            continue
        key = strip_accents(cleaned).lower()
        if key not in seen:
            seen.add(key)
            out.append(cleaned)
    return out


def _clean_inline(value: str) -> str:
    text = value.strip().replace("\\", "")
    text = re.sub(r"\s+", " ", text)
    return text
