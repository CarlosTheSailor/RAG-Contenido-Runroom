from __future__ import annotations

import json
import re

from .models import EpisodeContext, ExtractedEntities
from .retrieval import RetrievalResult
from .utils import parse_timestamp_lines


def build_generation_messages(
    context: EpisodeContext,
    entities: ExtractedEntities,
    retrieval: RetrievalResult,
) -> list[dict[str, str]]:
    current_timestamps = parse_timestamp_lines(context.current_description)
    existing_chapters = [{"timestamp": ts, "label": label} for ts, _sec, label in current_timestamps]
    framing_seed = _extract_framing_seed(context.current_description, context.brand_block)

    payload = {
        "episode": {
            "title": context.title,
            "episode_code": context.episode_code,
            "slug": context.slug,
            "runroom_identifier": context.runroom_identifier,
            "guest_names": context.guest_names,
            "language": context.language,
            "runroom_article_url": context.runroom_article_url,
            "youtube_url": context.youtube_url,
            "youtube_video_id": context.youtube_video_id,
            "current_description_source": context.current_description_source,
        },
        "entities": {
            "keywords": entities.keywords,
            "main_topics": entities.main_topics,
            "entities": entities.entities,
        },
        "current_description": context.current_description,
        "current_framing_seed": framing_seed,
        "brand_block": context.brand_block,
        "existing_chapters": existing_chapters,
        "related_episodes": [
            {
                "title": item.title,
                "url": item.url,
                "score": round(item.score, 4),
                "rationale": item.rationale,
            }
            for item in retrieval.related_episodes
        ],
        "related_case_studies": [
            {
                "title": item.title,
                "url": item.url,
                "score": round(item.score, 4),
                "rationale": item.rationale,
            }
            for item in retrieval.related_case_studies
        ],
        "transcript_excerpt": context.transcript[:15000],
    }

    system = (
        "Eres editor SEO de YouTube para el podcast Realworld. "
        "Devuelve SOLO JSON valido. No inventes enlaces. "
        "Mejora la descripcion existente; no la reescribas de forma genérica. "
        "Mantén el angulo editorial original cuando aporte valor."
    )

    user = (
        "Genera una propuesta de descripcion en espanol para YouTube. Reglas:\n"
        "1) Primer parrafo con invitado/a, tema principal y keywords.\n"
        "2) El cuerpo principal debe ser prosa editorial natural (2-4 parrafos), concreto y publicable. Evita estilo de acta o lista.\n"
        "3) Incluir capitulos. Si existing_chapters tiene datos, reutiliza EXACTAMENTE los timestamps y mejora solo los labels.\n"
        "4) Minimo 3 capitulos y en orden ascendente.\n"
        "5) Incluir episodios relacionados (max 3) y casos relacionados (max 2) solo cuando sean relevantes.\n"
        "6) Nunca inventes URLs. Si falta URL usa '[PENDIENTE: URL ...]'.\n"
        "7) Preserva el brand_block exacto al final si existe.\n"
        "8) Tono profesional y claro, alineado con Realworld.\n"
        "9) Si hay current_description util, parte de ese texto: conserva framing y enfoque, mejora claridad/SEO sin perder la voz editorial.\n"
        "JSON schema de salida:\n"
        "{\n"
        '  "intro": "string",\n'
        '  "summary_paragraphs": ["string"],\n'
        '  "chapters": [{"timestamp": "MM:SS o HH:MM:SS", "label": "string"}],\n'
        '  "related_episodes": [{"title": "string", "url": "string|null", "include": true|false}],\n'
        '  "related_case_studies": [{"title": "string", "url": "string|null", "include": true|false}],\n'
        '  "closing_note": "string|null"\n'
        "}\n\n"
        f"Contexto:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _extract_framing_seed(description: str, brand_block: str | None) -> str:
    raw = description.strip()
    if not raw:
        return ""
    if brand_block and brand_block in raw:
        raw = raw.replace(brand_block, "").strip()

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", raw) if p.strip()]
    for paragraph in paragraphs:
        lowered = paragraph.lower()
        if "http://" in lowered or "https://" in lowered:
            continue
        if re.search(r"\b\d{1,2}:\d{2}(?::\d{2})?\b", paragraph):
            continue
        return paragraph
    return ""
