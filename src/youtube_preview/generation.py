from __future__ import annotations

"""Generation module that creates the proposed YouTube description text."""

import json
import logging
import urllib.error
import urllib.request
from typing import Any

from src.pipeline.ai_client import AIClient
from src.pipeline.normalization import normalize_for_match

from .models import Chapter, EpisodeContext, ExtractedEntities, ProposedDescription, RelatedContentItem
from .prompt_builder import build_generation_messages
from .retrieval import RetrievalResult
from .utils import format_seconds_as_timestamp, parse_timestamp_lines, parse_timestamp_to_seconds

logger = logging.getLogger(__name__)


JSON_SCHEMA = {
    "name": "youtube_description_preview",
    "schema": {
        "type": "object",
        "properties": {
            "intro": {"type": "string"},
            "summary_paragraphs": {"type": "array", "items": {"type": "string"}},
            "chapters": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "timestamp": {"type": "string"},
                        "label": {"type": "string"},
                    },
                    "required": ["timestamp", "label"],
                    "additionalProperties": False,
                },
            },
            "related_episodes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "url": {"type": ["string", "null"]},
                        "include": {"type": "boolean"},
                    },
                    "required": ["title", "url", "include"],
                    "additionalProperties": False,
                },
            },
            "related_case_studies": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "url": {"type": ["string", "null"]},
                        "include": {"type": "boolean"},
                    },
                    "required": ["title", "url", "include"],
                    "additionalProperties": False,
                },
            },
            "closing_note": {"type": ["string", "null"]},
        },
        "required": ["intro", "summary_paragraphs", "chapters", "related_episodes", "related_case_studies", "closing_note"],
        "additionalProperties": False,
    },
    "strict": True,
}


def generate_proposed_description(
    ai: AIClient,
    context: EpisodeContext,
    entities: ExtractedEntities,
    retrieval: RetrievalResult,
) -> ProposedDescription:
    payload = _generate_json_payload(ai, context, entities, retrieval)
    return _payload_to_description(payload, context, entities, retrieval)


def _generate_json_payload(
    ai: AIClient,
    context: EpisodeContext,
    entities: ExtractedEntities,
    retrieval: RetrievalResult,
) -> dict[str, Any]:
    fallback = _fallback_payload(context, entities)
    if not ai.online:
        return fallback

    messages = build_generation_messages(context, entities, retrieval)
    request_payload = {
        "model": ai.settings.openai_metadata_model,
        "messages": messages,
        "temperature": 0.2,
        "response_format": {
            "type": "json_schema",
            "json_schema": JSON_SCHEMA,
        },
    }

    try:
        raw = _post_chat_json(ai, request_payload)
        content = raw["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
        return fallback
    except Exception as exc:  # pragma: no cover - network fallback
        logger.warning("YouTube preview generation fallback used: %s", exc)
        return fallback


def _payload_to_description(
    payload: dict[str, Any],
    context: EpisodeContext,
    entities: ExtractedEntities,
    retrieval: RetrievalResult,
) -> ProposedDescription:
    intro = str(payload.get("intro") or "").strip() or _build_intro(context, entities)

    summary_paragraphs = [str(item).strip() for item in payload.get("summary_paragraphs", []) if str(item).strip()]
    if len(summary_paragraphs) < 2:
        summary_paragraphs = _fallback_summary_paragraphs(context, entities)

    chapters, chapters_source, used_existing_timestamps = _build_chapters(payload=payload, context=context)

    selected_related_episodes = _select_related_items(
        source=retrieval.related_episodes,
        payload_rows=payload.get("related_episodes", []),
    )
    selected_related_cases = _select_related_items(
        source=retrieval.related_case_studies,
        payload_rows=payload.get("related_case_studies", []),
    )

    closing_note = str(payload.get("closing_note") or "").strip()
    markdown = _render_markdown(
        intro=intro,
        summary_paragraphs=summary_paragraphs,
        chapters=chapters,
        related_episodes=selected_related_episodes,
        related_case_studies=selected_related_cases,
        closing_note=closing_note,
        brand_block=context.brand_block,
    )

    return ProposedDescription(
        markdown=markdown,
        intro=intro,
        summary_paragraphs=summary_paragraphs,
        chapters=chapters,
        related_episodes=selected_related_episodes,
        related_case_studies=selected_related_cases,
        used_existing_timestamps=used_existing_timestamps,
        chapters_source=chapters_source,
    )


def _build_chapters(payload: dict[str, Any], context: EpisodeContext) -> tuple[list[Chapter], str, bool]:
    existing = parse_timestamp_lines(context.current_description)
    if existing:
        chapters = _chapters_from_existing(existing=existing, context=context)
        return chapters, "current_description", True

    chapters = _chapters_from_transcript(context=context, payload=payload)
    if chapters:
        return chapters, "transcript_chunks", False

    chapters = _chapters_from_payload(payload=payload)
    if chapters:
        return chapters, "model_payload", False

    # Last-resort fallback for very sparse inputs.
    return _minimal_fallback_chapters(), "fallback_minimal", False


def _chapters_from_existing(existing: list[tuple[str, int, str]], context: EpisodeContext) -> list[Chapter]:
    chapters: list[Chapter] = []
    for raw_ts, sec, label in existing:
        improved = _improve_label(label=label, context=context, timestamp_sec=sec)
        timestamp = raw_ts.strip() or format_seconds_as_timestamp(sec)
        chapters.append(Chapter(timestamp=timestamp, start_sec=sec, label=improved))

    return _normalize_chapters(chapters=chapters, context=context)


def _chapters_from_transcript(context: EpisodeContext, payload: dict[str, Any]) -> list[Chapter]:
    candidates = _select_real_timestamps_from_transcript(context)
    if not candidates:
        return []

    payload_labels = [
        str(row.get("label") or "").strip()
        for row in payload.get("chapters", [])
        if isinstance(row, dict) and str(row.get("label") or "").strip()
    ]

    chapters: list[Chapter] = []
    for idx, sec in enumerate(candidates):
        label = payload_labels[idx] if idx < len(payload_labels) else ""
        improved = _improve_label(label=label, context=context, timestamp_sec=sec)
        chapters.append(Chapter(timestamp=format_seconds_as_timestamp(sec), start_sec=sec, label=improved))

    return _normalize_chapters(chapters=chapters, context=context)


def _chapters_from_payload(payload: dict[str, Any]) -> list[Chapter]:
    chapters: list[Chapter] = []
    for row in payload.get("chapters", []):
        if not isinstance(row, dict):
            continue
        ts = str(row.get("timestamp") or "").strip()
        sec = parse_timestamp_to_seconds(ts)
        label = str(row.get("label") or "").strip()
        if sec is None or not label:
            continue
        chapters.append(Chapter(timestamp=ts, start_sec=sec, label=label))

    ordered = sorted(chapters, key=lambda chapter: chapter.start_sec)
    deduped: list[Chapter] = []
    seen: set[int] = set()
    for chapter in ordered:
        if chapter.start_sec in seen:
            continue
        seen.add(chapter.start_sec)
        deduped.append(chapter)
    return deduped


def _select_real_timestamps_from_transcript(context: EpisodeContext) -> list[int]:
    if not context.chunks:
        return []

    starts = sorted({max(0, int(round(chunk.start_ts_sec))) for chunk in context.chunks})
    if not starts:
        return []
    episode_end = int(max((chunk.end_ts_sec for chunk in context.chunks), default=0.0))

    target = 4 if len(starts) >= 8 else 3
    target = min(6, max(3, target))

    selected: list[int] = []
    for i in range(target):
        idx = round(i * (len(starts) - 1) / max(1, target - 1))
        sec = starts[idx]
        if episode_end > 0 and (episode_end - sec) < 10:
            continue
        if selected and sec - selected[-1] < 10:
            next_valid = next((cand for cand in starts[idx + 1 :] if cand - selected[-1] >= 10), None)
            if next_valid is None:
                continue
            sec = next_valid
        if not selected or sec > selected[-1]:
            selected.append(sec)

    if len(selected) < 3:
        for sec in starts:
            if sec in selected:
                continue
            if episode_end > 0 and (episode_end - sec) < 10:
                continue
            if not selected or sec - selected[-1] >= 10:
                selected.append(sec)
            if len(selected) >= 3:
                break

    return selected[:6]


def _normalize_chapters(chapters: list[Chapter], context: EpisodeContext) -> list[Chapter]:
    ordered = sorted(chapters, key=lambda row: row.start_sec)

    deduped: list[Chapter] = []
    seen_seconds: set[int] = set()
    for chapter in ordered:
        sec = max(0, int(chapter.start_sec))
        if sec in seen_seconds:
            continue
        seen_seconds.add(sec)
        deduped.append(
            Chapter(
                timestamp=chapter.timestamp or format_seconds_as_timestamp(sec),
                start_sec=sec,
                label=chapter.label.strip() or "Bloque del episodio",
            )
        )

    with_gap: list[Chapter] = []
    for chapter in deduped:
        if with_gap and (chapter.start_sec - with_gap[-1].start_sec) < 10:
            continue
        with_gap.append(chapter)

    if len(with_gap) >= 3:
        return with_gap

    # Try to complete with remaining transcript chunk starts before giving up.
    remaining = _select_real_timestamps_from_transcript(context)
    taken = {row.start_sec for row in with_gap}
    for sec in remaining:
        if sec in taken:
            continue
        if with_gap and sec - with_gap[-1].start_sec < 10:
            continue
        with_gap.append(Chapter(timestamp=format_seconds_as_timestamp(sec), start_sec=sec, label=_improve_label("", context, sec)))
        taken.add(sec)
        if len(with_gap) >= 3:
            break

    return with_gap


def _minimal_fallback_chapters() -> list[Chapter]:
    return [
        Chapter(timestamp="00:00", start_sec=0, label="Introducción"),
        Chapter(timestamp="00:10", start_sec=10, label="Desarrollo"),
        Chapter(timestamp="00:20", start_sec=20, label="Cierre"),
    ]


def _fallback_payload(context: EpisodeContext, entities: ExtractedEntities) -> dict[str, Any]:
    return {
        "intro": _build_intro(context, entities),
        "summary_paragraphs": _fallback_summary_paragraphs(context, entities),
        "chapters": [],
        "related_episodes": [],
        "related_case_studies": [],
        "closing_note": None,
    }


def _build_intro(context: EpisodeContext, entities: ExtractedEntities) -> str:
    guest = ", ".join(context.guest_names[:2]) if context.guest_names else "una invitada especial"
    topic = entities.main_topics[0] if entities.main_topics else "estrategia digital"
    keywords = ", ".join(entities.keywords[:3]) if entities.keywords else "customer experience"
    return (
        f"En este episodio de Realworld, hablamos con {guest} sobre {topic}. "
        f"La conversación aterriza ideas concretas de {keywords} al contexto de equipos reales."
    )


def _fallback_summary_paragraphs(context: EpisodeContext, entities: ExtractedEntities) -> list[str]:
    paragraphs: list[str] = []

    if context.current_description.strip():
        # Keep editorial continuity when we already have an existing description.
        first_line = context.current_description.strip().split("\n", 1)[0].strip()
        if first_line and len(first_line.split()) > 6 and "http" not in first_line.lower():
            paragraphs.append(first_line)

    topics_text = ", ".join(entities.main_topics[:3]) if entities.main_topics else "producto, experiencia de cliente y estrategia"
    paragraphs.append(
        f"A lo largo del episodio se analizan decisiones y aprendizajes sobre {topics_text}, "
        "con ejemplos que conectan visión, ejecución y resultados." 
    )

    if entities.keywords:
        paragraphs.append(
            f"También se profundiza en {', '.join(entities.keywords[:5])}, "
            "poniendo foco en cómo llevar estos marcos a la práctica sin perder contexto de negocio."
        )

    return paragraphs[:4]


def _improve_label(label: str, context: EpisodeContext, timestamp_sec: int) -> str:
    cleaned = label.strip("- ")
    if cleaned:
        lowered = normalize_for_match(cleaned)
        if len(lowered.split()) >= 2 and "capitulo" not in lowered:
            return cleaned

    nearby = ""
    for chunk in context.chunks:
        if chunk.start_ts_sec <= timestamp_sec <= chunk.end_ts_sec + 45:
            nearby = chunk.text
            break

    if not nearby and context.chunks:
        nearest = min(context.chunks, key=lambda row: abs(row.start_ts_sec - timestamp_sec))
        nearby = nearest.text

    excerpt = nearby.strip().split(".")[0].strip()
    if excerpt:
        excerpt = excerpt[:72].rstrip(" ,;:")
        if len(excerpt.split()) >= 3:
            return excerpt

    return "Bloque del episodio"


def _select_related_items(source: list[RelatedContentItem], payload_rows: Any) -> list[RelatedContentItem]:
    include_by_title: set[str] = set()
    if isinstance(payload_rows, list):
        for row in payload_rows:
            if not isinstance(row, dict):
                continue
            if not bool(row.get("include", True)):
                continue
            title = str(row.get("title") or "").strip().lower()
            if title:
                include_by_title.add(title)

    selected: list[RelatedContentItem] = []
    for item in source:
        if include_by_title and item.title.lower() not in include_by_title:
            continue
        selected.append(item)
    return selected


def _render_markdown(
    intro: str,
    summary_paragraphs: list[str],
    chapters: list[Chapter],
    related_episodes: list[RelatedContentItem],
    related_case_studies: list[RelatedContentItem],
    closing_note: str,
    brand_block: str | None,
) -> str:
    lines: list[str] = []

    lines.append(intro.strip())
    lines.append("")

    for paragraph in summary_paragraphs[:4]:
        text = paragraph.strip()
        if text:
            lines.append(text)
            lines.append("")

    lines.append("Capítulos:")
    for chapter in chapters:
        lines.append(f"- {chapter.timestamp} - {chapter.label}")
    lines.append("")

    if related_episodes:
        lines.append("Episodios relacionados:")
        for item in related_episodes[:3]:
            if item.url:
                lines.append(f"- [{item.title}]({item.url})")
            else:
                lines.append(f"- {item.title} - [PENDIENTE: URL episodio relacionado]")
        lines.append("")

    if related_case_studies:
        lines.append("Casos relacionados:")
        for item in related_case_studies[:2]:
            if item.url:
                lines.append(f"- [{item.title}]({item.url})")
            else:
                lines.append(f"- {item.title} - [PENDIENTE: URL caso relacionado]")
        lines.append("")

    if closing_note:
        lines.append(closing_note)
        lines.append("")

    if brand_block:
        lines.append(brand_block)

    return "\n".join(lines).strip() + "\n"


def _post_chat_json(ai: AIClient, payload: dict[str, Any]) -> dict[str, Any]:
    if not ai.settings.openai_api_key:
        raise ValueError("OPENAI_API_KEY missing")

    url = f"{ai.settings.openai_base_url}/chat/completions"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {ai.settings.openai_api_key}",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:  # pragma: no cover - network fallback
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"OpenAI HTTP {exc.code}: {body}") from exc
