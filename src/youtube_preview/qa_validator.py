from __future__ import annotations

"""QA/SEO validations for the generated YouTube description preview."""

from typing import Iterable

from src.pipeline.normalization import normalize_for_match

from .models import EpisodeContext, ExtractedEntities, ProposedDescription, QACheck, QAReport
from .utils import extract_urls, normalize_url


def validate_description(
    context: EpisodeContext,
    entities: ExtractedEntities,
    proposed: ProposedDescription,
) -> QAReport:
    checks: list[QACheck] = []

    checks.append(_check_intro_has_guest_topic_keywords(context, entities, proposed.markdown))
    checks.append(_check_not_generic(proposed.markdown, entities))
    checks.append(_check_editorial_style(proposed.markdown))
    checks.append(_check_length_reasonable(proposed.markdown))
    checks.append(_check_timestamps_start_at_zero_when_needed(proposed, context))
    checks.append(_check_timestamps_ascending(proposed))
    checks.append(_check_minimum_chapters(proposed))
    checks.append(_check_chapter_duration(proposed, context))
    checks.append(_check_related_links_quality(proposed.markdown))
    checks.append(_check_brand_block_preserved(context, proposed.markdown))
    checks.append(_check_no_hallucinated_urls(context, proposed))

    passed = all(check.passed or check.severity != "error" for check in checks)
    return QAReport(passed=passed, checks=checks)


def _check_intro_has_guest_topic_keywords(
    context: EpisodeContext,
    entities: ExtractedEntities,
    markdown: str,
) -> QACheck:
    first_paragraph = markdown.strip().split("\n\n", 1)[0].strip()
    normalized = normalize_for_match(first_paragraph)

    has_guest = True
    if context.guest_names:
        has_guest = any(normalize_for_match(name) in normalized for name in context.guest_names)

    topic_terms = [normalize_for_match(t) for t in entities.main_topics[:3] if t]
    keyword_terms = [normalize_for_match(k) for k in entities.keywords[:6] if k]

    has_topic = any(term in normalized for term in topic_terms) if topic_terms else len(normalized.split()) >= 8
    has_keyword = any(term in normalized for term in keyword_terms) if keyword_terms else len(normalized.split()) >= 8

    ok = has_guest and has_topic and has_keyword
    message = "El primer párrafo incluye invitado, tema principal y keywords." if ok else (
        "El primer párrafo no cubre claramente invitado/tema/keywords para SEO."
    )
    return QACheck(key="intro_seo", passed=ok, severity="error", message=message)


def _check_not_generic(markdown: str, entities: ExtractedEntities) -> QACheck:
    normalized = normalize_for_match(markdown)
    words = [word for word in normalized.split() if len(word) > 2]
    unique_ratio = (len(set(words)) / len(words)) if words else 0.0

    entity_hits = 0
    for term in (entities.entities[:8] + entities.keywords[:10] + entities.main_topics[:4]):
        token = normalize_for_match(term)
        if token and token in normalized:
            entity_hits += 1

    banned_patterns = [
        "en este episodio hablamos de muchos temas",
        "contenido muy interesante",
        "esperamos que te guste",
        "te contamos todo",
    ]
    has_banned = any(pattern in normalized for pattern in banned_patterns)

    ok = len(words) >= 120 and unique_ratio >= 0.35 and entity_hits >= 3 and not has_banned
    message = "La descripción es específica y contextual." if ok else "La descripción parece demasiado genérica o poco específica."
    return QACheck(key="non_generic", passed=ok, severity="warning", message=message)


def _check_editorial_style(markdown: str) -> QACheck:
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    if not lines:
        return QACheck(
            key="editorial_style",
            passed=False,
            severity="warning",
            message="La descripción está vacía.",
        )

    bullet_lines = [line for line in lines if line.startswith("- ") or line.startswith("* ")]
    paragraph_lines = [line for line in lines if not (line.startswith("- ") or line.startswith("* "))]

    bullet_ratio = len(bullet_lines) / len(lines)
    long_paragraphs = sum(1 for line in paragraph_lines if len(line.split()) >= 10)

    overly_bulleted = bullet_ratio > 0.55
    not_enough_prose = long_paragraphs < 2

    ok = not overly_bulleted and not not_enough_prose
    if ok:
        message = "El texto mantiene un estilo editorial natural (no excesivamente list-based)."
    else:
        message = "El output se percibe demasiado en formato lista/generic AI summary en lugar de prosa editorial."
    return QACheck(key="editorial_style", passed=ok, severity="warning", message=message)


def _check_length_reasonable(markdown: str) -> QACheck:
    length = len(markdown.strip())
    ok = 450 <= length <= 5000
    message = f"Longitud válida ({length} caracteres)." if ok else f"Longitud fuera de rango recomendado ({length} caracteres)."
    return QACheck(key="length_reasonable", passed=ok, severity="error", message=message)


def _check_timestamps_start_at_zero_when_needed(proposed: ProposedDescription, context: EpisodeContext) -> QACheck:
    if not proposed.chapters:
        return QACheck(
            key="timestamps_start",
            passed=False,
            severity="error",
            message="No hay capítulos para validar inicio.",
        )

    first_chapter = int(proposed.chapters[0].start_sec)
    if context.chunks:
        first_source_sec = int(min(chunk.start_ts_sec for chunk in context.chunks))
    else:
        first_source_sec = 0

    # Enforce source-aligned first timestamp; 00:00 only when source really starts at zero.
    if first_source_sec == 0:
        ok = first_chapter == 0
        msg = "Los timestamps empiezan en 00:00." if ok else "Los timestamps deberían empezar en 00:00 según la fuente."
    else:
        ok = first_chapter == first_source_sec
        msg = (
            "El primer timestamp coincide con el inicio real de la fuente."
            if ok
            else "El primer timestamp no coincide con el inicio real de la fuente."
        )
    return QACheck(key="timestamps_start", passed=ok, severity="error", message=msg)


def _check_timestamps_ascending(proposed: ProposedDescription) -> QACheck:
    start_points = [chapter.start_sec for chapter in proposed.chapters]
    ok = all(curr > prev for prev, curr in zip(start_points, start_points[1:]))
    msg = "Los timestamps son ascendentes." if ok else "Los timestamps no están en orden ascendente."
    return QACheck(key="timestamps_ascending", passed=ok, severity="error", message=msg)


def _check_minimum_chapters(proposed: ProposedDescription) -> QACheck:
    total = len(proposed.chapters)
    ok = total >= 3
    msg = f"Capítulos suficientes ({total})." if ok else f"Capítulos insuficientes ({total}); mínimo 3."
    return QACheck(key="chapters_minimum", passed=ok, severity="error", message=msg)


def _check_chapter_duration(proposed: ProposedDescription, context: EpisodeContext) -> QACheck:
    if len(proposed.chapters) < 2:
        return QACheck(
            key="chapter_duration",
            passed=False,
            severity="error",
            message="No se puede validar duración entre capítulos.",
        )

    # Duration is computed chapter-by-chapter against the next timestamp.
    durations = [curr.start_sec - prev.start_sec for prev, curr in zip(proposed.chapters, proposed.chapters[1:])]
    ok = all(duration >= 10 for duration in durations)

    end_sec = int(max((chunk.end_ts_sec for chunk in context.chunks), default=0.0))
    last_sec = proposed.chapters[-1].start_sec
    # Only trust final-chapter duration when source gives a real end beyond the last chapter start.
    episode_length_known = bool(context.chunks) and (end_sec > last_sec)
    if ok and episode_length_known:
        # Validate final chapter only when episode length is known.
        if end_sec - last_sec < 10:
            ok = False

    msg = "Duración mínima entre capítulos válida." if ok else "Hay capítulos con duración inferior a 10 segundos."
    return QACheck(key="chapter_duration", passed=ok, severity="error", message=msg)


def _check_related_links_quality(markdown: str) -> QACheck:
    related_urls = _extract_related_section_urls(markdown)
    duplicates = len(related_urls) != len(set(related_urls))
    irrelevant = [url for url in related_urls if "runroom.com" not in url.lower()]
    ok = not duplicates and not irrelevant

    if ok:
        message = "Los links relacionados no tienen duplicados ni señales claras de irrelevancia."
    else:
        message = "Hay links duplicados o potencialmente irrelevantes en secciones relacionadas."
    return QACheck(key="related_links_quality", passed=ok, severity="warning", message=message)


def _check_brand_block_preserved(context: EpisodeContext, markdown: str) -> QACheck:
    if not context.brand_block:
        return QACheck(
            key="brand_block_preserved",
            passed=True,
            severity="warning",
            message="No había brand block previo para preservar.",
        )

    ok = context.brand_block in markdown
    msg = "Brand block preservado exactamente." if ok else "No se preservó el brand block exacto original."
    return QACheck(key="brand_block_preserved", passed=ok, severity="error", message=msg)


def _check_no_hallucinated_urls(context: EpisodeContext, proposed: ProposedDescription) -> QACheck:
    current_urls = extract_urls(context.current_description)
    proposed_urls = extract_urls(proposed.markdown)

    allowed = set(_normalize_urls(current_urls))
    if context.runroom_article_url:
        allowed.add(normalize_url(context.runroom_article_url))

    for item in proposed.related_episodes + proposed.related_case_studies:
        if item.url:
            allowed.add(normalize_url(item.url))

    not_allowed = [url for url in proposed_urls if normalize_url(url) not in allowed]
    ok = not not_allowed

    msg = "No se detectan URLs alucinadas." if ok else "Hay URLs no soportadas por el contexto de entrada."
    return QACheck(key="no_hallucinated_urls", passed=ok, severity="error", message=msg)


def _extract_related_section_urls(markdown: str) -> list[str]:
    lines = markdown.splitlines()
    capture = False
    bucket: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        title = normalize_for_match(stripped)
        if "episodios relacionados" in title or "casos relacionados" in title:
            capture = True
            continue

        if capture and stripped.endswith(":") and "relacionados" not in normalize_for_match(stripped):
            capture = False

        if capture:
            bucket.extend(extract_urls(stripped))

    return [normalize_url(url) for url in bucket]


def _normalize_urls(urls: Iterable[str]) -> list[str]:
    out: list[str] = []
    for url in urls:
        try:
            out.append(normalize_url(url))
        except Exception:
            continue
    return out
