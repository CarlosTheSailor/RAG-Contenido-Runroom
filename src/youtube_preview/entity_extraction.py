from __future__ import annotations

import re
from collections import Counter

from src.pipeline.normalization import STOPWORDS_ES, normalize_for_match

from .models import EpisodeContext, ExtractedEntities


ENTITY_RE = re.compile(r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){0,3})\b")


def extract_entities(context: EpisodeContext) -> ExtractedEntities:
    keywords = _collect_keywords(context)
    entities = _collect_entities(context)
    topics = _collect_topics(context, keywords)

    guest_names = list(context.guest_names)
    for candidate in entities:
        if len(guest_names) >= 3:
            break
        if candidate.lower() not in {name.lower() for name in guest_names}:
            guest_names.append(candidate)

    return ExtractedEntities(
        keywords=keywords[:12],
        entities=entities[:20],
        main_topics=topics[:6],
        guest_names=guest_names[:4],
    )


def _collect_keywords(context: EpisodeContext) -> list[str]:
    freq: Counter[str] = Counter()

    for chunk in context.chunks:
        metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
        for key in metadata.get("keywords", []) if isinstance(metadata.get("keywords"), list) else []:
            term = normalize_for_match(str(key))
            if term and term not in STOPWORDS_ES:
                freq[term] += 3

        raw = normalize_for_match(chunk.text)
        for token in raw.split():
            if len(token) < 4 or token in STOPWORDS_ES:
                continue
            freq[token] += 1

    if not freq:
        raw = normalize_for_match(context.transcript)
        for token in raw.split():
            if len(token) >= 4 and token not in STOPWORDS_ES:
                freq[token] += 1

    return [token for token, _ in freq.most_common(20)]


def _collect_entities(context: EpisodeContext) -> list[str]:
    counts: Counter[str] = Counter()

    for chunk in context.chunks:
        metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
        for value in metadata.get("entities", []) if isinstance(metadata.get("entities"), list) else []:
            candidate = str(value).strip()
            if len(candidate) >= 3:
                counts[candidate] += 2

    if sum(counts.values()) < 4:
        for match in ENTITY_RE.findall(context.transcript[:24000]):
            candidate = match.strip()
            if len(candidate) >= 3:
                counts[candidate] += 1

    deduped: list[str] = []
    seen: set[str] = set()
    for entity, _ in counts.most_common(30):
        key = entity.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entity)
    return deduped


def _collect_topics(context: EpisodeContext, keywords: list[str]) -> list[str]:
    counts: Counter[str] = Counter()

    for chunk in context.chunks:
        metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
        topic = str(metadata.get("topic") or "").strip()
        subtopic = str(metadata.get("subtopic") or "").strip()
        if topic and topic.lower() != "general":
            counts[topic] += 3
        if subtopic and subtopic.lower() != "general":
            counts[subtopic] += 2

    for kw in keywords[:8]:
        counts[kw] += 1

    return [topic for topic, _ in counts.most_common(10)]
