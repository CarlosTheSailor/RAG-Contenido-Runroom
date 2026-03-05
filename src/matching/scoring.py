from __future__ import annotations

import re
from dataclasses import dataclass

from src.pipeline.normalization import normalize_for_match, tokenize_for_match

CODE_RE = re.compile(r"\b([er]\d{3})\b", re.IGNORECASE)


@dataclass
class ScoreBreakdown:
    score: float
    method: str
    lexical_score: float
    semantic_score: float



def episode_profile_text(episode: dict) -> str:
    guests = episode.get("guest_names") or []
    guest_text = ", ".join(guests) if isinstance(guests, list) else str(guests)
    parts = [
        str(episode.get("episode_code") or ""),
        str(episode.get("title") or ""),
        guest_text,
    ]
    return " | ".join(part for part in parts if part)



def article_profile_text(article: dict) -> str:
    return " | ".join(
        part
        for part in [
            str(article.get("episode_code_hint") or ""),
            str(article.get("title") or ""),
            str(article.get("slug") or ""),
            str(article.get("description") or ""),
        ]
        if part
    )



def lexical_score(episode: dict, article: dict) -> float:
    ep_tokens = set(tokenize_for_match(episode_profile_text(episode)))
    ar_tokens = set(tokenize_for_match(article_profile_text(article)))
    if not ep_tokens or not ar_tokens:
        return 0.0

    overlap = ep_tokens.intersection(ar_tokens)
    union = ep_tokens.union(ar_tokens)
    base = len(overlap) / max(1, len(union))

    # Boost if guest appears in slug/title.
    guest_boost = 0.0
    guests = episode.get("guest_names") or []
    if isinstance(guests, list):
        article_blob = normalize_for_match(article_profile_text(article))
        for guest in guests:
            guest_norm = normalize_for_match(str(guest))
            if guest_norm and guest_norm in article_blob:
                guest_boost += 0.08

    return min(1.0, base + guest_boost)



def code_exact_match(episode: dict, article: dict) -> bool:
    ep_code = (episode.get("episode_code") or "").lower()
    if not ep_code:
        return False

    hint = (article.get("episode_code_hint") or "").lower()
    if hint and hint == ep_code:
        return True

    slug = str(article.get("slug") or "")
    code_in_slug = CODE_RE.search(slug)
    if code_in_slug and code_in_slug.group(1).lower() == ep_code:
        return True

    return False



def aggregate_score(code_exact: bool, lexical: float, semantic: float) -> ScoreBreakdown:
    if code_exact:
        return ScoreBreakdown(
            score=0.99,
            method="code_exact",
            lexical_score=max(lexical, 0.7),
            semantic_score=max(semantic, 0.7),
        )

    if semantic >= lexical:
        method = "semantic"
    else:
        method = "name_slug"

    score = (0.6 * semantic) + (0.4 * lexical)
    return ScoreBreakdown(score=score, method=method, lexical_score=lexical, semantic_score=semantic)
