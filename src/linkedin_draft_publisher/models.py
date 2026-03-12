from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LinkedInDraftRunConfig:
    origin_category: str
    slack_channel: str
    buyer_persona_objetivo: str
    offline_mode: bool = False
    client_name: str = "linkedin_draft_publisher"
    topics_target_count: int = 5
    topics_fetch_limit: int = 40
    related_top_k: int = 10
    related_counts_by_type: dict[str, int] = field(default_factory=dict)
    triggered_by_email: str | None = None


@dataclass(frozen=True)
class TopicCandidate:
    topic_id: int
    title: str
    context_text: str
    canonical_text: str
    score: float
    last_seen_at: Any


@dataclass(frozen=True)
class DraftStage1Output:
    topic_id: int
    titulo: str
    por_que_importa_ahora: str
    borrador_post: str
    referencias_abstract: list[dict[str, Any]]
    stage_source: str = "llm"


@dataclass(frozen=True)
class DraftStage2Output:
    titulo: str
    por_que_importa_ahora: str
    borrador_post: str
    referencias_abstract: list[dict[str, Any]]
    selected_related_content_item_id: int | None = None
    selected_related_rationale: str = ""
    stage_source: str = "llm"
