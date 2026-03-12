from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class SourceDocumentInput:
    source_external_id: str
    source_thread_id: str | None
    subject: str
    sender: str
    received_at: datetime | None
    labels: list[str]
    raw_text: str
    cleaned_text: str
    links: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ThemeEvidence:
    dato: str
    fuente: str
    texto_fuente_breve: str
    url_referencia: str
    newsletter_origen: str


@dataclass(frozen=True)
class ExtractedTheme:
    tema: str
    contexto_newsletters: str
    keywords: list[str]
    datos_cuantitativos_relacionados: list[ThemeEvidence]


@dataclass(frozen=True)
class ExtractedThemesPayload:
    temas: list[ExtractedTheme]
    warnings: list[str]


@dataclass(frozen=True)
class ThemeRunConfig:
    gmail_query: str
    origin_category: str
    mark_as_read: bool
    limit_messages: int = 100
    source_type: str = "gmail"
    source_account: str = "newsletters@runroom.com"


@dataclass(frozen=True)
class ThemeTopicFilters:
    primary_category: str | None = None
    status: str | None = None
    tag_any: list[str] | None = None
    tag_all: list[str] | None = None
    min_score: float | None = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    semantic_query: str | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True)
class ThemeScheduleCreate:
    name: str
    enabled: bool
    every_n_days: int
    run_time_local: str
    timezone: str


@dataclass(frozen=True)
class ThemeScheduleConfigCreate:
    execution_order: int
    gmail_query: str
    origin_category: str
    mark_as_read: bool
    limit_messages: int
    enabled: bool
