from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TranscriptChunk:
    start_ts_sec: float
    end_ts_sec: float
    text: str
    speaker: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class EpisodeContext:
    episode_id: int
    runroom_identifier: str
    content_item_id: int | None
    source_filename: str
    episode_code: str | None
    title: str
    slug: str
    runroom_article_url: str | None
    youtube_url: str | None
    youtube_video_id: str | None
    guest_names: list[str]
    language: str
    transcript_path: str
    transcript: str
    chunks: list[TranscriptChunk]
    current_description: str
    current_description_source: str
    current_description_source_detail: str
    brand_block: str | None


@dataclass
class ExtractedEntities:
    keywords: list[str]
    entities: list[str]
    main_topics: list[str]
    guest_names: list[str]


@dataclass
class Chapter:
    timestamp: str
    start_sec: int
    label: str


@dataclass
class RelatedContentItem:
    content_item_id: int
    content_type: str
    title: str
    url: str | None
    score: float
    rationale: str
    selection_reason: str


@dataclass
class ProposedDescription:
    markdown: str
    intro: str
    summary_paragraphs: list[str]
    chapters: list[Chapter]
    related_episodes: list[RelatedContentItem]
    related_case_studies: list[RelatedContentItem]
    used_existing_timestamps: bool
    chapters_source: str


@dataclass
class QACheck:
    key: str
    passed: bool
    severity: str
    message: str


@dataclass
class QAReport:
    passed: bool
    checks: list[QACheck]
    debug: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "passed": self.passed,
            "checks": [
                {
                    "key": check.key,
                    "passed": check.passed,
                    "severity": check.severity,
                    "message": check.message,
                }
                for check in self.checks
            ],
        }
        if self.debug:
            payload["debug"] = self.debug
        return payload
