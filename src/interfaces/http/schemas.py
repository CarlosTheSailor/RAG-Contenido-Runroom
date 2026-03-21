from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QuerySimilarRequestModel(BaseModel):
    text: str = Field(..., min_length=1)
    top_k: int = Field(8, ge=1, le=50)
    offline_mode: bool = False


class QuerySimilarResponseModel(BaseModel):
    request_id: str
    query: str
    top_k: int
    results: List[Dict[str, Any]]


class RecommendContentRequestModel(BaseModel):
    text: str = Field(..., min_length=1)
    top_k: int = Field(8, ge=1, le=50)
    fetch_k: int = Field(60, ge=1, le=300)
    content_types: List[str] = Field(default_factory=list)
    source: Optional[str] = None
    language: Optional[str] = None
    group_by_type: bool = False
    offline_mode: bool = False


class RecommendContentResponseModel(BaseModel):
    request_id: str
    query: str
    top_k: int
    total_candidates: int
    grouped: bool
    results: Optional[List[Dict[str, Any]]] = None
    results_by_type: Optional[Dict[str, List[Dict[str, Any]]]] = None


class NewsletterLinkedInGenerateRequestModel(BaseModel):
    idea: str = Field(..., min_length=1)
    referencias: Optional[str] = None
    audiencia: Optional[str] = None
    objetivo_secundario: Optional[str] = None
    longitud: Optional[str] = None
    metafora_visual: Optional[str] = None
    texto_a_incluir: Optional[str] = None
    offline_mode: bool = False


class NewsletterLinkedInRelatedContentModel(BaseModel):
    title: str
    url: Optional[str] = None
    content_type: str
    score: float
    excerpt: str = ""


class NewsletterLinkedInGenerateResponseModel(BaseModel):
    request_id: str
    output_text: str
    related_content: List[NewsletterLinkedInRelatedContentModel]
    warnings: List[str]
    used_examples: List[str]


class NewsletterLinkedInIdeasRequestModel(BaseModel):
    exclude_topic_ids: List[int] = Field(default_factory=list)
    limit: int = Field(10, ge=1, le=20)
    offline_mode: bool = False


class NewsletterLinkedInIdeaModel(BaseModel):
    topic_id: int
    title: str
    context_preview: str
    canonical_text: str
    score: float
    last_seen_at: Any = None
    status: str


class NewsletterLinkedInIdeasResponseModel(BaseModel):
    request_id: str
    ideas: List[NewsletterLinkedInIdeaModel]
    pool_exhausted: bool


class CaseStudyIngestUrlRequestModel(BaseModel):
    url: str = Field(..., min_length=1)


class CaseStudyIngestUrlSummaryModel(BaseModel):
    documents_total: int
    items_upserted: int
    sections_written: int
    chunks_written: int
    dry_run: bool


class CaseStudyIngestUrlResponseModel(BaseModel):
    request_id: str
    url: str
    summary: CaseStudyIngestUrlSummaryModel


class RunroomLabIngestUrlRequestModel(BaseModel):
    url: str = Field(..., min_length=1)


class RunroomLabIngestUrlSummaryModel(BaseModel):
    documents_total: int
    items_upserted: int
    sections_written: int
    chunks_written: int
    dry_run: bool


class RunroomLabIngestUrlResponseModel(BaseModel):
    request_id: str
    url: str
    summary: RunroomLabIngestUrlSummaryModel


class EpisodeIngestSummaryModel(BaseModel):
    source_filename: str
    transcript_path: str
    episode_id: int
    content_item_id: Optional[int] = None
    episode_code: Optional[str] = None
    title: str
    runroom_url: str
    chunks_written: int
    canonical_synced: bool


class EpisodeIngestResponseModel(BaseModel):
    request_id: str
    runroom_url: str
    summary: EpisodeIngestSummaryModel


class ThemeIntelRunCreateRequestModel(BaseModel):
    gmailQuery: str = Field(..., min_length=1)
    originCategory: str = Field(..., min_length=1)
    markAsRead: bool = False
    limitMessages: int = Field(100, ge=1, le=200)
    offline_mode: bool = False


class ThemeIntelRunCreateResponseModel(BaseModel):
    request_id: str
    run_id: int
    status: str


class ThemeIntelRunGetResponseModel(BaseModel):
    request_id: str
    run: Dict[str, Any]


class ThemeIntelRunDocumentsResponseModel(BaseModel):
    request_id: str
    run_id: int
    total: int
    documents: List[Dict[str, Any]]


class ThemeIntelSourceDocumentResponseModel(BaseModel):
    request_id: str
    document: Dict[str, Any]


class ThemeIntelTopicTagModel(BaseModel):
    tag_key: str
    tag_label: str
    provenance: str
    confidence: Optional[float] = None


class ThemeIntelRelatedContentModel(BaseModel):
    content_item_id: int
    relation_rank: int
    score: float
    rationale: Optional[str] = None
    content_type: str
    title: str
    url: Optional[str] = None


class ThemeIntelTopicModel(BaseModel):
    id: int
    title: str
    context_text: str
    canonical_text: str
    primary_category_key: str
    primary_category_label: Optional[str] = None
    status: str
    score: float
    origin_source_type: str
    origin_source_account: str
    origin_query: str
    times_seen: int
    first_seen_at: Any
    last_seen_at: Any
    semantic_score: Optional[float] = None
    tags: List[ThemeIntelTopicTagModel] = Field(default_factory=list)
    related_content: List[ThemeIntelRelatedContentModel] = Field(default_factory=list)


class ThemeIntelTopicListResponseModel(BaseModel):
    request_id: str
    total: int
    topics: List[ThemeIntelTopicModel]


class ThemeIntelTopicStatusUpdateRequestModel(BaseModel):
    status: str = Field(..., pattern="^(new|in_progress|used|discarded)$")


class ThemeIntelTopicStatusUpdateResponseModel(BaseModel):
    request_id: str
    topic: Dict[str, Any]


class ThemeIntelTopicUsageRequestModel(BaseModel):
    client_name: str = Field(..., min_length=1)
    artifact_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ThemeIntelTopicUsageResponseModel(BaseModel):
    request_id: str
    usage: Dict[str, Any]


class ThemeIntelRelatedRefreshRequestModel(BaseModel):
    top_k: Optional[int] = Field(None, ge=1, le=50)
    content_types: List[str] = Field(default_factory=list)
    related_counts_by_type: Dict[str, int] = Field(default_factory=dict)
    offline_mode: bool = False


class ThemeIntelRelatedRefreshResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]


class ThemeIntelTopicDetailResponseModel(BaseModel):
    request_id: str
    topic: Dict[str, Any]


class ThemeIntelScheduleCreateRequestModel(BaseModel):
    name: str = Field(..., min_length=1)
    enabled: bool = True
    every_n_days: int = Field(1, ge=1, le=365)
    run_time_local: str = Field("09:00", pattern=r"^\d{2}:\d{2}(:\d{2})?$")
    timezone: str = Field("Europe/Madrid", min_length=1)


class ThemeIntelScheduleUpdateRequestModel(BaseModel):
    name: Optional[str] = Field(None, min_length=1)
    enabled: Optional[bool] = None
    every_n_days: Optional[int] = Field(None, ge=1, le=365)
    run_time_local: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}(:\d{2})?$")
    timezone: Optional[str] = None


class ThemeIntelScheduleConfigCreateRequestModel(BaseModel):
    execution_order: int = Field(1, ge=1)
    gmail_query: str = Field(..., min_length=1)
    origin_category: str = Field(..., min_length=1)
    mark_as_read: bool = False
    limit_messages: int = Field(100, ge=1, le=200)
    enabled: bool = True


class ThemeIntelScheduleConfigUpdateRequestModel(BaseModel):
    execution_order: Optional[int] = Field(None, ge=1)
    gmail_query: Optional[str] = Field(None, min_length=1)
    origin_category: Optional[str] = Field(None, min_length=1)
    mark_as_read: Optional[bool] = None
    limit_messages: Optional[int] = Field(None, ge=1, le=200)
    enabled: Optional[bool] = None


class ThemeIntelScheduleRunNowRequestModel(BaseModel):
    offline_mode: bool = False


class ThemeIntelSchedulerTickRequestModel(BaseModel):
    offline_mode: bool = False


class ThemeIntelScheduleListResponseModel(BaseModel):
    request_id: str
    total: int
    schedules: List[Dict[str, Any]]


class ThemeIntelScheduleResponseModel(BaseModel):
    request_id: str
    schedule: Dict[str, Any]


class ThemeIntelScheduleConfigResponseModel(BaseModel):
    request_id: str
    config: Dict[str, Any]


class ThemeIntelScheduleExecutionsResponseModel(BaseModel):
    request_id: str
    schedule_id: int
    total: int
    executions: List[Dict[str, Any]]


class ThemeIntelScheduleRunNowResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]


class ThemeIntelSchedulerTickResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]


class LinkedInDraftPublisherScheduleCreateRequestModel(BaseModel):
    name: str = Field(..., min_length=1)
    enabled: bool = True
    every_n_days: int = Field(1, ge=1, le=365)
    run_time_local: str = Field("09:00", pattern=r"^\d{2}:\d{2}(:\d{2})?$")
    timezone: str = Field("Europe/Madrid", min_length=1)


class LinkedInDraftPublisherScheduleUpdateRequestModel(BaseModel):
    name: Optional[str] = Field(None, min_length=1)
    enabled: Optional[bool] = None
    every_n_days: Optional[int] = Field(None, ge=1, le=365)
    run_time_local: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}(:\d{2})?$")
    timezone: Optional[str] = None


class LinkedInDraftPublisherScheduleConfigCreateRequestModel(BaseModel):
    executionOrder: int = Field(1, ge=1)
    originCategory: str = Field(..., min_length=1)
    slackChannel: str = Field(..., min_length=1)
    buyerPersonaObjetivo: str = Field(..., min_length=1)
    enabled: bool = True


class LinkedInDraftPublisherScheduleConfigUpdateRequestModel(BaseModel):
    executionOrder: Optional[int] = Field(None, ge=1)
    originCategory: Optional[str] = Field(None, min_length=1)
    slackChannel: Optional[str] = Field(None, min_length=1)
    buyerPersonaObjetivo: Optional[str] = Field(None, min_length=1)
    enabled: Optional[bool] = None


class LinkedInDraftPublisherScheduleRunNowRequestModel(BaseModel):
    offline_mode: bool = False


class LinkedInDraftPublisherSchedulerTickRequestModel(BaseModel):
    offline_mode: bool = False


class LinkedInDraftPublisherScheduleListResponseModel(BaseModel):
    request_id: str
    total: int
    schedules: List[Dict[str, Any]]


class LinkedInDraftPublisherScheduleResponseModel(BaseModel):
    request_id: str
    schedule: Dict[str, Any]


class LinkedInDraftPublisherScheduleConfigResponseModel(BaseModel):
    request_id: str
    config: Dict[str, Any]


class LinkedInDraftPublisherScheduleExecutionsResponseModel(BaseModel):
    request_id: str
    schedule_id: int
    total: int
    executions: List[Dict[str, Any]]


class LinkedInDraftPublisherScheduleRunNowResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]


class LinkedInDraftPublisherSchedulerTickResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]


class LinkedInDraftPublisherRunCreateRequestModel(BaseModel):
    originCategory: str = Field(..., min_length=1)
    slackChannel: str = Field(..., min_length=1)
    buyerPersonaObjetivo: str = Field(..., min_length=1)
    offline_mode: bool = False


class LinkedInDraftPublisherRunCreateResponseModel(BaseModel):
    request_id: str
    run_id: int
    status: str


class LinkedInDraftPublisherRunGetResponseModel(BaseModel):
    request_id: str
    run: Dict[str, Any]


class LinkedInDraftPublisherRunResultResponseModel(BaseModel):
    request_id: str
    result: Dict[str, Any]
