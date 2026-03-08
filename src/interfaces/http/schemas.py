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
