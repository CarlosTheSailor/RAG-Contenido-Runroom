from __future__ import annotations

import logging
from pathlib import Path

from src.config import RuntimeOptions, Settings
from src.content.case_study_markdown import parse_case_studies_markdown
from src.content.case_study_url import parse_case_study_url
from src.content.chunking import chunk_sections
from src.content.models import CanonicalChunk, CanonicalDocument
from src.pipeline.ai_client import AIClient
from src.pipeline.storage import SupabaseStorage

logger = logging.getLogger(__name__)


class ContentIngestSummary(dict):
    pass


def ingest_case_studies_markdown(
    settings: Settings,
    schema_path: Path,
    input_path: Path,
    target_tokens: int = 240,
    overlap_tokens: int = 40,
    batch_size: int = 32,
    offline_mode: bool = False,
    dry_run: bool = False,
) -> ContentIngestSummary:
    docs = parse_case_studies_markdown(input_path)
    return ingest_documents(
        settings=settings,
        schema_path=schema_path,
        documents=docs,
        target_tokens=target_tokens,
        overlap_tokens=overlap_tokens,
        batch_size=batch_size,
        offline_mode=offline_mode,
        dry_run=dry_run,
    )


def ingest_case_study_url(
    settings: Settings,
    schema_path: Path,
    url: str,
    target_tokens: int = 240,
    overlap_tokens: int = 40,
    batch_size: int = 32,
    offline_mode: bool = False,
    dry_run: bool = False,
) -> ContentIngestSummary:
    doc = parse_case_study_url(url)
    return ingest_documents(
        settings=settings,
        schema_path=schema_path,
        documents=[doc],
        target_tokens=target_tokens,
        overlap_tokens=overlap_tokens,
        batch_size=batch_size,
        offline_mode=offline_mode,
        dry_run=dry_run,
    )


def ingest_documents(
    settings: Settings,
    schema_path: Path,
    documents: list[CanonicalDocument],
    target_tokens: int,
    overlap_tokens: int,
    batch_size: int,
    offline_mode: bool,
    dry_run: bool,
) -> ContentIngestSummary:
    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings=settings, force_offline=offline_mode)

    summary: ContentIngestSummary = ContentIngestSummary(
        documents_total=len(documents),
        items_upserted=0,
        sections_written=0,
        chunks_written=0,
        dry_run=dry_run,
    )

    try:
        storage.ensure_schema(schema_path)

        for doc in documents:
            chunks = chunk_sections(doc.sections, target_tokens=target_tokens, overlap_tokens=overlap_tokens)
            texts = [chunk.text for chunk in chunks]
            embeddings = _embed_in_batches(ai, texts, batch_size=batch_size)

            for chunk, embedding in zip(chunks, embeddings):
                base_meta = ai.chunk_metadata(chunk.text, language=doc.item.language)
                chunk.embedding = embedding
                chunk.metadata = {
                    **chunk.metadata,
                    **base_meta,
                    "content_type": doc.item.content_type,
                    "source": doc.item.source,
                }

            if dry_run:
                summary["items_upserted"] += 1
                summary["sections_written"] += len(doc.sections)
                summary["chunks_written"] += len(chunks)
                continue

            item_id = storage.upsert_content_item(doc.item)
            section_id_map = storage.replace_content_sections(item_id=item_id, sections=doc.sections)
            _attach_section_ids(chunks, section_id_map)
            storage.replace_content_chunks(content_item_id=item_id, chunks=chunks)

            summary["items_upserted"] += 1
            summary["sections_written"] += len(doc.sections)
            summary["chunks_written"] += len(chunks)

    finally:
        storage.close()

    return summary


def _embed_in_batches(ai: AIClient, texts: list[str], batch_size: int) -> list[list[float]]:
    vectors: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        vectors.extend(ai.embed_texts(texts[i : i + batch_size]))
    return vectors


def _attach_section_ids(chunks: list[CanonicalChunk], section_id_map: dict[int, int]) -> None:
    for chunk in chunks:
        section_id = section_id_map.get(chunk.section_order)
        if section_id is not None:
            chunk.metadata["section_id"] = section_id
