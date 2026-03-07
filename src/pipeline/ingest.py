from __future__ import annotations

import logging
from pathlib import Path

from src.config import RuntimeOptions, Settings

from .ai_client import AIClient
from .chunking import build_chunks
from .episode_metadata import infer_episode_info
from .models import Chunk
from .parser import parse_transcript
from .storage import SupabaseStorage

logger = logging.getLogger(__name__)


class IngestSummary(dict):
    pass



def ingest_transcripts(settings: Settings, options: RuntimeOptions, schema_path: Path) -> IngestSummary:
    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings=settings, force_offline=options.offline_mode)

    summary: IngestSummary = IngestSummary(
        files_total=0,
        episodes_upserted=0,
        chunks_written=0,
        chunks_with_metadata=0,
    )

    try:
        storage.ensure_schema(schema_path)

        files = sorted(options.transcripts_dir.glob("*.txt"))
        summary["files_total"] = len(files)
        logger.info("Found %s transcript files", len(files))

        for idx, path in enumerate(files, start=1):
            segments = parse_transcript(path)
            if not segments:
                logger.warning("Skipping empty transcript: %s", path.name)
                continue

            episode = infer_episode_info(path, segments)
            episode_id = storage.upsert_episode(episode)
            summary["episodes_upserted"] += 1

            draft_chunks = build_chunks(
                segments,
                target_tokens=options.target_tokens,
                overlap_tokens=options.overlap_tokens,
            )
            if not draft_chunks:
                logger.warning("No chunks generated for %s", path.name)
                continue

            texts = [chunk.text for chunk in draft_chunks]
            embeddings = _embed_in_batches(ai, texts, options.batch_size)

            chunks: list[Chunk] = []
            for draft, embedding in zip(draft_chunks, embeddings):
                metadata = ai.chunk_metadata(draft.text, language=episode.language)
                chunks.append(
                    Chunk(
                        chunk_index=draft.chunk_index,
                        start_ts_sec=draft.start_ts_sec,
                        end_ts_sec=draft.end_ts_sec,
                        speaker=draft.speaker,
                        text=draft.text,
                        token_count=draft.token_count,
                        metadata=metadata,
                        embedding=embedding,
                    )
                )

            storage.replace_chunks(episode_id=episode_id, chunks=chunks)
            storage.sync_episode_to_canonical(episode_id)
            summary["chunks_written"] += len(chunks)
            summary["chunks_with_metadata"] += len(chunks)

            logger.info(
                "[%s/%s] Ingested %s | episode_id=%s | chunks=%s",
                idx,
                len(files),
                path.name,
                episode_id,
                len(chunks),
            )

    finally:
        storage.close()

    return summary


def _embed_in_batches(ai: AIClient, texts: list[str], batch_size: int) -> list[list[float]]:
    vectors: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        vectors.extend(ai.embed_texts(batch))
    return vectors
