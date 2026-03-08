from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from src.config import Settings
from src.matching.title_sync import extract_first_h1, fetch_url_text

from .ai_client import AIClient
from .chunking import build_chunks
from .episode_metadata import infer_episode_info
from .ingest import _embed_in_batches
from .models import Chunk, RunroomArticle
from .parser import parse_transcript
from .storage import SupabaseStorage


class ManualEpisodeIngestSummary(dict):
    pass


class DuplicateEpisodeSourceFilenameError(ValueError):
    pass


def ingest_uploaded_episode(
    settings: Settings,
    schema_path: Path,
    source_filename: str,
    transcript_bytes: bytes,
    runroom_url: str,
    target_tokens: int = 220,
    overlap_tokens: int = 40,
    batch_size: int = 32,
    offline_mode: bool = False,
    transcripts_dir: Path = Path("transcripciones"),
) -> ManualEpisodeIngestSummary:
    safe_filename = Path(source_filename or "").name.strip()
    if not safe_filename:
        raise ValueError("Debes indicar un nombre de archivo.")
    if Path(safe_filename).suffix.lower() != ".txt":
        raise ValueError("El archivo debe tener extension .txt.")
    if not transcript_bytes or not transcript_bytes.strip():
        raise ValueError("El archivo de transcripcion esta vacio.")

    storage = SupabaseStorage(settings.supabase_db_url)
    ai = AIClient(settings=settings, force_offline=offline_mode)

    try:
        storage.ensure_schema(schema_path)

        if storage.get_episode_by_source_filename(safe_filename):
            raise DuplicateEpisodeSourceFilenameError(
                f"Ya existe un episodio ingestado con source_filename={safe_filename}."
            )

        page_html = fetch_url_text(runroom_url)
        runroom_title = extract_first_h1(page_html)
        if not runroom_title:
            raise ValueError("No se pudo extraer un <h1> valido de la URL de Runroom.")

        transcripts_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = transcripts_dir / safe_filename
        transcript_path.write_bytes(transcript_bytes)

        segments = parse_transcript(transcript_path)
        if not segments:
            raise ValueError("No se pudieron extraer segmentos validos de la transcripcion.")

        episode = infer_episode_info(transcript_path, segments)
        episode.title = runroom_title

        episode_id = storage.upsert_episode(episode)

        draft_chunks = build_chunks(
            segments,
            target_tokens=target_tokens,
            overlap_tokens=overlap_tokens,
        )
        if not draft_chunks:
            raise ValueError("No se pudieron generar chunks para la transcripcion.")

        texts = [chunk.text for chunk in draft_chunks]
        embeddings = _embed_in_batches(ai, texts, batch_size=batch_size)

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

        storage.upsert_runroom_article(
            RunroomArticle(
                url=runroom_url,
                slug=_slug_from_runroom_url(runroom_url),
                title=runroom_title,
                description="",
                lang=_language_from_url(runroom_url),
                episode_code_hint=episode.episode_code,
            )
        )
        storage.set_episode_match(
            episode_id=episode_id,
            url=runroom_url,
            status="manual_matched",
            confidence=1.0,
        )

        content_item_id = storage.sync_episode_to_canonical(episode_id)

        return ManualEpisodeIngestSummary(
            source_filename=safe_filename,
            transcript_path=str(transcript_path),
            episode_id=episode_id,
            content_item_id=content_item_id,
            episode_code=episode.episode_code,
            title=runroom_title,
            runroom_url=runroom_url,
            chunks_written=len(chunks),
            canonical_synced=content_item_id is not None,
        )
    finally:
        storage.close()


def _slug_from_runroom_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return "realworld-episode"
    return path.split("/")[-1]


def _language_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if path.startswith("en/"):
        return "en"
    return "es"
