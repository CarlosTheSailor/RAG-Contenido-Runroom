from __future__ import annotations

from pathlib import Path

from src.config import Settings
from src.content.models import CanonicalChunk, CanonicalContentItem, CanonicalSection
from src.pipeline.normalization import estimate_tokens, slugify
from src.pipeline.storage import SupabaseStorage


class BackfillSummary(dict):
    pass


def backfill_episodes_to_canonical(
    settings: Settings,
    schema_path: Path,
    dry_run: bool = False,
    limit: int | None = None,
) -> BackfillSummary:
    storage = SupabaseStorage(settings.supabase_db_url)
    summary: BackfillSummary = BackfillSummary(
        episodes_seen=0,
        items_upserted=0,
        sections_written=0,
        chunks_written=0,
        dry_run=dry_run,
    )

    try:
        storage.ensure_schema(schema_path)
        episodes = storage.list_episodes()
        if limit is not None:
            episodes = episodes[:limit]

        for episode in episodes:
            summary["episodes_seen"] += 1
            episode_id = int(episode["id"])
            chunks = storage.list_chunks_for_episode(episode_id)

            raw_text = "\n\n".join(str(row.get("text") or "") for row in chunks if row.get("text"))
            title = str(episode.get("title") or f"Episode {episode_id}")
            slug = str(episode.get("episode_code") or "").strip().lower() or slugify(title)

            item = CanonicalContentItem(
                content_key=f"episode:{episode_id}",
                content_type="episode",
                title=title,
                slug=slug,
                url=str(episode.get("runroom_article_url") or "").strip() or None,
                source="realworld_transcript",
                language=str(episode.get("language") or "es"),
                status="active",
                metadata={
                    "content_type": "episode",
                    "source": "realworld_transcript",
                    "legacy_episode_id": episode_id,
                    "episode_code": episode.get("episode_code"),
                    "guest_names": episode.get("guest_names") or [],
                    "match_status": episode.get("match_status"),
                    "match_confidence": episode.get("match_confidence"),
                },
                custom_metadata={
                    "source_filename": episode.get("source_filename"),
                    "transcript_path": episode.get("transcript_path"),
                },
                raw_text=raw_text,
            )

            section = CanonicalSection(
                section_order=0,
                section_key="other",
                section_title="Transcript",
                text=raw_text,
                token_count=estimate_tokens(raw_text or " "),
                metadata={"section_key": "other", "section_title": "Transcript", "legacy_episode_id": episode_id},
                source_locator={"legacy_episode_id": episode_id},
            )

            canonical_chunks: list[CanonicalChunk] = []
            for row in chunks:
                embedding = storage.parse_vector(row.get("embedding"))
                canonical_chunks.append(
                    CanonicalChunk(
                        chunk_order=int(row.get("chunk_index") or 0),
                        section_order=0,
                        section_key="other",
                        section_title="Transcript",
                        text=str(row.get("text") or ""),
                        token_count=int(row.get("token_count") or 1),
                        metadata={
                            "legacy_chunk_id": row.get("id"),
                            "legacy_episode_id": episode_id,
                            "speaker": row.get("speaker"),
                            "metadata_json": row.get("metadata_json") or {},
                            "start_ts_sec": row.get("start_ts_sec"),
                            "end_ts_sec": row.get("end_ts_sec"),
                        },
                        source_locator={
                            "start_ts_sec": row.get("start_ts_sec"),
                            "end_ts_sec": row.get("end_ts_sec"),
                        },
                        embedding=embedding,
                    )
                )

            if dry_run:
                summary["items_upserted"] += 1
                summary["sections_written"] += 1
                summary["chunks_written"] += len(canonical_chunks)
                continue

            item_id = storage.upsert_content_item(item, legacy_episode_id=episode_id)
            section_map = storage.replace_content_sections(item_id, [section])
            section_id = section_map.get(0)
            for chunk in canonical_chunks:
                if section_id is not None:
                    chunk.metadata["section_id"] = section_id
            storage.replace_content_chunks(item_id, canonical_chunks)

            summary["items_upserted"] += 1
            summary["sections_written"] += 1
            summary["chunks_written"] += len(canonical_chunks)

    finally:
        storage.close()

    return summary
