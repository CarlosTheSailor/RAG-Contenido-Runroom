from __future__ import annotations

from pathlib import Path

from src.config import Settings
from src.pipeline.storage import SupabaseStorage

from .sitemap import build_runroom_articles


class SyncSummary(dict):
    pass



def sync_runroom_sitemap(
    settings: Settings,
    schema_path: Path,
    fetch_metadata: bool = True,
) -> SyncSummary:
    storage = SupabaseStorage(settings.supabase_db_url)
    try:
        storage.ensure_schema(schema_path)
        articles = build_runroom_articles(settings.runroom_sitemap_url, fetch_metadata=fetch_metadata)
        upserted = storage.upsert_runroom_articles(articles)
        return SyncSummary(urls_found=len(articles), upserted=upserted)
    finally:
        storage.close()
