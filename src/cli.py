from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from src.config import RuntimeOptions, Settings
from src.logging_utils import configure_logging

logger = logging.getLogger(__name__)



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realworld transcript RAG pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_cmd = sub.add_parser("ingest-transcripts", help="Parse transcripts, chunk, enrich and load into Supabase")
    ingest_cmd.add_argument("--transcripts-dir", default="transcripciones", type=Path)
    ingest_cmd.add_argument("--target-tokens", default=220, type=int)
    ingest_cmd.add_argument("--overlap-tokens", default=40, type=int)
    ingest_cmd.add_argument("--batch-size", default=32, type=int)
    ingest_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local metadata/embeddings")

    sync_cmd = sub.add_parser("sync-runroom-sitemap", help="Fetch Runroom sitemap and upsert realworld URLs")
    sync_cmd.add_argument("--fetch-meta", action="store_true", default=True)
    sync_cmd.add_argument("--no-fetch-meta", action="store_true", help="Skip article page fetch and use slug as title")

    match_cmd = sub.add_parser("match-episodes", help="Match episodes to runroom articles")
    match_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")
    match_cmd.add_argument("--auto-threshold", type=float, default=None)
    match_cmd.add_argument("--auto-margin", type=float, default=None)
    match_cmd.add_argument("--top-candidates", type=int, default=5)

    query_cmd = sub.add_parser("query-similar", help="Search chunks by semantic similarity")
    query_cmd.add_argument("--text", required=True)
    query_cmd.add_argument("--top-k", default=8, type=int)
    query_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")

    export_cmd = sub.add_parser("export-review-report", help="Export review_required matches to CSV")
    export_cmd.add_argument("--output", type=Path, default=Path("reports/review_report.csv"))

    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)

    schema_path = Path("sql/001_init.sql")
    if args.command == "ingest-transcripts":
        from src.pipeline.ingest import ingest_transcripts

        runtime = RuntimeOptions(
            transcripts_dir=args.transcripts_dir,
            target_tokens=args.target_tokens,
            overlap_tokens=args.overlap_tokens,
            batch_size=args.batch_size,
            offline_mode=args.offline_mode,
        )
        summary = ingest_transcripts(settings=settings, options=runtime, schema_path=schema_path)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "sync-runroom-sitemap":
        from src.matching.sync import sync_runroom_sitemap

        fetch_meta = not bool(args.no_fetch_meta)
        summary = sync_runroom_sitemap(settings=settings, schema_path=schema_path, fetch_metadata=fetch_meta)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "match-episodes":
        from src.matching.matcher import run_matching

        summary = run_matching(
            settings=settings,
            schema_path=schema_path,
            force_offline=args.offline_mode,
            auto_threshold=args.auto_threshold,
            auto_margin=args.auto_margin,
            top_candidates=args.top_candidates,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    if args.command == "query-similar":
        from src.pipeline.ai_client import AIClient
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            ai = AIClient(settings, force_offline=args.offline_mode)
            vector = ai.embed_texts([args.text])[0]
            results = storage.query_similar_chunks(vector, top_k=args.top_k)
        finally:
            storage.close()

        for row in results:
            ts = _fmt_seconds(float(row["start_ts_sec"]))
            sim = float(row["similarity"])
            print(f"[{sim:.4f}] {row['episode_code'] or '-'} {row['episode_title']} @ {ts}")
            print(f"  URL: {row.get('runroom_article_url') or '-'}")
            print(f"  Texto: {str(row['text'])[:240].strip()}\n")
        return

    if args.command == "export-review-report":
        from src.pipeline.storage import SupabaseStorage

        storage = SupabaseStorage(settings.supabase_db_url)
        try:
            storage.ensure_schema(schema_path)
            total = storage.export_review_report(args.output)
        finally:
            storage.close()
        print(json.dumps({"rows_exported": total, "output": str(args.output)}, indent=2, ensure_ascii=False))
        return



def _fmt_seconds(seconds: float) -> str:
    total = int(seconds)
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


if __name__ == "__main__":
    main()
