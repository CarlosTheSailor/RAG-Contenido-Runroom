from __future__ import annotations

import argparse
from pathlib import Path

from src.config import Settings
from src.interfaces.cli.command_handlers import dispatch_command
from src.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Runroom content knowledge layer CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate_cmd = sub.add_parser("migrate-schema", help="Apply SQL migrations in /sql")
    migrate_cmd.add_argument("--schema-path", type=Path, default=Path("sql"))

    ingest_cmd = sub.add_parser("ingest-transcripts", help="Parse transcripts, chunk, enrich and load into Supabase")
    ingest_cmd.add_argument("--transcripts-dir", default="transcripciones", type=Path)
    ingest_cmd.add_argument("--target-tokens", default=220, type=int)
    ingest_cmd.add_argument("--overlap-tokens", default=40, type=int)
    ingest_cmd.add_argument("--batch-size", default=32, type=int)
    ingest_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local metadata/embeddings")

    cs_md_cmd = sub.add_parser("ingest-case-studies-markdown", help="Ingest runroom case studies from markdown export")
    cs_md_cmd.add_argument("--input", required=True, type=Path)
    cs_md_cmd.add_argument("--target-tokens", default=240, type=int)
    cs_md_cmd.add_argument("--overlap-tokens", default=40, type=int)
    cs_md_cmd.add_argument("--batch-size", default=32, type=int)
    cs_md_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local metadata/embeddings")
    cs_md_cmd.add_argument("--dry-run", action="store_true")

    cs_url_cmd = sub.add_parser("ingest-case-study-url", help="Fetch and ingest a single runroom case study URL")
    cs_url_cmd.add_argument("--url", required=True)
    cs_url_cmd.add_argument("--target-tokens", default=240, type=int)
    cs_url_cmd.add_argument("--overlap-tokens", default=40, type=int)
    cs_url_cmd.add_argument("--batch-size", default=32, type=int)
    cs_url_cmd.add_argument("--offline-mode", action="store_true")
    cs_url_cmd.add_argument("--dry-run", action="store_true")

    backfill_cmd = sub.add_parser("backfill-canonical-content", help="Backfill legacy episodes/chunks into canonical tables")
    backfill_cmd.add_argument("--dry-run", action="store_true")
    backfill_cmd.add_argument("--limit", type=int, default=None)

    sync_cmd = sub.add_parser("sync-runroom-sitemap", help="Fetch Runroom sitemap and upsert realworld URLs")
    sync_cmd.add_argument("--fetch-meta", action="store_true", default=True)
    sync_cmd.add_argument("--no-fetch-meta", action="store_true", help="Skip article page fetch and use slug as title")

    match_cmd = sub.add_parser("match-episodes", help="Match episodes to runroom articles")
    match_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")
    match_cmd.add_argument("--auto-threshold", type=float, default=None)
    match_cmd.add_argument("--auto-margin", type=float, default=None)
    match_cmd.add_argument("--top-candidates", type=int, default=5)

    query_cmd = sub.add_parser("query-similar", help="Search legacy episode chunks by semantic similarity")
    query_cmd.add_argument("--text", required=True)
    query_cmd.add_argument("--top-k", default=8, type=int)
    query_cmd.add_argument("--offline-mode", action="store_true", help="Use deterministic local embeddings")

    recommend_cmd = sub.add_parser("recommend-content", help="Recommend canonical content from free text")
    text_group = recommend_cmd.add_mutually_exclusive_group(required=True)
    text_group.add_argument("--text", type=str)
    text_group.add_argument("--text-file", type=Path)
    recommend_cmd.add_argument("--top-k", type=int, default=8)
    recommend_cmd.add_argument("--fetch-k", type=int, default=60)
    recommend_cmd.add_argument("--content-types", type=str, default="")
    recommend_cmd.add_argument("--source", type=str, default=None)
    recommend_cmd.add_argument("--lang", type=str, default=None)
    recommend_cmd.add_argument("--group-by-type", action="store_true")
    recommend_cmd.add_argument("--offline-mode", action="store_true")

    preview_cmd = sub.add_parser(
        "preview-youtube-description",
        help="Generate a local preview of an improved YouTube description for one episode",
    )
    preview_cmd.add_argument("--episode", required=True, help="Episode identifier: id, slug, code, or Runroom URL")
    preview_cmd.add_argument("--youtube-url", type=str, default=None, help="Optional YouTube URL for the episode")
    preview_cmd.add_argument(
        "--current-description-file",
        type=Path,
        default=None,
        help="Optional local file to use as current description source for diff/preview",
    )
    preview_cmd.add_argument("--output-dir", type=Path, default=Path("output"))
    preview_cmd.add_argument("--offline-mode", action="store_true", help="Force deterministic local generation")

    reembed_cmd = sub.add_parser("reembed-content", help="Recompute embeddings for canonical content chunks")
    reembed_cmd.add_argument("--content-type", type=str, default=None)
    reembed_cmd.add_argument("--item-id", type=int, default=None)
    reembed_cmd.add_argument("--batch-size", type=int, default=64)
    reembed_cmd.add_argument("--offline-mode", action="store_true")

    rel_cmd = sub.add_parser("materialize-content-relations", help="Persist related-content candidates")
    rel_cmd.add_argument("--top-k-per-item", type=int, default=5)
    rel_cmd.add_argument("--limit-items", type=int, default=None)
    rel_cmd.add_argument("--content-types", type=str, default="")
    rel_cmd.add_argument("--min-score", type=float, default=0.55)

    export_cmd = sub.add_parser("export-review-report", help="Export review_required matches to CSV")
    export_cmd.add_argument("--output", type=Path, default=Path("reports/review_report.csv"))

    review_cmd = sub.add_parser("review-matches", help="Review and select matches interactively")
    review_cmd.add_argument("--limit", type=int, default=None, help="Max review episodes in this run")

    apply_cmd = sub.add_parser("apply-manual-overrides", help="Apply manual episode->URL matches from CSV")
    apply_cmd.add_argument("--csv", type=Path, required=True, help="CSV path with manual overrides")
    apply_cmd.add_argument("--dry-run", action="store_true", help="Validate input without writing to database")

    title_sync_cmd = sub.add_parser("sync-episode-titles-from-h1", help="Sync episode and article titles from each episode page h1")
    title_sync_cmd.add_argument("--dry-run", action="store_true", help="Only report differences without writing")
    title_sync_cmd.add_argument("--limit", type=int, default=None, help="Process only first N matched episodes")
    title_sync_cmd.add_argument(
        "--only-status",
        default="auto_matched,manual_matched",
        help="Comma-separated episode match_status filter",
    )
    title_sync_cmd.add_argument(
        "--report-csv",
        type=Path,
        default=None,
        help="Optional CSV file path with per-episode diff/result",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    dispatch_command(args=args, settings=settings, schema_path=Path("sql"))


if __name__ == "__main__":
    main()
