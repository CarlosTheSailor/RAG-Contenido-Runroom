# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Runroom Content RAG is a retrieval-augmented generation (RAG) system built with Python + FastAPI + Supabase pgvector. It manages multi-source content (podcast episodes, case studies, Runroom LABs, articles) and powers AI services: semantic search, newsletter generation, theme intelligence extraction, and LinkedIn draft publishing.

## Commands

### Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill in SUPABASE_DB_URL and OPENAI_API_KEY
```

### Run HTTP API
```bash
python -m src.interfaces.http
# FastAPI on http://localhost:8000 — /health, /docs
```

### Run CLI
```bash
python -m src.cli <command>

# Schema
python -m src.cli migrate-schema

# Ingestion
python -m src.cli ingest-transcripts
python -m src.cli ingest-case-studies-markdown --input <file.md> [--dry-run]
python -m src.cli ingest-case-study-url --url https://...
python -m src.cli ingest-runroom-labs
python -m src.cli backfill-canonical-content [--dry-run] [--limit N]

# Querying
python -m src.cli query-similar --text "search topic" --top-k 8
python -m src.cli recommend-content --text "newsletter draft" --top-k 10
```

### Run Tests
```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m unittest discover -s tests -p "test_*.py"
# Run a single test file:
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m unittest tests/test_<name>.py
```

### Docker
```bash
docker build -t runroom-rag .
docker run --rm -p 8000:8000 --env-file .env runroom-rag
# Override CMD for CLI jobs:
docker run --rm --env-file .env runroom-rag python -m src.cli migrate-schema
```

## Architecture

### Entry Points
- **CLI:** `src/cli.py` — argument parser dispatching to handlers in `src/interfaces/cli/`
- **HTTP API:** `src/interfaces/http/app.py` — FastAPI app with 30+ routes, business logic in `src/interfaces/http/services.py`
- Both use shared `src/config.py` (Settings dataclass loaded from env vars)

### Layered Structure
```
domain/ports.py         → Abstract interfaces (EmbeddingClientPort, etc.)
infrastructure/         → Concrete implementations (OpenAI wrappers, Supabase repositories)
application/use_cases/  → QuerySimilarUseCase, RecommendContentUseCase
interfaces/             → CLI handlers + FastAPI app
```

### Canonical Content Model
All content types converge into a shared schema:
- `content_items` — master entity with `content_type` (episode, case_study, runroom_lab, article…)
- `content_sections` — normalized editorial structure (description, challenge, solution, etc.)
- `content_chunks` — semantic units with `embedding vector(1536)` (OpenAI text-embedding-3-large)
- `content_relations` — optional materialized similarity links

Legacy podcast tables (`episodes`, `chunks`) are preserved unchanged; `backfill-canonical-content` syncs them to the canonical schema.

### Key Modules
| Module | Purpose |
|--------|---------|
| `src/content/` | Multi-source ingestion orchestrator, parsers, re-ranking |
| `src/content/recommendation.py` | ANN search (pgvector) + diversity re-ranking |
| `src/pipeline/` | Legacy podcast transcript ingestion |
| `src/matching/` | Episode↔article semantic matching |
| `src/theme_intel/` | Gmail-based theme extraction, topic persistence, scheduling |
| `src/linkedin_draft_publisher/` | 2-stage LLM draft generation with quality gates |
| `src/youtube_preview/` | YouTube description enhancement |

### Retrieval Flow
1. Embed query with OpenAI text-embedding-3-large
2. ANN search on `content_chunks.embedding` (pgvector cosine)
3. Aggregate chunks → items (dedup, max score)
4. Re-rank with diversity penalty
5. Return top-k with matched excerpts

### Theme Intel Flow
1. Fetch newsletters via Gmail OAuth
2. LLM extracts priority themes + evidence quotes
3. Semantic deduplication (0.90 cosine threshold, 30-day window)
4. Enrich topics with top-10 related canonical content via RAG
5. Cron-triggered via `POST /v1/theme-intel/scheduler/tick`

### LinkedIn Draft Publisher Flow (2-stage)
1. **Stage 1 (topic selection):** Score and frame a Theme Intel topic
2. **Stage 2a (draft generation):** Generate structured JSON draft (hook, body, references) using top-10 RAG-retrieved content
3. **Stage 2b (refinement):** Quality gates: length (1600–3200 chars), no template artifacts, URL integration
4. Publish to Slack + external drafts API on pass

### Disk-Based Prompt Assets
Prompts are loaded from disk at runtime (hot-reload without deploy):
- `theme-intel/prompts/` — Theme Intel system/user prompts
- `newsletters-linkedin/prompts/` and `examples/` — Newsletter base prompt + style examples
- `linkedin-draft-publisher/prompts/` — Multi-stage publisher prompts

### Database Migrations
SQL migration files live in `sql/` (numbered 001+). Apply via `python -m src.cli migrate-schema`. Migrations are idempotent (tracked in `schema_migrations` table).

## Authentication
- **API endpoints (`/v1/*`):** `X-Api-Key` header
- **Web UI:** Google OAuth2 (`GOOGLE_OAUTH_ALLOWED_DOMAIN=runroom.com`) + server-side sessions
- **Gmail integration (Theme Intel):** Separate Google OAuth refresh token (`GMAIL_OAUTH_REFRESH_TOKEN`)

## Critical Environment Variables
- `SUPABASE_DB_URL` — `postgresql://user:pass@host:6543/dbname`
- `OPENAI_API_KEY`
- `EMBEDDING_DIM=1536` — Fixed; changing breaks all existing vectors
- `API_KEY` — HTTP API authentication
- `NEWSLETTER_RAG_MIN_SCORE=0.74` — Semantic similarity threshold for newsletter RAG
