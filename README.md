# Realworld RAG

Pipeline en Python para indexar transcripciones de episodios de Realworld, enlazarlas con su URL de Runroom y habilitar búsqueda semántica sobre chunks.

## Qué hace el proyecto

1. Parsea `transcripciones/*.txt` (timestamps + speaker cuando existe).
2. Genera chunks con solape y metadata semántica.
3. Calcula embeddings y persiste en Supabase pgvector.
4. Sincroniza URLs de `https://www.runroom.com/sitemap.xml` (sección Realworld).
5. Hace matching episodio ↔ artículo con score híbrido.
6. Permite revisión manual y overrides.
7. Sincroniza títulos desde el `h1` real de cada URL de episodio.

## Requisitos

- Python 3.10+
- Supabase con acceso Postgres (`SUPABASE_DB_URL`)
- `OPENAI_API_KEY` (opcional si usas `--offline-mode`)

## Setup rápido

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
cp .env.example .env
```

Variables clave:

- `SUPABASE_DB_URL`
- `OPENAI_API_KEY`
- `EMBEDDING_DIM=1536` (importante para índice `ivfflat` en Supabase)

## Flujo recomendado end-to-end

```bash
python -m src.cli ingest-transcripts
python -m src.cli sync-runroom-sitemap
python -m src.cli match-episodes
python -m src.cli export-review-report --output reports/review_report.csv
python -m src.cli review-matches
python -m src.cli sync-episode-titles-from-h1 --dry-run --report-csv reports/title_sync_report.csv
python -m src.cli sync-episode-titles-from-h1 --report-csv reports/title_sync_report.csv
```

## Comandos CLI

### `ingest-transcripts`

Ingesta de transcripciones a `episodes` + `chunks`.

```bash
python -m src.cli ingest-transcripts
```

Opciones:

- `--transcripts-dir transcripciones`
- `--target-tokens 220`
- `--overlap-tokens 40`
- `--batch-size 32`
- `--offline-mode`

### `sync-runroom-sitemap`

Sincroniza catálogo de artículos Realworld en `runroom_articles`.

```bash
python -m src.cli sync-runroom-sitemap
```

### `match-episodes`

Asigna artículo por episodio (auto o revisión).

```bash
python -m src.cli match-episodes
```

Opciones:

- `--auto-threshold 0.86`
- `--auto-margin 0.06`
- `--top-candidates 5`
- `--offline-mode`

### `query-similar`

Busca chunks por similitud semántica.

```bash
python -m src.cli query-similar --text "borrador largo de newsletter" --top-k 8
```

### `export-review-report`

Exporta episodios `review_required` con sus candidatos.

```bash
python -m src.cli export-review-report --output reports/review_report.csv
```

### `review-matches`

Revisión interactiva en terminal (elige `1..5`, `s`, `q`).

```bash
python -m src.cli review-matches
```

Opcional:

- `--limit 10`

### `apply-manual-overrides`

Aplica matches manuales por CSV.

```bash
python -m src.cli apply-manual-overrides --csv reports/manual_overrides.csv
```

Validación sin escritura:

```bash
python -m src.cli apply-manual-overrides --csv reports/manual_overrides.csv --dry-run
```

Formato CSV (`episode_id,url` requeridos):

```csv
episode_id,url,title,description,lang,episode_code_hint,confidence
12,https://www.runroom.com/realworld/mi-articulo,Titulo opcional,,es,,1.0
```

Plantilla incluida: `manual_overrides.example.csv`.

### `sync-episode-titles-from-h1`

Sincroniza `episodes.title` y `runroom_articles.title` usando el primer `h1` de cada URL asociada.

```bash
python -m src.cli sync-episode-titles-from-h1 --dry-run --report-csv reports/title_sync_report.csv
python -m src.cli sync-episode-titles-from-h1 --report-csv reports/title_sync_report.csv
```

Opciones:

- `--limit 10`
- `--only-status auto_matched,manual_matched`

## Modelo de datos (Supabase)

Migración: `sql/001_init.sql`

- `episodes`: episodio, estado de matching y URL final.
- `chunks`: texto chunked + metadata JSON + embedding vector.
- `runroom_articles`: catálogo sincronizado desde sitemap / overrides.
- `episode_article_candidates`: candidatos y scores de matching.

## Verificación en SQL Editor

```sql
select count(*) as episodes from episodes;
select count(*) as chunks from chunks;
select match_status, count(*) from episodes group by match_status order by 1;
```

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```
