# Realworld RAG

Pipeline en Python para:

1. Parsear transcripciones de `transcripciones/*.txt`.
2. Crear chunks con metadata semántica.
3. Generar embeddings.
4. Cargar en Supabase pgvector.
5. Sincronizar sitemap de Runroom y vincular episodio ↔ artículo.

## Requisitos

- Python 3.10+
- Proyecto Supabase con acceso Postgres (`SUPABASE_DB_URL`)
- (Opcional) OpenAI API key (`OPENAI_API_KEY`)

## Configuración

```bash
cp .env.example .env
# Exporta variables, por ejemplo con direnv o manualmente.
```

Variables mínimas:

- `SUPABASE_DB_URL`
- `OPENAI_API_KEY` (opcional si usas `--offline-mode`)

Importante para Supabase + `ivfflat`:

- Usa `EMBEDDING_DIM=1536` (por defecto en este repo).

## Comandos CLI

### 1) Ingesta de transcripciones

```bash
python -m src.cli ingest-transcripts
```

Opciones útiles:

- `--transcripts-dir transcripciones`
- `--target-tokens 220`
- `--overlap-tokens 40`
- `--offline-mode`

### 2) Sincronizar sitemap Runroom

```bash
python -m src.cli sync-runroom-sitemap
```

### 3) Matching episodio ↔ artículo

```bash
python -m src.cli match-episodes
```

Opciones:

- `--auto-threshold 0.86`
- `--auto-margin 0.06`
- `--offline-mode`

### 4) Búsqueda semántica

```bash
python -m src.cli query-similar --text "borrador largo de newsletter" --top-k 8
```

### 5) Exportar cola de revisión

```bash
python -m src.cli export-review-report --output reports/review_report.csv
```

## Esquema SQL

Migración principal: `sql/001_init.sql`

Tablas:

- `episodes`
- `chunks`
- `runroom_articles`
- `episode_article_candidates`

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```
