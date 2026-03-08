# Runroom Content Knowledge Layer

Capa de conocimiento de contenidos de Runroom sobre Supabase + pgvector.

Esta versión evoluciona el repositorio desde un RAG centrado en transcripciones (`episodes/chunks`) a un modelo canónico multi-fuente (`content_items/content_sections/content_chunks`) compatible con:

- `episode`
- `case_study`
- `article` (preparado)
- `event` (preparado)
- `training` (preparado)
- `other`

Documentación extendida:

- [Arquitectura](docs/ARCHITECTURE.md)
- [Operación / Runbook](docs/OPERATIONS.md)

Mantiene compatibilidad con el stack actual:

- Python
- OpenAI embeddings
- Supabase pgvector
- `EMBEDDING_DIM=1536` (sin ruptura del modelo actual)

## Diagnóstico del estado anterior

Antes de esta evolución, el sistema estaba optimizado para podcast:

- modelo de datos especializado en `episodes/chunks`
- matching episodio↔artículo en tablas dedicadas
- retrieval limitado a chunks de episodios
- sin entidad canónica multi-fuente ni relaciones entre contenidos

## Arquitectura objetivo

## Núcleo canónico

- `content_items`: entidad principal de contenido (tipo, título, slug, url, fuente, idioma, estado, metadatos)
- `content_sections`: secciones editoriales normalizadas y trazables
- `content_chunks`: chunks semánticos por sección con embedding
- `content_relations`: relaciones persistidas (opcional) entre contenidos
- `schema_migrations`: control idempotente de migraciones SQL

## Compatibilidad legacy

Se mantienen sin romper:

- `episodes`
- `chunks`
- `runroom_articles`
- `episode_article_candidates`
- comandos legacy (`ingest-transcripts`, `query-similar`, matching y revisión manual)

Además, `ingest-transcripts` ahora sincroniza también al modelo canónico (dual-write progresivo).

## Migraciones SQL

- `sql/001_init.sql` (legacy)
- `sql/002_content_knowledge_layer.sql` (modelo canónico)

Aplicación automática idempotente mediante `schema_migrations`.

## Setup

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
- `YOUTUBE_API_KEY` (opcional, necesario para leer descripción actual desde YouTube API en preview)
- `API_KEY` (requerido para API HTTP)
- `HOST` / `PORT` (servidor FastAPI)
- `OPENAI_EMBEDDING_MODEL=text-embedding-3-large`
- `EMBEDDING_DIM=1536`

## Flujo recomendado

```bash
# 1) Migrar esquema (legacy + canónico)
python -m src.cli migrate-schema

# 2) Ingesta legacy de transcripciones (ahora también sincroniza al canónico)
python -m src.cli ingest-transcripts

# 3) Backfill completo legacy -> canónico (idempotente)
python -m src.cli backfill-canonical-content

# 4) Ingesta de case studies desde Markdown (fase 1)
python -m src.cli ingest-case-studies-markdown \
  --input /Users/carlos/Downloads/Runroom_Case_Studies_Completo.md

# 5) Ingesta de case study individual desde URL (fase 2)
python -m src.cli ingest-case-study-url \
  --url https://www.runroom.com/cases/energia-nufri-posicionamiento-marca-ecosistema-digital
```

## Comandos CLI

## Esquema y migración

```bash
python -m src.cli migrate-schema
```

Opcional:

- `--schema-path sql`

## Backfill canónico

```bash
python -m src.cli backfill-canonical-content
```

Opciones:

- `--dry-run`
- `--limit 20`

## Ingesta case studies (Markdown)

```bash
python -m src.cli ingest-case-studies-markdown \
  --input /Users/carlos/Downloads/Runroom_Case_Studies_Completo.md
```

Opciones:

- `--target-tokens 240`
- `--overlap-tokens 40`
- `--batch-size 32`
- `--offline-mode`
- `--dry-run`

## Ingesta case study (URL)

```bash
python -m src.cli ingest-case-study-url \
  --url https://www.runroom.com/cases/bayer-design-system-coherencia-global-flexibilidad-local
```

Opciones:

- `--target-tokens 240`
- `--overlap-tokens 40`
- `--batch-size 32`
- `--offline-mode`
- `--dry-run`

## Recomendación multi-fuente

```bash
python -m src.cli recommend-content \
  --text "Draft de newsletter sobre product discovery, design systems y formación ejecutiva" \
  --top-k 8
```

Con filtros:

```bash
python -m src.cli recommend-content \
  --text-file /tmp/newsletter_draft.txt \
  --content-types episode,case_study,training \
  --source runroom_case_studies_markdown \
  --lang es \
  --top-k 10 \
  --fetch-k 80
```

Salida agrupada por tipo:

```bash
python -m src.cli recommend-content \
  --text "contenido sobre customer centric y growth" \
  --group-by-type
```

## API HTTP v1 (consulta)

La API expone búsqueda semántica y recomendación multi-fuente con `X-API-Key`.

Arranque local:

```bash
export API_KEY=change-me
python -m src.interfaces.http
```

Endpoints:

- `GET /health`
- `POST /v1/query-similar`
- `POST /v1/recommend-content`

Ejemplo:

```bash
curl -X POST http://localhost:8000/v1/recommend-content \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"text":"newsletter sobre CX","top_k":5,"content_types":["episode","case_study"]}'
```

## Docker / Coolify

Imagen única para API y jobs CLI.

```bash
docker build -t runroom-rag .
docker run --rm -p 8000:8000 --env-file .env runroom-rag
```

En Coolify puedes reutilizar la misma imagen para tareas batch sobre CLI, por ejemplo:

```bash
python -m src.cli migrate-schema
python -m src.cli backfill-canonical-content
```

## Preview descripción YouTube (Phase 0)

Modo local/offline para previsualizar una descripción mejorada de YouTube para un único episodio, sin integrar todavía la API de YouTube.

```bash
python -m src.cli preview-youtube-description --episode r085
```

`--episode` admite:

- id numérico (`85`)
- código/slug (`r085`)
- URL de Runroom (`https://www.runroom.com/realworld/...`)

Opcionales:

- `--output-dir output` (por defecto)
- `--offline-mode` (fuerza generación determinista local)
- `--youtube-url https://youtube.com/watch?v=...` (o `https://youtu.be/...`, se parsea `video_id`)
- `--current-description-file /ruta/descripcion_actual.txt` (usa este texto como fuente actual para diff)

Salida:

- `output/<episode_slug>/proposed_description.md`
- `output/<episode_slug>/qa_report.json`
- `output/<episode_slug>/diff.md`

Notas de funcionamiento (Phase 0):

- la propuesta intenta mejorar la descripción actual (no reescribirla desde cero) cuando existe en datos fuente
- `diff.md` incluye bloque actual + bloque propuesto + diff unificado
- si hay bloque de marca Realworld/Runroom en la descripción actual, se preserva de forma exacta
- en episodios históricos, los capítulos priorizan timestamps reales (descripción actual o transcript/chunks), sin inventar tiempos arbitrarios
- `current_description_source` en debug se clasifica como `youtube_api`, `db`, `file` o `missing`
- si se pasa `--youtube-url` y hay `YOUTUBE_API_KEY`, la descripción actual se lee vía YouTube Data API (`current_description_source=youtube_api`) en modo solo lectura
- `qa_report.json` incluye checks QA/SEO y `debug` con:
  - fuente de descripción actual usada
  - identificadores de contexto (Runroom + YouTube + `video_id`)
  - origen de timestamps de capítulos
  - detalle de contenidos relacionados elegidos (score, título, URL y razón de selección)

## Re-embedding canónico

```bash
python -m src.cli reembed-content --content-type case_study
python -m src.cli reembed-content --item-id 42
```

## Relaciones persistidas (opcional)

```bash
python -m src.cli materialize-content-relations \
  --top-k-per-item 5 \
  --content-types episode,case_study \
  --min-score 0.58
```

## Legacy (se mantiene)

```bash
python -m src.cli sync-runroom-sitemap
python -m src.cli match-episodes
python -m src.cli query-similar --text "borrador newsletter" --top-k 8
python -m src.cli export-review-report --output reports/review_report.csv
python -m src.cli review-matches
python -m src.cli apply-manual-overrides --csv reports/manual_overrides.csv
python -m src.cli sync-episode-titles-from-h1 --dry-run
```

## Parser robusto de Markdown (case studies)

El parser de fase 1 está diseñado para variaciones reales del exportado:

- segmentación por bloques `Case Study #N`
- fallback por `H1/H2/H3` cuando falta uniformidad
- extracción flexible de `Cliente`, `URL`, `URL Original`
- normalización de secciones a taxonomía canónica:
  - `description`
  - `challenge`
  - `approach`
  - `process`
  - `solution`
  - `results`
  - `impact`
  - `technologies`
  - `areas`
  - `quotes`
  - `next_steps`
  - `other`
- trazabilidad por líneas (`source_locator`)

## Retrieval y reranking

`recommend-content` aplica:

- similitud semántica sobre `content_chunks.embedding`
- filtros por `content_type`, `source`, `language`
- agregación chunk→item
- reranking con penalización de repetición por tipo (diversidad mixta)

## Tests

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m unittest discover -s tests -p "test_*.py"
```

Incluye pruebas para:

- parser de Markdown (incluye validación sobre el fichero real si existe)
- parser URL (mock sin red)
- taxonomía de secciones
- chunking editorial
- reranking de recomendaciones
- tests legacy existentes

## Notas operativas

- El sistema mantiene la dimensión de embeddings en `1536` para coexistencia directa con el índice vectorial actual.
- Las relaciones en `content_relations` son opcionales; por defecto la recomendación se calcula on-the-fly.
- Campos canónicos no inferibles de forma fiable quedan opcionales y/o en `custom_metadata_json`.
