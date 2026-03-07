# Arquitectura: Content Knowledge Layer (Runroom)

## Objetivo

Evolucionar el RAG centrado en episodios a una capa de conocimiento de contenidos multi-fuente para:

- indexar tipos de contenido heterogéneos (`episode`, `case_study`, futuros `article/event/training`),
- recomendar contenido relacionado desde texto libre,
- mantener compatibilidad con el pipeline legacy.

## Estado anterior (diagnóstico)

El sistema original estaba optimizado para transcripciones:

- `episodes` + `chunks` como modelo principal,
- matching episodio↔artículo en tablas específicas,
- retrieval limitado a chunks de episodios.

No existía entidad canónica multi-fuente ni capa persistida de relaciones entre contenidos.

## Modelo canónico

## Tablas

- `content_items`
  - entidad principal del contenido
  - claves: `content_key`, `content_type`, `title`, `slug`, `url`, `source`, `language`, `status`
  - metadatos: `metadata_json`, `custom_metadata_json`
  - payload completo: `raw_text`
- `content_sections`
  - estructura editorial del documento
  - trazabilidad (`source_locator`), orden y taxonomía
- `content_chunks`
  - chunks semánticos por sección
  - embedding `vector(1536)` + metadata de chunk
- `content_relations`
  - relaciones persistidas opcionales (`related`, método, score, estado)
- `schema_migrations`
  - control de migraciones SQL idempotentes

## Compatibilidad

Se mantienen las tablas legacy:

- `episodes`
- `chunks`
- `runroom_articles`
- `episode_article_candidates`

`ingest-transcripts` sincroniza ahora a legacy + canónico (dual-write progresivo).

## Parser de case studies (Markdown)

Estrategia robusta frente a variaciones reales:

- segmentación por bloques `Case Study #N`,
- extracción flexible de labels (`Cliente`, `URL`, `URL Original`),
- fallback por `H1/H2/H3` no homogéneos,
- normalización a taxonomía canónica:
  - `description`, `challenge`, `approach`, `process`, `solution`, `results`, `impact`, `technologies`, `areas`, `quotes`, `next_steps`, `other`.

## Ingesta URL

Pipeline fase 2 sin dependencias extra:

- fetch HTML (stdlib),
- extracción de metadatos (`og:*`, `description`, JSON-LD cuando existe),
- normalización al mismo modelo canónico que Markdown.

## Chunking y embeddings

- chunking editorial por sección (sin mezclar secciones),
- trazabilidad por `source_locator`,
- embeddings compatibles con stack actual (`text-embedding-3-large`, `EMBEDDING_DIM=1536`).

## Retrieval/recommendation

`recommend-content` aplica:

- ANN sobre `content_chunks.embedding`,
- filtros por tipo/fuente/idioma,
- agregación chunk→item,
- reranking con diversidad por tipo,
- salida mixta o agrupada.

## Relaciones persistidas

`materialize-content-relations` persiste candidatos relacionados con:

- método de cálculo,
- score,
- estado,
- timestamp de cómputo.

Esto permite pasar de cálculo on-the-fly a links materializados cuando convenga.

## Decisiones clave

- No se rompe el esquema legacy.
- No se cambia la dimensión de embeddings.
- Se prioriza arquitectura extensible sobre parche específico de case studies.
- Se mantiene complejidad controlada (stdlib para scraping URL).
