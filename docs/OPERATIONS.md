# Operación y runbook

## Requisitos

- Python 3.9+
- `.venv` activo
- `SUPABASE_DB_URL` configurado
- `OPENAI_API_KEY` opcional (si no, usar `--offline-mode`)

## Setup rápido

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
set -a; source .env; set +a
```

## Flujo estándar

## 1) Migración

```bash
python -m src.cli migrate-schema
```

## 2) Backfill legacy -> canónico

```bash
python -m src.cli backfill-canonical-content
```

## 3) Ingesta Markdown case studies

```bash
python -m src.cli ingest-case-studies-markdown \
  --input /Users/carlos/Downloads/Runroom_Case_Studies_Completo.md
```

## 4) Ingesta URL puntual

```bash
python -m src.cli ingest-case-study-url \
  --url https://www.runroom.com/cases/bayer-design-system-coherencia-global-flexibilidad-local
```

## 5) Ingesta masiva Runroom LABs

```bash
python -m src.cli ingest-runroom-labs \
  --index-url https://info.runroom.com/runroom-lab-todas-las-ediciones
```

Notas:

- selecciona 1 URL resumen por acordeón
- excluye URLs de vídeo/social
- persiste como `content_type=runroom_lab`

## 6) Recomendación

```bash
python -m src.cli recommend-content \
  --text "Draft de newsletter sobre discovery, CX y growth" \
  --top-k 8
```

## 6b) API HTTP (query + recommendation)

```bash
export API_KEY=change-me
python -m src.interfaces.http
```

Con `curl`:

```bash
curl -X POST http://localhost:8000/v1/query-similar \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"text":"customer centric","top_k":5}'
```

## 6c) Ingesta manual de case study (Web autenticada)

Con OAuth de Google activo y sesión iniciada:

- UI: `GET /app/nuevo-case-study`
- Endpoint interno: `POST /app/api/case-studies/ingest-url`

Payload mínimo:

```json
{"url":"https://www.runroom.com/cases/bayer-design-system-coherencia-global-flexibilidad-local"}
```

Validaciones de URL:

- esquema `http/https`
- host `runroom.com` o `www.runroom.com`
- path iniciado por `/cases/`

La respuesta devuelve un `summary` con:

- `documents_total`
- `items_upserted`
- `sections_written`
- `chunks_written`
- `dry_run`

## 6d) Ingesta manual de episodio Realworld (Web autenticada)

Con OAuth de Google activo y sesion iniciada:

- UI: `GET /app/nuevo-episodio-realworld`
- Endpoint interno: `POST /app/api/episodes/ingest` (`multipart/form-data`)

Campos:

- `transcript_file` (obligatorio, extension `.txt`, no vacio)
- `runroom_url` (obligatorio)

Validaciones URL:

- esquema `http/https`
- host `runroom.com` o `www.runroom.com`
- path iniciado por `/realworld/` o `/en/realworld/`

Comportamiento:

- guarda el archivo en `transcripciones/` con su nombre original
- bloquea duplicados por `source_filename` con `409`
- extrae titulo del primer `<h1>` de Runroom (si falta, falla con `422`)
- ingesta legacy (`episodes/chunks`) + sync canónico (`content_items/content_chunks`)

## 7) Re-embedding selectivo

```bash
python -m src.cli reembed-content --content-type case_study
```

## 8) Materializar relaciones

```bash
python -m src.cli materialize-content-relations \
  --content-types episode,case_study,runroom_lab \
  --top-k-per-item 5 \
  --min-score 0.58
```

## Checks de validación

```sql
select content_type, count(*) from content_items group by 1 order by 1;
select count(*) as sections from content_sections;
select count(*) as chunks from content_chunks;
```

## Troubleshooting

## `ValueError: SUPABASE_DB_URL is required`

Cargar `.env` en la sesión:

```bash
set -a; source .env; set +a
```

## `ModuleNotFoundError: No module named 'psycopg'`

Instalar dependencias dentro de la `.venv` activa:

```bash
python -m pip install -r requirements.txt
```

## Consulta lenta o resultados pobres

- Usar `OPENAI_API_KEY` para embeddings reales.
- Aumentar ventana de recuperación (`--fetch-k`).
- Para mezcla por tipo, subir `--top-k` y/o ejecutar queries separadas por tipo.

## Comandos legacy (siguen operativos)

```bash
python -m src.cli ingest-transcripts
python -m src.cli sync-runroom-sitemap
python -m src.cli match-episodes
python -m src.cli query-similar --text "..."
```

## Deploy rápido en Docker/Coolify

```bash
docker build -t runroom-rag .
docker run --rm -p 8000:8000 --env-file .env runroom-rag
```
