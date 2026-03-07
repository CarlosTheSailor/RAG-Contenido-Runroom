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

## 5) Recomendación

```bash
python -m src.cli recommend-content \
  --text "Draft de newsletter sobre discovery, CX y growth" \
  --top-k 8
```

## 6) Re-embedding selectivo

```bash
python -m src.cli reembed-content --content-type case_study
```

## 7) Materializar relaciones

```bash
python -m src.cli materialize-content-relations \
  --content-types episode,case_study \
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
