CREATE TABLE IF NOT EXISTS theme_topic_source_documents (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL REFERENCES theme_topics(id) ON DELETE CASCADE,
    source_document_id BIGINT NOT NULL REFERENCES source_documents(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL DEFAULT 'run_scope' CHECK (link_type IN ('run_scope', 'primary', 'evidence')),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, source_document_id)
);

CREATE INDEX IF NOT EXISTS theme_topic_source_documents_topic_idx
ON theme_topic_source_documents (topic_id, link_type, id DESC);

CREATE INDEX IF NOT EXISTS theme_topic_source_documents_source_idx
ON theme_topic_source_documents (source_document_id, id DESC);

DROP TRIGGER IF EXISTS trg_theme_topic_source_documents_updated_at ON theme_topic_source_documents;
CREATE TRIGGER trg_theme_topic_source_documents_updated_at
BEFORE UPDATE ON theme_topic_source_documents
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

INSERT INTO theme_topic_source_documents (topic_id, source_document_id, link_type, metadata_json)
SELECT DISTINCT
    te.topic_id,
    te.source_document_id,
    'evidence',
    jsonb_build_object('backfill', true, 'source', 'theme_evidences')
FROM theme_evidences te
WHERE te.source_document_id IS NOT NULL
ON CONFLICT (topic_id, source_document_id)
DO UPDATE SET
    link_type = 'evidence',
    metadata_json = theme_topic_source_documents.metadata_json || EXCLUDED.metadata_json,
    updated_at = now();

INSERT INTO theme_topic_source_documents (topic_id, source_document_id, link_type, metadata_json)
SELECT DISTINCT
    tt.id,
    sd.id,
    'run_scope',
    jsonb_build_object('backfill', true, 'source', 'topic_run')
FROM theme_topics tt
JOIN source_documents sd ON sd.run_id = tt.run_id
ON CONFLICT (topic_id, source_document_id)
DO NOTHING;

WITH ranked AS (
    SELECT
        tsd.id,
        tsd.topic_id,
        row_number() OVER (
            PARTITION BY tsd.topic_id
            ORDER BY sd.received_at DESC NULLS LAST, sd.id DESC
        ) AS rn
    FROM theme_topic_source_documents tsd
    JOIN source_documents sd ON sd.id = tsd.source_document_id
    WHERE tsd.link_type = 'run_scope'
)
UPDATE theme_topic_source_documents target
SET
    link_type = 'primary',
    metadata_json = target.metadata_json || jsonb_build_object('backfill_primary', true),
    updated_at = now()
FROM ranked
WHERE target.id = ranked.id
  AND ranked.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM theme_topic_source_documents existing
      WHERE existing.topic_id = target.topic_id
        AND existing.link_type IN ('primary', 'evidence')
  );
