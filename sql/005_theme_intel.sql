CREATE TABLE IF NOT EXISTS theme_runs (
    id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'partial_failed', 'failed')),
    source_type TEXT NOT NULL DEFAULT 'gmail',
    source_account TEXT NOT NULL,
    gmail_query TEXT NOT NULL,
    origin_category TEXT NOT NULL,
    mark_as_read BOOLEAN NOT NULL DEFAULT false,
    limit_messages INTEGER NOT NULL DEFAULT 20 CHECK (limit_messages >= 1 AND limit_messages <= 200),
    triggered_by_email TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_runs_status_idx ON theme_runs (status);
CREATE INDEX IF NOT EXISTS theme_runs_created_at_idx ON theme_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS theme_categories (
    key TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'origin',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'deprecated')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_documents (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES theme_runs(id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    source_account TEXT NOT NULL,
    source_external_id TEXT NOT NULL,
    source_thread_id TEXT,
    subject TEXT,
    sender TEXT,
    received_at TIMESTAMPTZ,
    labels_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    links_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_text TEXT NOT NULL DEFAULT '',
    cleaned_text TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_type, source_account, source_external_id)
);

CREATE INDEX IF NOT EXISTS source_documents_run_id_idx ON source_documents (run_id);
CREATE INDEX IF NOT EXISTS source_documents_received_at_idx ON source_documents (received_at DESC);

CREATE TABLE IF NOT EXISTS theme_topics (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES theme_runs(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    context_text TEXT NOT NULL DEFAULT '',
    canonical_text TEXT NOT NULL,
    primary_category_key TEXT NOT NULL REFERENCES theme_categories(key),
    status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'in_progress', 'used', 'discarded')),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    origin_source_type TEXT NOT NULL,
    origin_source_account TEXT NOT NULL,
    origin_query TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    times_seen INTEGER NOT NULL DEFAULT 1,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_topics_category_idx ON theme_topics (primary_category_key);
CREATE INDEX IF NOT EXISTS theme_topics_status_idx ON theme_topics (status);
CREATE INDEX IF NOT EXISTS theme_topics_last_seen_idx ON theme_topics (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS theme_topic_tags (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL REFERENCES theme_topics(id) ON DELETE CASCADE,
    tag_key TEXT NOT NULL,
    tag_label TEXT NOT NULL,
    provenance TEXT NOT NULL CHECK (provenance IN ('origin', 'ai', 'manual')),
    confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, tag_key)
);

CREATE INDEX IF NOT EXISTS theme_topic_tags_key_idx ON theme_topic_tags (tag_key);

CREATE TABLE IF NOT EXISTS theme_evidences (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL REFERENCES theme_topics(id) ON DELETE CASCADE,
    source_document_id BIGINT REFERENCES source_documents(id) ON DELETE SET NULL,
    dato TEXT NOT NULL DEFAULT '',
    fuente TEXT NOT NULL DEFAULT '',
    texto_fuente_breve TEXT NOT NULL DEFAULT '',
    url_referencia TEXT NOT NULL DEFAULT '',
    newsletter_origen TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_evidences_topic_idx ON theme_evidences (topic_id);

CREATE TABLE IF NOT EXISTS theme_related_content (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL REFERENCES theme_topics(id) ON DELETE CASCADE,
    content_item_id BIGINT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    relation_rank INTEGER NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    rationale TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, content_item_id)
);

CREATE INDEX IF NOT EXISTS theme_related_content_topic_idx ON theme_related_content (topic_id, relation_rank);
CREATE INDEX IF NOT EXISTS theme_related_content_item_idx ON theme_related_content (content_item_id);

CREATE TABLE IF NOT EXISTS theme_topic_embeddings (
    topic_id BIGINT PRIMARY KEY REFERENCES theme_topics(id) ON DELETE CASCADE,
    embedding VECTOR(1536) NOT NULL,
    model TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_topic_embeddings_idx ON theme_topic_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS theme_topic_usage (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL REFERENCES theme_topics(id) ON DELETE CASCADE,
    client_name TEXT NOT NULL,
    artifact_id TEXT,
    used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS theme_topic_usage_topic_idx ON theme_topic_usage (topic_id, used_at DESC);
CREATE INDEX IF NOT EXISTS theme_topic_usage_client_idx ON theme_topic_usage (client_name, used_at DESC);

DROP TRIGGER IF EXISTS trg_theme_runs_updated_at ON theme_runs;
CREATE TRIGGER trg_theme_runs_updated_at
BEFORE UPDATE ON theme_runs
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

DROP TRIGGER IF EXISTS trg_theme_categories_updated_at ON theme_categories;
CREATE TRIGGER trg_theme_categories_updated_at
BEFORE UPDATE ON theme_categories
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

DROP TRIGGER IF EXISTS trg_theme_topics_updated_at ON theme_topics;
CREATE TRIGGER trg_theme_topics_updated_at
BEFORE UPDATE ON theme_topics
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();
