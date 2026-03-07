CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS content_items (
    id BIGSERIAL PRIMARY KEY,
    content_key TEXT NOT NULL UNIQUE,
    content_type TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT,
    url TEXT,
    source TEXT NOT NULL,
    language TEXT NOT NULL DEFAULT 'es',
    status TEXT NOT NULL DEFAULT 'active',
    published_at TIMESTAMPTZ,
    extracted_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    custom_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_text TEXT NOT NULL DEFAULT '',
    legacy_episode_id BIGINT REFERENCES episodes(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (content_type IN ('episode', 'case_study', 'article', 'event', 'training', 'other'))
);

CREATE INDEX IF NOT EXISTS content_items_content_type_idx ON content_items (content_type);
CREATE INDEX IF NOT EXISTS content_items_source_idx ON content_items (source);
CREATE INDEX IF NOT EXISTS content_items_language_idx ON content_items (language);
CREATE INDEX IF NOT EXISTS content_items_status_idx ON content_items (status);
CREATE INDEX IF NOT EXISTS content_items_slug_idx ON content_items (slug);
CREATE INDEX IF NOT EXISTS content_items_url_idx ON content_items (url);

CREATE TABLE IF NOT EXISTS content_sections (
    id BIGSERIAL PRIMARY KEY,
    content_item_id BIGINT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    section_order INTEGER NOT NULL,
    section_key TEXT NOT NULL,
    section_title TEXT,
    text TEXT NOT NULL,
    token_count INTEGER NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_locator JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (content_item_id, section_order)
);

CREATE INDEX IF NOT EXISTS content_sections_item_id_idx ON content_sections (content_item_id);
CREATE INDEX IF NOT EXISTS content_sections_section_key_idx ON content_sections (section_key);

CREATE TABLE IF NOT EXISTS content_chunks (
    id BIGSERIAL PRIMARY KEY,
    content_item_id BIGINT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    section_id BIGINT REFERENCES content_sections(id) ON DELETE SET NULL,
    chunk_order INTEGER NOT NULL,
    section_key TEXT NOT NULL,
    section_title TEXT,
    text TEXT NOT NULL,
    token_count INTEGER NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_locator JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding VECTOR(1536) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (content_item_id, chunk_order)
);

CREATE INDEX IF NOT EXISTS content_chunks_item_id_idx ON content_chunks (content_item_id);
CREATE INDEX IF NOT EXISTS content_chunks_section_key_idx ON content_chunks (section_key);
CREATE INDEX IF NOT EXISTS content_chunks_embedding_idx ON content_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS content_relations (
    id BIGSERIAL PRIMARY KEY,
    from_content_item_id BIGINT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    to_content_item_id BIGINT NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL DEFAULT 'related',
    method TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'suggested',
    rationale TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_content_item_id <> to_content_item_id),
    UNIQUE (from_content_item_id, to_content_item_id, relation_type, method)
);

CREATE INDEX IF NOT EXISTS content_relations_from_idx ON content_relations (from_content_item_id);
CREATE INDEX IF NOT EXISTS content_relations_to_idx ON content_relations (to_content_item_id);
CREATE INDEX IF NOT EXISTS content_relations_score_idx ON content_relations (score DESC);

CREATE OR REPLACE FUNCTION set_content_items_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_content_items_updated_at ON content_items;
CREATE TRIGGER trg_content_items_updated_at
BEFORE UPDATE ON content_items
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();
