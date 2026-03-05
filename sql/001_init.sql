CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS episodes (
    id BIGSERIAL PRIMARY KEY,
    source_filename TEXT NOT NULL UNIQUE,
    episode_code TEXT,
    title TEXT NOT NULL,
    guest_names TEXT[] NOT NULL DEFAULT '{}',
    language TEXT NOT NULL DEFAULT 'es',
    transcript_path TEXT NOT NULL,
    runroom_article_url TEXT,
    match_status TEXT NOT NULL DEFAULT 'unmatched',
    match_confidence DOUBLE PRECISION,
    matched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS episodes_episode_code_idx ON episodes (episode_code);
CREATE INDEX IF NOT EXISTS episodes_match_status_idx ON episodes (match_status);

CREATE TABLE IF NOT EXISTS chunks (
    id BIGSERIAL PRIMARY KEY,
    episode_id BIGINT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    start_ts_sec DOUBLE PRECISION NOT NULL,
    end_ts_sec DOUBLE PRECISION NOT NULL,
    speaker TEXT,
    text TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding VECTOR(1536) NOT NULL,
    token_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (episode_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS chunks_episode_id_idx ON chunks (episode_id);
CREATE INDEX IF NOT EXISTS chunks_start_ts_idx ON chunks (start_ts_sec);
CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS runroom_articles (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    lang TEXT NOT NULL DEFAULT 'es',
    episode_code_hint TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS runroom_articles_slug_idx ON runroom_articles (slug);
CREATE INDEX IF NOT EXISTS runroom_articles_lang_idx ON runroom_articles (lang);
CREATE INDEX IF NOT EXISTS runroom_articles_episode_code_hint_idx ON runroom_articles (episode_code_hint);

CREATE TABLE IF NOT EXISTS episode_article_candidates (
    id BIGSERIAL PRIMARY KEY,
    episode_id BIGINT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    article_id BIGINT NOT NULL REFERENCES runroom_articles(id) ON DELETE CASCADE,
    score DOUBLE PRECISION NOT NULL,
    method TEXT NOT NULL CHECK (method IN ('code_exact', 'name_slug', 'semantic')),
    is_selected BOOLEAN NOT NULL DEFAULT FALSE,
    review_required BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (episode_id, article_id, method)
);

CREATE INDEX IF NOT EXISTS episode_article_candidates_episode_idx ON episode_article_candidates (episode_id);
CREATE INDEX IF NOT EXISTS episode_article_candidates_article_idx ON episode_article_candidates (article_id);
CREATE INDEX IF NOT EXISTS episode_article_candidates_score_idx ON episode_article_candidates (score DESC);
