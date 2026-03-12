CREATE TABLE IF NOT EXISTS linkedin_draft_runs (
    id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','partial_failed','failed')),
    origin_category TEXT NOT NULL,
    slack_channel TEXT NOT NULL,
    buyer_persona_objetivo TEXT NOT NULL,
    offline_mode BOOLEAN NOT NULL DEFAULT false,
    client_name TEXT NOT NULL,
    target_count INTEGER NOT NULL DEFAULT 5,
    topics_fetch_limit INTEGER NOT NULL DEFAULT 40,
    related_top_k INTEGER NOT NULL DEFAULT 10,
    related_counts_by_type_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    triggered_by_email TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS linkedin_draft_runs_status_idx
ON linkedin_draft_runs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS linkedin_draft_runs_category_idx
ON linkedin_draft_runs (origin_category, created_at DESC);

CREATE TABLE IF NOT EXISTS linkedin_draft_run_items (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES linkedin_draft_runs(id) ON DELETE CASCADE,
    topic_id BIGINT REFERENCES theme_topics(id) ON DELETE SET NULL,
    item_index INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed')),
    title TEXT NOT NULL DEFAULT '',
    draft_stage1_text TEXT NOT NULL DEFAULT '',
    draft_final_text TEXT NOT NULL DEFAULT '',
    topic_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    related_candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    related_selected_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    references_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    draft_publish_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    slack_publish_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, item_index)
);

CREATE INDEX IF NOT EXISTS linkedin_draft_run_items_run_idx
ON linkedin_draft_run_items (run_id, item_index);

CREATE INDEX IF NOT EXISTS linkedin_draft_run_items_topic_idx
ON linkedin_draft_run_items (topic_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_linkedin_draft_runs_updated_at ON linkedin_draft_runs;
CREATE TRIGGER trg_linkedin_draft_runs_updated_at
BEFORE UPDATE ON linkedin_draft_runs
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

DROP TRIGGER IF EXISTS trg_linkedin_draft_run_items_updated_at ON linkedin_draft_run_items;
CREATE TRIGGER trg_linkedin_draft_run_items_updated_at
BEFORE UPDATE ON linkedin_draft_run_items
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();
