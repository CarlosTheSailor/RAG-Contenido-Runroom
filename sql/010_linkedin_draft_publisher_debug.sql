ALTER TABLE IF EXISTS linkedin_draft_run_items
ADD COLUMN IF NOT EXISTS debug_json JSONB NOT NULL DEFAULT '{}'::jsonb;
