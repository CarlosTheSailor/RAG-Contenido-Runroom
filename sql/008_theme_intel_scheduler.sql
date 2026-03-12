CREATE TABLE IF NOT EXISTS theme_schedules (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    every_n_days INTEGER NOT NULL CHECK (every_n_days >= 1 AND every_n_days <= 365),
    run_time_local TIME NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Europe/Madrid',
    next_run_at_utc TIMESTAMPTZ,
    last_run_at_utc TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_schedules_enabled_next_idx
ON theme_schedules (enabled, next_run_at_utc);

CREATE TABLE IF NOT EXISTS theme_schedule_configs (
    id BIGSERIAL PRIMARY KEY,
    schedule_id BIGINT NOT NULL REFERENCES theme_schedules(id) ON DELETE CASCADE,
    execution_order INTEGER NOT NULL DEFAULT 1,
    gmail_query TEXT NOT NULL,
    origin_category TEXT NOT NULL,
    mark_as_read BOOLEAN NOT NULL DEFAULT false,
    limit_messages INTEGER NOT NULL DEFAULT 100 CHECK (limit_messages >= 1 AND limit_messages <= 200),
    enabled BOOLEAN NOT NULL DEFAULT true,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_schedule_configs_schedule_order_idx
ON theme_schedule_configs (schedule_id, enabled, execution_order, id);

CREATE TABLE IF NOT EXISTS theme_schedule_executions (
    id BIGSERIAL PRIMARY KEY,
    schedule_id BIGINT NOT NULL REFERENCES theme_schedules(id) ON DELETE CASCADE,
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('cron_tick', 'manual_run_now')),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'partial_failed', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_schedule_executions_schedule_started_idx
ON theme_schedule_executions (schedule_id, started_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS theme_schedule_execution_items (
    id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES theme_schedule_executions(id) ON DELETE CASCADE,
    schedule_config_id BIGINT REFERENCES theme_schedule_configs(id) ON DELETE SET NULL,
    execution_order INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    theme_run_id BIGINT REFERENCES theme_runs(id) ON DELETE SET NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS theme_schedule_execution_items_execution_order_idx
ON theme_schedule_execution_items (execution_id, execution_order, id);

CREATE INDEX IF NOT EXISTS theme_schedule_execution_items_theme_run_idx
ON theme_schedule_execution_items (theme_run_id);

DROP TRIGGER IF EXISTS trg_theme_schedules_updated_at ON theme_schedules;
CREATE TRIGGER trg_theme_schedules_updated_at
BEFORE UPDATE ON theme_schedules
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

DROP TRIGGER IF EXISTS trg_theme_schedule_configs_updated_at ON theme_schedule_configs;
CREATE TRIGGER trg_theme_schedule_configs_updated_at
BEFORE UPDATE ON theme_schedule_configs
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();

DROP TRIGGER IF EXISTS trg_theme_schedule_executions_updated_at ON theme_schedule_executions;
CREATE TRIGGER trg_theme_schedule_executions_updated_at
BEFORE UPDATE ON theme_schedule_executions
FOR EACH ROW
EXECUTE FUNCTION set_content_items_updated_at();
