-- Enable RLS on Theme Intel and LinkedIn draft tables added after the initial
-- Supabase security hardening migration. We intentionally do not create public
-- policies here, so PostgREST access remains denied unless explicitly opened.

ALTER TABLE IF EXISTS public.theme_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_categories ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.source_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_topics ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_topic_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_evidences ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_related_content ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_topic_embeddings ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_topic_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_topic_source_documents ENABLE ROW LEVEL SECURITY;

ALTER TABLE IF EXISTS public.theme_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_schedule_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_schedule_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.theme_schedule_execution_items ENABLE ROW LEVEL SECURITY;

ALTER TABLE IF EXISTS public.linkedin_draft_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.linkedin_draft_run_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.linkedin_draft_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.linkedin_draft_schedule_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.linkedin_draft_schedule_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.linkedin_draft_schedule_execution_items ENABLE ROW LEVEL SECURITY;
