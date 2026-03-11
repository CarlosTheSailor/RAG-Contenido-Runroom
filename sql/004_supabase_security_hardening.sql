-- Security hardening for Supabase-exposed objects in `public`.

-- Move pgvector out of `public` to avoid exposing extension objects there.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'vector'
          AND n.nspname = 'public'
    ) THEN
        CREATE SCHEMA IF NOT EXISTS extensions;
        ALTER EXTENSION vector SET SCHEMA extensions;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Pin function search_path to avoid role/session-dependent resolution.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname = 'set_content_items_updated_at'
          AND pg_get_function_identity_arguments(p.oid) = ''
    ) THEN
        ALTER FUNCTION public.set_content_items_updated_at()
        SET search_path = pg_catalog;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Enable RLS on all tables in `public` exposed by PostgREST.
ALTER TABLE IF EXISTS public.episodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.episode_article_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.runroom_articles ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.schema_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.content_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.content_sections ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.content_chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.content_relations ENABLE ROW LEVEL SECURITY;
