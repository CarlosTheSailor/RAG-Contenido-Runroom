-- Drop the ivfflat indexes on the embedding columns and fall back to exact search.
--
-- Each index was built with lists = 100 and queried with the default ivfflat.probes = 1,
-- so a search only visited about 1% of the vectors. Measured on 2026-11-19 against the
-- exact nearest neighbours, the index returned roughly a third of them:
--   content_chunks          6,675 rows   53/160 of the true top-20   ~490 ms (exact: ~100 ms)
--   chunks                  5,449 rows   42/160                      ~370 ms (exact:  ~90 ms)
--   theme_topic_embeddings  7,973 rows   82/160                      ~790 ms (exact: ~140 ms)
-- Newly ingested content was hit hardest, since the lists were fixed when the index was built.
--
-- At this size a sequential scan is both exact and faster. If a table grows past roughly
-- 100k rows and exact search becomes slow, add an HNSW index instead of ivfflat:
--   CREATE INDEX ... ON <table> USING hnsw (embedding vector_cosine_ops);

-- DROP INDEX needs a brief exclusive lock on the table: fail fast instead of queueing behind a long query.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS public.content_chunks_embedding_idx;
DROP INDEX IF EXISTS public.chunks_embedding_idx;
DROP INDEX IF EXISTS public.theme_topic_embeddings_idx;
