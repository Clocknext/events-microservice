-- 002 · a minmax skip index on ingested_at
--
-- Companion to `docker/clickhouse/init/01-schema.sql`, which runs on FIRST START
-- ONLY. The init script describes the schema for a NEW database; this carries the
-- same change to one that already has rows, and is what production runs.
--
-- WHAT CHANGES, AND WHY
--
-- Dispatch is now a 1-minute cron over a time window, with no watermark and no
-- cursor. The window is over `ingested_at`, NOT `received_at`:
--
--   `received_at` is the CUSTOMER's time, stamped by N edge instances with N
--   clocks. A row can land in the archive minutes after it — the Kafka engine
--   flushes in batches, and a broker backlog or a ClickHouse restart delays
--   ingestion further. A window over `received_at` misses those rows PERMANENTLY,
--   because with no watermark nothing ever comes back for them.
--
--   `ingested_at` is DEFAULT now64(3), stamped by this server when the
--   materialized view's insert lands. One clock, one server, and it means
--   "arrived in the archive" — which is what "the latest data" has to mean.
--
-- `received_at` is still what travels to /api/internal/settle: billing windows
-- cut on it. Two clocks, two jobs.
--
-- The problem this index solves: `ingested_at` is neither the ORDER BY key
-- (received_at, signal_id) nor the partition key (toYYYYMM(received_at)), so a
-- window over it is a full scan across every partition, reading the fat `payload`
-- column. At 10k signals/min that is ~14M rows/day and it degrades fast.
--
-- minmax works here because inserts arrive in roughly `ingested_at` order, so
-- each granule's min/max range is tight and old parts prune out on the range
-- check alone.
--
-- Idempotent, and safe to run while the pipeline is live.

-- 1. The index. Cheap: one min/max pair per granule, no data rewritten.
ALTER TABLE signals.signal_log
  ADD INDEX IF NOT EXISTS idx_ingested_at ingested_at TYPE minmax GRANULARITY 1;

-- 2. ADD INDEX only applies to parts written AFTER it. Existing parts have no
--    index entries and are scanned in full until this backfills them.
--
--    This one is NOT instant — it rewrites index files for every existing part —
--    and it is asynchronous: it returns immediately and the work proceeds in the
--    background. Watch it with
--
--      SELECT * FROM system.mutations
--       WHERE table = 'signal_log' AND NOT is_done;
--
--    Ingestion is unaffected; the materialized view keeps writing throughout.
ALTER TABLE signals.signal_log
  MATERIALIZE INDEX idx_ingested_at;
