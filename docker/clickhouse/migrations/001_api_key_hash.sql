-- 001 · api_key_hash on signal_log
--
-- WHY A MIGRATIONS DIRECTORY AT ALL
--
-- `docker/clickhouse/init/01-schema.sql` runs on FIRST START ONLY — ClickHouse
-- skips /docker-entrypoint-initdb.d once the data directory exists. So the init
-- script describes the schema as it should be for a NEW database, and this
-- directory carries the same changes for one that already has rows. Both must be
-- edited together, and this is also the script to run against production.
--
-- WHAT CHANGES
--
-- The edge now stamps `apiKeyHash` on every message envelope — the SHA-256 digest
-- of the caller's `cnk_…` key, never the key itself. `/api/internal/settle`
-- resolves the signal's organisation from it (`ApiKey.hashedKey` stores the very
-- same digest), which is the only reason a signal in the archive can be
-- attributed to anyone at all.
--
-- Idempotent, and safe to run while the pipeline is live.

-- 1. The column. Non-Nullable String, so the 16k rows written before the edge
--    started sending a digest read as '' rather than as NULL — which is what
--    "we never learned whose key this was" should look like, and a JSON null into
--    a non-Nullable String would be an insert error anyway.
ALTER TABLE signals.signal_log
  ADD COLUMN IF NOT EXISTS api_key_hash String AFTER received_at;

-- 2. The materialized view has to be recreated: its SELECT is fixed at creation
--    and there is no ALTER for it.
--
--    Dropping it PAUSES ingestion rather than losing anything — an ENGINE = Kafka
--    table only consumes while something reads it, so with no view attached the
--    messages simply stay on the topic at the last committed offset. Recreating
--    the view resumes from exactly there. The gap must be shorter than the
--    topic's retention, which at 7 days it comfortably is.
DROP VIEW IF EXISTS signals.signal_log_mv;

CREATE MATERIALIZED VIEW signals.signal_log_mv TO signals.signal_log AS
SELECT
  JSONExtractString(raw, 'signalId')                              AS signal_id,
  parseDateTime64BestEffort(JSONExtractString(raw, 'receivedAt'))  AS received_at,
  JSONExtractString(raw, 'apiKeyHash')                            AS api_key_hash,
  JSONExtractRaw(raw, 'body')                                     AS payload
FROM signals.kafka_signals;
