-- 004 — the consumer's verdict columns, and ReplacingMergeTree(version).
--
-- For a database that ALREADY HAS ROWS. `init/01-schema.sql` describes the same
-- shape for a new one; both must be edited together.
--
-- This is a rebuild, not an ALTER, for one reason: `version` is an ENGINE
-- ARGUMENT, and `ALTER TABLE … MODIFY ENGINE` does not exist. The five new
-- columns could have been added in place; the engine change is what forces a new
-- table, a copy, and an EXCHANGE.
--
-- Run it with the consumer STOPPED. The DROP VIEW at the head is what makes the
-- copy safe — it stops the old ingestion path, and Kafka retains everything
-- meanwhile. See docs/AWS-SETUP.md Step 12 for the full cutover, including the
-- offset seeding that must happen before the new consumer starts.

-- The materialized view was the old writer. It holds the Kafka consumer, so
-- dropping it is also what releases the `clickhouse-signal-log` group.
DROP VIEW IF EXISTS signals.signal_log_mv;

CREATE TABLE IF NOT EXISTS signals.signal_log_next
(
  signal_id       String,
  received_at     DateTime64(3),
  api_key_hash    String,
  customer_id     String,
  organization_id LowCardinality(String) DEFAULT '',
  status          LowCardinality(String) DEFAULT 'PROCESSING',
  error_code      LowCardinality(String) DEFAULT '',
  error_message   String DEFAULT '' CODEC(ZSTD(3)),
  payload         String CODEC(ZSTD(3)),
  ingested_at     DateTime64(3) DEFAULT now64(3),
  version         UInt32 DEFAULT 1,
  INDEX idx_ingested_at ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, signal_id);

-- Pre-existing rows predate the resolver, so they carry no verdict. They are
-- marked PROCESSING with an empty `organization_id`, which is SAFE rather than a
-- guess: the dispatcher still forwards `apiKeyHash`, and settle falls back to
-- resolving the organisation from it whenever `organizationId` is empty.
--
-- `ingested_at` is copied, never re-defaulted. Taking `now64(3)` here would drop
-- every historical row into the dispatcher's next window at once.
INSERT INTO signals.signal_log_next
  (signal_id, received_at, api_key_hash, customer_id, organization_id,
   status, error_code, error_message, payload, ingested_at, version)
SELECT
  signal_id,
  received_at,
  api_key_hash,
  JSONExtractString(payload, 'customerId'),
  '',
  'PROCESSING',
  '',
  '',
  payload,
  ingested_at,
  1
FROM signals.signal_log;

-- Atomic swap. Requires the Atomic database engine, which has been the default
-- since ClickHouse 20.10 and is what ClickHouse Cloud uses.
EXCHANGE TABLES signals.signal_log AND signals.signal_log_next;

DROP TABLE IF EXISTS signals.signal_log_next;
