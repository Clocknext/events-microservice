-- Schema for the signal log's ClickHouse.
--
-- Run by the container's entrypoint on FIRST START ONLY — ClickHouse ignores
-- /docker-entrypoint-initdb.d once the data directory exists. To re-apply after
-- editing, drop the volume: `docker compose down -v`. On a live cluster this is
-- a migration instead — see docker/clickhouse/migrations/ and docs/PRODUCTION.md.
--
-- ONE TABLE. ClickHouse no longer ingests from Kafka itself: the `ENGINE = Kafka`
-- table and its materialized view are GONE, replaced by src/workers/consume/.
--
-- Why they had to go: a materialized view cannot make an HTTP request. A signal
-- reached the archive with no idea whose it was or whether it was acceptable, and
-- both questions were answered minutes later, inside settle, on the money path.
-- The consumer pulls the topic itself, asks /api/internal/resolve about each
-- signal, and writes the VERDICT alongside the row.
--
-- What that costs, said plainly: the archive is no longer a pure function of the
-- topic. Replaying Kafka re-runs the resolve calls, and a key that has since
-- expired answers differently. `signal_log` still has exactly ONE writer — the
-- consumer — but "rebuildable by replay" is weaker than it was.

CREATE DATABASE IF NOT EXISTS signals;

-- ─────────────────────────────────────────────────────────────────────────────
-- The durable signal log: one row per signal, the body as it arrived, and the
-- verdict the consumer resolved before archiving it.
--
-- ReplacingMergeTree(version) keyed on the ORDER BY tuple makes Kafka's
-- at-least-once delivery safe: the consumer commits offsets only AFTER the insert
-- lands, so a crash in between redelivers the message, and the re-inserted row
-- collapses on merge. Because merges are not immediate, every read that matters
-- uses `LIMIT 1 BY signal_id` (cheaper than `FINAL`) or `count(DISTINCT signal_id)`.
--
-- THE `version` ARGUMENT IS NOT DECORATION, and it is here before anything uses
-- it on purpose. It is what lets a row be UPDATED — re-inserted at a higher
-- version, newest wins. The daily reconciliation cron will do exactly that when
-- it copies settled state back out of Postgres. A table's ENGINE cannot be
-- ALTERed, so adding the argument later means building a second table, copying
-- every row and exchanging them; adding it now costs one UInt32 column.
CREATE TABLE IF NOT EXISTS signals.signal_log
(
  signal_id       String,                         -- ULID from the edge (envelope)
  received_at     DateTime64(3),                  -- edge arrival time; BILLING CUTS ON THIS
  api_key_hash    String,                         -- SHA-256 of the caller's key. NEVER the key.

  -- A LIFTED COPY of the payload's `customerId`, so the archive can be filtered
  -- without JSON extraction. A copy, not a move: `payload` below must stay
  -- byte-for-byte what the caller sent, or it stops being a record of the request
  -- and becomes a summary of it.
  customer_id     String,

  -- ── the consumer's verdict ─────────────────────────────────────────────────
  -- OURS, not the caller's — resolved against /api/internal/resolve. '' when the
  -- key itself never resolved, which is the one case with no tenant to name;
  -- settle's SignalStatus.organizationId is nullable for exactly that.
  organization_id LowCardinality(String) DEFAULT '',
  -- An INSTRUCTION to settle, not a description of it:
  --   PROCESSING  authenticated and acceptable — settle prices it.
  --   PENDING     rejected (bad key / bad body / unknown customer). STILL
  --               dispatched, so settle records a terminal row and the failure
  --               stays visible in the Signals UI instead of living only here.
  --   SUCCESS     already settled. Written ONLY by the daily reconciliation cron
  --               (that side calls it PROCESSED — the cron owns the mapping).
  status          LowCardinality(String) DEFAULT 'PROCESSING',
  -- Machine-readable and low cardinality, so "how many signals failed with
  -- API_KEY_REJECTED this hour, and for which orgs" is a cheap GROUP BY.
  -- `error_message` is the human sentence payments gave and aggregates over
  -- nothing.
  error_code      LowCardinality(String) DEFAULT '',
  error_message   String DEFAULT '' CODEC(ZSTD(3)),

  payload         String CODEC(ZSTD(3)),          -- the full body as sent, verbatim
  ingested_at     DateTime64(3) DEFAULT now64(3), -- when the CONSUMER wrote it (see below)

  -- Bumped only by a process that REWRITES a row. The consumer always writes 1.
  -- A rewrite MUST carry the original `ingested_at` forward rather than taking
  -- the default: a fresh stamp would drop a day-old row back inside the
  -- dispatcher's 3-minute window and re-present it to the money path.
  version         UInt32 DEFAULT 1,

  -- THE DISPATCH SELECTION INDEX. The dispatcher selects on `ingested_at`, never
  -- on `received_at`, because `received_at` is the CUSTOMER's time stamped by N
  -- edge instances with N clocks: a row can land here minutes after it — the
  -- consumer batches, and each signal costs an HTTP round trip — and a window
  -- over `received_at` would miss it permanently, with no watermark to come back
  -- for it. `ingested_at` is one clock on one server and means "arrived in the
  -- archive", which is what "the latest data" has to mean.
  --
  -- But it is neither the ORDER BY key nor the partition key, so without this a
  -- window over it is a full scan across every partition, reading the fat
  -- `payload` column. Inserts arrive in roughly `ingested_at` order, so minmax
  -- granule ranges stay tight and old partitions prune out.
  INDEX idx_ingested_at ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, signal_id);
