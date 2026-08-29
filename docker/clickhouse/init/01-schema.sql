-- Schema for the signal log's ClickHouse.
--
-- Run by the container's entrypoint on FIRST START ONLY — ClickHouse ignores
-- /docker-entrypoint-initdb.d once the data directory exists. To re-apply after
-- editing, drop the volume: `docker compose down -v`. On a live cluster this is
-- an ALTER instead — see docs/PRODUCTION.md.
--
-- ClickHouse ingests from Kafka ITSELF — there is no consumer/worker process.
-- Three objects:
--   kafka_signals   an ENGINE = Kafka stream over the `signals` topic
--   signal_log      the durable log (ReplacingMergeTree)
--   signal_log_mv   a materialized view moving messages from the stream to the log

CREATE DATABASE IF NOT EXISTS signals;

-- ─────────────────────────────────────────────────────────────────────────────
-- The durable signal log: one row per signal, the body as it arrived.
--
-- ReplacingMergeTree keyed on the ORDER BY tuple makes Kafka's at-least-once
-- delivery safe: the Kafka engine can redeliver a message (e.g. on a ClickHouse
-- restart before offsets commit), and the re-inserted row — identical values —
-- collapses on merge. This is billing data, so money reads use `FINAL` or
-- `count(DISTINCT signal_id)` to never count a not-yet-merged duplicate twice.
CREATE TABLE IF NOT EXISTS signals.signal_log
(
  signal_id    String,                         -- ULID from the edge; unique per request (envelope)
  received_at  DateTime64(3),                  -- edge arrival time (envelope)
  api_key_hash String,                         -- SHA-256 of the caller's key (envelope), NEVER the key
  payload      String CODEC(ZSTD(3)),          -- the full body as sent, serialized verbatim
  ingested_at  DateTime64(3) DEFAULT now64(3), -- when ClickHouse wrote it (see the index below)

  -- THE DISPATCH SELECTION INDEX. The cron selects on `ingested_at`, never on
  -- `received_at`, because `received_at` is the CUSTOMER's time stamped by N edge
  -- instances with N clocks: a row can land here minutes after it, and a window
  -- over `received_at` would miss it permanently — there is no watermark to come
  -- back for it. `ingested_at` is one clock on one server and means "arrived in
  -- the archive", which is what "the latest data" has to mean.
  --
  -- But it is neither the ORDER BY key nor the partition key, so without this a
  -- window over it is a full scan across every partition, reading the fat
  -- `payload` column. Inserts arrive in roughly `ingested_at` order, so minmax
  -- granule ranges stay tight and old partitions prune out.
  INDEX idx_ingested_at ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, signal_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- The Kafka source stream. Reading it consumes messages, so it is never queried
-- directly — the materialized view below is what drains it.
--
-- `JSONAsString` puts each whole message (one JSON object) into the single `raw`
-- column, nesting intact — no per-field mapping, and the body sub-object survives
-- untouched. The broker is the compose-INTERNAL listener `kafka:29092`; the
-- host's `localhost:9092` is not reachable from inside this container.
CREATE TABLE IF NOT EXISTS signals.kafka_signals
(
  raw String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list   = 'kafka:29092',
  kafka_topic_list    = 'signals',
  kafka_group_name    = 'clickhouse-signal-log',
  kafka_format        = 'JSONAsString',
  kafka_num_consumers = 1,
  -- 'earliest', NOT the librdkafka default of 'latest'. This is what makes
  -- growing the topic's partition count safe later.
  --
  -- A brand-new partition has no committed offset for our consumer group, so
  -- `auto.offset.reset` decides where it starts. On 'latest' any message the
  -- producer writes to that partition BEFORE the consumer's rebalance assigns it
  -- is skipped forever — the producer refreshes topic metadata on its own clock,
  -- so it can start using a new partition before ClickHouse has noticed it
  -- exists. Signals are money; a narrow race that silently drops them is not a
  -- race worth having.
  --
  -- 'earliest' costs nothing today (partition 0 has a committed offset, so this
  -- setting is never consulted) and is only load-bearing on the day partitions
  -- are added.
  kafka_auto_offset_reset = 'earliest';

-- ─────────────────────────────────────────────────────────────────────────────
-- Moves each message into the log: lift the three envelope fields, keep the body
-- verbatim. `JSONExtractRaw(raw,'body')` returns the body sub-object as its exact
-- JSON text — that is the payload. The Kafka engine commits offsets only after
-- this insert lands, which is what makes the pipeline at-least-once (no loss).
--
-- `api_key_hash` is lifted into its own column rather than left in the payload
-- because it is OURS, not the caller's: `payload` must stay byte-for-byte what
-- was sent. The dispatcher reads this column and forwards it to
-- `/api/internal/settle`, which resolves the signal's organisation from it —
-- `ApiKey.hashedKey` stores the very same digest.
CREATE MATERIALIZED VIEW IF NOT EXISTS signals.signal_log_mv TO signals.signal_log AS
SELECT
  JSONExtractString(raw, 'signalId')                             AS signal_id,
  parseDateTime64BestEffort(JSONExtractString(raw, 'receivedAt')) AS received_at,
  JSONExtractString(raw, 'apiKeyHash')                           AS api_key_hash,
  JSONExtractRaw(raw, 'body')                                    AS payload
FROM signals.kafka_signals;
