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
  ingested_at  DateTime64(3) DEFAULT now64(3)  -- when ClickHouse wrote it (ops/lag; not in the body)
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
  kafka_num_consumers = 1;

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
