-- Schema for the signal pipeline's ClickHouse.
--
-- Run by the container's entrypoint on FIRST START ONLY — ClickHouse ignores
-- /docker-entrypoint-initdb.d once the data directory exists. To re-apply after
-- editing, drop the volume: `docker compose down -v clickhouse`.
--
-- Two tables:
--   raw_signals           one row per signal — the request as it arrived.
--   signal_status_events  an append-only event log — one row per status change
--                         of a signal (Processing -> Processed | Failed).

CREATE DATABASE IF NOT EXISTS signals;

-- ─────────────────────────────────────────────────────────────────────────────
-- Every signal that reached the edge, whatever became of it.
--
-- ReplacingMergeTree keyed on signal_id: SQS is at-least-once, so a redelivered
-- batch re-inserts a row — the engine collapses the duplicate on merge. That is
-- what makes at-least-once delivery safe.
--
-- organization_id and customer_id are non-Nullable, so the edge sends '' (not
-- null) when it does not know them — a JSON null into a non-Nullable column is
-- an INSERT ERROR. On a rejected signal organization_id is ALWAYS '': every 4xx
-- is decided before the key resolves, so the edge never learns whose it was.
CREATE TABLE IF NOT EXISTS signals.raw_signals
(
  signal_id        String,                       -- ULID stamped at the edge; the key that threads everything
  organization_id  String,                       -- '' on a reject (unknown until the key resolves)
  customer_id      String,                       -- read off the payload at the edge; '' when absent
  type             Nullable(String),             -- wallet | credit | outcome; null when the caller sent none/invalid
  idempotency_key  Nullable(String),             -- the caller's retry key, if sent
  payload          String CODEC(ZSTD(3)),        -- the request body, word for word; compressed, the only large column
  received_at      DateTime64(3)                 -- arrival time at the edge
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (organization_id, received_at, signal_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- The lifecycle of each signal, as an append-only event log.
--
-- One signal produces several rows over time — Processing when a consumer picks
-- it up, then Processed or Failed once it resolves. The current state of a
-- signal is the row with the newest timestamp:
--
--   SELECT argMax(status, timestamp) FROM signal_status_events GROUP BY signal_id
--
-- ReplacingMergeTree(timestamp) keyed on (signal_id, status, attempt): a
-- REDELIVERED event (same signal, same status, same delivery) collapses to one
-- row, but genuinely distinct events (Processing then Processed, or attempt 1
-- then attempt 2) coexist. batch_id is deliberately NOT in the sort key — it
-- changes on every redelivery, so keying on it would defeat the dedup; the
-- newest timestamp wins and carries the most recent invocation's batch_id.
CREATE TABLE IF NOT EXISTS signals.signal_status_events
(
  signal_id   String,                            -- the raw_signals row this is about
  batch_id    String,                            -- the consumer invocation that wrote this event
  status      String,                            -- Processing | Processed | Failed
  error_code  Nullable(String),                  -- set on Failed; null otherwise
  attempt     UInt32,                            -- which delivery of the signal this event belongs to (SQS receive count)
  timestamp   DateTime64(3)                      -- when the event was written; also the ReplacingMergeTree version
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (signal_id, status, attempt);
