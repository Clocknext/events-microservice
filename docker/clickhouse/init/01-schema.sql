-- Schema for the signal pipeline's ClickHouse.
--
-- Run by the container's entrypoint on FIRST START ONLY — ClickHouse ignores
-- /docker-entrypoint-initdb.d once the data directory exists. To re-apply after
-- editing, drop the volume: `docker compose down -v clickhouse`.
--
-- The two tables are exactly the two objects a vent message carries, so a
-- worker draining the queue does `INSERT INTO <key> FORMAT JSONEachRow` with
-- the object under that key and no transformation at all.

CREATE DATABASE IF NOT EXISTS signals;

-- ─────────────────────────────────────────────────────────────────────────────
-- Every request that reached the edge, whatever became of it.
--
-- ReplacingMergeTree is NOT a preference. SQS is at-least-once, and the worker
-- inserts raw_signals, then signal_status, then deletes the message — so a
-- failure between the two inserts redelivers the batch and re-inserts these
-- rows. The engine is what makes that harmless.
--
-- organization_id, customer_id and api_key_id are non-Nullable, which is why
-- the edge sends '' rather than null for the ones it does not know: a JSON null
-- into a non-Nullable column is an INSERT ERROR, not a default. On a rejected
-- signal organization_id is ALWAYS '' — the key never resolved.
CREATE TABLE IF NOT EXISTS signals.raw_signals
(
  signal_id        String,                       -- ULID stamped at the edge; the key that threads everything
  organization_id  String,
  customer_id      String,                       -- read off the payload at the EDGE (the worker may get a payload that never parsed)
  received_at      DateTime64(3),                -- arrival time at the edge
  idempotency_key  Nullable(String),             -- caller's retry key, if sent
  api_key_id       String,                       -- which cnk_ key sent it
  payload          String CODEC(ZSTD(3)),        -- the request body, word for word; compressed because it is the only large column
  batch_id         Nullable(String)              -- the consumer invocation that ingested this row; set by the accepted consumer, null on a pending (rejected) row
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (organization_id, received_at, signal_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- What became of each signal. One row per signal, not per attempt:
-- ReplacingMergeTree(updated_at) keeps the newest row for a given
-- (organization_id, signal_id), so a retry OVERWRITES its predecessor and the
-- UI reads current state without a GROUP BY.
--
-- If attempt history is ever wanted instead, `attempt` joins the ORDER BY —
-- but that is a table rebuild once rows exist, so it is a decision to make now
-- rather than later.
--
-- Every column the settlement worker would fill is present and Nullable: a
-- rejected signal was never priced, so it carries nulls in all of them, and the
-- edge writes them out explicitly rather than omitting them.
CREATE TABLE IF NOT EXISTS signals.signal_status
(
  signal_id             String,                  -- the raw_signals row this describes
  organization_id       String,
  attempt               UInt32,                  -- 1, 2, 3… which delivery of this signal this run settled
  status                String,                  -- 'PROCESSED' | 'PENDING'
  -- Who has to act: USER_ERROR needs the caller's data fixed, SERVER_ERROR is
  -- ours and is safe to retry as-is. Null when settled.
  error_type            Nullable(String),        -- 'USER_ERROR' | 'SERVER_ERROR'
  error_code            Nullable(String),        -- the ErrorReason union, verbatim off the wire contract
  error_message         Nullable(String),        -- the first problem only; there is no issues column
  -- What the signal metered. Read from the payload, so it is set even on a
  -- signal that failed; null only when the caller sent something that is not
  -- one of the three kinds.
  signal_type           Nullable(String),        -- 'wallet' | 'credit' | 'outcome'
  -- Points at SignalLog.id in Supabase, which is the truth of record for the
  -- billing numbers below. These columns are a display copy, which is why
  -- Float64 is acceptable here and would not be there.
  usage_log_id          Nullable(String),
  credits_used          Nullable(Float64),       -- for credit kind
  provided_cost         Nullable(Float64),
  customer_cost         Nullable(Float64),
  -- The credit's id, so the UI can link the row even after a rename — names are
  -- snapshots and drift; ids don't. Null for wallet / outcome signals.
  credit_id             Nullable(String),
  credit_name           Nullable(String),
  model_name            Nullable(String),
  provider              Nullable(String),        -- display label of the model's provider ("OpenAI")
  -- Who consumed it, labelled the way the Signals table does — the customer
  -- member's own name first, falling back to the workspace user.
  member_name           Nullable(String),
  currency_code         Nullable(String),        -- the customer's display currency, the unit customer_cost reads in
  -- The credit rule that fired, as a JSON array
  -- [{ruleId, ruleName, mode, marginPercentApplied, creditsApplied}] — at most
  -- one entry, '[]' when none matched. JSON rather than columns because it is a
  -- snapshot the UI renders whole.
  applied_rules         Nullable(String),
  -- USD actually taken off the wallet. NOT only for signal_type 'wallet' — on a
  -- wallet-funded plan an ARREAR credit and an ARREAR outcome completion debit
  -- it too. 0 when untouched; null on a replay.
  wallet_debit_usd      Nullable(Float64),
  -- The metered dimension's remaining balance AFTER this signal. Null when the
  -- meter is uncapped, and on a replay.
  balance_remaining     Nullable(Float64),

  -- ── outcome attribution ───────────────────────────────────────────────────
  -- All null unless the signal was tagged with an outcome. Flat rather than
  -- nested so each one can be its own column.
  outcome_id            Nullable(String),        -- ids don't drift, names do
  outcome_name          Nullable(String),        -- the workflow this signal was attributed to
  outcome_step          Nullable(String),        -- which step of that workflow this signal performed
  -- That step's position in the catalogue (1-based). An identifier, not
  -- progress — a step may repeat freely within a run.
  outcome_steps_done    Nullable(UInt32),
  outcome_run_id        Nullable(String),        -- the caller's run id; every signal of one workflow shares it
  outcome_closed_run    Nullable(Bool),          -- this signal carried complete:true and ended the run
  -- The completion was COUNTED against the allowance. Diverges from
  -- outcome_closed_run when the run closed with none left.
  outcome_completed     Nullable(Bool),
  outcome_signal_count  Nullable(UInt32),        -- how many signals the run holds after this one
  outcome_total_steps   Nullable(UInt32),        -- catalogue size, not a progress target

  updated_at            DateTime64(3)            -- the version column: newest write wins
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (organization_id, signal_id);
