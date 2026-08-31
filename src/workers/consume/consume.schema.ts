/** Types and ports for the consumer. A leaf — it imports only the Kafka wire
 *  contract, which `signal.schema.ts` already declares is "the entire contract
 *  with whatever consumer drains the topic". This is that consumer.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHY THIS PROCESS EXISTS AT ALL
 *
 *  ClickHouse used to ingest from Kafka ITSELF — an `ENGINE = Kafka` table and a
 *  materialized view, no process in between. That could not call anything: a
 *  materialized view cannot make an HTTP request, so a signal reached the archive
 *  with no idea whose it was or whether it was acceptable, and both questions
 *  were answered minutes later inside settle.
 *
 *  This process replaces the Kafka engine. It pulls, asks
 *  `/api/internal/resolve` about each signal, and writes the VERDICT alongside
 *  the row. Settle then trusts that verdict instead of re-deriving it.
 *
 *  THE COST, stated plainly: the archive is no longer a pure function of the
 *  topic. Replaying Kafka re-runs the resolve calls, and a key that has since
 *  expired answers differently. `signal_log` still has exactly ONE writer — it is
 *  this process now, not the materialized view — but "rebuildable by replay" is
 *  weaker than it was, and no comment anywhere should still claim otherwise.
 *  ───────────────────────────────────────────────────────────────────────────── */
import type { SignalMessage } from '../../modules/signal/signal.schema.js'
import type { ResolveVerdict } from '../../client/resolve-client.js'

export type { ResolveVerdict }

/** What `signal_log.status` holds.
 *
 *  The consumer only ever writes the first two. `SUCCESS` is written by the daily
 *  reconciliation cron, which copies the settled state back out of Postgres —
 *  note that side calls it `PROCESSED` (`SignalLifecycle`), so that cron owns the
 *  one-line mapping. */
export type SignalStatus = 'PROCESSING' | 'PENDING' | 'SUCCESS'

/** One row as the consumer writes it.
 *
 *  `ingested_at` is deliberately ABSENT: ClickHouse stamps it with
 *  `DEFAULT now64(3)`, so it is one clock on one server, and the dispatcher's
 *  window compares it against `now64(3)` on that same server. Sending a value
 *  from this process would shift every window by whatever skew exists between two
 *  machines, silently. */
export interface SignalLogInsert {
  signal_id: string
  /** The edge's ISO-8601 stamp, straight off the Kafka message. ClickHouse parses
   *  it because the client sets `date_time_input_format=best_effort`; its default
   *  `basic` rejects both the `T` and the `Z`. */
  received_at: string
  api_key_hash: string
  /** A lifted COPY of the payload's `customerId`, for querying. The payload keeps
   *  its own, byte-for-byte — the same treatment `api_key_hash` gets, and for the
   *  same reason: `payload` must stay exactly what the caller sent. */
  customer_id: string
  /** OURS, resolved against payments. `''` when the key never resolved. */
  organization_id: string
  status: SignalStatus
  error_code: string
  error_message: string
  /** The caller's body, serialized verbatim. */
  payload: string
  /** Always 1 from here. Bumped only by a process that REWRITES a row, which
   *  today is nobody — `ReplacingMergeTree(version)` is in place ahead of the
   *  daily cron because a table's engine cannot be ALTERed afterwards. */
  version: number
}

// ─────────────────────────────────────────────────────────────────────────────
// The ports

export interface Resolver {
  /** ONE call to `/api/internal/resolve` for the whole chunk, returning one
   *  verdict per message POSITIONALLY.
   *
   *  It was one call per signal until the batch route landed. That is a named
   *  Kafka anti-pattern — a synchronous external call per message caps throughput
   *  at (concurrency ÷ latency) and head-of-line blocks the partition behind it.
   *  Measured against real data it capped near 150 signals/sec at two Postgres
   *  queries each; a batch of 500 is now one call and two queries.
   *
   *  Returns verdicts rather than throwing, except for the two things that are
   *  not about any signal: `MisconfiguredError` (our shared secret) and
   *  `BadBatchError` (our envelope). Both exit 2. */
  resolveBatch(messages: readonly SignalMessage[]): Promise<readonly ResolveVerdict[]>
}

export interface ArchiveWriter {
  /** ONE insert, however many rows. Batched by the caller: ClickHouse punishes
   *  per-row inserts, and that batching is what the Kafka engine used to do for
   *  us. */
  write(rows: readonly SignalLogInsert[]): Promise<void>
}

/** Counters that must outlive a batch but must never outlive the process.
 *
 *  WHAT THIS USED TO CARRY, AND WHY IT NO LONGER HAS TO. With one HTTP call per
 *  signal, "this signal is poison" and "the route is down" were indistinguishable
 *  from a single failure, so poison had to be INFERRED — a signal counted as
 *  poison only if some other call had succeeded since its failure streak began,
 *  compared as a monotonic count because timestamps tie inside a millisecond.
 *  That inference was subtle enough to get wrong once, in production: a success
 *  from 70 seconds earlier made a route that was demonstrably down look alive,
 *  and a good signal was archived as a caller error during a real outage.
 *
 *  One call per BATCH makes it direct evidence instead. If the call returned 200
 *  and one item came back transient, the route answered — see
 *  `ResolveVerdict.routeAnswered`. So `succeededSinceStreak` is GONE, and what is
 *  left here is a plain cross-batch failure count plus the runner's outage clock:
 *
 *    a signal transient in N successive answered batches → poison. Archive it and
 *      move on, or it stalls the whole topic behind it forever.
 *    nothing has succeeded for a long time              → payments is down.
 *      Commit nothing and exit, so systemd backs off instead of hot-looping.
 *
 *  In memory, not on disk, because they only need to survive a batch. A restart
 *  resetting them is correct: after a restart we genuinely do not know yet. */
export interface ResolveHealth {
  /** A resolve call answered — whatever it said, the route is alive. Takes the
   *  signal id so THAT signal's failure streak can be cleared: a global clear
   *  would erase the very count that identifies a poison message, since poison is
   *  defined by failing *while other calls succeed*. */
  recordSuccess(signalId: string): void
  /** @returns how many times in a row THIS signal has now failed transiently. */
  recordFailure(signalId: string): number
  /** ms since the last successful call, or since the tracker was CREATED when
   *  there has not been one. Seeding it from creation gives a process that starts
   *  during an outage a full grace window before it gives up, instead of exiting
   *  on its first batch. */
  msSinceSuccess(): number
  /** Has any call succeeded in this process AT ALL? */
  hasAnswered(): boolean
}

export interface ConsumeConfig {
  /** Most signals in ONE resolve call. A Kafka poll can exceed the route's cap,
   *  so a batch is split into chunks of at most this many; a poll under the limit
   *  is exactly one call.
   *
   *  This replaced `concurrency`, and it is not the same kind of knob. Concurrency
   *  was a throughput CEILING — in-flight HTTP calls against a serverless
   *  function. This is a request SIZE, and raising it makes the pipeline faster
   *  rather than merely more parallel. */
  batchMax: number
  /** Consecutive transient failures on one signal, IN ANSWERED BATCHES, before it
   *  is quarantined.
   *
   *  The "answered" half used to be inferred (`succeededSinceStreak`); it is now
   *  read straight off the verdict as `routeAnswered`, because a per-item
   *  transient inside a 200 is proof the route is alive.
   *
   *  The outage side of the rule is still the RUNNER's (`RESOLVE_OUTAGE_MS`),
   *  because it is about the process, not about a batch. */
  poisonAfter: number
}

export interface ConsumeDeps {
  resolve: Resolver
  archive: ArchiveWriter
  health: ResolveHealth
  config: ConsumeConfig
  /** Called between resolve CHUNKS. The runner binds kafkajs's `heartbeat` here.
   *
   *  This used to be load-bearing: 500 signals meant 500 sequential HTTP calls,
   *  long enough that the broker decided we were dead and rebalanced the group
   *  mid-batch without it. One call per chunk cannot outlast a session timeout the
   *  same way, so it is now ordinary defensiveness for the multi-chunk case rather
   *  than the thing standing between this consumer and a rebalance loop. */
  onProgress?: (() => Promise<void>) | undefined
  log?: ((event: string, detail: Record<string, unknown>) => void) | undefined
  /** Injected so a test can assert on `ms` without a real clock. */
  now?: (() => number) | undefined
}

/** What one batch did. Returned rather than logged so a test can assert on it,
 *  and so the RUNNER — not this logic — decides what to commit. */
export interface BatchOutcome {
  read: number
  /** Resolved and acceptable. Settle will price these. */
  processing: number
  /** Rejected by payments. Terminal, archived with the reason, still dispatched
   *  so settle records a row and the failure stays visible. */
  pending: number
  /** Poison messages archived as PENDING/`RESOLVE_FAILED` rather than stalling
   *  the topic. Not routine — alarm on a nonzero value. */
  quarantined: number
  /** Index of the first message that could NOT be resolved, or -1 when all were.
   *
   *  Everything before it is written and its offsets are safe to commit;
   *  it and everything after must be redelivered. Committing a contiguous PREFIX
   *  rather than failing the whole batch matters here in a way it would not for a
   *  cheap consumer: re-resolving 500 signals because #487 timed out is 500 HTTP
   *  calls we already paid for. */
  stoppedAt: number
  ms: number
}
