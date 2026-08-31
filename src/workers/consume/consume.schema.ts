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
  /** One call to `/api/internal/resolve`. Returns a verdict rather than throwing,
   *  except for `MisconfiguredError` — which is about us, not this signal. */
  resolve(message: SignalMessage): Promise<ResolveVerdict>
}

export interface ArchiveWriter {
  /** ONE insert, however many rows. Batched by the caller: ClickHouse punishes
   *  per-row inserts, and that batching is what the Kafka engine used to do for
   *  us. */
  write(rows: readonly SignalLogInsert[]): Promise<void>
}

/** Counters that must outlive a batch but must never outlive the process.
 *
 *  They exist to separate the two failures a single batch cannot tell apart —
 *  and on a low-traffic topic a batch can be ONE message, where "every call
 *  failed" and "one poison message" are literally the same observation:
 *
 *    one signal keeps failing WHILE OTHERS SUCCEED  → a poison message. Archive
 *      it and move on, or it stalls the whole topic behind it forever.
 *    nothing has succeeded for a long time          → payments is down. Commit
 *      nothing and exit, so systemd backs off instead of hot-looping.
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
  /** Has some OTHER call succeeded since THIS signal's current failure streak
   *  began?
   *
   *  This is the quarantine's real question, stated exactly. "Poison" means a
   *  signal that fails *while others succeed*, and only a success that landed
   *  AFTER the streak started proves that.
   *
   *  An elapsed-time proxy (`msSinceSuccess() < outageMs`) is NOT good enough, and
   *  the difference is not academic — it was observed. Five consecutive failures
   *  accumulate in about five seconds; a two-minute outage threshold does not
   *  expire for another two minutes. So a success from 70 seconds ago made a route
   *  that was demonstrably DOWN look alive, and a perfectly good signal was
   *  archived as a caller error during a real outage. A stale success is not
   *  evidence of a live route. */
  succeededSinceStreak(signalId: string): boolean
}

export interface ConsumeConfig {
  /** In-flight resolve calls. The ceiling on throughput — one HTTP call per
   *  signal against a serverless function is this design's whole cost. */
  concurrency: number
  /** Consecutive transient failures on one signal before it is quarantined —
   *  necessary, but NOT sufficient. `succeededSinceStreak` is the other half, and
   *  the count alone would archive good signals during an outage.
   *
   *  The outage side of the rule is the RUNNER's (`RESOLVE_OUTAGE_MS`), because it
   *  is about the process, not about a batch. */
  poisonAfter: number
}

export interface ConsumeDeps {
  resolve: Resolver
  archive: ArchiveWriter
  health: ResolveHealth
  config: ConsumeConfig
  /** Called between resolve calls. The runner binds kafkajs's `heartbeat` here.
   *  A 500-signal batch is 500 HTTP calls — long enough that the broker decides
   *  we are dead and rebalances the group mid-batch without it. */
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
