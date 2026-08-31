/** Types and ports for the dispatcher. A leaf — imports nothing else of ours.
 *
 *  The dispatcher is NOT part of the Fastify app (AGENTS.md): it is a one-shot
 *  process, run by a systemd timer every 60s, that reads one window of the
 *  ClickHouse archive and posts it to the payments app in a single call. Its two
 *  dependencies arrive as the narrow ports below, so the logic is a pure function
 *  over them and a test can hand it fakes.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHY THERE IS NO WATERMARK, NO CURSOR AND NO STATE OF ANY KIND
 *
 *  `/api/internal/settle` is idempotent on `signalId`: `SignalLog.signalId` is
 *  UNIQUE and settle uses it as the idempotency key, so a signal sent twice
 *  replays onto the original money row instead of charging again.
 *
 *  That one fact lets the run be stateless. It does not ask what it already sent;
 *  it re-reads a window WIDER than the timer's interval and sends the lot, so
 *  every signal goes about three times and settle discards the duplicates. The
 *  overlap is not waste — it is the whole of the error recovery, since a run that
 *  fails leaves nothing behind to resume from and is simply covered by the next
 *  two.
 *  ───────────────────────────────────────────────────────────────────────────── */

// ─────────────────────────────────────────────────────────────────────────────
// What ClickHouse holds

/** One row of `signals.signal_log`, as the dispatcher selects it.
 *
 *  The last five columns are the CONSUMER's verdict, written when the signal was
 *  archived rather than derived here. Everything before them is the caller's and
 *  the edge's. */
export interface SignalLogRow {
  signal_id: string
  /** ClickHouse renders DateTime64(3) as `YYYY-MM-DD hh:mm:ss.SSS` — a space,
   *  no zone. It is UTC; `toIso` puts it back into the shape settle validates. */
  received_at: string
  /** SHA-256 digest of the caller's key. `''` on rows written before the edge
   *  started stamping it — the column is non-Nullable. */
  api_key_hash: string
  /** A lifted copy of the payload's `customerId`. The payload still holds its
   *  own; this exists so the archive can be queried without JSON extraction. */
  customer_id: string
  /** OURS, resolved by the consumer against `/api/internal/resolve`. `''` on a
   *  row whose key never resolved, and on rows archived before the consumer
   *  existed — settle falls back to `api_key_hash` for both. */
  organization_id: string
  /** `PROCESSING` (price it), `PENDING` (record the failure, price nothing) or
   *  `SUCCESS` (already settled; written only by the daily reconciliation cron). */
  status: string
  /** Machine-readable, from payments. `''` when the signal was accepted. */
  error_code: string
  /** The human sentence payments gave. `''` when the signal was accepted. */
  error_message: string
  /** The caller's body, verbatim, as JSON text. */
  payload: string
}

// ─────────────────────────────────────────────────────────────────────────────
// What the payments app wants

/** One signal as `/api/internal/settle` wants it: the payload's own fields, plus
 *  the envelope fields the route reads, spread on top. */
export interface SettleSignal {
  signalId: string
  receivedAt: string
  /** Kept for audit, and as settle's fallback for a row archived before the
   *  consumer existed. The raw key is never sent. */
  apiKeyHash: string
  /** The organisation the CONSUMER resolved, which settle trusts rather than
   *  re-deriving. `''` when the key never resolved — settle's
   *  `SignalStatus.organizationId` is nullable for exactly that case. */
  organizationId: string
  /** What settle should DO with this signal, decided upstream:
   *  `PROCESSING` price it; `PENDING` record the terminal failure below and
   *  price nothing. */
  status: string
  /** Present only on a `PENDING` signal. */
  errorCode: string | null
  errorMessage: string | null
  /** Always 1. Nothing on THIS side counts attempts any more — the dispatcher
   *  keeps no state, so it cannot know which delivery this is. The attempt count
   *  belongs to `SignalStatus`, which is the side that can actually see it. */
  attempt: number
  [payloadField: string]: unknown
}

/** The slice of a settle result the dispatcher acts on. */
export interface SettleResult {
  signal_id: string
  status: 'PROCESSED' | 'PENDING'
  error_type: 'USER_ERROR' | 'SERVER_ERROR' | null
  error_code: string | null
  error_message: string | null
}

// ─────────────────────────────────────────────────────────────────────────────
// The ports

/** The window one run reads, over `ingested_at`.
 *
 *  Either a relative width (`windowMs`, the normal case) or an explicit
 *  `[since, until)` for a manual replay. Never over `received_at` — see the
 *  comment on `readIngested`. */
export interface ArchiveWindow {
  /** How far back from now, in ms. Ignored when `since` is set. */
  windowMs: number
  /** Explicit lower bound, ISO-8601. Empty in normal operation. */
  since?: string | undefined
  /** Explicit upper bound, ISO-8601, exclusive. Empty means "up to now". */
  until?: string | undefined
  /** Row ceiling. An OOM guard, not a batch size. */
  cap: number
}

export interface ArchiveReader {
  /**
   * Every row INGESTED in the window, oldest first, deduplicated on `signal_id`.
   *
   * The window is over `ingested_at` and never `received_at`. `received_at` is
   * the caller's time, stamped by N edge instances off N clocks, and a row can
   * land here minutes after it — the consumer batches, resolves each signal over
   * HTTP, and a broker backlog delays it further. A window over `received_at`
   * would miss
   * every such row PERMANENTLY, because nothing persists a watermark to come
   * back for it. `ingested_at` is one clock on one server and means "arrived in
   * the archive", which is what this run needs to mean by "the latest data".
   */
  readIngested(window: ArchiveWindow): Promise<SignalLogRow[]>
}

/** One settle call: its per-signal results, and how big the body actually was.
 *
 *  The sizes come back with the results rather than being measured again by the
 *  caller, so the body is serialized exactly once. They are not decoration:
 *  `gzipBytes` against Vercel's 4.5MB request-body ceiling is the number that
 *  says how close this pipeline is to the limit that would force batching. */
export interface SettleTransfer {
  results: SettleResult[]
  bytes: number
  gzipBytes: number
}

export interface SettleClient {
  /** Posts the WHOLE window in one call and returns a result per signal. */
  settle(batchId: string, signals: SettleSignal[]): Promise<SettleTransfer>
}

/** What one run did. Returned rather than logged so a test can assert on it. */
export interface RunOutcome {
  /** Rows read out of the archive. */
  read: number
  /** Signals actually posted (read, minus anything unusable). */
  sent: number
  /** Rows the archive held but that could not be turned into a signal. */
  skipped: number
  processed: number
  /** Arrived already rejected by the consumer — a bad key, a body that failed
   *  the rulebook, an unknown customer. Settle records these and prices nothing. */
  pending: number
  /** Refused for good — the caller must change something. Never retried. */
  userError: number
  /** Ours. Re-sent by the next overlapping window, and by the hourly
   *  reconciliation run, for as long as those windows still cover it. */
  serverError: number
  /** True when the read hit `cap`, which means ROWS WERE LEFT UNSENT and the
   *  next window has already moved past some of them. An alarm, not a state. */
  capped: boolean
  /** Uncompressed and on-the-wire body size, for watching the 4.5MB ceiling. */
  bytes: number
  gzipBytes: number
  ms: number
  batchId: string
}
