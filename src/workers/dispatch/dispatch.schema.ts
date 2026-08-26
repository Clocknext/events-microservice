/** Types and ports for the dispatcher. A leaf — imports nothing else of ours.
 *
 *  The dispatcher is NOT part of the Fastify app (AGENTS.md): it is a separate
 *  long-lived process that reads the ClickHouse archive and posts batches to the
 *  payments app. Its two dependencies arrive as the narrow ports below, so the
 *  logic is a pure function over them and a test can hand it fakes. */

// ─────────────────────────────────────────────────────────────────────────────
// What ClickHouse holds

/** One row of `signals.signal_log`, as the dispatcher selects it. `ingested_at`
 *  is deliberately not read — it is an ops column, not part of the signal. */
export interface SignalLogRow {
  signal_id: string
  /** ClickHouse renders DateTime64(3) as `YYYY-MM-DD hh:mm:ss.SSS` — a space,
   *  no zone. It is UTC; `toIso` puts it back into the shape settle validates. */
  received_at: string
  /** SHA-256 digest of the caller's key. `''` on rows written before the edge
   *  started stamping it — the column is non-Nullable. */
  api_key_hash: string
  /** The caller's body, verbatim, as JSON text. */
  payload: string
}

// ─────────────────────────────────────────────────────────────────────────────
// What the payments app says

/** `GET /api/internal/signals/cursor` */
export interface CursorResponse {
  /** Newest `receivedAt` that has a status row — everything up to it has been
   *  through settle at least once. Null when nothing has ever settled. */
  sentThrough: string | null
  /** Signals individually due for another attempt. Named rather than folded into
   *  the watermark so one old failure cannot drag the sweep back with it. */
  retry: { signalId: string; receivedAt: string; nextAttempt: number }[]
  maxAttempts: number
}

/** One signal as `/api/internal/settle` wants it: the payload's own fields, plus
 *  the envelope fields the route reads, spread on top. */
export interface SettleSignal {
  signalId: string
  receivedAt: string
  /** How the organisation is resolved. The raw key is never sent. */
  apiKeyHash: string
  /** Which delivery this is. Settle stores it verbatim, so the count only
   *  advances because the dispatcher advanced it. */
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

/** One page of the archive.
 *
 *  `after` is a KEYSET cursor, not an offset. It exists because the sweep has to
 *  be able to page PAST rows it has already settled: the overlap window re-reads
 *  them, and if the SQL LIMIT is reached before the known-filter runs, every
 *  sweep reads the same page of known rows and never sees the new work behind
 *  them. That starves the pipeline permanently — see the test named for it. */
export interface ArchivePage {
  /** Lower bound from the watermark, already shifted back by the overlap. */
  sinceIso: string | null
  /** Exclusive keyset position, for page 2 onwards. Null for the first page. */
  after: { receivedAt: string; signalId: string } | null
  limit: number
}

export interface ArchiveReader {
  /** One page, oldest first, deduplicated on `signal_id`. */
  readNewer(page: ArchivePage): Promise<SignalLogRow[]>
  /** Rows for specific ids — how a retry gets its payload back. */
  readByIds(signalIds: string[]): Promise<SignalLogRow[]>
}

export interface SettleClient {
  cursor(retryLimit: number): Promise<CursorResponse>
  /** Of these ids, which already have a status row? Bounded by what is asked, so
   *  the answer never grows with traffic. */
  known(signalIds: string[]): Promise<string[]>
  settle(batchId: string, signals: SettleSignal[]): Promise<SettleResult[]>
}

/** What one sweep did. Returned rather than logged so a test can assert on it
 *  and the runner can decide whether to nap. */
export interface SweepOutcome {
  /** Rows read out of the archive. */
  read: number
  /** Signals actually posted to settle (read, minus anything unusable). */
  sent: number
  processed: number
  /** Refused for good — the caller must change something. Not retried. */
  userError: number
  /** Ours. Retried on a later sweep until `maxAttempts`. */
  serverError: number
  /** Rows the archive held but that could not be turned into a signal. */
  skipped: number
  /** Candidates dropped because they had already been settled — the overlap
   *  window doing its job without re-pricing anything. */
  alreadyKnown: number
  /** How many pages of the archive it had to read to fill the batch. >1 means
   *  the overlap window held more settled rows than one page. */
  pages: number
  /** True when the batch came back full, i.e. there is probably more waiting. */
  saturated: boolean
  batchIds: string[]
}
