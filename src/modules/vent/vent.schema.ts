/** Types for the queue messages and the ClickHouse rows they carry. A leaf:
 *  imports nothing from the module.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  `RawSignalRow` IS the `raw_signals` table, column for column.
 *
 *  Field names are `snake_case` because they are column names, and the message
 *  carries the row so a consumer bulk-inserts it with no mapping. So nothing may
 *  be added that is not a column, and nothing omitted that is one.
 *
 *  `organization_id` and `customer_id` are non-`Nullable` `String`: the edge
 *  sends `''` when it does not know them (a JSON null into a non-Nullable column
 *  is an INSERT ERROR). On a reject `organization_id` is ALWAYS `''` — every 4xx
 *  is decided before the key resolves, so the edge never learns whose it was.
 *  ─────────────────────────────────────────────────────────────────────────── */

/** One row of `raw_signals`. */
export interface RawSignalRow {
  /** ULID stamped at the edge; the key that threads everything. */
  signal_id: string
  /** `''` on a reject: unknown until the key resolves, and it never did. */
  organization_id: string
  /** Read off the payload at the edge; `''` when the caller sent none. */
  customer_id: string
  /** What was metered — `wallet` | `credit` | `outcome`; null when the caller
   *  sent none or something that is not one of the three. */
  type: string | null
  /** The caller's retry key, if sent. */
  idempotency_key: string | null
  /** The request body, word for word — the RAW bytes, so a body that is not
   *  valid JSON is still recorded verbatim rather than lost. `''` only when the
   *  caller sent no body at all. */
  payload: string
  /** Arrival time at the edge, ISO-8601 UTC. */
  received_at: string
}

/** The lifecycle states a signal moves through, and the only values that ever
 *  go in `signal_status_events.status`.
 *
 *   Processing  a consumer picked the signal up; not yet resolved.
 *   Processed   settlement succeeded (accepted signals only).
 *   Failed      the signal was rejected at the edge, or settlement refused it
 *               with a terminal (caller's-fault) error.
 *
 *  The events are written by the CONSUMERS, not the edge — the edge does not
 *  know a signal's fate — so this type lives here only because it is the shared
 *  vocabulary of the pipeline. */
export type SignalStatus = 'Processing' | 'Processed' | 'Failed'

/** One row of `signal_status_events`. Built by a consumer, never by the edge. */
export interface StatusEventRow {
  signal_id: string
  /** The consumer invocation that wrote this event. */
  batch_id: string
  status: SignalStatus
  /** Set on `Failed`; null otherwise. The `ErrorReason`/settle code verbatim. */
  error_code: string | null
  /** Which delivery of the signal this event belongs to — the SQS receive
   *  count, so a redelivery is attempt 2, 3, … */
  attempt: number
  /** When the event was written; also the ReplacingMergeTree version. */
  timestamp: string
}

/**
 * A `signals_pending` message: one rejected signal.
 *
 * `raw_signals` is inserted as-is; `error_code` is what the consumer needs to
 * write the signal's one `Failed` event (a rejected signal is terminal, so it
 * never enters settlement). `error_code` is NOT a `raw_signals` column, which is
 * why the pending consumer is not a pure passthrough — it builds a status event.
 */
export interface PendingMessage {
  raw_signals: RawSignalRow
  /** The `ErrorReason` the edge refused the signal with. */
  error_code: string
}

/**
 * A `signals_accepted` message: one accepted signal.
 *
 * Carries the FULL `raw_signals` row, because the accepted consumer both inserts
 * it AND calls `/internal/settle`, and settle needs the customer, the type and
 * the payload as input. The consumer parses `payload` back into the body fields
 * settle wants.
 */
export interface AcceptedMessage {
  raw_signals: RawSignalRow
}

/**
 * The slice of SQS the edge needs — deliberately tiny so the service takes a
 * port, not a driver, exactly like `KeyCache` does for Redis. The AWS SDK client
 * satisfies it through the thin wrapper in `plugins/sqs.ts`; a test hands it an
 * array-backed fake.
 */
export interface SignalQueue {
  send(body: string): Promise<void>
}
