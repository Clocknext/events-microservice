/** Types for the reject vent. A leaf: imports nothing from the module.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  THESE TWO INTERFACES ARE CLICKHOUSE COLUMNS, NOT A WIRE SHAPE.
 *
 *  Field names are `snake_case` because they are column names, and the whole
 *  point of the message is that a worker can take a batch off the queue and
 *  bulk-insert it with no transformation at all:
 *
 *      insert('raw_signals',   batch.map((m) => m.raw_signals))
 *      insert('signal_status', batch.map((m) => m.signal_status))
 *
 *  So nothing may be added to these interfaces that is not a column, and
 *  nothing may be omitted that is one. Two consequences follow from the DDL
 *  rather than from taste:
 *
 *   · `organization_id`, `customer_id` and `api_key_id` are non-`Nullable`
 *     `String`, and a JSON `null` into a non-Nullable column is an INSERT
 *     ERROR, not a default. They carry `''` when unknown — which, for
 *     `organization_id`, is every single reject: every 4xx on the signal route
 *     is decided before `resolveApiKeyAndBody` returns, so the edge never
 *     learns whose signal it was.
 *   · every `| null` field below must be declared `Nullable(...)` in the DDL
 *     for the same reason, in the other direction.
 *  ─────────────────────────────────────────────────────────────────────────── */

/** One row of `raw_signals` — the request as it arrived, whatever became of it. */
export interface RawSignalRow {
  /** ULID stamped at the edge; the key that threads everything. */
  signal_id: string
  /** `''` on a reject: unknown until the key resolves, and it never did. */
  organization_id: string
  /** Read off the payload at the EDGE, not by the worker — the edge already
   *  parsed the body, and on a reject the worker may get a payload that never
   *  parsed at all. `''` when the caller sent none. */
  customer_id: string
  /** Arrival time at the edge, ISO-8601 UTC. */
  received_at: string
  /** The caller's retry key, if sent. */
  idempotency_key: string | null
  /** Which `cnk_` key sent it. `''` on a reject — see the note above. */
  api_key_id: string
  /** The request body, word for word — the RAW bytes, so a body that is not
   *  valid JSON is still recorded verbatim rather than lost. `''` only when the
   *  caller sent no body at all. */
  payload: string
}

/** One row of `signal_status`. Every column the settlement worker would fill is
 *  present and `null`: a rejected signal was never priced, so there are no
 *  money numbers, no credit, no outcome. */
export interface SignalStatusRow {
  signal_id: string
  organization_id: string
  /** Which delivery of this signal this run settled. Always 1 from the edge —
   *  a reject is a first and only delivery. */
  attempt: number
  status: 'PROCESSED' | 'PENDING'
  /** Who has to act: USER_ERROR needs the caller's data fixed, SERVER_ERROR is
   *  ours and is safe to retry as-is. Derived from the status code — 4xx is the
   *  caller's, 5xx is ours. */
  error_type: 'USER_ERROR' | 'SERVER_ERROR' | null
  /** The `ErrorReason` union off the wire contract, verbatim, so the column and
   *  `result.errorReason` share one vocabulary. */
  error_code: string | null
  /** The first problem only, the same sentence `statusDetail.message` carried. */
  error_message: string | null
  /** What the signal metered, read from the payload — set even on a signal that
   *  failed; null when the caller sent something that is not one of the three. */
  signal_type: 'wallet' | 'credit' | 'outcome' | null
  usage_log_id: string | null
  credits_used: number | null
  provided_cost: number | null
  customer_cost: number | null
  credit_id: string | null
  credit_name: string | null
  model_name: string | null
  provider: string | null
  member_name: string | null
  currency_code: string | null
  applied_rules: string | null
  wallet_debit_usd: number | null
  balance_remaining: number | null
  outcome_id: string | null
  outcome_name: string | null
  outcome_step: string | null
  outcome_steps_done: number | null
  outcome_run_id: string | null
  outcome_closed_run: boolean | null
  outcome_completed: boolean | null
  outcome_signal_count: number | null
  outcome_total_steps: number | null
  updated_at: string
}

/** What one SQS message carries. The keys are TABLE NAMES on purpose — a batch
 *  off the queue maps straight onto two inserts. Nothing else belongs at this
 *  level; a field here that is not a table is a field the worker has to know to
 *  skip. */
export interface VentMessage {
  raw_signals: RawSignalRow
  signal_status: SignalStatusRow
}

/**
 * What one `signals_accepted` message carries.
 *
 * Deliberately only the two columns the edge is certain of. The rest of
 * `raw_signals` — organisation, customer, the payload — is known here too, but
 * this pipeline starts minimal: the id and the arrival time are what everything
 * downstream joins on, and `signal_status` is written later in the pipeline,
 * not by the edge.
 *
 * Keyed by table name for the same reason `VentMessage` is: the consumer does
 * `INSERT INTO raw_signals FORMAT JSONEachRow` with the object under that key
 * and nothing else. The omitted non-Nullable columns take their ClickHouse
 * defaults, which is `''` — the same value the pending pipeline sends
 * explicitly.
 */
export interface AcceptedMessage {
  raw_signals: Pick<RawSignalRow, 'signal_id' | 'received_at'>
}

/**
 * The slice of SQS the vent needs — deliberately tiny so the service takes a
 * port, not a driver, exactly like `KeyCache` does for Redis. The AWS SDK
 * client satisfies it through the thin wrapper in `plugins/sqs.ts`; a test
 * hands it an array-backed fake.
 */
export interface SignalQueue {
  send(body: string): Promise<void>
}
