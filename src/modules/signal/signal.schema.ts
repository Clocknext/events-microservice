/** JSON schemas for validation/serialization, plus the TS types routes are
 *  generic over. A leaf — imports from none of the other four files.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHY THIS SCHEMA IS THIN
 *
 *  The edge is a gate, not a validator. It checks only that a signal carries the
 *  three fields nothing downstream can proceed without — `customerId`,
 *  `inputTokens`, `outputTokens` — and pushes everything else, untouched, to
 *  Kafka. The full rulebook (types, models, agent keys, cross-field rules) is a
 *  consumer's job now, not the edge's: a signal that breaks a deeper rule is
 *  still recorded and settled/failed downstream, so refusing it here would only
 *  lose it.
 *
 *  `additionalProperties` is therefore LEFT OPEN and every field beyond the
 *  three required ones is unconstrained — the whole body must survive to the
 *  topic verbatim.
 *  ─────────────────────────────────────────────────────────────────────────── */

/** Token counts are stored as Postgres DOUBLE PRECISION, which represents
 *  integers exactly only up to 2^53-1. Above that a count loses precision
 *  before it ever reaches the database, so it is refused here. AJV coercion is
 *  off (see app.ts), so `"1200"` is rejected rather than read as 1200. */
const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER

const tokenCount = {
  type: 'integer',
  minimum: 0,
  maximum: MAX_SAFE_INTEGER,
} as const

export const signalBodySchema = {
  type: 'object',
  required: ['customerId', 'inputTokens', 'outputTokens'],
  properties: {
    // `pattern: '\\S'` rejects a whitespace-only id: it is empty once trimmed.
    customerId: { type: 'string', minLength: 1, pattern: '\\S' },
    inputTokens: tokenCount,
    outputTokens: tokenCount,
  },
  // Everything else the caller sends rides through to Kafka untouched.
} as const

/** `result` on a 202. The signal passed the gate and is on the topic — nothing
 *  is settled and no money has moved. */
export const signalAcceptedResultSchema = {
  type: 'object',
  required: ['signalId', 'receivedAt', 'accepted'],
  properties: {
    /** Our id for this signal, stamped before anything could fail. The caller
     *  correlates on it. */
    signalId: { type: 'string' },
    receivedAt: { type: 'string' },
    accepted: { type: 'boolean' },
  },
} as const

/** The gate only reads three fields; the rest of the body is caller-shaped and
 *  passes through, so the index signature is how a valid body carries anything
 *  else (a pricing metric's `refId`, `type`, `model`, `custom`, …). */
export interface SignalBody {
  customerId: string
  inputTokens: number
  outputTokens: number
  [extra: string]: unknown
}

/** The identity stamped on a request before anything can fail — the id and time
 *  minted in `plugins/identity.ts`, the digest in `signal.module.ts`. Passed in
 *  rather than generated, so an accepted signal carries the same identity the
 *  caller was handed. */
export interface SignalIdentity {
  signalId: string
  receivedAt: string
  /** SHA-256 digest of the caller's `cnk_…` key. See `digestApiKey`. */
  apiKeyHash: string
}

/** What the service hands back before the envelope wraps it. */
export interface AcceptedSignalResult {
  accepted: true
  signalId: string
  receivedAt: string
}

/** One Kafka message: the three edge-stamped fields plus the body as sent. This
 *  is the entire contract with whatever consumer drains the topic.
 *
 *  `apiKeyHash` rides on the ENVELOPE, never inside `body` — `body` is the
 *  caller's bytes verbatim and becomes `signal_log.payload`, so putting our own
 *  field in it would corrupt the archive. ClickHouse lifts the digest into its
 *  own column, which is what lets `/api/internal/settle` resolve the signal's
 *  organisation without the raw key ever leaving this process. */
export interface SignalMessage {
  signalId: string
  receivedAt: string
  apiKeyHash: string
  body: SignalBody
}
