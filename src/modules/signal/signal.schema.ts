/** JSON schemas for validation/serialization, plus the TS types routes are
 *  generic over. A leaf — imports from none of the other four files.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHY THIS SCHEMA EXISTS AT THE EDGE
 *
 *  `signalBodySchema` mirrors the rulebook in the payments app
 *  (`validateSignalBody` in `@/lib/settle/resolve-signal`, itself a hand-kept
 *  copy of settlement's own `recordSchema`). It is duplicated on purpose: once
 *  a key is cached, a signal never reaches the payments app at all, so if the
 *  edge did not judge the body nothing would until settlement — and a signal
 *  rejected there has already been accepted, queued and acknowledged.
 *
 *  Keep it in sync by hand. If a rule changes upstream, change it here too;
 *  the upstream 400 remains the backstop on every cache miss.
 *
 *  TWO RULES ARE DELIBERATELY NOT ENFORCED HERE, because only the org's own
 *  data can decide them — they belong to settlement:
 *    · whether the customer, model, credit or outcome actually exists
 *    · whether the customer's active plan grants the named component
 *
 *  `additionalProperties` is deliberately LEFT OPEN. A pricing metric's `refId`
 *  is a caller-chosen property NAME on this body (`{ "voice_ai": "wb21" }`), so
 *  unknown top-level keys are legitimate and must survive to settlement.
 *  ─────────────────────────────────────────────────────────────────────────── */

/** Token counts are stored as Postgres DOUBLE PRECISION, which represents
 *  integers exactly only up to 2^53-1. Above that a count loses precision
 *  before it ever reaches the database, so it is refused here. */
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
    // `pattern: '\\S'` reproduces the upstream `.trim().min(1)`: a
    // whitespace-only string is empty once trimmed, so it is not an id.
    customerId: { type: 'string', minLength: 1, pattern: '\\S' },
    /** Which meter this usage records against — exactly one. Accepted
     *  case-insensitively; `signal.routes.ts` lowercases it before validation,
     *  which is why only lowercase appears here. `"unit"` is no longer
     *  accepted. Schema-optional so the upstream no-active-plan check keeps
     *  precedence over a "type is required" complaint. */
    type: { type: 'string', enum: ['wallet', 'credit', 'outcome'] },
    /** The model's catalog id, matched case-insensitively against the org's
     *  enabled models. Required whenever `type` is present. */
    model: { type: 'string', minLength: 1, pattern: '\\S' },
    /** The customer member this usage is attributed to, by email. */
    member: { type: 'string' },
    inputTokens: tokenCount,
    outputTokens: tokenCount,
    /** Provider-specific and absent on most calls, hence optional with a
     *  default of 0 rather than required like the input/output counts. */
    cacheTokens: { ...tokenCount, default: 0 },
    /** The metered target's stable agent key: the Credit's for `type: credit`,
     *  the OutcomeStep's for `type: outcome`. Matched case-insensitively. */
    agentKey: { type: 'string', minLength: 1, pattern: '\\S' },
    /** Deprecated alias for `agentKey` — older integrations send `key`. Used as
     *  a fallback only when `agentKey` is absent. */
    key: { type: 'string', minLength: 1, pattern: '\\S' },
    /** The caller's id for one run of a workflow; every signal sharing it
     *  belongs to the same run, so a step may repeat freely. Required for
     *  `type: outcome`. */
    runId: { type: 'string', minLength: 1, maxLength: 255, pattern: '\\S' },
    /** Set on the LAST step signal to declare the outcome finished — completion
     *  is never inferred from step coverage. Sending it twice is a no-op. */
    complete: { type: 'boolean' },
    /** Free-form metadata. Credit rules can target `custom.<key>` paths. */
    custom: { type: 'object' },
    /** A repeat request carrying the same key returns the original signal's
     *  result instead of recording a duplicate. Deduped per organisation. */
    idempotencyKey: { type: 'string', minLength: 1, maxLength: 255, pattern: '\\S' },
    /** Validate and price but do not record — powers integration checks. */
    dryRun: { type: 'boolean' },
  },
  // The cross-field rules, matching the `.refine()` calls upstream.
  allOf: [
    {
      if: { required: ['type'] },
      then: { required: ['model'] },
    },
    // `key`, the deprecated alias, is folded into `agentKey` by the route's
    // preValidation hook, so these rules only ever have to name `agentKey`.
    {
      if: { properties: { type: { const: 'credit' } }, required: ['type'] },
      then: { required: ['agentKey'] },
    },
    {
      if: { properties: { type: { const: 'outcome' } }, required: ['type'] },
      then: { required: ['agentKey', 'runId'] },
    },
  ],
} as const

/** `result` on a 202. TWO different things answer 202 now:
 *
 *    accepted: true      the signal passed every check. Not settled and no money
 *                        has moved — "we will take it from here". Unchanged.
 *    status: 'PENDING'   the signal was REFUSED, and is on the queue with its
 *                        payload and its error. The caller reads why in the UI
 *                        and resends. Same word `signal_status.status` uses.
 *
 *  `signalId` is the one field both carry, because it is the one field a caller
 *  needs either way. */
export const signalAcceptedResultSchema = {
  type: 'object',
  required: ['signalId', 'receivedAt'],
  properties: {
    /** Our id for this signal, stamped before anything could fail. The caller
     *  correlates on it, and the UI finds the row by it. */
    signalId: { type: 'string' },
    receivedAt: { type: 'string' },
    // --- an accepted signal ---------------------------------------------------
    accepted: { type: 'boolean' },
    organizationId: { type: 'string' },
    apiKeyId: { type: 'string' },
    /** Whether the key was resolved from Redis. Purely observability — this is
     *  the number to watch to know the cache is earning its place. */
    cached: { type: 'boolean' },
    // --- a refused signal, queued ---------------------------------------------
    status: { type: 'string', enum: ['PENDING'] },
  },
} as const

export interface SignalBody {
  customerId: string
  inputTokens: number
  outputTokens: number
  cacheTokens?: number
  type?: 'wallet' | 'credit' | 'outcome'
  model?: string
  member?: string
  agentKey?: string
  key?: string
  runId?: string
  complete?: boolean
  custom?: Record<string, unknown>
  idempotencyKey?: string
  dryRun?: boolean
  /** A pricing metric's `refId` is a caller-chosen property name, so a valid
   *  body may carry keys this interface cannot name. */
  [extra: string]: unknown
}


/** The identity stamped on a request before anything can fail — minted in
 *  `plugins/vent.ts`, passed in rather than generated, so an accepted signal
 *  and a refused one are the same signal under the same id. */
export interface SignalIdentity {
  signalId: string
  receivedAt: string
}

/** What the service hands back, before the envelope wraps it. Unchanged — an
 *  accepted signal answers exactly what it always did. */
export interface AcceptedSignalResult {
  accepted: true
  signalId: string
  organizationId: string
  apiKeyId: string
  receivedAt: string
  cached: boolean
}

/** What the error handler answers instead of a rejection: the signal is on the
 *  queue, and everything else about it lives in ClickHouse. */
export interface QueuedSignalResult {
  status: 'PENDING'
  signalId: string
  receivedAt: string
}
