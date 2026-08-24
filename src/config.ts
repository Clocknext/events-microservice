/** Environment-derived configuration, validated once at boot.
 *
 *  The ONLY place in the service that reads `process.env` (AGENTS.md). Anything
 *  that needs a setting imports `config` from here. */
const nodeEnv = process.env.NODE_ENV ?? 'development'

function intFromEnv(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return fallback
  const parsed = Number.parseInt(raw, 10)
  if (Number.isNaN(parsed)) throw new Error(`${name} must be an integer, got "${raw}"`)
  return parsed
}

export const config = {
  nodeEnv,
  isProduction: nodeEnv === 'production',
  host: process.env.HOST ?? '0.0.0.0',
  port: intFromEnv('PORT', 3000),
  logLevel: process.env.LOG_LEVEL ?? (nodeEnv === 'production' ? 'info' : 'debug'),

  // --- payments app (Vercel) -------------------------------------------------
  /** Origin of the payments app, e.g. https://payments.clocknext.com. */
  paymentsUrl: process.env.PAYMENTS_URL ?? '',
  /** Shared secret proving to `/api/internal/*` that the caller is us. Rides as
   *  `Authorization: Bearer <secret>` — the scheme is required by that route. */
  internalSecret: process.env.INTERNAL_SETTLE_SECRET ?? '',
  /** Timeout for one `/api/internal/resolve` call. It reads a single indexed row,
   *  so anything slower than this is a fault, not work in progress. */
  resolveTimeoutMs: intFromEnv('RESOLVE_TIMEOUT_MS', 3000),

  // --- redis (api-key resolution cache) --------------------------------------
  /** Empty disables the cache entirely: every signal then resolves upstream. */
  redisUrl: process.env.REDIS_URL ?? '',
  /** How long a resolved key stays trusted. Short on purpose — a revoked key
   *  keeps working for at most this long. */
  keyCacheTtlSeconds: intFromEnv('KEY_CACHE_TTL_SECONDS', 60),
  /** How long a rejected key stays rejected. Shorter than the hit TTL so a key
   *  created seconds ago is not locked out. */
  keyCacheMissTtlSeconds: intFromEnv('KEY_CACHE_MISS_TTL_SECONDS', 30),

  // --- aws / sqs (the reject vent) -------------------------------------------
  /** Region the queue lives in. */
  awsRegion: process.env.AWS_REGION ?? 'us-east-1',
  /** Set to LocalStack's gateway in development. Empty means the real AWS
   *  endpoint for the region, which the SDK resolves on its own. */
  awsEndpointUrl: process.env.AWS_ENDPOINT_URL ?? '',
  /** Where every non-2xx response is vented. Empty disables venting entirely:
   *  responses are unchanged, nothing is queued. Credentials are deliberately
   *  NOT config — the SDK's default chain reads them (AWS_PROFILE locally, the
   *  instance role in production). */
  pendingQueueUrl: process.env.SQS_PENDING_QUEUE_URL ?? '',
  /** Where every ACCEPTED signal goes. Empty disables the hand-off, and a 202
   *  then means only "well-formed and authenticated" — nothing is queued. */
  acceptedQueueUrl: process.env.SQS_ACCEPTED_QUEUE_URL ?? '',

  // --- request limits --------------------------------------------------------
  /** Reject bodies over this size (Fastify bodyLimit -> 413). */
  bodyBytes: intFromEnv('BODY_BYTES', 64 * 1024),
  /** Reject a `custom` blob over this size (413). */
  customBytes: intFromEnv('CUSTOM_BYTES', 32 * 1024),
} as const

export type Config = typeof config
