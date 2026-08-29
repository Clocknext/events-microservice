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

  // --- kafka (the ingest topic) ----------------------------------------------
  /** Comma-separated bootstrap brokers. Empty disables producing entirely: a 202
   *  then means only "passed the gate", nothing is published. Local dev points
   *  at the KRaft container (`localhost:9092`); production is the MSK Serverless
   *  bootstrap endpoint. */
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? '')
    .split(',')
    .map((b) => b.trim())
    .filter((b) => b.length > 0),
  /** Client id the broker sees — handy in MSK's connection metrics. */
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? 'signal-edge',
  /** The one topic every gate-passing signal is produced to. */
  kafkaTopic: process.env.KAFKA_TOPIC ?? 'signals',
  /** On for MSK Serverless: authenticate with IAM (SASL/OAUTHBEARER over TLS)
   *  instead of PLAINTEXT. Credentials come from the AWS default chain, never
   *  config — the instance role in production. Off for the local broker. */
  kafkaUseIam: (process.env.KAFKA_USE_IAM ?? '').toLowerCase() === 'true',
  /** Region used to sign the IAM auth token. Only read when `kafkaUseIam`. */
  awsRegion: process.env.AWS_REGION ?? 'us-east-1',

  // --- clickhouse (read-only, dispatcher only) --------------------------------
  // The EDGE still never touches ClickHouse — it produces to Kafka and ClickHouse
  // ingests on its own (Kafka table engine + materialized view, see
  // docker/clickhouse/init). These keys are for the DISPATCHER, a separate
  // process that READS the archive and posts batches to the payments app. It
  // never writes: `signal_log` has exactly one writer, the materialized view.
  clickhouseUrl: process.env.CLICKHOUSE_URL ?? 'http://127.0.0.1:8123',
  clickhouseDatabase: process.env.CLICKHOUSE_DATABASE ?? 'signals',
  clickhouseUser: process.env.CLICKHOUSE_USER ?? 'default',
  clickhousePassword: process.env.CLICKHOUSE_PASSWORD ?? '',

  // --- the payments app (dispatcher only) -------------------------------------
  /** Base URL of the payments app that owns pricing and Postgres. */
  paymentsUrl: process.env.PAYMENTS_URL ?? 'http://127.0.0.1:3001',
  /** Shared secret proving the CALLER is us, sent as `Authorization: Bearer`.
   *  Not a customer credential — it authenticates this service, and both
   *  `/api/internal/settle` and `/api/internal/signals/cursor` require it. */
  internalSecret: process.env.INTERNAL_SETTLE_SECRET ?? '',

  // --- the dispatcher (the 1-minute cron) -------------------------------------
  //
  // Dispatch is a ONE-SHOT process on a timer, not a loop: it reads one window of
  // the archive, posts it to /api/internal/settle in a single call, and exits.
  // There is no watermark, no cursor, no claim and no local state — settle is
  // idempotent on `signalId`, so windows are allowed to overlap and the duplicates
  // are discarded upstream. See docs/IMPLEMENTATION.md.
  /** How far back the window reaches, over `ingested_at`.
   *
   *  Deliberately WIDER than the timer's interval — at the default, 3x. That
   *  overlap is the entire error recovery: a run that fails is covered by the next
   *  two, with nothing persisted to recover from. Every signal is therefore sent
   *  about three times and settle throws away all but the first.
   *
   *  The window is over `ingested_at` and never `received_at`: `received_at` is
   *  the caller's time, stamped by N edge instances off N clocks, and a row can
   *  land here minutes later — a window over it would miss that row for good.
   *  `received_at` is still what settle bills on. */
  dispatchWindowMs: intFromEnv('DISPATCH_WINDOW_MS', 3 * 60 * 1000),
  /** Hard ceiling on rows per run. An OOM GUARD, not a batch size — the whole
   *  window goes to settle in one call, so this bounds only how much a single run
   *  will hold in memory (~150MB of Node heap at the default).
   *
   *  Hitting it MEANS ROWS WERE NOT SENT: the next window has already moved past
   *  some of them, so the run logs at error level and must page someone. The
   *  hourly reconciliation timer covers up to two hours of that; past it, replay
   *  with DISPATCH_SINCE. Raise it only alongside --max-old-space-size. */
  dispatchMaxRows: intFromEnv('DISPATCH_MAX_ROWS', 100_000),
  /** Timeout on the one settle call, in ms. Must sit ABOVE the settle route's own
   *  maxDuration: a call that is still committing must never be abandoned, or the
   *  next window re-sends work that in fact succeeded. */
  dispatchTimeoutMs: intFromEnv('DISPATCH_TIMEOUT_MS', 310_000),
  /** gzip the settle request body. On by default, and effectively required:
   *  Vercel caps a serverless function's request body at 4.5MB, which raw JSON
   *  crosses somewhere around 15k signals. Signal JSON is the same keys repeated
   *  thousands of times, so it compresses roughly 10:1. Off only for debugging
   *  against a server that does not decompress. */
  dispatchGzip: (process.env.DISPATCH_GZIP ?? 'true').toLowerCase() !== 'false',
  /** Manual replay window, ISO-8601, both optional and both empty in normal
   *  operation. Set them and the run ignores DISPATCH_WINDOW_MS and reads exactly
   *  [since, until) over `ingested_at` instead — which is how a gap left by an
   *  outage longer than the reconciliation window gets filled, without a
   *  code change or a cursor to rewind. */
  dispatchSince: process.env.DISPATCH_SINCE ?? '',
  dispatchUntil: process.env.DISPATCH_UNTIL ?? '',

  // --- request limits --------------------------------------------------------
  /** Reject bodies over this size (Fastify bodyLimit -> 413). */
  bodyBytes: intFromEnv('BODY_BYTES', 64 * 1024),
} as const

export type Config = typeof config
