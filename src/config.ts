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

  // --- the dispatcher --------------------------------------------------------
  /** Signals per settle call. 500 is the route's own enforced cap. A bigger
   *  batch buys NO throughput — settle splits it across `INTERNAL_WORKER`
   *  workers that each walk their chunk one signal at a time — it only lengthens
   *  the wall time and worsens a timeout. */
  dispatchBatchSize: intFromEnv('DISPATCH_BATCH_SIZE', 500),
  /** Settle calls in flight at once. Throughput scales here, not on batch size.
   *  Keep at or below the payments app's `INTERNAL_BATCH_CONCURRENCY`, or the
   *  extra batches only queue for a pool slot. */
  dispatchConcurrency: intFromEnv('DISPATCH_CONCURRENCY', 2),
  /** Nap when a sweep came back short. A FULL batch loops again immediately, so
   *  this is the idle cost, never the throughput ceiling. */
  dispatchIdleMs: intFromEnv('DISPATCH_IDLE_MS', 1000),
  /** How far behind the watermark to re-read. ClickHouse's Kafka engine flushes
   *  in batches, and several edge instances stamp `receivedAt` off their own
   *  clocks, so a row can appear with a timestamp just below one already seen.
   *  Re-reading this window catches it.
   *
   *  Keep it TIGHT. Every settled row inside the window is re-read on every
   *  sweep and then discarded by the known-filter, so the sweep has to page past
   *  them to reach new work; a window far wider than the batch size makes that
   *  paging the dominant cost. 60s comfortably covers a 7.5s flush interval and
   *  any NTP-synced clock skew. Widen it only if rows are demonstrably arriving
   *  later than that, and watch for `sweep.page_cap` in the log. */
  dispatchOverlapMs: intFromEnv('DISPATCH_OVERLAP_MS', 60 * 1000),
  /** Cold-start guard, in signals.
   *
   *  The watermark is `max(receivedAt)` over `SignalStatus`. When that table is
   *  empty there is no watermark, so EVERY unsettled row in the archive counts as
   *  outstanding and the first run backfills the lot. That is right when the
   *  archive is new or the backfill is wanted, and badly wrong when the archive
   *  predates the dispatcher — the difference is a judgement nobody can make from
   *  inside a loop.
   *
   *  So: on a cold start, refuse if the archive is bigger than this and say what
   *  the choices are. Set to 0 to allow any size (the deliberate-backfill case).
   *  Once anything has settled there is a watermark and the guard never fires. */
  dispatchColdStartMax: intFromEnv('DISPATCH_COLD_START_MAX', 1000),

  // --- request limits --------------------------------------------------------
  /** Reject bodies over this size (Fastify bodyLimit -> 413). */
  bodyBytes: intFromEnv('BODY_BYTES', 64 * 1024),
} as const

export type Config = typeof config
