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

  // --- clickhouse (the two workers; never the edge) ---------------------------
  // The EDGE still never touches ClickHouse — it produces to Kafka and nothing
  // else. These keys are read by two separate processes:
  //
  //   the CONSUMER   writes. It drains the topic itself, resolves each signal
  //                  against payments, and archives the row WITH that verdict.
  //   the DISPATCHER reads. One window per run, posted to /api/internal/settle.
  //
  // `signal_log` still has exactly ONE writer — it is the consumer now, not the
  // materialized view, which is gone along with the Kafka table engine. A
  // materialized view cannot make an HTTP call, which is why it had to go.
  clickhouseUrl: process.env.CLICKHOUSE_URL ?? 'http://127.0.0.1:8123',
  clickhouseDatabase: process.env.CLICKHOUSE_DATABASE ?? 'signals',
  clickhouseUser: process.env.CLICKHOUSE_USER ?? 'default',
  clickhousePassword: process.env.CLICKHOUSE_PASSWORD ?? '',

  // --- the payments app (dispatcher only) -------------------------------------
  /** Base URL of the payments app that owns pricing and Postgres. */
  paymentsUrl: process.env.PAYMENTS_URL ?? 'http://127.0.0.1:3001',
  /** Shared secret proving the CALLER is us, sent as `Authorization: Bearer`.
   *  Not a customer credential — it authenticates this service. Required by both
   *  `/api/internal/resolve` (the consumer) and `/api/internal/settle` (the
   *  dispatcher); each exits 2 without it.
   *
   *  It now also protects tenant ATTRIBUTION, not just access: settle trusts the
   *  `organizationId` the consumer resolved instead of re-deriving it, so a leak
   *  of this secret lets a caller name any organisation rather than merely replay
   *  signals. Same trust boundary, larger blast radius. */
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

  // --- the consumer (Kafka -> resolve -> ClickHouse) --------------------------
  //
  // A LONG-LIVED process, unlike the dispatcher — a consumer group member that
  // started and exited every 60s would spend its life rebalancing the group. It
  // replaced ClickHouse's `ENGINE = Kafka` table and its materialized view,
  // because neither can make the HTTP call that resolves a signal.
  /** Consumer group id. Changing it makes a brand-new group with no committed
   *  offsets, which then starts wherever `consumeFromBeginning` says — and
   *  re-resolving the topic is one HTTP call per signal. Change it deliberately
   *  or not at all. */
  consumeGroupId: process.env.CONSUME_GROUP_ID ?? 'signal-resolver',
  /** In-flight resolve calls per batch. THE throughput ceiling of this design:
   *  every signal costs one HTTP round trip to a serverless function. Raising it
   *  buys throughput until payments starts refusing connections. */
  consumeConcurrency: intFromEnv('CONSUME_CONCURRENCY', 16),
  /** Start a new group at the OLDEST offset instead of resuming. True only for a
   *  deliberate full replay — it re-resolves every signal the topic still holds. */
  consumeFromBeginning: (process.env.CONSUME_FROM_BEGINNING ?? '').toLowerCase() === 'true',
  /** Timeout on ONE resolve call. Short on purpose: a slow call blocks a slot in
   *  the concurrency pool, and a timeout is retryable — the message stays on the
   *  topic. */
  resolveTimeoutMs: intFromEnv('RESOLVE_TIMEOUT_MS', 10_000),
  /** Consecutive transient failures on ONE signal, WHILE OTHER CALLS SUCCEED,
   *  before it is archived as PENDING/`RESOLVE_FAILED` and stepped over.
   *
   *  Without this a single message that crashes the resolve route stalls the
   *  entire topic behind it forever, because offsets are committed in order and
   *  that one never becomes committable. */
  resolvePoisonAfter: intFromEnv('RESOLVE_POISON_AFTER', 5),
  /** Nothing at all has resolved successfully for this long -> payments is down,
   *  not a poison message. The consumer exits 1, commits nothing, and systemd
   *  backs off; the signals are safe on the topic meanwhile.
   *
   *  This is the other half of `resolvePoisonAfter`: on a low-traffic topic a
   *  batch can be ONE message, where "every call failed" and "one poison message"
   *  are the same observation. Time apart is what separates them. */
  resolveOutageMs: intFromEnv('RESOLVE_OUTAGE_MS', 120_000),

  // --- request limits --------------------------------------------------------
  /** Reject bodies over this size (Fastify bodyLimit -> 413). */
  bodyBytes: intFromEnv('BODY_BYTES', 64 * 1024),
} as const

export type Config = typeof config
