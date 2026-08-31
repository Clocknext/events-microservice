/** The consumer process: a LONG-LIVED Kafka consumer, unlike everything else
 *  under `workers/`.
 *
 *  The dispatcher is a one-shot on a timer. This is not, and cannot be: a
 *  consumer group member that started and exited every 60 seconds would spend its
 *  life rebalancing the group. It is a systemd SERVICE (see
 *  deploy/systemd/signal-consumer.service) and the first always-on process in
 *  this repository besides the edge.
 *
 *  One batch is:
 *
 *    · parse the messages the edge produced
 *    · one /api/internal/resolve call per signal, bounded concurrency
 *    · ONE ClickHouse insert for the resolved prefix
 *    · commit offsets — last, and only for what was actually written
 *
 *  EXIT CODES — systemd, and whatever watches it, read these:
 *    0  clean shutdown on SIGTERM/SIGINT
 *    1  transient: payments unreachable, ClickHouse down, broker gone. Restart.
 *    2  misconfigured: no INTERNAL_SETTLE_SECRET, no KAFKA_BROKERS, or payments
 *       refused our shared secret. Every restart would fail identically, which is
 *       why the unit sets RestartPreventExitStatus=2 and stops LOUDLY instead. */
import { Kafka, type Consumer, type SASLOptions } from 'kafkajs'
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js'
import { config } from '../../config.js'
import { createClickHouseWriter } from '../../client/clickhouse.js'
import { MisconfiguredError } from '../../client/payments-client.js'
import { resolveSignal } from '../../client/resolve-client.js'
import { createArchiveWriter } from './consume.archive.js'
import { processBatch } from './consume.service.js'
import type { ConsumeDeps, ResolveHealth } from './consume.schema.js'
import type { SignalMessage } from '../../modules/signal/signal.schema.js'

function line(event: string, detail: Record<string, unknown>): void {
  console.log(JSON.stringify({ at: new Date().toISOString(), event, ...detail }))
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/** Payments has answered nothing at all for `RESOLVE_OUTAGE_MS`. That is an
 *  outage, not a poison message, and the correct response is to commit nothing
 *  and let systemd back off — every signal is safe on the topic meanwhile. */
export class ResolveUnavailableError extends Error {}

/** MSK Serverless auth, duplicated from `plugins/kafka.ts` on purpose: a worker
 *  imports no plugin (AGENTS.md), and the alternative — a shared module the
 *  Fastify plugin and this process both reach into — would couple the edge's
 *  lifecycle to a process that has none. Eight lines is the cheaper coupling. */
function iamSasl(): SASLOptions {
  return {
    mechanism: 'oauthbearer',
    oauthBearerProvider: async () => {
      const { token } = await generateAuthToken({ region: config.awsRegion })
      return { value: token }
    },
  }
}

/** In-memory, and deliberately not persisted: these counters only need to outlive
 *  a batch. A restart resetting them is CORRECT — after a restart we genuinely do
 *  not know yet whether a message is poison or the route was simply down.
 *
 *  The map holds only signals currently failing; a signal's entry is dropped the
 *  moment it resolves, so it stays bounded to the stuck window rather than to
 *  everything that ever failed. */
export function createResolveHealth(now: () => number = Date.now): ResolveHealth {
  // Seeded from creation so a process that starts mid-outage gets a full grace
  // window before it exits — but `answered` stays false until a call actually
  // succeeds, because "never answered" must never read as "answering fine".
  let lastSuccessAt = now()
  let answered = false
  // A monotonic COUNT of successes, not a timestamp. `lastSuccessAt` has
  // millisecond resolution, and batches on a busy topic complete inside one
  // millisecond — so "has a success landed since the streak began" compared as
  // timestamps can tie, and a genuine poison message would never be quarantined.
  // A counter cannot tie.
  let successes = 0
  // Per signal: how many times in a row it has failed, and the success count when
  // that streak STARTED. The second field is what makes `succeededSinceStreak`
  // exact.
  const failures = new Map<string, { count: number; successesAtStreakStart: number }>()
  return {
    recordSuccess(signalId: string): void {
      lastSuccessAt = now()
      successes += 1
      answered = true
      // Per-signal, NOT a global clear. Poison is defined by failing while other
      // calls succeed, so clearing every counter on any success would erase the
      // one signal the count exists to catch.
      failures.delete(signalId)
    },
    recordFailure(signalId: string): number {
      const existing = failures.get(signalId)
      if (existing) {
        existing.count += 1
        return existing.count
      }
      failures.set(signalId, { count: 1, successesAtStreakStart: successes })
      return 1
    },
    succeededSinceStreak(signalId: string): boolean {
      const streak = failures.get(signalId)
      if (!streak) return false
      // Strictly MORE successes than when the streak began. A success that
      // predates it says nothing about whether the route is answering now.
      return successes > streak.successesAtStreakStart
    },
    msSinceSuccess: () => now() - lastSuccessAt,
    hasAnswered: () => answered,
  }
}

/** One Kafka message back into the envelope the edge produced.
 *
 *  Returns null for anything that will not parse or carries no `signalId`. The
 *  edge is the only producer and always writes both, so this is corruption rather
 *  than a case — and it is skipped rather than quarantined because a row in
 *  `signal_log` needs a `signal_id` to exist at all. It is logged at the point of
 *  loss, which is the only honest thing to do with a message we cannot key. */
export function parseSignalMessage(value: Buffer | null): SignalMessage | null {
  if (!value) return null
  let parsed: unknown
  try {
    parsed = JSON.parse(value.toString('utf8'))
  } catch {
    return null
  }
  if (parsed === null || typeof parsed !== 'object') return null
  const message = parsed as Partial<SignalMessage>
  if (typeof message.signalId !== 'string' || message.signalId === '') return null
  if (typeof message.receivedAt !== 'string') return null
  if (message.body === null || typeof message.body !== 'object') return null
  return {
    signalId: message.signalId,
    receivedAt: message.receivedAt,
    apiKeyHash: typeof message.apiKeyHash === 'string' ? message.apiKeyHash : '',
    body: message.body,
  }
}

export function buildDeps(health: ResolveHealth): ConsumeDeps {
  return {
    resolve: { resolve: resolveSignal },
    archive: createArchiveWriter(createClickHouseWriter()),
    health,
    config: {
      concurrency: config.consumeConcurrency,
      poisonAfter: config.resolvePoisonAfter,
    },
    log: line,
  }
}

export async function run(): Promise<void> {
  // Refuse rather than discover it per signal: without the secret every resolve
  // call is a 401, and a 401 is the one answer this process must never treat as a
  // customer's problem.
  if (!config.internalSecret) {
    throw new MisconfiguredError(
      'INTERNAL_SETTLE_SECRET is not set — the consumer cannot authenticate to payments',
    )
  }
  if (config.kafkaBrokers.length === 0) {
    throw new MisconfiguredError('KAFKA_BROKERS is not set — the consumer has nothing to drain')
  }

  const health = createResolveHealth()
  const deps = buildDeps(health)

  const kafka = new Kafka({
    clientId: `${config.kafkaClientId}-consumer`,
    brokers: config.kafkaBrokers,
    ...(config.kafkaUseIam ? { ssl: true, sasl: iamSasl() } : {}),
  })

  const consumer: Consumer = kafka.consumer({
    groupId: config.consumeGroupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
    // Two attempts, then crash and let systemd restart. A long in-process retry
    // ladder would only hide an outage the exit code is supposed to announce.
    retry: { retries: 2, initialRetryTime: 1_000 },
  })

  await consumer.connect()
  await consumer.subscribe({
    topic: config.kafkaTopic,
    // `false` is "resume from this group's committed offsets". True is ONLY for a
    // deliberate full replay — it re-resolves every signal on the topic, one HTTP
    // call each.
    fromBeginning: config.consumeFromBeginning,
  })

  line('consumer.started', {
    brokers: config.kafkaBrokers,
    topic: config.kafkaTopic,
    groupId: config.consumeGroupId,
    concurrency: config.consumeConcurrency,
    iam: config.kafkaUseIam,
  })

  // A MisconfiguredError recorded AT THE POINT IT IS THROWN, because it does not
  // survive the trip out through kafkajs. An error thrown from `eachBatch` is
  // retried, and once the retries are exhausted kafkajs raises
  // `KafkaJSNumberOfRetriesExceeded`, which COPIES the original's message but is a
  // different object (the original hangs off `.originalError`). So the message
  // looked right while `instanceof` was false, and a refused shared secret exited
  // 1 instead of 2 — systemd would have restarted it every 5s forever instead of
  // stopping and showing a human the reason.
  //
  // Reading `.originalError` would also work and would also be a bet on kafkajs's
  // internals. This does not need to be.
  let fatal: Error | null = null

  // Resolves on a signal, rejects when kafkajs gives up. The process stays alive
  // on the consumer's own event loop until one of the two happens.
  let settle: (error?: Error) => void = () => {}
  const finished = new Promise<void>((resolve, reject) => {
    settle = (error?: Error) => (error ? reject(error) : resolve())
  })

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    // `fatal` wins: it is the error that was actually THROWN, where `payload.error`
    // is kafkajs's retry wrapper around it. Rejecting with the real one is what
    // lets the exit-code check below use a plain `instanceof`.
    settle(
      fatal ??
        (payload.error instanceof Error ? payload.error : new Error(String(payload.error))),
    )
  })

  const stop = (signal: string) => {
    line('consumer.stopping', { signal })
    void consumer.disconnect().then(() => settle())
  }
  process.once('SIGTERM', () => stop('SIGTERM'))
  process.once('SIGINT', () => stop('SIGINT'))

  await consumer.run({
    // This process decides when an offset is safe…
    autoCommit: false,
    // …and kafkajs must not decide it for us.
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      if (!isRunning() || isStale()) return
      try {
      // Parse first, keeping a map back to the raw offsets: the prefix rule below
      // is positional over TOPIC order, so the two arrays cannot drift.
      const parsed: SignalMessage[] = []
      const rawIndexOf: number[] = []
      let unparsable = 0
      for (const [index, raw] of batch.messages.entries()) {
        const message = parseSignalMessage(raw.value)
        if (!message) {
          unparsable += 1
          line('message.unparsable', { offset: raw.offset, partition: batch.partition })
          continue
        }
        parsed.push(message)
        rawIndexOf.push(index)
      }

      // `heartbeat` is bound here because a batch of 500 signals is 500 HTTP
      // calls — long enough that the broker decides we are dead and rebalances
      // the group mid-batch without it.
      const outcome = await processBatch({ ...deps, onProgress: heartbeat }, parsed)

      // Everything strictly before the first unresolved message is written and
      // therefore safe. Unparsable messages inside that span are safe too — they
      // are being dropped deliberately, and leaving them uncommitted would stall
      // the topic on a message that can never be keyed into a row.
      const safeUntil =
        outcome.stoppedAt === -1
          ? batch.messages.length
          : (rawIndexOf[outcome.stoppedAt] ?? batch.messages.length)

      // `resolveOffset` is bookkeeping for `eachBatchAutoResolve: false` — it tells
      // kafkajs which messages NOT to refetch. It does not commit anything.
      for (let index = 0; index < safeUntil; index += 1) {
        const raw = batch.messages[index]
        if (raw) resolveOffset(raw.offset)
      }

      // COMMIT EXPLICITLY, and only now that the insert has landed.
      //
      // NOT `commitOffsetsIfNecessary()`: that helper honours the autoCommit
      // configuration, and with `autoCommit: false` — which this consumer sets
      // deliberately, so nothing commits behind its back — it is a silent no-op.
      // Observed against a live broker: batches logged `committed: 7` while the
      // group's CURRENT-OFFSET never moved off its seeded value, so every restart
      // re-read and re-resolved the whole backlog. One HTTP call per signal, paid
      // again on every restart, forever.
      //
      // The committed offset is the NEXT one to read, hence +1. BigInt because
      // Kafka offsets outgrow Number.MAX_SAFE_INTEGER on a long-lived topic.
      const lastSafe = safeUntil > 0 ? batch.messages[safeUntil - 1] : undefined
      if (lastSafe) {
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: (BigInt(lastSafe.offset) + 1n).toString(),
          },
        ])
      }

      line('batch', {
        partition: batch.partition,
        ...outcome,
        unparsable,
        committed: safeUntil,
      })

      if (outcome.stoppedAt !== -1) {
        // Nothing has answered in a long time: an outage, not a poison message.
        // Commit nothing further and let the unit back off.
        if (health.msSinceSuccess() >= config.resolveOutageMs) {
          throw new ResolveUnavailableError(
            `no resolve call has succeeded in ${health.msSinceSuccess()}ms — payments looks down`,
          )
        }
        // The unresolved suffix stays uncommitted and is refetched. Pause briefly
        // so a persistent failure does not become a hot loop against payments.
        await delay(1_000)
      }
      } catch (error) {
        if (error instanceof MisconfiguredError) fatal = error
        throw error
      }
    },
  })

  await finished
}

// Only run when this file is the entry point, so a test can import `run`,
// `buildDeps` and `createResolveHealth`. Compared as a URL, not by substring:
// `consume.runner.test.ts` contains `consume.runner`, and a substring check
// started a real consumer the moment its own test file was executed.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  run()
    .then(() => {
      line('consumer.stopped', {})
      process.exit(0)
    })
    .catch((error: unknown) => {
      const misconfigured = error instanceof MisconfiguredError
      line('consumer.failed', {
        error: error instanceof Error ? error.message : String(error),
        exit: misconfigured ? 2 : 1,
      })
      process.exit(misconfigured ? 2 : 1)
    })
}
