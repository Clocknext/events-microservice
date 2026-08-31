/** The consumer's logic. A pure function over its ports — no kafkajs, no HTTP
 *  client, no ClickHouse driver. The runner binds the real ones; a test hands it
 *  fakes, exactly as `runOnce` does for the dispatcher.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  ONE BATCH: RESOLVE EVERY SIGNAL, WRITE THE PREFIX, LET THE RUNNER COMMIT
 *
 *  The order is load-bearing and it is the one thing here that cannot be
 *  rearranged:
 *
 *    1. resolve every message (bounded concurrency, heartbeating between calls)
 *    2. write the resolved PREFIX to ClickHouse in ONE insert
 *    3. the RUNNER commits offsets — only for what step 2 actually wrote
 *
 *  Commit before the insert and a crash in between loses the signal outright, with
 *  the broker believing it was handled. Commit after, and a crash redelivers it —
 *  the re-inserted row collapses on merge, because `signal_log` is a
 *  ReplacingMergeTree keyed on `(received_at, signal_id)`. At-least-once is the
 *  safe direction; at-most-once is not, because an accepted signal is billable.
 *
 *  This function never commits and never touches an offset. It reports what it
 *  wrote and where it stopped, and the runner decides — the same split that lets
 *  `runOnce` be tested without a systemd timer.
 *  ───────────────────────────────────────────────────────────────────────────── */
import type { SignalMessage } from '../../modules/signal/signal.schema.js'
import type {
  BatchOutcome,
  ConsumeDeps,
  ResolveVerdict,
  SignalLogInsert,
} from './consume.schema.js'

/** A fixed pool of workers over a shared cursor, rather than `Promise.all` over
 *  everything at once: a batch can hold thousands of messages and each one is an
 *  HTTP call, so unbounded concurrency would open thousands of sockets against a
 *  serverless function and be refused. */
async function mapWithConcurrency<T>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  if (items.length === 0) return
  let cursor = 0
  const width = Math.max(1, Math.min(limit, items.length))
  await Promise.all(
    Array.from({ length: width }, async () => {
      for (;;) {
        const index = cursor
        cursor += 1
        if (index >= items.length) return
        const item = items[index]
        if (item === undefined) return
        await fn(item, index)
      }
    }),
  )
}

/**
 * Turns a resolved message into the row that goes in the archive.
 *
 * The verdict decides three columns and nothing else — the payload, the id, the
 * stamp and the digest are the caller's and the edge's, and are copied through
 * untouched. `customer_id` is a LIFTED COPY: it also stays inside `payload`,
 * because `payload` is the archive of what was actually sent and lifting a field
 * OUT of it would make the archive a summary instead of a record.
 */
export function toRow(
  message: SignalMessage,
  verdict: Exclude<ResolveVerdict, { kind: 'transient' }>,
): SignalLogInsert {
  const customerId = message.body?.customerId
  const base = {
    signal_id: message.signalId,
    received_at: message.receivedAt,
    api_key_hash: message.apiKeyHash,
    customer_id: typeof customerId === 'string' ? customerId : '',
    payload: JSON.stringify(message.body),
    version: 1,
  }
  return verdict.kind === 'ok'
    ? {
        ...base,
        organization_id: verdict.organizationId,
        status: 'PROCESSING',
        error_code: '',
        error_message: '',
      }
    : {
        ...base,
        organization_id: verdict.organizationId,
        status: 'PENDING',
        error_code: verdict.errorCode,
        error_message: verdict.errorMessage,
      }
}

/**
 * Resolves and archives one batch.
 *
 * Throws only what the ports throw — `MisconfiguredError` from the resolver (our
 * shared secret is wrong, which is not about any one signal) and whatever
 * ClickHouse throws on a failed insert. Both must propagate: the runner turns
 * them into an exit code, and swallowing either would commit offsets for rows
 * that never landed.
 */
export async function processBatch(
  deps: ConsumeDeps,
  messages: readonly SignalMessage[],
): Promise<BatchOutcome> {
  const { resolve, archive, health, config } = deps
  const now = deps.now ?? Date.now
  const startedAt = now()

  const empty: BatchOutcome = {
    read: 0, processing: 0, pending: 0, quarantined: 0, stoppedAt: -1, ms: 0,
  }
  if (messages.length === 0) return { ...empty, ms: now() - startedAt }

  // Resolve concurrently; results are written back BY INDEX because the pool
  // finishes out of order and the prefix rule below depends on topic order.
  const verdicts = new Array<ResolveVerdict | null>(messages.length).fill(null)
  const failures = new Array<number>(messages.length).fill(0)

  await mapWithConcurrency(messages, config.concurrency, async (message, index) => {
    await deps.onProgress?.()
    const verdict = await resolve.resolve(message)
    verdicts[index] = verdict
    // Counted ONCE, here — the loop below only reads what this recorded. A
    // transient failure is the only kind that says anything about the route's
    // health; a rejection is a perfectly healthy answer.
    if (verdict.kind === 'transient') failures[index] = health.recordFailure(message.signalId)
    else health.recordSuccess(message.signalId)
  })

  const rows: SignalLogInsert[] = []
  let stoppedAt = -1
  let processing = 0
  let pending = 0
  let quarantined = 0

  for (const [index, message] of messages.entries()) {
    const verdict = verdicts[index]
    if (!verdict) {
      // The pool exited early — only possible if a worker threw, which means the
      // throw is already on its way up. Stop the prefix here regardless.
      stoppedAt = index
      break
    }

    if (verdict.kind === 'transient') {
      // Poison, or an outage? BOTH conditions are required, and neither alone is
      // enough:
      //
      //   the signal has failed `poisonAfter` times in a row              AND
      //   some OTHER call has succeeded SINCE that streak began
      //
      // The second is the definition of poison, not a safety margin. An
      // elapsed-time proxy is not equivalent and was observed failing: five
      // failures accumulate in about five seconds, so a success from 70 seconds
      // ago still sat inside a two-minute outage window and made a route that was
      // demonstrably down look alive — archiving a good signal as a caller error
      // during a real outage.
      //
      // With nothing succeeding there is nothing to call this signal poison
      // against, so the batch stalls and the RUNNER decides: once nothing has
      // answered for RESOLVE_OUTAGE_MS it exits, systemd backs off, and every
      // signal stays safe on the topic.
      const routeIsAnswering = health.succeededSinceStreak(message.signalId)
      if ((failures[index] ?? 0) < config.poisonAfter || !routeIsAnswering) {
        stoppedAt = index
        break
      }
      rows.push(
        toRow(message, {
          kind: 'rejected',
          // No org: we never got an answer, so there is nobody to attribute it to.
          organizationId: '',
          errorCode: 'RESOLVE_FAILED',
          errorMessage: verdict.detail,
        }),
      )
      quarantined += 1
      deps.log?.('signal.quarantined', {
        signalId: message.signalId,
        failures: failures[index] ?? 0,
        detail: verdict.detail,
      })
      continue
    }

    rows.push(toRow(message, verdict))
    if (verdict.kind === 'ok') processing += 1
    else pending += 1
  }

  // ONE insert for the whole prefix, and it lands BEFORE the runner commits
  // anything. An empty prefix still calls through: the writer no-ops, and a test
  // asserting "nothing was committed" reads better than a hidden early return.
  await archive.write(rows)

  return {
    read: messages.length,
    processing,
    pending,
    quarantined,
    stoppedAt,
    ms: now() - startedAt,
  }
}
