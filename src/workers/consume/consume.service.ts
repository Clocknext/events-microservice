/** The consumer's logic. A pure function over its ports — no kafkajs, no HTTP
 *  client, no ClickHouse driver. The runner binds the real ones; a test hands it
 *  fakes, exactly as `runOnce` does for the dispatcher.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  ONE BATCH: RESOLVE IT IN ONE CALL, WRITE THE PREFIX, LET THE RUNNER COMMIT
 *
 *  The order is load-bearing and it is the one thing here that cannot be
 *  rearranged:
 *
 *    1. resolve every message — ONE call per chunk, not one per message
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
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHAT CHANGED WHEN RESOLVE LEARNED TO TAKE A BATCH
 *
 *  Step 1 used to be a bounded-concurrency POOL of one HTTP call per message —
 *  the throughput ceiling of the whole pipeline, and a named Kafka anti-pattern.
 *  It is now one call per chunk of `batchMax`.
 *
 *  That deleted more than the pool. Poison detection used to be INFERRED: with
 *  one call per signal, "this message is poison" and "the route is down" look
 *  identical from a single failure, so a signal counted as poison only if some
 *  other call had succeeded since its streak began. One call per batch makes it
 *  direct — a per-item transient inside a 200 is proof the route answered — so the
 *  rule below reads `verdict.routeAnswered` instead of interrogating a monotonic
 *  success counter.
 *  ───────────────────────────────────────────────────────────────────────────── */
import type { SignalMessage } from '../../modules/signal/signal.schema.js'
import type {
  BatchOutcome,
  ConsumeDeps,
  ResolveVerdict,
  SignalLogInsert,
} from './consume.schema.js'

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

/** Did this chunk's CALL fail, as opposed to some of its signals?
 *
 *  `routeAnswered: false` on every item is exactly that condition — the client
 *  sets it when the request itself failed (timeout, 5xx, an unusable response).
 *  Worth detecting because the prefix cannot extend past such a chunk anyway, so
 *  every later chunk is a request paid for nothing. */
function callFailed(verdicts: readonly ResolveVerdict[]): boolean {
  return (
    verdicts.length > 0 &&
    verdicts.every((verdict) => verdict.kind === 'transient' && !verdict.routeAnswered)
  )
}

/**
 * Resolves and archives one batch.
 *
 * Throws only what the ports throw — `MisconfiguredError` and `BadBatchError` from
 * the resolver (our shared secret and our envelope, neither about any one signal)
 * and whatever ClickHouse throws on a failed insert. All three must propagate: the
 * runner turns them into an exit code, and swallowing any would commit offsets for
 * rows that never landed.
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

  // Positional throughout: the prefix rule below depends on TOPIC order, and the
  // resolver returns its verdicts in the order the messages were sent.
  const verdicts = new Array<ResolveVerdict | null>(messages.length).fill(null)
  const failures = new Array<number>(messages.length).fill(0)

  // ONE call per chunk. A poll under `batchMax` is exactly one call; the loop
  // exists because a poll can exceed the route's own cap.
  const width = Math.max(1, config.batchMax)
  for (let start = 0; start < messages.length; start += width) {
    // Between chunks, not between signals. One call cannot outlast the broker's
    // session timeout the way 500 sequential ones could.
    await deps.onProgress?.()

    const chunk = messages.slice(start, start + width)
    const answered = await resolve.resolveBatch(chunk)

    for (const [offset, verdict] of answered.entries()) {
      const index = start + offset
      const message = messages[index]
      if (!message) continue
      verdicts[index] = verdict
      // Counted ONCE, here — the loop below only reads what this recorded. A
      // transient failure is the only kind that says anything about the route's
      // health; a rejection is a perfectly healthy answer.
      if (verdict.kind === 'transient') failures[index] = health.recordFailure(message.signalId)
      else health.recordSuccess(message.signalId)
    }

    // The call itself failed, so the prefix stops inside this chunk no matter
    // what the remaining chunks would say. Stop asking.
    if (callFailed(answered)) break
  }

  const rows: SignalLogInsert[] = []
  let stoppedAt = -1
  let processing = 0
  let pending = 0
  let quarantined = 0

  for (const [index, message] of messages.entries()) {
    const verdict = verdicts[index]
    if (!verdict) {
      // A chunk was never asked (the call above failed and we stopped early), or
      // the resolver returned short. Either way this message has no answer, so
      // the prefix ends here and it is redelivered.
      stoppedAt = index
      break
    }

    if (verdict.kind === 'transient') {
      // Poison, or an outage? BOTH conditions are still required, but the second
      // is now EVIDENCE rather than inference:
      //
      //   the signal has failed `poisonAfter` times in a row              AND
      //   the route ANSWERED this time (a per-item transient inside a 200)
      //
      // `routeAnswered` is what the old `succeededSinceStreak` was trying to
      // approximate. It had to compare a monotonic success COUNT — five failures
      // accumulate in about five seconds, so an elapsed-time proxy let a success
      // from 70 seconds earlier make a route that was demonstrably down look
      // alive, and archived a good signal as a caller error during a real outage.
      // With one call per batch there is nothing to approximate: either the call
      // came back or it did not.
      //
      // When it did not, nothing in the batch is poison, so the batch stalls and
      // the RUNNER decides: once nothing has answered for RESOLVE_OUTAGE_MS it
      // exits, systemd backs off, and every signal stays safe on the topic.
      if (!verdict.routeAnswered || (failures[index] ?? 0) < config.poisonAfter) {
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
