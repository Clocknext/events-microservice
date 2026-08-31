/** The consumer's call to the payments app's `/api/internal/resolve` — ONE call
 *  per Kafka batch, and the only thing the consumer talks to besides ClickHouse.
 *
 *  Like `payments-client.ts` it authenticates the CALLER with the shared secret
 *  as `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`. No customer credential is
 *  ever sent: each signal's SHA-256 digest rides in its own `apiKeyHash` field
 *  and the payments app resolves the organisation from it, so the raw `cnk_…` key
 *  never leaves the edge process that first saw it.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  ONE CALL PER BATCH, NOT PER SIGNAL
 *
 *  This used to be one HTTP call per message. That is a named Kafka
 *  anti-pattern — one synchronous external call per message caps throughput at
 *  (concurrency ÷ latency) and head-of-line blocks everything behind it on the
 *  partition. Measured against real data it topped out near 150 signals/sec while
 *  costing payments two Postgres queries per signal. A batch of 500 is now one
 *  call and two queries.
 *
 *  The digest moved from an `X-Api-Key-Hash` HEADER into each item, and it had to:
 *  one Kafka batch holds signals from many different customers' keys, so a single
 *  header cannot describe them.
 *  ─────────────────────────────────────────────────────────────────────────────
 *  THREE OUTCOMES PER SIGNAL, AND CONFLATING ANY TWO BREAKS THE PIPELINE
 *
 *    ok        the key resolved and the body passed — PROCESSING, with a
 *              TRUSTED `organizationId` the dispatcher forwards to settle.
 *    rejected  the caller must change something — PENDING, terminal, archived
 *              with the reason payments gave so it stays readable.
 *    transient OURS. The message must be redelivered and NOTHING committed;
 *              archiving it as a rejection would refuse a perfectly good signal.
 *
 *  These are now per-item FIELDS, not HTTP statuses, and that closes a bug class
 *  rather than merely tidying the wire. When a bad customer key could answer
 *  `401`, this client read it as "payments refused OUR shared secret", threw
 *  `MisconfiguredError`, and the runner exited 2 — so one customer sending an
 *  expired key stopped ingestion for everyone. There is now no shape in which a
 *  customer's mistake can wear a status that also means something about us: a
 *  `401` on this route can only be ours.
 *  ───────────────────────────────────────────────────────────────────────────── */
import { config } from '../config.js'
import { MisconfiguredError } from './payments-client.js'
import type { SignalMessage } from '../modules/signal/signal.schema.js'

/** What resolve decided about ONE signal. `organizationId` is present on a
 *  rejection too — the route resolves the key BEFORE judging the body, so a
 *  validation failure is still attributable to the org that sent it. It is `''`
 *  only when the key itself never resolved, which is the one case with no tenant
 *  to name. */
export type ResolveVerdict =
  | { kind: 'ok'; organizationId: string }
  | { kind: 'rejected'; organizationId: string; errorCode: string; errorMessage: string }
  | {
      kind: 'transient'
      detail: string
      /** Did the ROUTE answer at all?
       *
       *  This is what replaced `succeededSinceStreak`, and it is direct evidence
       *  where that was statistical inference.
       *
       *  `true`  — the batch call returned 200 and THIS item came back transient.
       *            The route is demonstrably alive, so a signal that keeps
       *            failing here is failing on its own: a poison candidate.
       *  `false` — the CALL failed (timeout, 5xx, an unusable response). Nothing
       *            in the batch is poison, because nothing was answered. The
       *            batch stalls and the runner's outage timer decides.
       *
       *  Conflating the two is how a good signal gets archived as a caller error
       *  during a real outage — which is exactly what the old per-signal design
       *  had to infer from a monotonic success count, and got wrong once. */
      routeAnswered: boolean
    }

/** The ENVELOPE was refused — `signals` was not an array, was empty, or was over
 *  the route's cap. That is OUR bug, not any signal's, and it will fail
 *  identically on every retry, so it must never be read as "every signal in this
 *  batch is invalid". The runner turns it into exit 2. */
export class BadBatchError extends Error {}

/** The payments app's public v1 envelope, as `apiSuccess`/`apiError` build it. */
interface ResolveEnvelope {
  statusDetail?: { message?: string }
  result?: Record<string, unknown> | null
}

function readEnvelope(text: string): ResolveEnvelope {
  try {
    const parsed = JSON.parse(text) as unknown
    if (parsed === null || typeof parsed !== 'object') return {}
    return parsed as ResolveEnvelope
  } catch {
    return {}
  }
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

/** Every signal in the batch gets the same transient verdict. Used when the CALL
 *  failed rather than any one signal — a timeout, a 5xx, an unreadable body. */
function allTransient(count: number, detail: string): ResolveVerdict[] {
  // `routeAnswered: false` — by definition. This is the CALL failing, so no
  // signal in the batch can be called poison against it.
  return Array.from({ length: count }, () => ({
    kind: 'transient' as const,
    detail,
    routeAnswered: false,
  }))
}

/** One item's verdict, read out of the response array. */
function verdictOf(entry: Record<string, unknown>): ResolveVerdict {
  const outcome = str(entry.outcome)

  if (outcome === 'ok') {
    const organizationId = str(entry.organizationId)
    // An `ok` that names no organisation is a contract violation, not a
    // rejection. Retrying is right: refusing the signal would lose it over our
    // own bug.
    if (organizationId === '') {
      return {
        kind: 'transient',
        detail: 'resolve returned ok with no organizationId',
        routeAnswered: true,
      }
    }
    return { kind: 'ok', organizationId }
  }

  if (outcome === 'rejected') {
    return {
      kind: 'rejected',
      organizationId: str(entry.organizationId),
      errorCode: str(entry.errorCode) || 'REJECTED',
      errorMessage: str(entry.errorMessage) || 'resolve rejected the signal',
    }
  }

  if (outcome === 'transient') {
    return {
      kind: 'transient',
      detail: str(entry.errorMessage) || 'resolve could not answer for this signal',
      routeAnswered: true,
    }
  }

  // An outcome we do not recognise. Transient rather than rejected: an unknown
  // answer is not evidence the caller did anything wrong.
  return {
    kind: 'transient',
    detail: `resolve returned an unknown outcome: ${outcome || '(none)'}`,
    routeAnswered: true,
  }
}

/**
 * Resolves a WHOLE batch in one call: is each key ours, is each body acceptable,
 * and does each named customer belong to that key's organisation?
 *
 * Returns one verdict per message, POSITIONALLY, in the order the messages were
 * sent. Never throws for a signal-level outcome — a caller error and an outage
 * are both verdicts, because the consumer has to tell them apart and act
 * differently. It throws only for things that are not about any signal:
 * `MisconfiguredError` (our shared secret) and `BadBatchError` (our envelope).
 *
 * MISMATCHED RESULTS ARE A WHOLE-BATCH TRANSIENT, NOT A GUESS. The response
 * carries `signalId` on every item and this checks it against the request
 * position. A shifted array would archive verdicts against the wrong rows, and on
 * this path that means attributing usage — money — to the wrong tenant. Better to
 * redeliver the batch than to write one wrong row.
 */
export async function resolveBatch(
  messages: readonly SignalMessage[],
): Promise<ResolveVerdict[]> {
  if (messages.length === 0) return []

  const url = new URL('/api/internal/resolve', config.paymentsUrl)
  const body = JSON.stringify({
    signals: messages.map((message) => ({
      signalId: message.signalId,
      // The DIGEST, never the key. Per item, because a batch spans keys.
      apiKeyHash: message.apiKeyHash,
      // The caller's body verbatim — the same bytes that become
      // `signal_log.payload`.
      body: message.body,
    })),
  })

  let res: Response
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${config.internalSecret}`,
      },
      body,
      signal: AbortSignal.timeout(config.resolveTimeoutMs),
    })
  } catch (error) {
    // A timeout, a DNS failure, a refused connection. Ours, always retryable.
    const detail = error instanceof Error ? error.message : String(error)
    return allTransient(messages.length, detail)
  }

  const text = await res.text()
  const envelope = readEnvelope(text)
  const message = envelope.statusDetail?.message ?? `resolve answered ${res.status}`

  // OUR credential, not any customer's. Waiting cannot fix it, and no per-signal
  // outcome can produce this status any more.
  if (res.status === 401) {
    throw new MisconfiguredError(
      `payments refused our shared secret on /api/internal/resolve: ${message}`,
    )
  }

  // The envelope itself was refused. Also ours, and also never self-healing.
  if (res.status === 400) {
    throw new BadBatchError(`resolve refused our batch envelope: ${message}`)
  }

  if (res.status === 413) {
    throw new BadBatchError(
      `resolve refused a ${body.length}-byte batch of ${messages.length} signals: ${message}. ` +
        'Lower RESOLVE_BATCH_MAX.',
    )
  }

  if (!res.ok) {
    // 5xx is the payments app's own failure — its database, or an unhandled
    // throw. Every signal retries; none is refused over our upstream's hiccup.
    return allTransient(messages.length, `resolve answered ${res.status}: ${message}`)
  }

  const signals = envelope.result?.signals
  if (!Array.isArray(signals) || signals.length !== messages.length) {
    return allTransient(
      messages.length,
      `resolve answered 200 with ${
        Array.isArray(signals) ? signals.length : 'no'
      } results for ${messages.length} signals`,
    )
  }

  const verdicts: ResolveVerdict[] = []
  for (const [index, raw] of signals.entries()) {
    const expected = messages[index]?.signalId ?? ''
    if (raw === null || typeof raw !== 'object') {
      return allTransient(messages.length, `resolve returned a non-object result at index ${index}`)
    }
    const entry = raw as Record<string, unknown>
    // Positional AND identified. See the note above: a shift here is a
    // mis-attribution, so the whole batch is redelivered rather than trusted.
    if (str(entry.signalId) !== expected) {
      return allTransient(
        messages.length,
        `resolve returned results out of order: index ${index} named ` +
          `${str(entry.signalId) || '(none)'}, expected ${expected}`,
      )
    }
    verdicts.push(verdictOf(entry))
  }

  return verdicts
}
