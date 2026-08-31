/** The consumer's call to the payments app's `/api/internal/resolve` — one per
 *  signal, and the only thing the consumer talks to besides ClickHouse.
 *
 *  Like `payments-client.ts` it authenticates the CALLER with the shared secret
 *  as `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`. No customer credential is
 *  ever sent: the signal's SHA-256 digest rides in `X-Api-Key-Hash` and the
 *  payments app resolves the organisation from it, so the raw `cnk_…` key never
 *  leaves the edge process that first saw it.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  THREE OUTCOMES, AND CONFLATING ANY TWO OF THEM BREAKS THE PIPELINE
 *
 *    ok        the key resolved and the body passed — PROCESSING, with a
 *              TRUSTED `organizationId` the dispatcher forwards to settle.
 *    rejected  the caller must change something — PENDING, terminal, archived
 *              with the reason payments gave so it stays readable.
 *    transient OURS. The message must be redelivered and NOTHING committed;
 *              archiving it as a rejection would refuse a perfectly good signal.
 *
 *  A `401` is never a customer problem. It means OUR shared secret is wrong, and
 *  every signal on the topic would otherwise be archived as a caller error until
 *  a human noticed — so it throws `MisconfiguredError`, which the runner turns
 *  into exit code 2. That is why the route separates `401` (us) from `403` (the
 *  customer's key): before it did, the two were distinguishable only by matching
 *  a human-readable sentence.
 *  ───────────────────────────────────────────────────────────────────────────── */
import { config } from '../config.js'
import { MisconfiguredError } from './payments-client.js'
import type { SignalMessage } from '../modules/signal/signal.schema.js'

/** What one resolve call decided. `organizationId` is present on a rejection too
 *  — the route resolves the key BEFORE judging the body, so a validation failure
 *  is still attributable to the org that sent it. It is `''` only when the key
 *  itself never resolved, which is the one case with no tenant to name. */
export type ResolveVerdict =
  | { kind: 'ok'; organizationId: string }
  | { kind: 'rejected'; organizationId: string; errorCode: string; errorMessage: string }
  | { kind: 'transient'; detail: string }

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

/**
 * Resolves ONE signal: is this key ours, and is this body acceptable?
 *
 * Never throws for a signal-level outcome — a caller error and an outage are both
 * returned, because the consumer has to tell them apart and act differently. The
 * single exception is `MisconfiguredError`, which is not about this signal at all.
 */
export async function resolveSignal(message: SignalMessage): Promise<ResolveVerdict> {
  const url = new URL('/api/internal/resolve', config.paymentsUrl)

  let res: Response
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${config.internalSecret}`,
        // The DIGEST, never the key.
        'x-api-key-hash': message.apiKeyHash,
      },
      // The caller's body verbatim — the same bytes that become `signal_log.payload`.
      body: JSON.stringify(message.body),
      signal: AbortSignal.timeout(config.resolveTimeoutMs),
    })
  } catch (error) {
    // A timeout, a DNS failure, a refused connection. Ours, always retryable.
    return { kind: 'transient', detail: error instanceof Error ? error.message : String(error) }
  }

  const envelope = readEnvelope(await res.text())
  const result = envelope.result ?? {}

  // OUR credential, not the customer's. Waiting cannot fix it.
  if (res.status === 401) {
    throw new MisconfiguredError(
      `payments refused our shared secret on /api/internal/resolve: ${
        envelope.statusDetail?.message ?? '401'
      }`,
    )
  }

  if (res.ok) {
    const apiKey = result.apiKey
    const organizationId =
      apiKey !== null && typeof apiKey === 'object'
        ? str((apiKey as Record<string, unknown>).organizationId)
        : ''
    // A 200 that names no organisation is a contract violation, not a rejection.
    // Retrying is right: refusing the signal would lose it over our own bug.
    if (organizationId === '') {
      return { kind: 'transient', detail: 'resolve answered 200 with no organizationId' }
    }
    return { kind: 'ok', organizationId }
  }

  // 5xx is the payments app's own failure — its database, or an unhandled throw.
  // The route returns it deliberately rather than a 401, precisely so a hiccup
  // does not reject a signal from a perfectly good key.
  if (res.status >= 500) {
    return { kind: 'transient', detail: `resolve answered ${res.status}` }
  }

  // 400 (body), 403 (key), 404 (customer) — terminal, and attributable.
  return {
    kind: 'rejected',
    organizationId: str(result.organizationId),
    errorCode: str(result.errorCode) || 'REJECTED',
    errorMessage: envelope.statusDetail?.message ?? `resolve answered ${res.status}`,
  }
}
