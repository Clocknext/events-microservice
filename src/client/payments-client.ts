/** HTTP client for the payments app's internal endpoints (it runs on Vercel).
 *
 *  Both routes authenticate the CALLER with the shared secret as
 *  `Authorization: Bearer <INTERNAL_SETTLE_SECRET>` — the `Bearer` scheme is
 *  required, a bare token is rejected. */
import { request } from 'undici'
import { config } from '../config.js'
import type { ApiEnvelope, ResolveOutcome, SettleSignal, Verdict } from './types.js'
import type { ResolvedApiKey, SignalIssue } from '../modules/auth/auth.schema.js'

/**
 * Asks `POST /api/internal/resolve` the two questions the edge cannot answer
 * itself: is this body acceptable, and whose key is this?
 *
 * The customer's raw `cnk_…` key never leaves this process — only its SHA-256
 * digest travels, in `X-Api-Key-Hash`, which is exactly what the payments app
 * stores. So the key cannot leak through an internal request log.
 *
 * Never throws: every failure comes back as an `outcome` the service can act on.
 */
export async function resolveSignal(
  digest: string,
  body: unknown,
): Promise<ResolveOutcome> {
  let statusCode: number
  let envelope: ApiEnvelope<unknown>
  try {
    const res = await request(`${config.paymentsUrl}/api/internal/resolve`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${config.internalSecret}`,
        'x-api-key-hash': digest,
      },
      body: JSON.stringify(body),
      headersTimeout: config.resolveTimeoutMs,
      bodyTimeout: config.resolveTimeoutMs,
    })
    statusCode = res.statusCode
    envelope = (await res.body.json()) as ApiEnvelope<unknown>
  } catch (err) {
    return {
      outcome: 'unavailable',
      message: err instanceof Error ? err.message : 'resolve request failed',
    }
  }

  const message = envelope?.statusDetail?.message ?? `resolve returned ${statusCode}`

  if (statusCode === 200) {
    const result = envelope.result as { apiKey?: ResolvedApiKey } | null
    if (!result?.apiKey) {
      // A 200 without a key means the contract changed under us. Treat it as an
      // outage rather than as a rejection — do not cache a shape we don't grasp.
      return { outcome: 'unavailable', message: 'resolve returned no apiKey' }
    }
    return { outcome: 'resolved', key: result.apiKey }
  }

  if (statusCode === 400) {
    const result = envelope.result as { issues?: SignalIssue[] } | null
    return { outcome: 'rejected-body', message, issues: result?.issues ?? [] }
  }

  if (statusCode === 401) {
    // A 401 means one of two very different things, and confusing them is
    // expensive: if OUR shared secret is wrong we would cache "invalid key" for
    // every customer and 401 the whole world. The route answers a bad shared
    // secret with exactly "Unauthorized." and every customer-key rejection with
    // a specific sentence, so that one message is read as our own fault.
    if (message === 'Unauthorized.') {
      return {
        outcome: 'unavailable',
        misconfigured: true,
        message: 'payments rejected our shared secret — check INTERNAL_SETTLE_SECRET',
      }
    }
    return { outcome: 'rejected-key', status: 401, message }
  }

  // 500 (including "INTERNAL_SETTLE_SECRET is not configured") and anything
  // else: no verdict was reached, so the signal deserves a retry.
  return { outcome: 'unavailable', message }
}

export async function settle(signals: SettleSignal[]): Promise<Verdict[]> {
  const res = await request(`${config.paymentsUrl}/api/internal/settle`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${config.internalSecret}`,
    },
    body: JSON.stringify({ signals }),
    // Wait longer than the settle route can legitimately take: its interactive
    // tx budget is 120s (maxDuration 300s). At 60s the worker used to abandon a
    // still-succeeding call and replay the whole batch — 130s ends that.
    headersTimeout: 130_000,
    bodyTimeout: 130_000,
  })
  if (res.statusCode !== 200) throw new Error(`settle failed with ${res.statusCode}`)
  // Per-signal outcomes live at `result.signals`, and the status stays 200 even
  // when some of them are left PENDING — a batch is never all-or-nothing.
  const { result } = (await res.body.json()) as ApiEnvelope<{ signals: Verdict[] }>
  return result.signals
}
