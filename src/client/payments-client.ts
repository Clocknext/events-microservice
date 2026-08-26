/** HTTP client for the payments app's internal endpoints — the only two things
 *  the dispatcher talks to besides ClickHouse.
 *
 *  Both authenticate the CALLER with the shared secret as
 *  `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`; the `Bearer` scheme is
 *  required, a bare token is refused. No CUSTOMER credential is ever sent — the
 *  signal's `apiKeyHash` rides in the body and the payments app resolves the
 *  organisation from it, so the raw `cnk_…` key stays in the edge process. */
import { config } from '../config.js'
import type { CursorResponse, SettleResult, SettleSignal } from '../workers/dispatch/dispatch.schema.js'

/** The payments app's public v1 envelope. */
interface Envelope<T> {
  statusCode?: number
  statusDetail?: { status?: string; message?: string }
  result?: T
}

function authHeaders(): Record<string, string> {
  return {
    'content-type': 'application/json',
    authorization: `Bearer ${config.internalSecret}`,
  }
}

/** A non-200 that means "our shared secret is wrong", which never fixes itself
 *  by waiting and must not be mistaken for an outage. */
export class MisconfiguredError extends Error {}

function readEnvelope<T>(status: number, body: string, route: string): T {
  let envelope: Envelope<T>
  try {
    envelope = JSON.parse(body) as Envelope<T>
  } catch {
    throw new Error(`${route} returned ${status} with a non-JSON body: ${body.slice(0, 200)}`)
  }
  const message = envelope.statusDetail?.message ?? `${route} returned ${status}`
  // The payments app answers a bad SHARED SECRET with exactly "Unauthorized." A
  // customer-key rejection never reaches this level — it comes back as a
  // per-signal result — so a 401 here is always ours.
  if (status === 401) throw new MisconfiguredError(`payments refused our shared secret: ${message}`)
  if (status !== 200) throw new Error(`${route} failed (${status}): ${message}`)
  if (envelope.result === undefined || envelope.result === null) {
    throw new Error(`${route} returned 200 with no result`)
  }
  return envelope.result
}

/** Asks what is left to do: the watermark, plus the signals due for a retry. */
export async function fetchCursor(retryLimit: number): Promise<CursorResponse> {
  const url = new URL('/api/internal/signals/cursor', config.paymentsUrl)
  url.searchParams.set('retryLimit', String(retryLimit))
  const res = await fetch(url, { headers: authHeaders() })
  return readEnvelope<CursorResponse>(res.status, await res.text(), 'cursor')
}

/**
 * Asks which of these signals already has a status row.
 *
 * This is what keeps the overlap window from re-settling the same signals on
 * every sweep. An empty list short-circuits without a round trip.
 */
export async function fetchKnown(signalIds: string[]): Promise<string[]> {
  if (signalIds.length === 0) return []
  const url = new URL('/api/internal/signals/known', config.paymentsUrl)
  const res = await fetch(url, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ signalIds }),
  })
  return readEnvelope<{ known?: string[] }>(res.status, await res.text(), 'known').known ?? []
}

/**
 * Settles one batch and returns a result per signal.
 *
 * Throws on a transport failure or a non-200 — which the caller treats as "the
 * whole batch is unresolved, send it again". That is correct and safe: a settle
 * that never answered may still have committed some signals, and re-sending them
 * replays onto the same money rows rather than charging twice
 * (`SignalLog.signalId` is unique and is the idempotency key).
 */
export async function settleBatch(
  batchId: string,
  signals: SettleSignal[],
): Promise<SettleResult[]> {
  const url = new URL('/api/internal/settle', config.paymentsUrl)
  const res = await fetch(url, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ batchId, signals }),
    // The route's own transaction budget runs to ~120s; wait past it so a batch
    // that is still succeeding is not abandoned and replayed.
    signal: AbortSignal.timeout(130_000),
  })
  const result = readEnvelope<{ signals?: SettleResult[] }>(
    res.status,
    await res.text(),
    'settle',
  )
  return result.signals ?? []
}
