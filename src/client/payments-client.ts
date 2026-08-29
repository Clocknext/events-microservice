/** HTTP client for the payments app's internal settle route — the only thing the
 *  dispatcher talks to besides ClickHouse.
 *
 *  It authenticates the CALLER with the shared secret as
 *  `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`; the `Bearer` scheme is
 *  required, a bare token is refused. No CUSTOMER credential is ever sent — the
 *  signal's `apiKeyHash` rides in the body and the payments app resolves the
 *  organisation from it, so the raw `cnk_…` key stays in the edge process. */
import { gzip as gzipCb } from 'node:zlib'
import { promisify } from 'node:util'
import { config } from '../config.js'
import type { SettleResult, SettleSignal, SettleTransfer } from '../workers/dispatch/dispatch.schema.js'

const gzip = promisify(gzipCb)

/** The payments app's public v1 envelope. */
interface Envelope<T> {
  statusCode?: number
  statusDetail?: { status?: string; message?: string }
  result?: T
}

/** A non-200 that means "our shared secret is wrong", which never fixes itself
 *  by waiting and must not be mistaken for an outage. */
export class MisconfiguredError extends Error {}

/** The body exceeded what the platform will accept. Distinguished from a generic
 *  failure because retrying it unchanged is pointless — the next window will be
 *  at least as big — and the fix is a smaller `DISPATCH_WINDOW_MS` or, if gzip is
 *  already on and still over, batching. */
export class PayloadTooLargeError extends Error {}

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

/**
 * Settles the WHOLE window in one call and returns a result per signal.
 *
 * Throws on a transport failure or a non-200 — which the runner treats as "this
 * window is unresolved". That is safe rather than lossy: the next run's window
 * overlaps this one, so it sends the same signals again, and settle replays them
 * onto the same money rows rather than charging twice (`SignalLog.signalId` is
 * unique and is the idempotency key). Nothing needs to be recorded for that to
 * work, which is the entire reason this process keeps no state.
 *
 * THE BODY IS GZIPPED. Vercel caps a serverless function's request body at
 * 4.5MB and raw signal JSON crosses that somewhere around 15k signals — well
 * inside one window at production volume. Signal JSON is the same keys repeated
 * thousands of times, so it compresses roughly 10:1 and the ceiling stops being
 * the thing that decides the window width.
 */
export async function settleAll(
  batchId: string,
  signals: SettleSignal[],
): Promise<SettleTransfer> {
  const url = new URL('/api/internal/settle', config.paymentsUrl)
  const raw = Buffer.from(JSON.stringify({ batchId, signals }))
  // Async gzip, not gzipSync: at 10k signals this is several MB, and blocking the
  // only thread of a one-shot process for it buys nothing.
  const body = config.dispatchGzip ? await gzip(raw) : raw

  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${config.internalSecret}`,
  }
  if (config.dispatchGzip) headers['content-encoding'] = 'gzip'

  const res = await fetch(url, {
    method: 'POST',
    headers,
    body,
    // Must sit ABOVE the route's own maxDuration. A settle call that is still
    // committing must never be abandoned: the window would re-send work that in
    // fact succeeded, and the run would report a failure that did not happen.
    signal: AbortSignal.timeout(config.dispatchTimeoutMs),
  })

  // 413 is its own class of failure: the body was refused before the route ran,
  // so no signal was settled and re-sending the same window cannot help.
  if (res.status === 413) {
    throw new PayloadTooLargeError(
      `settle refused a ${body.length}-byte body (${signals.length} signals, ` +
        `gzip=${config.dispatchGzip}). Lower DISPATCH_WINDOW_MS, or if gzip is ` +
        `already on, the window has outgrown a single call.`,
    )
  }

  const result = readEnvelope<{ signals?: SettleResult[] }>(
    res.status,
    await res.text(),
    'settle',
  )
  return { results: result.signals ?? [], bytes: raw.length, gzipBytes: body.length }
}
