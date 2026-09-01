/** HTTP client for the payments app's internal settle route — the only thing the
 *  dispatcher talks to besides ClickHouse.
 *
 *  It authenticates the CALLER with the shared secret as
 *  `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`; the `Bearer` scheme is
 *  required, a bare token is refused. No CUSTOMER credential is ever sent — the
 *  signal's `apiKeyHash` rides in the body and the payments app resolves the
 *  organisation from it, so the raw `cnk_…` key stays in the edge process. */
import { gzip as gzipCb } from 'node:zlib'
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'
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
 * Writes the body and resolves the moment it is fully flushed — WITHOUT waiting
 * for, or reading, the reply.
 *
 * `fetch()` cannot express this. Its promise settles when response HEADERS
 * arrive, and settle is not a streaming route: Next.js emits headers only once
 * the handler has returned, so awaiting `fetch` is awaiting the whole pricing
 * run. Dropping to `node:http` gives access to the request stream's `finish`
 * event, which fires when the last byte of the body has been handed to the
 * kernel — the earliest moment at which the send is genuinely complete.
 *
 * `finish`, not `end()`'s callback queue and not a timer: anything looser would
 * let the process exit with part of the body still in a userland buffer, and the
 * run would report a send that never happened.
 */
function sendWithoutWaiting(
  url: URL,
  headers: Record<string, string>,
  body: Buffer,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const https = url.protocol === 'https:'
    const req = (https ? httpsRequest : httpRequest)({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port !== '' ? url.port : https ? 443 : 80,
      path: `${url.pathname}${url.search}`,
      method: 'POST',
      // Explicit, because without it node falls back to chunked encoding and
      // some edges buffer a chunked body before forwarding it.
      headers: { ...headers, 'content-length': String(body.length) },
      timeout: timeoutMs,
    })
    // Only failures BEFORE the body is away can be reported. Once `finish` has
    // fired the promise is already settled and a later socket error is moot —
    // the request is gone and nobody is listening for the answer by design.
    req.on('error', reject)
    req.on('timeout', () => {
      req.destroy(new Error(`settle did not accept the body within ${timeoutMs}ms`))
    })
    // If the reply beats our exit, drain it so the socket is not left half-open.
    // Nothing is parsed: not the status, not the envelope. That is the trade.
    req.on('response', (res) => res.resume())
    req.on('finish', () => {
      // Stop the pending response holding the event loop open — the runner exits
      // on its own, and waiting for a reply is the thing being avoided.
      req.socket?.unref()
      resolve()
    })
    req.end(body)
  })
}

/**
 * Settles the WHOLE window in one call and returns a result per signal.
 *
 * Throws on a transport failure or a non-200 — which the runner treats as "this
 * window is unresolved". Historically that was safe rather than lossy because
 * the next run's window overlapped this one. With `DISPATCH_WINDOW_MS` equal to
 * the cron interval the windows TILE instead, so a failed run is covered by the
 * hourly reconciliation rather than by the next minute. Settle stays idempotent
 * on `SignalLog.signalId` either way, so a replay lands on the same money row.
 *
 * THE BODY IS GZIPPED. Vercel caps a serverless function's request body at
 * 4.5MB and raw signal JSON crosses that somewhere around 15k signals — well
 * inside one window at production volume. Signal JSON is the same keys repeated
 * thousands of times, so it compresses roughly 10:1 and the ceiling stops being
 * the thing that decides the window width.
 *
 * WITH `DISPATCH_FIRE_AND_FORGET` the reply is never read: the call returns as
 * soon as the body is flushed, `results` comes back empty, and no status is
 * inspected — so a refused secret cannot raise `MisconfiguredError` and a 413
 * cannot raise `PayloadTooLargeError` from this path. See the flag's note in
 * config.ts for what that costs.
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

  if (config.dispatchFireAndForget) {
    await sendWithoutWaiting(url, headers, body, config.dispatchTimeoutMs)
    // No results, and that is not "settle returned nothing" — nobody looked. The
    // caller marks the outcome `fireAndForget` so a run of zeroes is not read as
    // a batch that failed to price.
    return { results: [], bytes: raw.length, gzipBytes: body.length, fireAndForget: true }
  }

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
