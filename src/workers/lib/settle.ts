/** Calls the payments app's `POST /api/internal/settle` from the accepted
 *  consumer. Not the edge's `payments-client.ts` — that one resolves keys; this
 *  one settles a batch, and the two routes have different contracts.
 *
 *  The batch is 1–500 signals. Every signal comes back as one result, in the
 *  order sent, whether it settled or not. */
import { request } from 'undici'
import { config } from '../../config.js'

/** One signal as `/internal/settle` wants it: the named fields it reads, plus
 *  the rest of the payload (model, token counts, custom, unit keys…) spread in
 *  and passed through untouched to the pricing pipeline. */
export interface SettleSignal {
  signalId: string
  receivedAt: string
  organizationId: string
  customerId: string
  type: string | null
  /** Which delivery this is — the SQS receive count. Absent-as-1 upstream. */
  attempt: number
  [payloadField: string]: unknown
}

/** The slice of one settle result this consumer acts on. The route returns far
 *  more (the money columns), but the status events table only needs these:
 *
 *    status PROCESSED                 -> Processed event
 *    status PENDING + USER_ERROR      -> Failed event (terminal, do not retry)
 *    status PENDING + SERVER_ERROR    -> retry (SQS redelivers), no event yet
 */
export interface SettleResult {
  signal_id: string
  status: 'PROCESSED' | 'PENDING'
  error_type: 'USER_ERROR' | 'SERVER_ERROR' | null
  error_code: string | null
}

/**
 * Settles one batch and returns a result per signal.
 *
 * Throws on a transport failure or a non-200 — the caller treats that as "the
 * whole batch is unresolved, retry it", which is correct: a settle that never
 * answered settled nothing.
 */
export async function settleBatch(
  batchId: string,
  signals: SettleSignal[],
): Promise<SettleResult[]> {
  const res = await request(`${config.paymentsUrl}/api/internal/settle`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${config.internalSecret}`,
    },
    body: JSON.stringify({ batchId, signals }),
    // The settle route's interactive tx budget is 120s; wait past it so a
    // still-succeeding batch is not abandoned and replayed.
    headersTimeout: 130_000,
    bodyTimeout: 130_000,
  })

  if (res.statusCode !== 200) {
    const detail = await res.body.text()
    throw new Error(`settle failed (${res.statusCode}): ${detail.slice(0, 300)}`)
  }

  // The payments app answers in its envelope; per-signal results are at
  // result.signals, in the order the signals were sent.
  const envelope = (await res.body.json()) as { result?: { signals?: SettleResult[] } }
  return envelope.result?.signals ?? []
}
