/** The settle call carries the whole window in one gzipped body. gzip is not an
 *  optimisation here: Vercel refuses a serverless function's request body over
 *  4.5MB, which raw signal JSON crosses somewhere around 15k signals — inside one
 *  window at production volume. So these pin the encoding and the two failures
 *  that must not be mistaken for an outage. */
import assert from 'node:assert/strict'
import { gunzipSync } from 'node:zlib'
import { test } from 'node:test'
import { MisconfiguredError, PayloadTooLargeError, settleAll } from './payments-client.js'
import type { SettleSignal } from '../workers/dispatch/dispatch.schema.js'

function signals(n: number): SettleSignal[] {
  return Array.from({ length: n }, (_, i) => ({
    signalId: `sig-${i}`,
    receivedAt: '2026-08-26T10:00:00.000Z',
    apiKeyHash: 'a'.repeat(64),
    organizationId: 'org_1',
    status: 'PROCESSING',
    errorCode: null,
    errorMessage: null,
    attempt: 1,
    customerId: 'cus_1',
    inputTokens: 1_200,
    outputTokens: 340,
  }))
}

/** Replaces global fetch for one call and hands back what the client sent. */
async function capture(
  respond: () => { status: number; body: string },
  run: () => Promise<unknown>,
) {
  const original = globalThis.fetch
  let sent: { headers: Record<string, string>; body: Buffer } | undefined
  globalThis.fetch = (async (_url: unknown, init: RequestInit) => {
    sent = {
      headers: init.headers as Record<string, string>,
      body: init.body as unknown as Buffer,
    }
    const { status, body } = respond()
    return { status, text: async () => body } as Response
  }) as typeof fetch
  try {
    const result = await run()
    return { sent: sent!, result }
  } finally {
    globalThis.fetch = original
  }
}

const ok = (n: number) => () => ({
  status: 200,
  body: JSON.stringify({
    statusCode: 200,
    statusDetail: { status: 'SUCCESS', message: 'ok' },
    result: {
      signals: Array.from({ length: n }, (_, i) => ({
        signal_id: `sig-${i}`, status: 'PROCESSED',
        error_type: null, error_code: null, error_message: null,
      })),
    },
  }),
})

test('the body is gzipped, declared as such, and round-trips to the same JSON', async () => {
  const batch = signals(50)
  const { sent } = await capture(ok(50), () => settleAll('batch-1', batch))

  assert.equal(sent.headers['content-encoding'], 'gzip')
  assert.equal(sent.headers['content-type'], 'application/json')
  const decoded = JSON.parse(gunzipSync(sent.body).toString('utf8')) as {
    batchId: string
    signals: SettleSignal[]
  }
  assert.equal(decoded.batchId, 'batch-1')
  assert.equal(decoded.signals.length, 50)
  assert.deepEqual(decoded.signals[0], batch[0])
})

test('gzip actually buys the headroom the 4.5MB ceiling needs', async () => {
  // Signal JSON is the same keys repeated thousands of times. If this ratio ever
  // collapses, the window width stops being a free choice.
  const { sent, result } = await capture(ok(5_000), () => settleAll('b', signals(5_000)))
  const transfer = result as { bytes: number; gzipBytes: number }
  assert.equal(transfer.gzipBytes, sent.body.length)
  assert.ok(transfer.bytes > transfer.gzipBytes * 5, `ratio was only ${transfer.bytes / transfer.gzipBytes}`)
})

test('the reported sizes are the raw and on-the-wire byte counts', async () => {
  const { sent, result } = await capture(ok(10), () => settleAll('b', signals(10)))
  const transfer = result as { bytes: number; gzipBytes: number; results: unknown[] }
  assert.equal(transfer.gzipBytes, sent.body.length)
  assert.equal(transfer.bytes, Buffer.byteLength(JSON.stringify({ batchId: 'b', signals: signals(10) })))
  assert.equal(transfer.results.length, 10)
})

test('a 413 is its own error — re-sending the same window cannot help', async () => {
  await assert.rejects(
    () => capture(() => ({ status: 413, body: 'Payload Too Large' }), () => settleAll('b', signals(3))),
    PayloadTooLargeError,
  )
})

test('a 401 is our shared secret, never a customer key, and never an outage', async () => {
  await assert.rejects(
    () => capture(
      () => ({ status: 401, body: JSON.stringify({ statusDetail: { message: 'Unauthorized.' } }) }),
      () => settleAll('b', signals(3)),
    ),
    MisconfiguredError,
  )
})

test('a 5xx throws so the run exits non-zero and the next window re-sends', async () => {
  await assert.rejects(
    () => capture(
      () => ({ status: 503, body: JSON.stringify({ statusDetail: { message: 'upstream down' } }) }),
      () => settleAll('b', signals(3)),
    ),
    /settle failed \(503\)/,
  )
})
