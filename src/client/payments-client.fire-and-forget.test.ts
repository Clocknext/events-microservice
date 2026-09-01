/** `DISPATCH_FIRE_AND_FORGET` is the one setting that changes what "sent" MEANS,
 *  so its behaviour is pinned here rather than read off the source.
 *
 *  Its own file, and not a case inside `payments-client.test.ts`, because
 *  `src/config.ts` snapshots `process.env` at import time: the flag and the
 *  server's port both have to be in the environment BEFORE the client is
 *  imported. Node's test runner gives each file its own process, which is what
 *  makes that safe to do here and impossible to do there.
 *
 *  The path also bypasses `fetch` entirely — it drops to `node:http` to get at
 *  the request stream's `finish` event — so the fetch stub the sibling file uses
 *  cannot observe it. These run against a real socket. */
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createServer, type Server } from 'node:http'
import { test } from 'node:test'
import { gunzipSync } from 'node:zlib'
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

/** A server that reads the whole request and then NEVER answers — the condition
 *  the flag exists to survive. A run that waits for a reply hangs here. */
function silentServer(onBody: (body: Buffer) => void): {
  server: Server
  ready: Promise<number>
} {
  const server = createServer((req) => {
    const chunks: Buffer[] = []
    req.on('data', (c: Buffer) => chunks.push(c))
    // No `res.end()`, deliberately: the reply is never written.
    req.on('end', () => onBody(Buffer.concat(chunks)))
  })
  const ready = (async () => {
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    return address.port
  })()
  return { server, ready }
}

test('fire-and-forget resolves against a server that never replies', async () => {
  let body: Buffer | undefined
  let arrived: () => void = () => {}
  const bodyArrived = new Promise<void>((resolve) => {
    arrived = resolve
  })
  const { server, ready } = silentServer((b) => {
    body = b
    arrived()
  })
  const port = await ready

  process.env.PAYMENTS_URL = `http://127.0.0.1:${port}`
  process.env.INTERNAL_SETTLE_SECRET = 'shared-secret'
  process.env.DISPATCH_FIRE_AND_FORGET = 'true'
  process.env.DISPATCH_GZIP = 'true'
  const { settleAll } = await import('./payments-client.js')

  try {
    // The assertion IS that this returns. With the flag off it would sit here
    // until DISPATCH_TIMEOUT_MS, and the test would time out.
    const transfer = await settleAll('batch-1', signals(3))

    // Empty because nobody LOOKED, and the flag is what says so. Without it a
    // caller cannot tell this apart from a batch that priced nothing.
    assert.equal(transfer.fireAndForget, true)
    assert.deepEqual(transfer.results, [])
    assert.ok(transfer.gzipBytes > 0)
    assert.ok(transfer.bytes > transfer.gzipBytes)

    // Resolving early must not mean sending less: the whole body still has to
    // reach the server, or "fire and forget" would just be "forget".
    await bodyArrived
    assert.ok(body, 'the server received no body')
    const sent = JSON.parse(gunzipSync(body).toString()) as {
      batchId: string
      signals: SettleSignal[]
    }
    assert.equal(sent.batchId, 'batch-1')
    assert.equal(sent.signals.length, 3)
    assert.equal(sent.signals[0]?.signalId, 'sig-0')
  } finally {
    server.closeAllConnections()
    server.close()
  }
})
