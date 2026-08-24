import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import type { SignalQueue } from '../vent/vent.schema.js'

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

/**
 * A cache holding one already-resolved key.
 *
 * This is what makes the accepted path testable at all: `resolveApiKeyAndBody`
 * answers from Redis on a hit, so planting the entry means a signal can be
 * accepted without the payments app existing. Every other test in this repo
 * only ever exercised rejections for exactly that reason.
 */
function cacheWithResolvedKey() {
  const digest = createHash('sha256').update(KEY, 'utf8').digest('hex')
  const entry = JSON.stringify({
    ok: true,
    key: {
      id: 'key_abc',
      organizationId: 'org_acme',
      createdById: 'usr_dana',
      expiresAt: null,
    },
  })
  const store = new Map([[`cnk:apikey:v1:${digest}`, entry]])
  return {
    async get(key: string) {
      return store.get(key) ?? null
    },
    async set() {
      return 'OK'
    },
  }
}

interface Collected {
  queue: SignalQueue
  bodies: string[]
}

function collector(): Collected {
  const bodies: string[] = []
  return { bodies, queue: { async send(body) { bodies.push(body) } } }
}

const VALID = {
  customerId: 'cus_abc123',
  type: 'credit',
  agentKey: 'credit.research',
  model: 'openai/gpt-4o',
  inputTokens: 1200,
  outputTokens: 350,
}

/** Posts a signal that will be accepted, and returns both queues' traffic. */
async function postAccepted({
  acceptedQueue,
  body = VALID,
}: { acceptedQueue?: SignalQueue | null; body?: unknown } = {}) {
  const app = await buildApp()
  const accepted = collector()
  const pending = collector()
  app.cache = cacheWithResolvedKey()
  app.queue = pending.queue
  app.acceptedQueue = acceptedQueue === undefined ? accepted.queue : acceptedQueue
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(body),
    })
    return {
      status: res.statusCode,
      envelope: res.json(),
      accepted: accepted.bodies,
      pending: pending.bodies,
    }
  } finally {
    await app.close()
  }
}

test('an accepted signal is published to signals_accepted', async () => {
  const result = await postAccepted()
  assert.equal(result.status, 202)
  assert.equal(result.envelope.result.accepted, true)
  assert.equal(result.envelope.result.organizationId, 'org_acme')

  assert.equal(result.accepted.length, 1, 'one message per accepted signal')
  assert.equal(result.pending.length, 0, 'and nothing on the reject queue')
})

test('the message carries the full raw_signals row', async () => {
  const { accepted } = await postAccepted()
  const message = JSON.parse(accepted[0]!) as Record<string, Record<string, unknown>>

  // Keyed by table name, so the consumer inserts raw_signals directly.
  assert.deepEqual(Object.keys(message), ['raw_signals'])
  // The FULL row — the consumer both inserts it and settles it, and settle needs
  // the customer, the type and the payload.
  assert.deepEqual(
    Object.keys(message.raw_signals!).sort(),
    ['customer_id', 'idempotency_key', 'organization_id', 'payload', 'received_at', 'signal_id', 'type'],
  )
  assert.equal(message.raw_signals!.organization_id, 'org_acme')
  assert.equal(message.raw_signals!.customer_id, 'cus_abc123')
  assert.equal(message.raw_signals!.type, 'credit')
})

test('the id on the queue is the id the caller was given', async () => {
  const { envelope, accepted } = await postAccepted()
  const message = JSON.parse(accepted[0]!) as { raw_signals: { signal_id: string; received_at: string } }

  assert.equal(message.raw_signals.signal_id, envelope.result.signalId)
  assert.equal(message.raw_signals.received_at, envelope.result.receivedAt)
  assert.match(message.raw_signals.signal_id, /^[0-9A-HJKMNP-TV-Z]{26}$/, 'a ULID')
})

test('a signal is queued BEFORE it is acknowledged', async () => {
  // Proving the order matters more than it looks: were the publish after the
  // 202, a crash in between would lose a billable signal that the caller has
  // already been told is safe.
  let acknowledged = false
  const app = await buildApp()
  app.cache = cacheWithResolvedKey()
  app.queue = collector().queue
  app.acceptedQueue = {
    async send() {
      assert.equal(acknowledged, false, 'the 202 must not have been sent yet')
    },
  }
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    acknowledged = true
    assert.equal(res.statusCode, 202)
  } finally {
    await app.close()
  }
})

test('a failed publish is a 502, never a 202', async () => {
  const app = await buildApp()
  const pending = collector()
  app.cache = cacheWithResolvedKey()
  app.queue = pending.queue
  app.acceptedQueue = {
    async send() {
      throw new Error('sqs is unreachable')
    },
  }
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    // Losing an analytics row is survivable; losing a billable signal after
    // acknowledging it is not. So this one fails closed.
    assert.equal(res.statusCode, 502)
    assert.equal(res.json().result.errorReason, 'QUEUE_UNAVAILABLE')
    assert.match(res.json().result.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/)
  } finally {
    await app.close()
  }
})

test('a failed publish is not then promised to the pending queue', async () => {
  // QUEUE_UNAVAILABLE must never be deferred: answering "Processing in the
  // queue." because a queue just failed is how one broken queue becomes a
  // silent loss.
  const app = await buildApp()
  const pending = collector()
  app.cache = cacheWithResolvedKey()
  app.queue = pending.queue
  app.acceptedQueue = {
    async send() {
      throw new Error('sqs is unreachable')
    },
  }
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    assert.equal(res.statusCode, 502)
    assert.notEqual(res.json().statusDetail.message, 'Processing in the queue.')
  } finally {
    await app.close()
  }
})

test('with no accepted queue configured, a signal is still accepted', async () => {
  const result = await postAccepted({ acceptedQueue: null })
  assert.equal(result.status, 202)
  assert.equal(result.envelope.result.accepted, true)
  assert.equal(result.accepted.length, 0, 'nothing queued, and nothing broken')
})

test('a refused body never reaches the accepted queue', async () => {
  const result = await postAccepted({ body: { ...VALID, model: undefined } })
  assert.equal(result.status, 202)
  assert.equal(result.envelope.result.status, 'PENDING')
  assert.equal(result.accepted.length, 0)
  assert.equal(result.pending.length, 1, 'it went to signals_pending instead')
})
