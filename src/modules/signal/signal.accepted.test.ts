import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import type { SignalProducer } from '../../plugins/kafka.js'
import type { SignalMessage } from './signal.schema.js'

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

const VALID = {
  customerId: 'cus_abc123',
  inputTokens: 1200,
  outputTokens: 350,
  type: 'credit',
  model: 'openai/gpt-4o',
}

/** Collects everything handed to the producer port. */
function collector(): { messages: SignalMessage[]; producer: SignalProducer } {
  const messages: SignalMessage[] = []
  return { messages, producer: { async send(m) { messages.push(m) } } }
}

test('an accepted signal is produced to the topic exactly once', async () => {
  const app = await buildApp()
  const { messages, producer } = collector()
  app.producer = producer
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    assert.equal(res.statusCode, 202)
    assert.equal(res.json().result.accepted, true)
    assert.equal(messages.length, 1, 'one message per accepted signal')
  } finally {
    await app.close()
  }
})

test('the message is exactly { signalId, receivedAt, apiKeyHash, body }', async () => {
  const app = await buildApp()
  const { messages, producer } = collector()
  app.producer = producer
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    const message = messages[0]!
    assert.deepEqual(Object.keys(message).sort(), [
      'apiKeyHash',
      'body',
      'receivedAt',
      'signalId',
    ])
    assert.deepEqual(message.body, VALID, 'the body as sent, nothing added or dropped')
    // The id/time on the message are the id/time the caller was handed.
    assert.equal(message.signalId, res.json().result.signalId)
    assert.equal(message.receivedAt, res.json().result.receivedAt)
    assert.match(message.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/, 'a ULID')
  } finally {
    await app.close()
  }
})

test('a signal is produced BEFORE it is acknowledged', async () => {
  // Order matters: were the produce after the 202, a crash in between would lose
  // a billable signal the caller has already been told is safe.
  let acknowledged = false
  const app = await buildApp()
  app.producer = {
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

test('a failed produce is a 502, never a 202', async () => {
  const app = await buildApp()
  app.producer = {
    async send() {
      throw new Error('kafka is unreachable')
    },
  }
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    // A billable signal must not be acknowledged unless it is durably on the
    // topic. So this fails closed.
    assert.equal(res.statusCode, 502)
    assert.equal(res.json().result.errorReason, 'QUEUE_UNAVAILABLE')
    assert.match(res.json().result.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/)
  } finally {
    await app.close()
  }
})

test('with no producer configured, a signal is still accepted', async () => {
  const app = await buildApp()
  app.producer = null
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    assert.equal(res.statusCode, 202)
    assert.equal(res.json().result.accepted, true)
  } finally {
    await app.close()
  }
})

test('the message carries the key DIGEST, never the key', async () => {
  const app = await buildApp()
  const { messages, producer } = collector()
  app.producer = producer
  try {
    await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    const message = messages[0]!
    // Exactly what `ApiKey.hashedKey` stores upstream, so settle resolves the
    // organisation from it with one unique-index hit.
    assert.equal(message.apiKeyHash, createHash('sha256').update(KEY).digest('hex'))
    assert.match(message.apiKeyHash, /^[0-9a-f]{64}$/, '64 lowercase hex')

    // The point of the digest: the raw key must appear NOWHERE on the wire.
    assert.ok(
      !JSON.stringify(message).includes(KEY),
      'the raw cnk_ key must never reach the topic',
    )
  } finally {
    await app.close()
  }
})

test('the digest is on the envelope, not mixed into the body', async () => {
  // `body` becomes `signal_log.payload`, which is the archive of what the caller
  // actually sent. Our own fields must not appear in it.
  const app = await buildApp()
  const { messages, producer } = collector()
  app.producer = producer
  try {
    await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    const body = messages[0]!.body as Record<string, unknown>
    assert.equal(body.apiKeyHash, undefined)
    assert.equal(body.signalId, undefined)
    assert.equal(body.receivedAt, undefined)
  } finally {
    await app.close()
  }
})

test('the 202 does not echo the digest back to the caller', async () => {
  const app = await buildApp()
  app.producer = null
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify(VALID),
    })
    assert.equal(res.statusCode, 202)
    assert.equal(res.json().result.apiKeyHash, undefined)
  } finally {
    await app.close()
  }
})
