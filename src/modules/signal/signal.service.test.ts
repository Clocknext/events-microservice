import assert from 'node:assert/strict'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import type { SignalProducer } from '../../plugins/kafka.js'
import type { SignalMessage } from './signal.schema.js'

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

/**
 * Posts a body and returns the response plus whatever the producer was handed.
 *
 * An API key is sent by default: a request without one is a hard 401, so
 * omitting it would turn every gate test into a 401 and prove nothing about the
 * gate. A fake producer is planted so a valid signal is accepted without a real
 * broker — `buildApp` decorates `producer` as null when KAFKA_BROKERS is unset,
 * and these tests overwrite it, exactly as they need.
 */
async function post(
  body: unknown,
  headers: Record<string, string> = { authorization: `Bearer ${KEY}` },
  { raw = false }: { raw?: boolean } = {},
) {
  const app = await buildApp()
  const messages: SignalMessage[] = []
  const producer: SignalProducer = {
    async send(message) {
      messages.push(message)
    },
  }
  app.producer = producer
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', ...headers },
      payload: raw ? (body as string) : JSON.stringify(body),
    })
    return { status: res.statusCode, envelope: res.json(), messages }
  } finally {
    await app.close()
  }
}

interface ErrorEnvelope {
  statusCode: number
  statusDetail: { status: string; message: string }
  result: { errorReason: string; signalId?: string; issues?: { field: string; message: string }[] }
}

/** Asserts the error envelope shape, then the reason. */
function assertRejected(
  result: { status: number; envelope: unknown },
  status: number,
  errorReason: string,
): ErrorEnvelope {
  const envelope = result.envelope as ErrorEnvelope
  assert.equal(result.status, status)
  assert.equal(envelope.statusCode, status, 'statusCode mirrors the HTTP status')
  assert.equal(envelope.statusDetail.status, 'ERROR')
  assert.ok(envelope.statusDetail.message.length > 0, 'a human-readable message')
  assert.equal(envelope.result.errorReason, errorReason)
  return envelope
}

const VALID = {
  customerId: 'cus_abc123',
  inputTokens: 1200,
  outputTokens: 350,
  // Everything past the three required fields is unconstrained and must ride
  // through to Kafka untouched.
  type: 'Credit',
  agentKey: 'credit.research',
  model: 'openai/gpt-4o',
  custom: { feature: 'chat', region: 'eu' },
  voice_ai: 'wbwbewj21',
}

// --- the gate accepts ---------------------------------------------------------

test('a well-formed signal is accepted with 202 and produced once', async () => {
  const result = await post(VALID)
  assert.equal(result.status, 202)
  assert.equal(result.envelope.result.accepted, true)
  assert.match(result.envelope.result.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/, 'a ULID')
  assert.equal(result.messages.length, 1, 'exactly one message produced')
})

test('the whole body rides through untouched — the gate does not normalise', async () => {
  const { messages } = await post(VALID)
  const message = messages[0]!
  // Not lowercased, not folded, unknown keys kept: the edge changed nothing.
  assert.deepEqual(message.body, VALID)
})

test('the id and time on the message are the ones the caller was given', async () => {
  const { envelope, messages } = await post(VALID)
  const message = messages[0]!
  assert.equal(message.signalId, envelope.result.signalId)
  assert.equal(message.receivedAt, envelope.result.receivedAt)
})

// --- the API key gate ---------------------------------------------------------

test('a missing API key is a hard 401', async () => {
  const result = await post(VALID, {})
  assertRejected(result, 401, 'API_KEY_MISSING')
  assert.equal(result.messages.length, 0, 'nothing is produced for a refused signal')
})

test('a bare token without the Bearer scheme is also API_KEY_MISSING', async () => {
  assertRejected(await post(VALID, { authorization: KEY }), 401, 'API_KEY_MISSING')
})

test('a 401 still carries a signalId, so the caller can quote it', async () => {
  const envelope = assertRejected(await post(VALID, {}), 401, 'API_KEY_MISSING')
  assert.match(envelope.result.signalId ?? '', /^[0-9A-HJKMNP-TV-Z]{26}$/)
})

test('the key is checked before the body is parsed', async () => {
  // No key AND an unparseable body is a 401 — the key check is at onRequest.
  assertRejected(await post('{nope', {}, { raw: true }), 401, 'API_KEY_MISSING')
})

// --- the required-fields gate -------------------------------------------------

test('customerId is required', async () => {
  const { customerId: _drop, ...missing } = VALID
  assertRejected(await post(missing), 400, 'INVALID_BODY')
})

test('a whitespace-only customerId is empty once trimmed, so it is refused', async () => {
  assertRejected(await post({ ...VALID, customerId: '   ' }), 400, 'INVALID_BODY')
})

test('both token counts are required, and 0 is a valid count', async () => {
  const { inputTokens: _drop, ...missing } = VALID
  assertRejected(await post(missing), 400, 'INVALID_BODY')
  // 0 must pass — "send 0 when there are none".
  const zero = await post({ ...VALID, inputTokens: 0, outputTokens: 0 })
  assert.equal(zero.status, 202)
})

test('a token count is not coerced from a string', async () => {
  // AJV coercion is off, so "1200" is refused rather than read as 1200.
  assertRejected(await post({ ...VALID, inputTokens: '1200' }), 400, 'INVALID_BODY')
})

test('a negative or fractional token count is refused', async () => {
  assertRejected(await post({ ...VALID, inputTokens: -1 }), 400, 'INVALID_BODY')
  assertRejected(await post({ ...VALID, outputTokens: 1.5 }), 400, 'INVALID_BODY')
})

test('deeper rules are NOT enforced at the edge — an unknown type is accepted', async () => {
  // The gate does not judge `type`; a consumer settles or fails it later.
  const result = await post({ ...VALID, type: 'unit' })
  assert.equal(result.status, 202)
})

// --- the request never got as far as being understood -------------------------

test('an unknown route is a 404', async () => {
  const app = await buildApp()
  try {
    const res = await app.inject({ method: 'POST', url: '/api/v1/nope' })
    assertRejected({ status: res.statusCode, envelope: res.json() }, 404, 'NOT_FOUND')
  } finally {
    await app.close()
  }
})

test('a non-JSON content type is a 415', async () => {
  assertRejected(
    await post('hi', { authorization: `Bearer ${KEY}`, 'content-type': 'text/plain' }, { raw: true }),
    415,
    'UNSUPPORTED_MEDIA_TYPE',
  )
})

test('/health keeps its own parsers — removing text/plain is module-scoped', async () => {
  const app = await buildApp()
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/health/echo',
      headers: { 'content-type': 'text/plain' },
      payload: 'hi',
    })
    assert.notEqual(res.statusCode, 415, 'health still parses text/plain')
  } finally {
    await app.close()
  }
})

test('malformed JSON is a 400 MALFORMED_JSON', async () => {
  assertRejected(await post('{nope', undefined, { raw: true }), 400, 'MALFORMED_JSON')
})

test('an empty body is a 400 EMPTY_BODY', async () => {
  assertRejected(await post('', undefined, { raw: true }), 400, 'EMPTY_BODY')
})

test('a body over the limit is a 413, before validation', async () => {
  assertRejected(await post({ ...VALID, filler: 'x'.repeat(80 * 1024) }), 413, 'BODY_TOO_LARGE')
})
