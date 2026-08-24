import assert from 'node:assert/strict'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import type { PendingMessage } from '../vent/vent.schema.js'

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

/**
 * Posts a body and returns the response plus whatever reached the queue.
 *
 * Every case here is decided before the payments app would be called — by
 * routing, parsing, the schema, or the missing-key check — so these tests make
 * no network requests.
 *
 * An API key is sent by default. That is not incidental: a request without one
 * is the single hard rejection left, so omitting it would turn every rulebook
 * test into a 401 and prove nothing about the rulebook.
 */
async function post(
  body: unknown,
  headers: Record<string, string> = { authorization: `Bearer ${KEY}` },
  { raw = false }: { raw?: boolean } = {},
) {
  const app = await buildApp()
  const messages: PendingMessage[] = []
  app.queue = {
    async send(payload) {
      messages.push(JSON.parse(payload) as PendingMessage)
    },
  }
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

interface QueuedEnvelope {
  statusCode: number
  statusDetail: { status: string; message: string }
  result: { signalId: string; receivedAt: string; status: string }
}

/** Asserts the envelope shape every hard rejection shares, then the reason.
 *  Only an unidentified caller still gets one of these. */
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

/**
 * Asserts a refused signal was ACCEPTED onto the queue rather than rejected.
 *
 * This is the shape every rulebook failure takes now: the caller gets 202 and
 * an id, and the reason it was refused travels to ClickHouse instead of onto
 * the wire. So the assertion moved from the envelope to the message — the rule
 * still fires, it is just reported somewhere else.
 */
function assertQueued(
  result: { status: number; envelope: unknown; messages: PendingMessage[] },
  errorReason: string,
): PendingMessage {
  const envelope = result.envelope as QueuedEnvelope
  assert.equal(result.status, 202, `expected 202, got ${result.status}`)
  assert.equal(envelope.statusDetail.status, 'SUCCESS')
  assert.equal(envelope.statusDetail.message, 'Processing in the queue.')
  assert.equal(envelope.result.status, 'PENDING', 'queued, not settled')
  assert.match(envelope.result.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/, 'a ULID the caller can quote')

  assert.equal(result.messages.length, 1, 'exactly one message per signal')
  const message = result.messages[0]!
  assert.equal(message.error_code, errorReason)
  assert.equal(
    message.raw_signals.signal_id,
    envelope.result.signalId,
    'the id the caller was given is the id in the queue',
  )
  return message
}

const VALID = {
  customerId: 'cus_abc123',
  type: 'credit',
  agentKey: 'credit.research',
  member: 'dana@acme.com',
  model: 'openai/gpt-4o',
  inputTokens: 1200,
  outputTokens: 350,
  cacheTokens: 200,
  custom: { feature: 'chat', region: 'eu' },
}


// --- the one hard rejection left ---------------------------------------------
//
// Everything else this route refuses now leaves as a 202 with a signalId, and
// the reason travels to ClickHouse instead of onto the wire. An unidentified
// caller cannot: a 202 would accept work from anyone, and the row could never
// be attributed to an organisation, so nobody could ever see it to retry it.

test('a missing API key is a hard 401, not a queued signal', async () => {
  assertRejected(await post(VALID, {}), 401, 'API_KEY_MISSING')
})

test('a bare token without the Bearer scheme is also API_KEY_MISSING', async () => {
  assertRejected(await post(VALID, { authorization: KEY }), 401, 'API_KEY_MISSING')
})

test('a 401 still carries a signalId, so the caller can quote it', async () => {
  const envelope = assertRejected(await post(VALID, {}), 401, 'API_KEY_MISSING')
  assert.match(envelope.result.signalId ?? '', /^[0-9A-HJKMNP-TV-Z]{26}$/)
})

test('the key is checked before the body is parsed', async () => {
  // A request with no key AND an unparseable body is a 401, not a 202. Were it
  // the other way round we would hand out a receipt for a signal nobody could
  // ever claim.
  assertRejected(await post('{nope', {}, { raw: true }), 401, 'API_KEY_MISSING')
})

test('an unknown route is still a 404 — there is no signal in it', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/v1/nope' })
  assertRejected({ status: res.statusCode, envelope: res.json() }, 404, 'NOT_FOUND')
  await app.close()
})

// --- the request never got as far as being understood -------------------------

test('a non-JSON content type is queued as UNSUPPORTED_MEDIA_TYPE', async () => {
  assertQueued(
    await post('hi', { authorization: `Bearer ${KEY}`, 'content-type': 'text/plain' }, { raw: true }),
    'UNSUPPORTED_MEDIA_TYPE',
  )
})

test('/health keeps its own parsers — removing text/plain is module-scoped', async () => {
  // Proves the removeContentTypeParser call in signal.routes is encapsulated:
  // it must not reach across into another module's routes.
  const app = await buildApp()
  const res = await app.inject({
    method: 'POST',
    url: '/health/echo',
    headers: { 'content-type': 'text/plain' },
    payload: 'hi',
  })
  assert.notEqual(res.statusCode, 415, 'health still parses text/plain')
  await app.close()
})

test('malformed JSON is queued as MALFORMED_JSON, not INVALID_BODY', async () => {
  const message = assertQueued(await post('{nope', undefined, { raw: true }), 'MALFORMED_JSON')
  assert.equal(message.raw_signals.payload, '{nope', 'and the bytes are kept verbatim')
})

test('an empty body is queued as EMPTY_BODY', async () => {
  assertQueued(await post('', undefined, { raw: true }), 'EMPTY_BODY')
})

test('a body over the limit is queued as BODY_TOO_LARGE, before validation', async () => {
  assertQueued(await post({ ...VALID, filler: 'x'.repeat(80 * 1024) }), 'BODY_TOO_LARGE')
})

// --- the signal itself was judged and refused --------------------------------
//
// The rulebook still fires on every one of these. What changed is where the
// verdict is reported: `error_code` in the queue message (and later the Failed
// status event), not `result.errorReason` on the wire. The slim schema keeps
// only the code — which field failed is no longer recorded anywhere.

test('a refused body is queued as INVALID_BODY', async () => {
  assertQueued(await post({ ...VALID, customerId: '  ', inputTokens: -1, outputTokens: '5' }), 'INVALID_BODY')
})

test('a whitespace-only id is empty once trimmed, so it is refused', async () => {
  assertQueued(await post({ ...VALID, customerId: '   ' }), 'INVALID_BODY')
})

test('token counts are required, and 0 is a valid count', async () => {
  const { inputTokens: _drop, ...missing } = VALID
  assertQueued(await post(missing), 'INVALID_BODY')
  // 0 must pass: "send 0 when there are none" is the upstream contract. Reaching
  // the key lookup (a 502, since payments is unreachable) proves it was accepted.
  assertQueued(await post({ ...VALID, inputTokens: 0, outputTokens: 0 }), 'UPSTREAM_UNAVAILABLE')
})

test('a token count is not coerced from a string', async () => {
  // Zod rejects "1200" upstream, so accepting it here would mean accepting a
  // signal that settlement later refuses.
  assertQueued(await post({ ...VALID, inputTokens: '1200' }), 'INVALID_BODY')
})

test('a negative or fractional token count is refused', async () => {
  assertQueued(await post({ ...VALID, inputTokens: -1 }), 'INVALID_BODY')
  assertQueued(await post({ ...VALID, outputTokens: 1.5 }), 'INVALID_BODY')
})

test('any type requires a model', async () => {
  const { model: _drop, ...noModel } = VALID
  assertQueued(await post(noModel), 'INVALID_BODY')
})

test('type credit requires an agentKey', async () => {
  const { agentKey: _drop, ...noKey } = VALID
  assertQueued(await post(noKey), 'INVALID_BODY')
})

test('the deprecated key alias still satisfies the agentKey rule', async () => {
  const { agentKey: _drop, ...noKey } = VALID
  assertQueued(await post({ ...noKey, key: 'credit.research' }), 'UPSTREAM_UNAVAILABLE')
})

test('type outcome requires a runId as well', async () => {
  const outcome = { ...VALID, type: 'outcome' }
  assertQueued(await post(outcome), 'INVALID_BODY')
  assertQueued(await post({ ...outcome, runId: 'run_1' }), 'UPSTREAM_UNAVAILABLE')
})

test('type is accepted case-insensitively', async () => {
  // Reaching the key lookup rather than INVALID_BODY proves it was lowercased.
  const message = assertQueued(await post({ ...VALID, type: ' Credit ' }), 'UPSTREAM_UNAVAILABLE')
  assert.equal(message.raw_signals.type, 'credit')
})

test('an unknown type is refused', async () => {
  assertQueued(await post({ ...VALID, type: 'unit' }), 'INVALID_BODY')
})

test('an unknown top-level key survives — it may be a pricing-metric refId', async () => {
  assertQueued(await post({ ...VALID, voice_ai: 'wbwbewj21' }), 'UPSTREAM_UNAVAILABLE')
})

test('an oversized custom blob is CUSTOM_TOO_LARGE, not BODY_TOO_LARGE', async () => {
  // The request as a whole was fine — one field needed shrinking, and the row
  // has to say which.
  assertQueued(
    await post({ ...VALID, custom: { blob: 'x'.repeat(40 * 1024) } }),
    'CUSTOM_TOO_LARGE',
  )
})

// --- our side failed ---------------------------------------------------------

test('an unreachable payments app is queued, and leaks nothing', async () => {
  const result = await post(VALID)
  const message = assertQueued(result, 'UPSTREAM_UNAVAILABLE')

  const onTheWire = JSON.stringify(result.envelope)
  const inTheRow = JSON.stringify(message)
  for (const [label, text] of [['the wire', onTheWire], ['the row', inTheRow]] as const) {
    assert.ok(!text.includes('ECONNREFUSED'), `the underlying cause stays out of ${label}`)
    assert.ok(!/\d+\.\d+\.\d+\.\d+/.test(text), `no internal address in ${label}`)
  }
})

test('a signal that cannot be queued is a hard error, not a false 202', async () => {
  // `Processing in the queue.` is a promise. If there is no queue to keep it,
  // saying so would lose the signal in silence and the caller would never resend.
  const app = await buildApp()
  app.queue = {
    async send() {
      throw new Error('sqs is unreachable')
    },
  }
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
      payload: JSON.stringify({ ...VALID, model: undefined }),
    })
    assert.equal(res.statusCode, 502)
    assert.equal(res.json().result.errorReason, 'QUEUE_UNAVAILABLE')
    assert.match(res.json().result.signalId, /^[0-9A-HJKMNP-TV-Z]{26}$/)
  } finally {
    await app.close()
  }
})
