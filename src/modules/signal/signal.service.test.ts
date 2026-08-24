import assert from 'node:assert/strict'
import { test } from 'node:test'
import { buildApp } from '../../app.js'

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

/** Posts a body and returns `{ status, envelope }`. Every case here is decided
 *  before the payments app would be called — by routing, parsing, the schema,
 *  or the missing-key check — so these tests make no network requests. */
async function post(
  body: unknown,
  headers: Record<string, string> = {},
  { raw = false }: { raw?: boolean } = {},
) {
  const app = await buildApp()
  try {
    const res = await app.inject({
      method: 'POST',
      url: '/api/v1/signal',
      headers: { 'content-type': 'application/json', ...headers },
      payload: raw ? (body as string) : JSON.stringify(body),
    })
    return { status: res.statusCode, envelope: res.json() }
  } finally {
    await app.close()
  }
}

interface ErrorEnvelope {
  statusCode: number
  statusDetail: { status: string; message: string }
  result: { errorReason: string; issues?: { field: string; message: string }[] }
}

/** Asserts the envelope shape every response shares, then the reason. */
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
  type: 'credit',
  agentKey: 'credit.research',
  member: 'dana@acme.com',
  model: 'openai/gpt-4o',
  inputTokens: 1200,
  outputTokens: 350,
  cacheTokens: 200,
  custom: { feature: 'chat', region: 'eu' },
}

// --- the request never got as far as being understood -------------------------

test('an unknown route answers in the same envelope', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/v1/nope' })
  assertRejected({ status: res.statusCode, envelope: res.json() }, 404, 'NOT_FOUND')
  await app.close()
})

test('a non-JSON content type is a 415, not a confusing 400', async () => {
  assertRejected(await post('hi', { 'content-type': 'text/plain' }, { raw: true }), 415, 'UNSUPPORTED_MEDIA_TYPE')
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

test('malformed JSON is MALFORMED_JSON, not INVALID_BODY', async () => {
  assertRejected(await post('{nope', {}, { raw: true }), 400, 'MALFORMED_JSON')
})

test('an empty body is EMPTY_BODY', async () => {
  assertRejected(await post('', {}, { raw: true }), 400, 'EMPTY_BODY')
})

test('a body over the limit is BODY_TOO_LARGE, before validation', async () => {
  assertRejected(await post({ ...VALID, filler: 'x'.repeat(80 * 1024) }), 413, 'BODY_TOO_LARGE')
})

// --- the signal itself was judged and refused --------------------------------

test('a rejected body lists every broken field, not just the first', async () => {
  const envelope = assertRejected(
    await post({ ...VALID, customerId: '  ', inputTokens: -1, outputTokens: '5' }),
    400,
    'INVALID_BODY',
  )
  const fields = (envelope.result.issues ?? []).map((i) => i.field)
  assert.deepEqual([...new Set(fields)].sort(), ['customerId', 'inputTokens', 'outputTokens'])
})

test('a whitespace-only id is empty once trimmed, so it is refused', async () => {
  assertRejected(await post({ ...VALID, customerId: '   ' }), 400, 'INVALID_BODY')
})

test('token counts are required, and 0 is a valid count', async () => {
  const { inputTokens: _drop, ...missing } = VALID
  const envelope = assertRejected(await post(missing), 400, 'INVALID_BODY')
  assert.equal((envelope.result.issues ?? [])[0]?.field, 'inputTokens')
  // 0 must pass: "send 0 when there are none" is the upstream contract.
  assertRejected(
    await post({ ...VALID, inputTokens: 0, outputTokens: 0 }),
    401,
    'API_KEY_MISSING',
  )
})

test('a token count is not coerced from a string', async () => {
  // Zod rejects "1200" upstream, so accepting it here would mean accepting a
  // signal that settlement later refuses.
  assertRejected(await post({ ...VALID, inputTokens: '1200' }), 400, 'INVALID_BODY')
})

test('a negative or fractional token count is refused', async () => {
  assertRejected(await post({ ...VALID, inputTokens: -1 }), 400, 'INVALID_BODY')
  assertRejected(await post({ ...VALID, outputTokens: 1.5 }), 400, 'INVALID_BODY')
})

test('any type requires a model', async () => {
  const { model: _drop, ...noModel } = VALID
  const envelope = assertRejected(await post(noModel), 400, 'INVALID_BODY')
  assert.equal((envelope.result.issues ?? [])[0]?.field, 'model')
})

test('type credit requires an agentKey, and names only agentKey', async () => {
  const { agentKey: _drop, ...noKey } = VALID
  const envelope = assertRejected(await post(noKey), 400, 'INVALID_BODY')
  const fields = (envelope.result.issues ?? []).map((i) => i.field)
  assert.deepEqual(fields, ['agentKey'])
  assert.ok(!fields.includes('key'), 'never tells the caller to send the deprecated alias')
})

test('the deprecated key alias still satisfies the agentKey rule', async () => {
  const { agentKey: _drop, ...noKey } = VALID
  assertRejected(
    await post({ ...noKey, key: 'credit.research' }),
    401,
    'API_KEY_MISSING',
  )
})

test('type outcome requires a runId as well', async () => {
  const outcome = { ...VALID, type: 'outcome' }
  assertRejected(await post(outcome), 400, 'INVALID_BODY')
  assertRejected(await post({ ...outcome, runId: 'run_1' }), 401, 'API_KEY_MISSING')
})

test('type is accepted case-insensitively', async () => {
  // API_KEY_MISSING (not INVALID_BODY) proves the value was lowercased first.
  assertRejected(await post({ ...VALID, type: ' Credit ' }), 401, 'API_KEY_MISSING')
})

test('an unknown type is refused', async () => {
  assertRejected(await post({ ...VALID, type: 'unit' }), 400, 'INVALID_BODY')
})

test('an unknown top-level key survives — it may be a pricing-metric refId', async () => {
  assertRejected(await post({ ...VALID, voice_ai: 'wbwbewj21' }), 401, 'API_KEY_MISSING')
})

test('an oversized custom blob is CUSTOM_TOO_LARGE, not BODY_TOO_LARGE', async () => {
  // The request as a whole was fine — one field needs shrinking, and the reason
  // has to say which.
  assertRejected(
    await post({ ...VALID, custom: { blob: 'x'.repeat(40 * 1024) } }, { authorization: `Bearer ${KEY}` }),
    413,
    'CUSTOM_TOO_LARGE',
  )
})

// --- the caller could not be identified --------------------------------------

test('a missing API key is API_KEY_MISSING', async () => {
  assertRejected(await post(VALID), 401, 'API_KEY_MISSING')
})

test('a bare token without the Bearer scheme is also API_KEY_MISSING', async () => {
  assertRejected(await post(VALID, { authorization: KEY }), 401, 'API_KEY_MISSING')
})

// --- our side failed ---------------------------------------------------------

test('an unreachable payments app is a retryable 502, and leaks nothing', async () => {
  const envelope = assertRejected(
    await post(VALID, { authorization: `Bearer ${KEY}` }),
    502,
    'UPSTREAM_UNAVAILABLE',
  )
  const body = JSON.stringify(envelope)
  assert.ok(!body.includes('ECONNREFUSED'), 'the underlying cause stays in the logs')
  assert.ok(!/\d+\.\d+\.\d+\.\d+/.test(body), 'no internal address on the wire')
})
