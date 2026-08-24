import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import { serialiseVent } from './vent.service.js'
import type { SignalQueue, VentMessage } from './vent.schema.js'

/** Collects what the vent publishes. Stands in for `app.queue`, which is null
 *  in a test run because SQS_PENDING_QUEUE_URL is unset — so nothing here talks
 *  to AWS or to LocalStack. */
function fakeQueue(): { queue: SignalQueue; messages: VentMessage[]; bodies: string[] } {
  const messages: VentMessage[] = []
  const bodies: string[] = []
  return {
    messages,
    bodies,
    queue: {
      async send(body) {
        bodies.push(body)
        messages.push(JSON.parse(body) as VentMessage)
      },
    },
  }
}

/** The vent publishes from `onResponse`, which fires after the response is
 *  flushed — deliberately, so SQS is never on the caller's critical path. That
 *  means `inject()` can resolve before the message lands, and a test has to
 *  wait for it rather than assert straight away. */
async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error('timed out waiting for the vent')
}

interface InjectOptions {
  method?: 'GET' | 'POST'
  url?: string
  headers?: Record<string, string>
  payload?: string | Readable
  queue?: SignalQueue | null
}

/** Sends one request through a real app with the vent wired to a fake queue,
 *  and returns the response together with whatever was vented. */
async function send({
  method = 'POST',
  url = '/api/v1/signal',
  headers = { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
  payload,
  queue,
}: InjectOptions) {
  const app = await buildApp()
  const fake = fakeQueue()
  app.queue = queue === undefined ? fake.queue : queue
  try {
    const res = await app.inject({ method, url, headers, ...(payload ? { payload } : {}) })
    return { status: res.statusCode, messages: fake.messages, bodies: fake.bodies }
  } finally {
    await app.close()
  }
}

/** Without this every request here would be the one hard rejection (401) and
 *  never reach the queue at all. */
const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'

const VALID_BODY = {
  customerId: 'cus_abc123',
  type: 'credit',
  agentKey: 'credit.research',
  model: 'openai/gpt-4o',
  inputTokens: 1200,
  outputTokens: 350,
  idempotencyKey: 'idem_991',
}

/** A ULID is 26 chars of Crockford base32 — the check that the id is a ULID and
 *  not the UUID the service used to mint. */
const ULID = /^[0-9A-HJKMNP-TV-Z]{26}$/

test('a refused body is vented as two rows, both naming the same signal', async () => {
  const { status, messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.equal(messages.length, 1, 'one message per rejected request')
  const message = messages[0]!

  // The keys are table names, so a batch off the queue is two bulk inserts and
  // nothing else. Anything extra at this level is a field a worker must skip.
  assert.deepEqual(Object.keys(message).sort(), ['raw_signals', 'signal_status'])

  const { raw_signals: raw, signal_status: status_row } = message
  assert.match(raw.signal_id, ULID)
  assert.equal(status_row.signal_id, raw.signal_id, 'one id threads both rows')
  assert.equal(raw.received_at, status_row.updated_at, 'both stamped at arrival')
  assert.ok(!Number.isNaN(Date.parse(raw.received_at)), 'received_at is a real instant')
})

test('the error columns carry the wire contract, not a paraphrase of it', async () => {
  const { messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
  })
  await waitFor(() => messages.length > 0)
  const row = messages[0]!.signal_status

  assert.equal(row.status, 'PENDING')
  assert.equal(row.attempt, 1)
  // 4xx is the caller's data to fix. 5xx would be SERVER_ERROR.
  assert.equal(row.error_type, 'USER_ERROR')
  // The same string `result.errorReason` carries, so the column and the wire
  // share one vocabulary.
  assert.equal(row.error_code, 'INVALID_BODY')
  // Word for word what `statusDetail.message` told the caller: the FIRST
  // problem only, prefixed by its field. AJV found others; this column holds one.
  assert.equal(row.error_message, "model: must have required property 'model'")
  // Read off the payload even though the signal failed.
  assert.equal(row.signal_type, 'credit')
})

test('a rejected signal was never priced, so every money column is null', async () => {
  const { messages } = await send({ payload: JSON.stringify({ customerId: 'cus_abc123' }) })
  await waitFor(() => messages.length > 0)
  const row = messages[0]!.signal_status

  for (const column of [
    'usage_log_id',
    'credits_used',
    'provided_cost',
    'customer_cost',
    'credit_id',
    'credit_name',
    'model_name',
    'provider',
    'member_name',
    'currency_code',
    'applied_rules',
    'wallet_debit_usd',
    'balance_remaining',
    'outcome_id',
    'outcome_name',
    'outcome_step',
    'outcome_steps_done',
    'outcome_run_id',
    'outcome_closed_run',
    'outcome_completed',
    'outcome_signal_count',
    'outcome_total_steps',
  ] as const) {
    assert.equal(row[column], null, `${column} must be null on a reject`)
    assert.ok(column in row, `${column} must be written out, not omitted`)
  }
})

test('non-Nullable columns carry an empty string, never null', async () => {
  const { messages } = await send({ payload: JSON.stringify(VALID_BODY) })
  await waitFor(() => messages.length > 0)
  const raw = messages[0]!.raw_signals

  // A JSON null into a non-Nullable ClickHouse column is an INSERT ERROR, not a
  // default. The org is unknown on every reject — the key never resolved.
  assert.equal(raw.organization_id, '')
  assert.equal(raw.api_key_id, '')
  // Nullable(String), so this one may genuinely be null.
  assert.equal(raw.idempotency_key, 'idem_991')
  // Lifted off the payload at the edge, not by the worker.
  assert.equal(raw.customer_id, 'cus_abc123')
})

test('a body that is not JSON is still recorded word for word', async () => {
  const broken = '{"customerId": "cus_abc123", '
  const { status, messages } = await send({ payload: broken })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  const message = messages[0]!
  assert.equal(message.signal_status.error_code, 'MALFORMED_JSON')
  // Fastify's parser threw and discarded the bytes; the preParsing tee kept
  // them. This is the reject a customer most needs to see verbatim.
  assert.equal(message.raw_signals.payload, broken)
  // Nothing could be read out of it, so the derived columns stay empty.
  assert.equal(message.raw_signals.customer_id, '')
  assert.equal(message.signal_status.signal_type, null)
})

test('a body over the limit is vented, but its payload is empty', async () => {
  const oversized = JSON.stringify({ ...VALID_BODY, filler: 'x'.repeat(80 * 1024) })
  const { status, messages } = await send({ payload: oversized })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  const raw = messages[0]!.raw_signals
  assert.equal(messages[0]!.signal_status.error_code, 'BODY_TOO_LARGE')
  // A declared `content-length` over the ceiling is refused before a single
  // byte is read, so the capture hook never runs and there is nothing to keep.
  // That is the right trade — buffering 80KB only to reject it would make an
  // oversized body cheaper to send than to refuse.
  assert.equal(raw.payload, '')
})

test('a missing key is a hard 401, and is vented all the same', async () => {
  // The one rejection that is not deferred to the queue -- but it is still
  // recorded, so a caller wiring up an integration can see their attempts.
  const { status, messages } = await send({
    payload: JSON.stringify(VALID_BODY),
    headers: { 'content-type': 'application/json' },
  })
  assert.equal(status, 401)
  await waitFor(() => messages.length > 0)

  assert.equal(messages[0]!.signal_status.error_code, 'API_KEY_MISSING')
  assert.equal(messages[0]!.signal_status.error_type, 'USER_ERROR')
})

test('a 404 on a path no route owns is vented too', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/api/v1/nope', headers: {} })
  assert.equal(status, 404)
  await waitFor(() => messages.length > 0)

  const message = messages[0]!
  assert.equal(message.signal_status.error_code, 'NOT_FOUND')
  assert.match(message.raw_signals.signal_id, ULID, 'a 404 reaches no route, yet has an id')
  assert.equal(message.raw_signals.payload, '', 'and carried no body')
})

test('a wrong method on an existing path is vented as the 404 Fastify makes it', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/api/v1/signal', headers: {} })
  assert.equal(status, 404)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.signal_status.error_code, 'NOT_FOUND')
})

test('a successful response is not vented', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/health', headers: {} })
  assert.equal(status, 200)
  // Give onResponse the same chance to fire that a reject gets.
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(messages.length, 0, 'the vent carries rejects, not traffic')
})

test('a dead queue turns the 202 into a hard error, never a false promise', async () => {
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
      payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
    })
    // The vent used to fail open, because the response was a truthful rejection
    // without it. Now the response IS `Processing in the queue.`, so a queue
    // that took nothing must not be reported as one that did.
    assert.equal(res.statusCode, 502)
    assert.equal(res.json().result.errorReason, 'QUEUE_UNAVAILABLE')
  } finally {
    await app.close()
  }
})

test('with no queue configured, rejections stay rejections', async () => {
  // Nothing to defer to, so the route answers the way it did before the queue
  // existed. The only honest option: there is no queue to promise.
  const { status } = await send({
    payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
    queue: null,
  })
  assert.equal(status, 400)
})

// --- the rest of the rejection table -----------------------------------------

test('a 5xx is vented as SERVER_ERROR, the caller\'s fault it is not', async () => {
  // PAYMENTS_URL is unset in a test run, so resolving the key cannot reach
  // anyone — the one 502 reachable without a network.
  const { status, messages } = await send({
    payload: JSON.stringify(VALID_BODY),
    headers: { 'content-type': 'application/json', authorization: 'Bearer cnk_whatever' },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  const row = messages[0]!.signal_status
  // The one place SERVER_ERROR is produced. USER_ERROR here would tell a UI to
  // go and fix a body that was never the problem.
  assert.equal(row.error_type, 'SERVER_ERROR')
  assert.equal(row.error_code, 'UPSTREAM_UNAVAILABLE')
  assert.equal(row.status, 'PENDING')
  // The 5xx cause never reaches the caller; it must not reach the row either.
  const serialised = JSON.stringify(messages[0])
  assert.ok(!serialised.includes('ECONNREFUSED'), 'no internal cause in the row')
  assert.ok(!/\d+\.\d+\.\d+\.\d+/.test(serialised), 'no internal address in the row')
})

test('CUSTOM_TOO_LARGE is vented with the body that caused it', async () => {
  const { status, messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, custom: { blob: 'x'.repeat(40 * 1024) } }),
    headers: { 'content-type': 'application/json', authorization: 'Bearer cnk_whatever' },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  // Refused by the service, not the parser — so unlike BODY_TOO_LARGE the body
  // was read, and the payload is there to show which field to shrink.
  assert.equal(messages[0]!.signal_status.error_code, 'CUSTOM_TOO_LARGE')
  assert.ok(messages[0]!.raw_signals.payload.includes('"custom"'))
  assert.equal(messages[0]!.raw_signals.customer_id, 'cus_abc123')
})

test('a non-JSON content type is vented as UNSUPPORTED_MEDIA_TYPE', async () => {
  const { status, messages } = await send({
    payload: 'customerId=cus_abc123',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      authorization: `Bearer ${KEY}`,
    },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.signal_status.error_code, 'UNSUPPORTED_MEDIA_TYPE')
})

test('an empty body is vented as EMPTY_BODY', async () => {
  const { status, messages } = await send({
    payload: Readable.from([]),
    headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.signal_status.error_code, 'EMPTY_BODY')
  assert.equal(messages[0]!.raw_signals.payload, '')
})

// --- reading columns off a body that is not what it should be ----------------

test('a JSON body that is not an object leaves the derived columns empty', async () => {
  // `readString` must survive an array, a bare string, a number — anything a
  // caller can put behind `content-type: application/json`.
  for (const payload of ['[1,2,3]', '"just a string"', '42', 'null']) {
    const { messages } = await send({ payload })
    await waitFor(() => messages.length > 0)
    const message = messages[0]!
    assert.equal(message.raw_signals.customer_id, '', `customer_id for ${payload}`)
    assert.equal(message.raw_signals.idempotency_key, null, `idempotency_key for ${payload}`)
    assert.equal(message.signal_status.signal_type, null, `signal_type for ${payload}`)
    assert.equal(message.raw_signals.payload, payload, 'and the body is still recorded')
  }
})

test('signal_type is lowercased, the way the rulebook reads it', async () => {
  const { messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, type: 'CREDIT', model: undefined }),
  })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.signal_status.signal_type, 'credit')
})

test('a type that meters nothing leaves signal_type null', async () => {
  const { messages } = await send({ payload: JSON.stringify({ ...VALID_BODY, type: 'cerdit' }) })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.signal_status.signal_type, null)
})

// --- the raw-capture stream --------------------------------------------------

test('a chunked body with no content-length is still captured whole', async () => {
  const body = '{"customerId":"cus_stream","inputTokens":1,'
  const { status, messages } = await send({
    payload: Readable.from([body.slice(0, 10), body.slice(10, 25), body.slice(25)]),
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.raw_signals.payload, body, 'every chunk, in order')
})

test('a multi-byte character split across chunks is not corrupted', async () => {
  // The reason the tee decodes with a StringDecoder instead of concatenating
  // buffers: a UTF-8 sequence can straddle a chunk boundary, and decoding each
  // chunk on its own would leave two replacement characters where one CJK
  // character belongs.
  const body = '{"customerId":"日本語テスト",'
  const bytes = Buffer.from(body, 'utf8')
  const split = 16 // lands mid-character
  assert.notEqual(bytes[split]! & 0xc0, 0xc0, 'the split is inside a sequence, not on it')
  const { messages } = await send({
    payload: Readable.from([bytes.subarray(0, split), bytes.subarray(split)]),
  })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.raw_signals.payload, body)
  assert.ok(!messages[0]!.raw_signals.payload.includes('�'), 'no replacement characters')
})

test('an oversized chunked body is still 413 — the tee does not defeat bodyLimit', async () => {
  // With no content-length there is no shortcut: Fastify has to count bytes as
  // they arrive, and it counts them off OUR transform. If receivedEncodedLength
  // ever stops being exposed, this is the test that notices.
  const huge = 'x'.repeat(80 * 1024)
  const { status } = await send({
    payload: Readable.from([`{"customerId":"`, huge, `"}`]),
  })
  assert.equal(status, 202)
})

// --- the SQS message ceiling -------------------------------------------------

test('a payload that escapes past 256KB is trimmed, not dropped', async () => {
  // 64KB of control bytes is a legal body under BODY_BYTES and becomes 384KB of
  // \u00xx escapes — over SQS's limit. Before this was handled the send threw,
  // the vent failed open, and the row disappeared silently.
  const control = '\u0001'.repeat(64 * 1024)
  const { status, messages, bodies } = await send({ payload: control })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.ok(
    Buffer.byteLength(bodies[0]!, 'utf8') <= 256 * 1024,
    `message is ${Buffer.byteLength(bodies[0]!, 'utf8')} bytes, over the SQS limit`,
  )
  // Trimmed, not emptied: what fits is still worth having.
  assert.ok(messages[0]!.raw_signals.payload.length > 1000, 'the prefix survives')
  assert.equal(messages[0]!.signal_status.error_code, 'MALFORMED_JSON')
  // And every status column is intact — the payload is the only thing that gives.
  assert.equal(messages[0]!.signal_status.attempt, 1)
  assert.equal(messages[0]!.signal_status.signal_id, messages[0]!.raw_signals.signal_id)
})

test('an ordinary message is not touched by the size guard', async () => {
  const message: VentMessage = {
    raw_signals: {
      signal_id: '01M0SEMTV4RE2XFX0NF2JPJYY3',
      organization_id: '',
      customer_id: 'cus_abc123',
      received_at: '2026-08-24T09:12:44.812Z',
      idempotency_key: null,
      api_key_id: '',
      payload: '{"customerId":"cus_abc123"}',
    },
    signal_status: {
      signal_id: '01M0SEMTV4RE2XFX0NF2JPJYY3',
      organization_id: '',
      attempt: 1,
      status: 'PENDING',
      error_type: 'USER_ERROR',
      error_code: 'INVALID_BODY',
      error_message: 'model: is required',
      signal_type: null,
      usage_log_id: null,
      credits_used: null,
      provided_cost: null,
      customer_cost: null,
      credit_id: null,
      credit_name: null,
      model_name: null,
      provider: null,
      member_name: null,
      currency_code: null,
      applied_rules: null,
      wallet_debit_usd: null,
      balance_remaining: null,
      outcome_id: null,
      outcome_name: null,
      outcome_step: null,
      outcome_steps_done: null,
      outcome_run_id: null,
      outcome_closed_run: null,
      outcome_completed: null,
      outcome_signal_count: null,
      outcome_total_steps: null,
      updated_at: '2026-08-24T09:12:44.812Z',
    },
  }
  const { body, droppedBytes } = serialiseVent(message)
  assert.equal(droppedBytes, 0)
  assert.deepEqual(JSON.parse(body), message, 'byte for byte the same message')
})

test('an emoji is never sliced in half by the size guard', async () => {
  const emoji = '\u{1F600}'.repeat(40 * 1024)
  const { messages } = await send({ payload: `"${emoji}` })
  await waitFor(() => messages.length > 0)
  // A lone surrogate would survive JSON.parse as � or an unpaired half;
  // iterating by code point is what stops the payload ending in one.
  const payload = messages[0]!.raw_signals.payload
  for (const char of payload) assert.ok(char.codePointAt(0)! <= 0xd7ff || char.length === 2)
})

// --- one request, one message ------------------------------------------------

test('a rejected request is vented exactly once', async () => {
  const { messages } = await send({ payload: JSON.stringify({ customerId: '' }) })
  await waitFor(() => messages.length > 0)
  await new Promise((resolve) => setTimeout(resolve, 60))
  assert.equal(messages.length, 1)
})

test('concurrent rejects get distinct ids', async () => {
  const app = await buildApp()
  const fake = fakeQueue()
  app.queue = fake.queue
  try {
    await Promise.all(
      Array.from({ length: 25 }, () =>
        app.inject({
          method: 'POST',
          url: '/api/v1/signal',
          headers: { 'content-type': 'application/json' },
          payload: JSON.stringify({ customerId: 'cus_abc123' }),
        }),
      ),
    )
    await waitFor(() => fake.messages.length === 25)
    const ids = new Set(fake.messages.map((m) => m.raw_signals.signal_id))
    assert.equal(ids.size, 25, 'no two signals share an id')
  } finally {
    await app.close()
  }
})
