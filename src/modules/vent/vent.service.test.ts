import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import { serialiseMessage } from './vent.service.js'
import type { PendingMessage, SignalQueue } from './vent.schema.js'

/** Collects what the vent publishes. Stands in for `app.queue`, which is null
 *  in a test run because SQS_PENDING_QUEUE_URL is unset — so nothing here talks
 *  to AWS or to LocalStack. */
function fakeQueue(): { queue: SignalQueue; messages: PendingMessage[]; bodies: string[] } {
  const messages: PendingMessage[] = []
  const bodies: string[] = []
  return {
    messages,
    bodies,
    queue: {
      async send(body) {
        bodies.push(body)
        messages.push(JSON.parse(body) as PendingMessage)
      },
    },
  }
}

/** The vent publishes from `onResponse`, which fires after the response is
 *  flushed. `inject()` can resolve before the message lands, so wait for it. */
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

const ULID = /^[0-9A-HJKMNP-TV-Z]{26}$/

test('a refused signal is vented as one message: a raw_signals row + an error_code', async () => {
  const { status, messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.equal(messages.length, 1, 'one message per rejected request')
  const message = messages[0]!

  // raw_signals inserts directly; error_code is what the consumer needs to
  // build the signal's Failed event. Nothing else belongs at this level.
  assert.deepEqual(Object.keys(message).sort(), ['error_code', 'raw_signals'])

  assert.match(message.raw_signals.signal_id, ULID)
  assert.ok(!Number.isNaN(Date.parse(message.raw_signals.received_at)), 'received_at is real')
})

test('error_code carries the wire contract verbatim', async () => {
  const { messages } = await send({ payload: JSON.stringify({ ...VALID_BODY, model: undefined }) })
  await waitFor(() => messages.length > 0)
  // The same string `result.errorReason` carries, so the column and the wire
  // share one vocabulary. The pending consumer writes it onto a Failed event.
  assert.equal(messages[0]!.error_code, 'INVALID_BODY')
  // The type is read off the payload even though the signal failed.
  assert.equal(messages[0]!.raw_signals.type, 'credit')
})

test('non-Nullable columns carry an empty string, never null', async () => {
  const { messages } = await send({ payload: JSON.stringify(VALID_BODY) })
  await waitFor(() => messages.length > 0)
  const raw = messages[0]!.raw_signals

  // A JSON null into a non-Nullable ClickHouse column is an INSERT ERROR. The
  // org is unknown on every reject — the key never resolved.
  assert.equal(raw.organization_id, '')
  // Nullable(String), so this one may genuinely be null.
  assert.equal(raw.idempotency_key, 'idem_991')
  assert.equal(raw.customer_id, 'cus_abc123')
})

test('a body that is not JSON is still recorded word for word', async () => {
  const broken = '{"customerId": "cus_abc123", '
  const { status, messages } = await send({ payload: broken })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  const message = messages[0]!
  assert.equal(message.error_code, 'MALFORMED_JSON')
  assert.equal(message.raw_signals.payload, broken)
  // Nothing could be read out of it, so the derived columns stay empty/null.
  assert.equal(message.raw_signals.customer_id, '')
  assert.equal(message.raw_signals.type, null)
})

test('a body over the limit is vented, but its payload is empty', async () => {
  const oversized = JSON.stringify({ ...VALID_BODY, filler: 'x'.repeat(80 * 1024) })
  const { status, messages } = await send({ payload: oversized })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.equal(messages[0]!.error_code, 'BODY_TOO_LARGE')
  // A declared content-length over the ceiling is refused before a byte is
  // read, so the capture hook never runs and there is nothing to keep.
  assert.equal(messages[0]!.raw_signals.payload, '')
})

test('a missing key is a hard 401, and is vented all the same', async () => {
  const { status, messages } = await send({
    payload: JSON.stringify(VALID_BODY),
    headers: { 'content-type': 'application/json' },
  })
  assert.equal(status, 401)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.error_code, 'API_KEY_MISSING')
})

test('a 404 on a path no route owns is vented too', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/api/v1/nope', headers: {} })
  assert.equal(status, 404)
  await waitFor(() => messages.length > 0)
  const message = messages[0]!
  assert.equal(message.error_code, 'NOT_FOUND')
  assert.match(message.raw_signals.signal_id, ULID, 'a 404 reaches no route, yet has an id')
  assert.equal(message.raw_signals.payload, '', 'and carried no body')
})

test('a wrong method on an existing path is vented as the 404 Fastify makes it', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/api/v1/signal', headers: {} })
  assert.equal(status, 404)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.error_code, 'NOT_FOUND')
})

test('a successful response is not vented', async () => {
  const { status, messages } = await send({ method: 'GET', url: '/health', headers: {} })
  assert.equal(status, 200)
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
    assert.equal(res.statusCode, 502)
    assert.equal(res.json().result.errorReason, 'QUEUE_UNAVAILABLE')
  } finally {
    await app.close()
  }
})

test('with no queue configured, rejections stay rejections', async () => {
  const { status } = await send({
    payload: JSON.stringify({ ...VALID_BODY, model: undefined }),
    queue: null,
  })
  assert.equal(status, 400)
})

test('a 5xx is vented, and leaks no internal cause', async () => {
  // PAYMENTS_URL is unset in a test run, so resolving the key cannot reach
  // anyone — the one 502 reachable without a network. It is deferred to 202.
  const { status, messages } = await send({
    payload: JSON.stringify(VALID_BODY),
    headers: { 'content-type': 'application/json', authorization: 'Bearer cnk_whatever' },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.equal(messages[0]!.error_code, 'UPSTREAM_UNAVAILABLE')
  const serialised = JSON.stringify(messages[0])
  assert.ok(!serialised.includes('ECONNREFUSED'), 'no internal cause in the message')
  assert.ok(!/\d+\.\d+\.\d+\.\d+/.test(serialised), 'no internal address in the message')
})

test('CUSTOM_TOO_LARGE is vented with the body that caused it', async () => {
  const { status, messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, custom: { blob: 'x'.repeat(40 * 1024) } }),
    headers: { 'content-type': 'application/json', authorization: 'Bearer cnk_whatever' },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.error_code, 'CUSTOM_TOO_LARGE')
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
  assert.equal(messages[0]!.error_code, 'UNSUPPORTED_MEDIA_TYPE')
})

test('an empty body is vented as EMPTY_BODY', async () => {
  const { status, messages } = await send({
    payload: Readable.from([]),
    headers: { 'content-type': 'application/json', authorization: `Bearer ${KEY}` },
  })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.error_code, 'EMPTY_BODY')
  assert.equal(messages[0]!.raw_signals.payload, '')
})

test('a JSON body that is not an object leaves the derived columns empty', async () => {
  for (const payload of ['[1,2,3]', '"just a string"', '42', 'null']) {
    const { messages } = await send({ payload })
    await waitFor(() => messages.length > 0)
    const message = messages[0]!
    assert.equal(message.raw_signals.customer_id, '', `customer_id for ${payload}`)
    assert.equal(message.raw_signals.idempotency_key, null, `idempotency_key for ${payload}`)
    assert.equal(message.raw_signals.type, null, `type for ${payload}`)
    assert.equal(message.raw_signals.payload, payload, 'and the body is still recorded')
  }
})

test('type is lowercased, the way the rulebook reads it', async () => {
  const { messages } = await send({
    payload: JSON.stringify({ ...VALID_BODY, type: 'CREDIT', model: undefined }),
  })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.raw_signals.type, 'credit')
})

test('a type that meters nothing leaves type null', async () => {
  const { messages } = await send({ payload: JSON.stringify({ ...VALID_BODY, type: 'cerdit' }) })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.raw_signals.type, null)
})

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
  const body = '{"customerId":"日本語テスト",'
  const bytes = Buffer.from(body, 'utf8')
  const split = 16
  assert.notEqual(bytes[split]! & 0xc0, 0xc0, 'the split is inside a sequence, not on it')
  const { messages } = await send({
    payload: Readable.from([bytes.subarray(0, split), bytes.subarray(split)]),
  })
  await waitFor(() => messages.length > 0)
  assert.equal(messages[0]!.raw_signals.payload, body)
  assert.ok(!messages[0]!.raw_signals.payload.includes('�'), 'no replacement characters')
})

test('an oversized chunked body is still refused — the tee does not defeat bodyLimit', async () => {
  const huge = 'x'.repeat(80 * 1024)
  const { status } = await send({ payload: Readable.from([`{"customerId":"`, huge, `"}`]) })
  assert.equal(status, 202)
})

test('a payload that escapes past 256KB is trimmed, not dropped', async () => {
  const control = '\u0001'.repeat(64 * 1024)
  const { status, messages, bodies } = await send({ payload: control })
  assert.equal(status, 202)
  await waitFor(() => messages.length > 0)

  assert.ok(
    Buffer.byteLength(bodies[0]!, 'utf8') <= 256 * 1024,
    `message is ${Buffer.byteLength(bodies[0]!, 'utf8')} bytes, over the SQS limit`,
  )
  assert.ok(messages[0]!.raw_signals.payload.length > 1000, 'the prefix survives')
  assert.equal(messages[0]!.error_code, 'MALFORMED_JSON')
})

test('an ordinary message is not touched by the size guard', async () => {
  const message: PendingMessage = {
    raw_signals: {
      signal_id: '01M0SEMTV4RE2XFX0NF2JPJYY3',
      organization_id: '',
      customer_id: 'cus_abc123',
      type: null,
      idempotency_key: null,
      payload: '{"customerId":"cus_abc123"}',
      received_at: '2026-08-24T09:12:44.812Z',
    },
    error_code: 'INVALID_BODY',
  }
  const { body, droppedBytes } = serialiseMessage(message)
  assert.equal(droppedBytes, 0)
  assert.deepEqual(JSON.parse(body), message, 'byte for byte the same message')
})

test('an emoji is never sliced in half by the size guard', async () => {
  const emoji = '\u{1F600}'.repeat(40 * 1024)
  const { messages } = await send({ payload: `"${emoji}` })
  await waitFor(() => messages.length > 0)
  const payload = messages[0]!.raw_signals.payload
  for (const char of payload) assert.ok(char.codePointAt(0)! <= 0xd7ff || char.length === 2)
})

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
