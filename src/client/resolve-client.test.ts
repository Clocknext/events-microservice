/** The consumer's resolve call. What these pin is the STATUS MAPPING, because
 *  conflating any two of its three outcomes breaks the pipeline in a different
 *  way:
 *
 *    treating an outage as a rejection  → good signals archived as caller errors
 *    treating a rejection as an outage  → the topic stalls behind one bad body
 *    treating OUR 401 as the customer's → every signal on the topic refused,
 *                                         silently, until a human notices */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { MisconfiguredError } from './payments-client.js'
import { resolveSignal } from './resolve-client.js'
import type { SignalMessage } from '../modules/signal/signal.schema.js'

const DIGEST = 'a'.repeat(64)

function message(over: Partial<SignalMessage> = {}): SignalMessage {
  return {
    signalId: '01M0X91S3X2SK86WYYWVENM3N7',
    receivedAt: '2026-08-26T10:00:00.000Z',
    apiKeyHash: DIGEST,
    body: { customerId: 'cus_1', inputTokens: 10, outputTokens: 5 },
    ...over,
  }
}

/** Replaces global fetch for one call and hands back what the client sent. */
async function capture(
  respond: () => { status: number; body: string } | Error,
  run: () => Promise<unknown>,
) {
  const original = globalThis.fetch
  let sent: { url: string; headers: Record<string, string>; body: string } | undefined
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    sent = {
      url: String(input),
      headers: (init?.headers ?? {}) as Record<string, string>,
      body: String(init?.body ?? ''),
    }
    const answer = respond()
    if (answer instanceof Error) throw answer
    return new Response(answer.body, { status: answer.status })
  }) as typeof globalThis.fetch
  try {
    const result = await run()
    return { result, sent }
  } finally {
    globalThis.fetch = original
  }
}

function envelope(message: string, result: unknown): string {
  return JSON.stringify({ statusDetail: { message }, result })
}

// ── the request ──────────────────────────────────────────────────────────────

test('the DIGEST travels and the raw key never does', async () => {
  const { sent } = await capture(
    () => ({ status: 200, body: envelope('ok', { apiKey: { organizationId: 'org_1' } }) }),
    () => resolveSignal(message()),
  )
  assert.equal(sent?.headers['x-api-key-hash'], DIGEST)
  assert.match(sent?.headers.authorization ?? '', /^Bearer /)
  // The `cnk_…` key never left the edge process that first saw it — there is
  // nothing here that could carry it.
  assert.doesNotMatch(sent?.body ?? '', /cnk_/)
  assert.match(sent?.url ?? '', /\/api\/internal\/resolve$/)
})

test('the body sent is the caller’s body, verbatim', async () => {
  const body = { customerId: 'cus_1', inputTokens: 1, outputTokens: 2, custom: { a: 1 } }
  const { sent } = await capture(
    () => ({ status: 200, body: envelope('ok', { apiKey: { organizationId: 'org_1' } }) }),
    () => resolveSignal(message({ body })),
  )
  // The same bytes that become `signal_log.payload` — payments judges exactly
  // what the caller sent, not an envelope we built around it.
  assert.deepEqual(JSON.parse(sent?.body ?? '{}'), body)
})

// ── ok ───────────────────────────────────────────────────────────────────────

test('200 with an organizationId is the accepted path', async () => {
  const { result } = await capture(
    () => ({ status: 200, body: envelope('ok', { apiKey: { organizationId: 'org_1' } }) }),
    () => resolveSignal(message()),
  )
  assert.deepEqual(result, { kind: 'ok', organizationId: 'org_1' })
})

test('200 naming NO organisation is transient, not accepted', async () => {
  // A contract violation on our own side. Retrying is right — refusing the signal
  // would lose it over our bug, and accepting it would archive a row with no
  // tenant while claiming it was resolved.
  const { result } = await capture(
    () => ({ status: 200, body: envelope('ok', { apiKey: {} }) }),
    () => resolveSignal(message()),
  )
  assert.equal((result as { kind: string }).kind, 'transient')
})

// ── ours, not the caller's ───────────────────────────────────────────────────

test('401 is OUR shared secret and throws — it is never a customer problem', async () => {
  // The single most expensive confusion available here: a 401 read as a customer
  // rejection archives every signal on the topic as a caller error.
  await assert.rejects(
    () =>
      capture(
        () => ({ status: 401, body: envelope('Unauthorized.', null) }),
        () => resolveSignal(message()),
      ),
    MisconfiguredError,
  )
})

// ── rejected ─────────────────────────────────────────────────────────────────

test('403 is the customer’s key — rejected, with the code payments gave', async () => {
  const { result } = await capture(
    () => ({ status: 403, body: envelope('API key expired.', { errorCode: 'API_KEY_REJECTED' }) }),
    () => resolveSignal(message()),
  )
  assert.deepEqual(result, {
    kind: 'rejected',
    organizationId: '',
    errorCode: 'API_KEY_REJECTED',
    errorMessage: 'API key expired.',
  })
})

test('400 keeps the organisation, so a bad body is still attributable', async () => {
  // Resolve resolves the key BEFORE judging the body precisely so this is
  // possible. Without it a validation failure would archive with no org and be
  // invisible in the Signals UI.
  const { result } = await capture(
    () => ({
      status: 400,
      body: envelope('inputTokens: must be a whole number.', {
        errorCode: 'VALIDATION_FAILED',
        organizationId: 'org_1',
      }),
    }),
    () => resolveSignal(message()),
  )
  assert.deepEqual(result, {
    kind: 'rejected',
    organizationId: 'org_1',
    errorCode: 'VALIDATION_FAILED',
    errorMessage: 'inputTokens: must be a whole number.',
  })
})

test('404 is an unknown customer, and still names the org', async () => {
  const { result } = await capture(
    () => ({
      status: 404,
      body: envelope('Customer "cus_nope" not found.', {
        errorCode: 'CUSTOMER_NOT_FOUND',
        organizationId: 'org_1',
      }),
    }),
    () => resolveSignal(message()),
  )
  assert.equal((result as { errorCode: string }).errorCode, 'CUSTOMER_NOT_FOUND')
  assert.equal((result as { organizationId: string }).organizationId, 'org_1')
})

test('a 4xx with an unreadable body still rejects, with a usable message', async () => {
  const { result } = await capture(
    () => ({ status: 400, body: '<html>gateway</html>' }),
    () => resolveSignal(message()),
  )
  assert.equal((result as { kind: string }).kind, 'rejected')
  assert.equal((result as { errorCode: string }).errorCode, 'REJECTED')
  assert.match((result as { errorMessage: string }).errorMessage, /400/)
})

// ── transient ────────────────────────────────────────────────────────────────

test('5xx is the payments app failing, not the caller', async () => {
  // The route answers 500 on a database hiccup deliberately, rather than a 401,
  // so a blip never rejects a signal from a perfectly good key.
  for (const status of [500, 502, 503, 504]) {
    const { result } = await capture(
      () => ({ status, body: envelope('Could not resolve the API key.', null) }),
      () => resolveSignal(message()),
    )
    assert.equal((result as { kind: string }).kind, 'transient', `status ${status}`)
  }
})

test('a network failure or timeout is transient, never a rejection', async () => {
  const { result } = await capture(
    () => new Error('fetch failed: ECONNREFUSED'),
    () => resolveSignal(message()),
  )
  assert.equal((result as { kind: string }).kind, 'transient')
  assert.match((result as { detail: string }).detail, /ECONNREFUSED/)
})
