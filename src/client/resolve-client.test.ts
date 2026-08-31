/** The consumer's resolve call, now ONE call per batch. What these pin is the
 *  mapping from a response to verdicts, because conflating any two of the three
 *  outcomes breaks the pipeline in a different way:
 *
 *    treating an outage as a rejection  → good signals archived as caller errors
 *    treating a rejection as an outage  → the topic stalls behind one bad body
 *    treating OUR 401 as the customer's → every signal on the topic refused,
 *                                         silently, until a human notices
 *
 *  And two properties that only exist because it is a batch:
 *
 *    `routeAnswered` separates "this item failed" from "the call failed", which
 *      is what the poison rule turns on
 *    results are matched POSITIONALLY AND BY `signalId`, because a shifted array
 *      would archive verdicts against the wrong rows — money, wrong tenant */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { MisconfiguredError } from './payments-client.js'
import { BadBatchError, resolveBatch } from './resolve-client.js'
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

function messages(n: number): SignalMessage[] {
  return Array.from({ length: n }, (_, i) => message({ signalId: `sig-${i}` }))
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

/** A 200 whose `result.signals` echoes the ids it was given. */
function answer(outcomes: (Record<string, unknown> | null)[], ids?: string[]) {
  return {
    status: 200,
    body: JSON.stringify({
      statusDetail: { message: 'Batch resolved.' },
      result: {
        signals: outcomes.map((o, i) =>
          o === null ? null : { signalId: ids ? ids[i] : `sig-${i}`, ...o },
        ),
      },
    }),
  }
}

function fail(status: number, message: string) {
  return { status, body: JSON.stringify({ statusDetail: { message }, result: null }) }
}

// ── the request ──────────────────────────────────────────────────────────────

test('one call carries the WHOLE batch, with a digest per item', async () => {
  const batch = [
    message({ signalId: 's0', apiKeyHash: 'a'.repeat(64) }),
    message({ signalId: 's1', apiKeyHash: 'b'.repeat(64) }),
  ]
  const { sent } = await capture(
    () => answer([{ outcome: 'ok', organizationId: 'org_1' }, { outcome: 'ok', organizationId: 'org_2' }], ['s0', 's1']),
    () => resolveBatch(batch),
  )

  const body = JSON.parse(sent?.body ?? '{}')
  assert.equal(body.signals.length, 2)
  // PER ITEM, not a header. A Kafka batch spans customers, so one
  // `X-Api-Key-Hash` header could never describe it — that limitation is the
  // whole reason the route grew a batch shape.
  assert.equal(body.signals[0].apiKeyHash, 'a'.repeat(64))
  assert.equal(body.signals[1].apiKeyHash, 'b'.repeat(64))
  assert.equal(sent?.headers['x-api-key-hash'], undefined)
  // The raw `cnk_…` key never leaves the edge process.
  assert.ok(!sent?.body.includes('cnk_'))
})

test('each body travels verbatim, and signalId identifies the result', async () => {
  const body = { customerId: 'cus_x', inputTokens: 7, outputTokens: 8, custom: { a: [1, 2] } }
  const { sent } = await capture(
    () => answer([{ outcome: 'ok', organizationId: 'org_1' }], ['s0']),
    () => resolveBatch([message({ signalId: 's0', body })]),
  )
  const parsed = JSON.parse(sent?.body ?? '{}')
  assert.deepEqual(parsed.signals[0].body, body)
  assert.equal(parsed.signals[0].signalId, 's0')
})

test('an empty batch makes no call at all', async () => {
  let called = false
  const original = globalThis.fetch
  globalThis.fetch = (async () => {
    called = true
    return new Response('{}', { status: 200 })
  }) as typeof globalThis.fetch
  try {
    assert.deepEqual(await resolveBatch([]), [])
    assert.equal(called, false)
  } finally {
    globalThis.fetch = original
  }
})

// ── per-item outcomes ────────────────────────────────────────────────────────

test('a mixed 200 maps each item to its own verdict, in order', async () => {
  const { result } = await capture(
    () =>
      answer([
        { outcome: 'ok', organizationId: 'org_1' },
        {
          outcome: 'rejected',
          organizationId: 'org_1',
          errorCode: 'CUSTOMER_NOT_FOUND',
          errorMessage: 'No customer with id "cus_x".',
        },
        { outcome: 'transient', errorMessage: 'Could not resolve the customer.' },
      ]),
    () => resolveBatch(messages(3)),
  )

  assert.deepEqual(result, [
    { kind: 'ok', organizationId: 'org_1' },
    {
      kind: 'rejected',
      organizationId: 'org_1',
      errorCode: 'CUSTOMER_NOT_FOUND',
      errorMessage: 'No customer with id "cus_x".',
    },
    // Inside a 200, so the ROUTE ANSWERED — this item can become poison.
    { kind: 'transient', detail: 'Could not resolve the customer.', routeAnswered: true },
  ])
})

test('an ok naming NO organisation is transient, not accepted', async () => {
  // A contract violation, not a rejection. Refusing the signal would lose it over
  // our own bug; retrying is right.
  const { result } = await capture(
    () => answer([{ outcome: 'ok', organizationId: '' }]),
    () => resolveBatch(messages(1)),
  )
  assert.deepEqual(result, [
    {
      kind: 'transient',
      detail: 'resolve returned ok with no organizationId',
      routeAnswered: true,
    },
  ])
})

test('an unrecognised outcome is transient, never a rejection', async () => {
  const { result } = await capture(
    () => answer([{ outcome: 'weird' }]),
    () => resolveBatch(messages(1)),
  )
  const [verdict] = result as { kind: string; routeAnswered: boolean }[]
  assert.equal(verdict?.kind, 'transient')
  assert.equal(verdict?.routeAnswered, true)
})

// ── whole-call failures: routeAnswered must be FALSE ─────────────────────────

test('401 is OUR shared secret and throws — it is never a customer problem', async () => {
  // Nothing per-signal can produce this status any more, which is the structural
  // half of the fix: a customer's expired key CANNOT wear a status that also means
  // something about us.
  await assert.rejects(
    () => capture(() => fail(401, 'Unauthorized.'), () => resolveBatch(messages(3))),
    (error: unknown) => error instanceof MisconfiguredError && /shared secret/.test(String(error)),
  )
})

test('400 is OUR envelope and throws — not "every signal is invalid"', async () => {
  // The dangerous misreading. A refused envelope must never be applied to the
  // signals inside it, or one bad request archives a whole batch as caller errors.
  await assert.rejects(
    () =>
      capture(
        () => fail(400, '`signals` must hold at most 5000 entries, got 6000.'),
        () => resolveBatch(messages(3)),
      ),
    (error: unknown) => error instanceof BadBatchError && /envelope/.test(String(error)),
  )
})

test('413 throws BadBatchError — resending the same batch cannot help', async () => {
  await assert.rejects(
    () => capture(() => fail(413, 'Payload Too Large'), () => resolveBatch(messages(3))),
    (error: unknown) => error instanceof BadBatchError && /RESOLVE_BATCH_MAX/.test(String(error)),
  )
})

test('5xx makes EVERY item transient, with routeAnswered false', async () => {
  const { result } = await capture(
    () => fail(503, 'Service Unavailable'),
    () => resolveBatch(messages(3)),
  )
  assert.equal((result as unknown[]).length, 3)
  for (const verdict of result as { kind: string; routeAnswered: boolean }[]) {
    assert.equal(verdict.kind, 'transient')
    // The CALL failed, so nothing in this batch can be called poison.
    assert.equal(verdict.routeAnswered, false)
  }
})

test('a network failure or timeout is transient for the whole batch', async () => {
  const { result } = await capture(
    () => new Error('fetch failed'),
    () => resolveBatch(messages(4)),
  )
  assert.equal((result as unknown[]).length, 4)
  assert.ok(
    (result as { kind: string; routeAnswered: boolean; detail: string }[]).every(
      (v) => v.kind === 'transient' && v.routeAnswered === false && /fetch failed/.test(v.detail),
    ),
  )
})

// ── the mis-attribution guards ───────────────────────────────────────────────

test('a SHORT result array is a whole-batch transient, not a partial answer', async () => {
  const { result } = await capture(
    () => answer([{ outcome: 'ok', organizationId: 'org_1' }]),
    () => resolveBatch(messages(3)),
  )
  assert.equal((result as unknown[]).length, 3)
  const [verdict] = result as { kind: string; detail: string; routeAnswered: boolean }[]
  assert.equal(verdict?.kind, 'transient')
  assert.equal(verdict?.routeAnswered, false)
  assert.match(verdict?.detail ?? '', /1 results for 3 signals/)
})

test('results OUT OF ORDER are refused outright — a shift would bill the wrong tenant', async () => {
  // The response is positional AND carries `signalId`. If they disagree, the safe
  // move is to redeliver the batch, not to guess: writing one verdict against the
  // wrong row attributes usage to the wrong organisation.
  const { result } = await capture(
    () =>
      answer(
        [
          { outcome: 'ok', organizationId: 'org_1' },
          { outcome: 'ok', organizationId: 'org_2' },
        ],
        ['sig-1', 'sig-0'],
      ),
    () => resolveBatch(messages(2)),
  )
  const verdicts = result as { kind: string; detail: string; routeAnswered: boolean }[]
  assert.equal(verdicts.length, 2)
  assert.ok(verdicts.every((v) => v.kind === 'transient' && v.routeAnswered === false))
  assert.match(verdicts[0]?.detail ?? '', /out of order/)
})

test('a non-object result entry is refused rather than coerced', async () => {
  const { result } = await capture(
    () => answer([null, { outcome: 'ok', organizationId: 'org_1' }]),
    () => resolveBatch(messages(2)),
  )
  const verdicts = result as { kind: string; detail: string }[]
  assert.ok(verdicts.every((v) => v.kind === 'transient'))
  assert.match(verdicts[0]?.detail ?? '', /non-object result at index 0/)
})

test('a 200 with an unreadable body is transient, never a rejection', async () => {
  const { result } = await capture(
    () => ({ status: 200, body: 'not json at all' }),
    () => resolveBatch(messages(2)),
  )
  const verdicts = result as { kind: string; routeAnswered: boolean }[]
  assert.ok(verdicts.every((v) => v.kind === 'transient' && v.routeAnswered === false))
})
