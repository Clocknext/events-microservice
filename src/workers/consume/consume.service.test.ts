/** The consumer's logic, over fakes. No broker, no ClickHouse, no payments.
 *
 *  What these pin, in order of how expensive getting them wrong is:
 *    · the row is written BEFORE anything is committable, and `stoppedAt` is the
 *      only thing that says what is committable
 *    · a rejected signal is archived WITH its reason, not dropped
 *    · a poison message is quarantined ONLY while the route is answering others
 *    · `payload` stays byte-for-byte what the caller sent */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { processBatch, toRow } from './consume.service.js'
import { createResolveHealth } from './consume.runner.js'
import type {
  ConsumeDeps,
  ResolveHealth,
  ResolveVerdict,
  SignalLogInsert,
} from './consume.schema.js'
import type { SignalMessage } from '../../modules/signal/signal.schema.js'

function message(over: Partial<SignalMessage> = {}): SignalMessage {
  return {
    signalId: '01M0X91S3X2SK86WYYWVENM3N7',
    receivedAt: '2026-08-26T10:00:00.000Z',
    apiKeyHash: 'a'.repeat(64),
    body: { customerId: 'cus_1', inputTokens: 10, outputTokens: 5 },
    ...over,
  }
}

function messages(n: number): SignalMessage[] {
  return Array.from({ length: n }, (_, i) => message({ signalId: `sig-${i}` }))
}

const OK: ResolveVerdict = { kind: 'ok', organizationId: 'org_1' }

function harness(
  opts: {
    verdict?: (m: SignalMessage, index: number) => ResolveVerdict | Promise<ResolveVerdict>
    poisonAfter?: number
    concurrency?: number
    health?: ResolveHealth
  } = {},
) {
  const written: SignalLogInsert[][] = []
  const logs: { event: string; detail: Record<string, unknown> }[] = []
  let calls = 0
  let inFlight = 0
  let maxInFlight = 0

  const deps: ConsumeDeps = {
    resolve: {
      async resolve(m) {
        const index = calls
        calls += 1
        inFlight += 1
        maxInFlight = Math.max(maxInFlight, inFlight)
        try {
          return opts.verdict ? await opts.verdict(m, index) : OK
        } finally {
          inFlight -= 1
        }
      },
    },
    archive: {
      async write(rows) {
        written.push([...rows])
      },
    },
    health: opts.health ?? createResolveHealth(),
    config: {
      concurrency: opts.concurrency ?? 4,
      poisonAfter: opts.poisonAfter ?? 3,
    },
    log: (event, detail) => logs.push({ event, detail }),
  }
  return { deps, written, logs, stats: () => ({ calls, maxInFlight }) }
}

// ── the happy path ───────────────────────────────────────────────────────────

test('every resolved signal is archived PROCESSING, in ONE insert', async () => {
  const { deps, written } = harness()
  const outcome = await processBatch(deps, messages(5))

  assert.equal(outcome.read, 5)
  assert.equal(outcome.processing, 5)
  assert.equal(outcome.pending, 0)
  assert.equal(outcome.stoppedAt, -1)
  // One insert, not five. ClickHouse punishes per-row inserts, and this is the
  // batching the Kafka engine used to do for us.
  assert.equal(written.length, 1)
  assert.equal(written[0]?.length, 5)
  assert.ok(written[0]?.every((r) => r.status === 'PROCESSING' && r.organization_id === 'org_1'))
})

test('an empty batch writes nothing and stops nowhere', async () => {
  const { deps, written } = harness()
  const outcome = await processBatch(deps, [])
  assert.equal(outcome.read, 0)
  assert.equal(outcome.stoppedAt, -1)
  assert.equal(written.length, 0)
})

test('concurrency is bounded — a batch does not open a socket per signal', async () => {
  const { deps, stats } = harness({
    concurrency: 3,
    verdict: async () => {
      await new Promise((r) => setTimeout(r, 1))
      return OK
    },
  })
  await processBatch(deps, messages(20))
  assert.equal(stats().calls, 20)
  assert.ok(stats().maxInFlight <= 3, `saw ${stats().maxInFlight} in flight`)
})

// ── the row ──────────────────────────────────────────────────────────────────

test('customer_id is a LIFTED COPY — the payload keeps its own, verbatim', async () => {
  const body = { customerId: 'cus_9', inputTokens: 1, outputTokens: 2, extra: { a: [1, 2] } }
  const row = toRow(message({ body }), OK)
  assert.equal(row.customer_id, 'cus_9')
  // The archive is a record of the request, not a summary of it: everything the
  // caller sent survives, including the field we lifted.
  assert.deepEqual(JSON.parse(row.payload), body)
})

test('the row carries version 1 and never stamps ingested_at itself', () => {
  const row = toRow(message(), OK)
  assert.equal(row.version, 1)
  // ClickHouse stamps it with DEFAULT now64(3) — one clock, and the same one the
  // dispatcher's window compares against. Sending our own would shift every
  // window by the skew between two machines, silently.
  assert.equal('ingested_at' in row, false)
})

test('a body with no usable customerId still produces a row', () => {
  assert.equal(toRow(message({ body: {} as SignalMessage['body'] }), OK).customer_id, '')
})

// ── rejection ────────────────────────────────────────────────────────────────

test('a rejected signal is archived PENDING, with the reason payments gave', async () => {
  const { deps, written } = harness({
    verdict: () => ({
      kind: 'rejected',
      organizationId: 'org_1',
      errorCode: 'CUSTOMER_NOT_FOUND',
      errorMessage: 'Customer "cus_nope" not found.',
    }),
  })
  const outcome = await processBatch(deps, messages(2))

  assert.equal(outcome.pending, 2)
  assert.equal(outcome.processing, 0)
  assert.equal(outcome.stoppedAt, -1)
  const row = written[0]?.[0]
  assert.equal(row?.status, 'PENDING')
  assert.equal(row?.error_code, 'CUSTOMER_NOT_FOUND')
  assert.equal(row?.error_message, 'Customer "cus_nope" not found.')
  // Attributable: resolve resolves the key BEFORE judging the body, so even a
  // validation failure names the org that sent it.
  assert.equal(row?.organization_id, 'org_1')
})

test('a rejection does NOT stop the batch — it is a healthy answer', async () => {
  const { deps, written } = harness({
    verdict: (_m, i) =>
      i === 1
        ? { kind: 'rejected', organizationId: 'org_1', errorCode: 'VALIDATION_FAILED', errorMessage: 'bad' }
        : OK,
  })
  const outcome = await processBatch(deps, messages(3))
  assert.equal(outcome.stoppedAt, -1)
  assert.equal(written[0]?.length, 3)
})

// ── transient: the prefix rule ───────────────────────────────────────────────

test('a transient failure stops the prefix — earlier rows are still written', async () => {
  const { deps, written } = harness({
    verdict: (m) => (m.signalId === 'sig-2' ? { kind: 'transient', detail: 'ECONNREFUSED' } : OK),
  })
  const outcome = await processBatch(deps, messages(5))

  // Committing a contiguous PREFIX rather than failing the whole batch matters
  // here in a way it would not for a cheap consumer: re-resolving five signals
  // because the third timed out is five HTTP calls already paid for.
  assert.equal(outcome.stoppedAt, 2)
  assert.equal(outcome.processing, 2)
  assert.equal(written[0]?.length, 2)
  assert.deepEqual(written[0]?.map((r) => r.signal_id), ['sig-0', 'sig-1'])
})

test('nothing at all is written when the FIRST signal is unresolvable', async () => {
  const { deps, written } = harness({ verdict: () => ({ kind: 'transient', detail: 'timeout' }) })
  const outcome = await processBatch(deps, messages(3))
  assert.equal(outcome.stoppedAt, 0)
  // The writer is still called — with nothing. A silent early return would make
  // "wrote nothing" and "never tried" indistinguishable in a test.
  assert.equal(written[0]?.length, 0)
})

// ── poison vs outage ─────────────────────────────────────────────────────────

test('a poison message is quarantined once the route is answering others', async () => {
  // The health tracker is shared across batches, the way the runner shares it.
  const health = createResolveHealth()
  const poison = 'sig-poison'
  const verdict = (m: SignalMessage): ResolveVerdict =>
    m.signalId === poison ? { kind: 'transient', detail: 'resolve answered 500' } : OK

  let last = await processBatch(harness({ health, verdict, poisonAfter: 3 }).deps, [
    message({ signalId: poison }),
    message({ signalId: 'sig-ok' }),
  ])
  assert.equal(last.stoppedAt, 0, 'first failure must not quarantine')

  last = await processBatch(harness({ health, verdict, poisonAfter: 3 }).deps, [
    message({ signalId: poison }),
  ])
  assert.equal(last.stoppedAt, 0, 'second failure must not quarantine')

  const third = harness({ health, verdict, poisonAfter: 3 })
  last = await processBatch(third.deps, [message({ signalId: poison }), message({ signalId: 'ok-2' })])

  // Third strike, and other calls are demonstrably succeeding: archive it and
  // step over it, or the topic behind it never moves again.
  assert.equal(last.quarantined, 1)
  assert.equal(last.stoppedAt, -1)
  const row = third.written[0]?.[0]
  assert.equal(row?.status, 'PENDING')
  assert.equal(row?.error_code, 'RESOLVE_FAILED')
  // No org — we never got an answer, so there is nobody to attribute it to.
  assert.equal(row?.organization_id, '')
  assert.ok(third.logs.some((l) => l.event === 'signal.quarantined'))
})

test('an OUTAGE never quarantines, however many times a signal fails', async () => {
  // The degenerate case this rule exists for: one message per batch, payments
  // down. Without the liveness gate the signal would fail `poisonAfter` times in
  // as many seconds and be archived as a caller error while the outage was still
  // seconds old. Nothing is succeeding, so there is nothing to call it poison
  // against — the runner stalls loudly instead.
  const health = createResolveHealth()
  const verdict = (): ResolveVerdict => ({ kind: 'transient', detail: 'ECONNREFUSED' })

  for (let attempt = 0; attempt < 10; attempt += 1) {
    const h = harness({ health, verdict, poisonAfter: 3 })
    const outcome = await processBatch(h.deps, [message({ signalId: 'sig-lonely' })])
    assert.equal(outcome.quarantined, 0, `quarantined on attempt ${attempt}`)
    assert.equal(outcome.stoppedAt, 0)
    assert.equal(h.written[0]?.length, 0)
  }
})

test('a signal that recovers loses its failure streak', async () => {
  const health = createResolveHealth()
  let fail = true
  const verdict = (): ResolveVerdict =>
    fail ? { kind: 'transient', detail: 'blip' } : OK

  await processBatch(harness({ health, verdict, poisonAfter: 2 }).deps, [message({ signalId: 's' })])
  fail = false
  await processBatch(harness({ health, verdict, poisonAfter: 2 }).deps, [message({ signalId: 's' })])
  fail = true

  // Streak cleared by its own success, so this is failure #1 again, not #2.
  const outcome = await processBatch(
    harness({ health, verdict, poisonAfter: 2 }).deps,
    [message({ signalId: 's' })],
  )
  assert.equal(outcome.quarantined, 0)
  assert.equal(outcome.stoppedAt, 0)
})

// ── propagation ──────────────────────────────────────────────────────────────

test('a throwing resolver PROPAGATES — it must not look like a clean batch', async () => {
  const { deps, written } = harness({
    verdict: () => {
      throw new Error('payments refused our shared secret')
    },
  })
  await assert.rejects(() => processBatch(deps, messages(2)), /shared secret/)
  // And nothing was archived, so the runner cannot commit anything either.
  assert.equal(written.length, 0)
})

test('a failing archive write PROPAGATES — offsets must not be committed', async () => {
  const { deps } = harness()
  deps.archive = {
    async write() {
      throw new Error('clickhouse insert 503')
    },
  }
  await assert.rejects(() => processBatch(deps, messages(2)), /clickhouse insert 503/)
})

test('a stale success does NOT license a quarantine (the observed bug)', async () => {
  // Seen for real against a live broker: the consumer resolved four signals, the
  // resolve route then went down, and the next signal failed five times in five
  // seconds. `msSinceSuccess()` was ~70s — inside a 120s outage window — so the
  // old elapsed-time gate called the route "answering" and archived a perfectly
  // good signal as a caller error. Five failures accumulate far faster than any
  // outage threshold expires, so elapsed time can never be the test.
  let clock = 1_000
  const health = createResolveHealth(() => clock)

  health.recordSuccess('sig-earlier')       // the route WAS alive...
  clock += 70_000                           // ...70 seconds ago. Nothing since.

  const verdict = (): ResolveVerdict => ({ kind: 'transient', detail: 'fetch failed' })
  for (let attempt = 0; attempt < 8; attempt += 1) {
    clock += 1_000
    const h = harness({ health, verdict, poisonAfter: 5 })
    const outcome = await processBatch(h.deps, [message({ signalId: 'sig-x' })])
    assert.equal(outcome.quarantined, 0, `quarantined on attempt ${attempt}`)
    assert.equal(outcome.stoppedAt, 0)
    assert.equal(h.written[0]?.length, 0)
  }
})

test('a success DURING the streak is what makes a signal poison', async () => {
  // The same failing signal, but now other traffic is demonstrably getting
  // through. That — and only that — is "fails while others succeed".
  const health = createResolveHealth()
  const poison = 'sig-poison'
  const verdict = (m: SignalMessage): ResolveVerdict =>
    m.signalId === poison ? { kind: 'transient', detail: 'resolve answered 500' } : OK

  // Streak begins. No success has landed since it started, so no quarantine yet.
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const outcome = await processBatch(
      harness({ health, verdict, poisonAfter: 3 }).deps,
      [message({ signalId: poison })],
    )
    assert.equal(outcome.quarantined, 0, 'no other call has succeeded yet')
  }

  // Other traffic gets through, mid-streak.
  health.recordSuccess('sig-other')

  const h = harness({ health, verdict, poisonAfter: 3 })
  const outcome = await processBatch(h.deps, [message({ signalId: poison })])
  assert.equal(outcome.quarantined, 1)
  assert.equal(outcome.stoppedAt, -1)
  const row = h.written[0]?.[0]
  assert.equal(row?.status, 'PENDING')
  assert.equal(row?.error_code, 'RESOLVE_FAILED')
  assert.equal(row?.organization_id, '')
})

test('a signal that recovers loses its streak, and its streak baseline', async () => {
  const health = createResolveHealth()
  let fail = true
  const verdict = (): ResolveVerdict => (fail ? { kind: 'transient', detail: 'blip' } : OK)

  await processBatch(harness({ health, verdict, poisonAfter: 2 }).deps, [message({ signalId: 's' })])
  fail = false
  await processBatch(harness({ health, verdict, poisonAfter: 2 }).deps, [message({ signalId: 's' })])
  fail = true

  // Failure #1 again, not #2 — and a fresh baseline, so that success cannot
  // license a quarantine on the NEXT streak either.
  const outcome = await processBatch(
    harness({ health, verdict, poisonAfter: 2 }).deps,
    [message({ signalId: 's' })],
  )
  assert.equal(outcome.quarantined, 0)
  assert.equal(outcome.stoppedAt, 0)
})
