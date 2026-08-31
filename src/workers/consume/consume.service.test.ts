/** The consumer's logic, over fakes. No broker, no ClickHouse, no payments.
 *
 *  What these pin, in order of how expensive getting them wrong is:
 *    · the row is written BEFORE anything is committable, and `stoppedAt` is the
 *      only thing that says what is committable
 *    · a rejected signal is archived WITH its reason, not dropped
 *    · a poison message is quarantined ONLY when the route actually answered
 *      (`routeAnswered`), never during an outage
 *    · a poll bigger than `batchMax` is chunked, and the prefix stays contiguous
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

/** A transient the ROUTE ANSWERED — a per-item transient inside a 200. This is
 *  the only kind that can ever make a signal poison. */
const ANSWERED = (detail = 'resolve answered 500'): ResolveVerdict => ({
  kind: 'transient',
  detail,
  routeAnswered: true,
})

/** A transient because the CALL failed — timeout, 5xx, unusable response.
 *  Nothing in such a batch is poison, because nothing was answered. */
const CALL_FAILED = (detail = 'ECONNREFUSED'): ResolveVerdict => ({
  kind: 'transient',
  detail,
  routeAnswered: false,
})

function harness(
  opts: {
    verdict?: (m: SignalMessage, index: number) => ResolveVerdict | Promise<ResolveVerdict>
    poisonAfter?: number
    batchMax?: number
    health?: ResolveHealth
  } = {},
) {
  const written: SignalLogInsert[][] = []
  const logs: { event: string; detail: Record<string, unknown> }[] = []
  // The SIZE of each resolve call, in order. One entry means one HTTP call for
  // the whole batch, which is the entire point of the batch route.
  const chunks: number[] = []
  let seen = 0

  const deps: ConsumeDeps = {
    resolve: {
      async resolveBatch(ms) {
        chunks.push(ms.length)
        const out: ResolveVerdict[] = []
        for (const m of ms) {
          const index = seen
          seen += 1
          out.push(opts.verdict ? await opts.verdict(m, index) : OK)
        }
        return out
      },
    },
    archive: {
      async write(rows) {
        written.push([...rows])
      },
    },
    health: opts.health ?? createResolveHealth(),
    config: {
      batchMax: opts.batchMax ?? 1_000,
      poisonAfter: opts.poisonAfter ?? 3,
    },
    log: (event, detail) => logs.push({ event, detail }),
  }
  return { deps, written, logs, stats: () => ({ calls: chunks.length, chunks }) }
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

test('a whole batch is ONE resolve call, not one per signal', async () => {
  const { deps, stats } = harness()
  await processBatch(deps, messages(500))
  // The entire reason the batch route exists. This used to be 500 calls.
  assert.deepEqual(stats().chunks, [500])
})

test('a poll larger than batchMax is chunked, and the prefix stays contiguous', async () => {
  const { deps, written, stats } = harness({ batchMax: 40 })
  const outcome = await processBatch(deps, messages(100))
  assert.deepEqual(stats().chunks, [40, 40, 20])
  assert.equal(outcome.processing, 100)
  assert.equal(outcome.stoppedAt, -1)
  // Still ONE insert — chunking is about the request size, not the write.
  assert.equal(written.length, 1)
  assert.deepEqual(written[0]?.map((r) => r.signal_id).slice(0, 3), ['sig-0', 'sig-1', 'sig-2'])
})

test('a chunk whose CALL failed stops the remaining chunks being asked', async () => {
  // The prefix cannot reach past a chunk that was never answered, so every later
  // chunk would be a request paid for nothing.
  const { deps, stats } = harness({
    batchMax: 10,
    verdict: (_m, i) => (i < 10 ? OK : CALL_FAILED()),
  })
  const outcome = await processBatch(deps, messages(50))
  assert.deepEqual(stats().chunks, [10, 10], 'must not ask for chunks 3, 4 and 5')
  assert.equal(outcome.stoppedAt, 10)
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
    verdict: (m) => (m.signalId === 'sig-2' ? ANSWERED('resolve could not answer') : OK),
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
  const { deps, written } = harness({ verdict: () => CALL_FAILED('timeout') })
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
    m.signalId === poison ? ANSWERED() : OK

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
  const verdict = (): ResolveVerdict => CALL_FAILED()

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
  const verdict = (): ResolveVerdict => (fail ? ANSWERED('blip') : OK)

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

test('a STALE SUCCESS cannot license a quarantine (the bug this once shipped)', async () => {
  // Seen for real against a live broker, back when resolve was one call per
  // signal. The consumer resolved four signals, the route went down, and the next
  // signal failed five times in five seconds. `msSinceSuccess()` was ~70s — inside
  // a 120s outage window — so the elapsed-time gate called the route "answering"
  // and archived a perfectly good signal as a caller error.
  //
  // The fix then was to compare a monotonic success COUNT. The fix now is that
  // there is nothing left to compare: a batch call either came back or it did not,
  // and `routeAnswered: false` says so outright. This test keeps the original
  // scenario — a real success 70 seconds ago, an outage since — and asserts the
  // invariant survives the redesign.
  let clock = 1_000
  const health = createResolveHealth(() => clock)

  health.recordSuccess('sig-earlier')       // the route WAS alive...
  clock += 70_000                           // ...70 seconds ago. Nothing since.

  for (let attempt = 0; attempt < 8; attempt += 1) {
    clock += 1_000
    const h = harness({ health, verdict: () => CALL_FAILED('fetch failed'), poisonAfter: 5 })
    const outcome = await processBatch(h.deps, [message({ signalId: 'sig-x' })])
    assert.equal(outcome.quarantined, 0, `quarantined on attempt ${attempt}`)
    assert.equal(outcome.stoppedAt, 0)
    assert.equal(h.written[0]?.length, 0)
  }
})

test('the ROUTE ANSWERING is what makes a signal poison — not a success elsewhere', async () => {
  // The rule used to be "this signal fails while OTHER calls succeed", inferred
  // across batches. It is now read off the verdict: a per-item transient inside a
  // 200 is the route saying "I answered, and this one still failed".
  //
  // So a success elsewhere is NOT sufficient any more, and this pins that: the
  // route is recorded as healthy, yet every call fails as a whole, and nothing is
  // ever quarantined however long the streak runs.
  const health = createResolveHealth()
  for (let attempt = 0; attempt < 8; attempt += 1) {
    health.recordSuccess('sig-other')   // other traffic demonstrably getting through
    const h = harness({ health, verdict: () => CALL_FAILED(), poisonAfter: 3 })
    const outcome = await processBatch(h.deps, [message({ signalId: 'sig-poison' })])
    assert.equal(outcome.quarantined, 0, `quarantined on attempt ${attempt}`)
    assert.equal(outcome.stoppedAt, 0)
  }

  // Same signal, same streak length — but now the route answers and rejects only
  // this item. THAT is poison.
  const h = harness({ health, verdict: () => ANSWERED(), poisonAfter: 3 })
  let outcome = await processBatch(h.deps, [message({ signalId: 'sig-poison' })])
  assert.equal(outcome.quarantined, 1)
  assert.equal(outcome.stoppedAt, -1)
  const row = h.written[0]?.[0]
  assert.equal(row?.status, 'PENDING')
  assert.equal(row?.error_code, 'RESOLVE_FAILED')
  // No org — we never got an answer about it, so there is nobody to attribute to.
  assert.equal(row?.organization_id, '')

  // And a quarantine does not stall the batch behind it.
  const after = harness({ health, verdict: () => OK, poisonAfter: 3 })
  outcome = await processBatch(after.deps, messages(3))
  assert.equal(outcome.stoppedAt, -1)
  assert.equal(outcome.processing, 3)
})

test('one answered transient among many OK signals is quarantined, not the batch', async () => {
  // The mixed case the batch route made expressible at all: a 200 whose item #1
  // is transient while #0 and #2 resolved fine.
  const health = createResolveHealth()
  const poison = 'sig-1'
  const verdict = (m: SignalMessage): ResolveVerdict => (m.signalId === poison ? ANSWERED() : OK)

  // Build the streak. Each round stops at the poison message.
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const outcome = await processBatch(harness({ health, verdict, poisonAfter: 3 }).deps, messages(3))
    assert.equal(outcome.stoppedAt, 1, `attempt ${attempt} must stop at the poison message`)
    assert.equal(outcome.processing, 1)
  }

  const h = harness({ health, verdict, poisonAfter: 3 })
  const outcome = await processBatch(h.deps, messages(3))
  assert.equal(outcome.quarantined, 1)
  assert.equal(outcome.processing, 2)
  assert.equal(outcome.stoppedAt, -1, 'the whole batch is now committable')
  assert.deepEqual(h.written[0]?.map((r) => r.status), ['PROCESSING', 'PENDING', 'PROCESSING'])
})
