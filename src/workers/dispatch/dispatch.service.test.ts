import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  overlapFrom,
  sweepOnce,
  toIso,
  toSettleSignal,
  type SweepDeps,
} from './dispatch.service.js'
import type {
  CursorResponse,
  SettleResult,
  SettleSignal,
  SignalLogRow,
} from './dispatch.schema.js'

function row(over: Partial<SignalLogRow> = {}): SignalLogRow {
  return {
    signal_id: '01M0X91S3X2SK86WYYWVENM3N7',
    received_at: '2026-08-26 10:00:00.000',
    api_key_hash: 'a'.repeat(64),
    payload: '{"customerId":"cus_1","inputTokens":10,"outputTokens":5}',
    ...over,
  }
}

/** A fake archive + payments pair that records everything it was asked. */
function harness(opts: {
  cursor?: Partial<CursorResponse>
  newer?: SignalLogRow[]
  byIds?: SignalLogRow[]
  /** Ids the payments app already has a status row for. */
  known?: string[]
  onSettle?: (batchId: string, signals: SettleSignal[]) => Promise<SettleResult[]>
} = {}) {
  const calls = {
    cursorLimits: [] as number[],
    newer: [] as { since: string | null; limit: number }[],
    byIds: [] as string[][],
    known: [] as string[][],
    batches: [] as { batchId: string; signals: SettleSignal[] }[],
    inFlight: 0,
    maxInFlight: 0,
  }
  let n = 0
  const deps: SweepDeps = {
    archive: {
      async readNewer({ sinceIso, after, limit }) {
        calls.newer.push({ since: sinceIso, limit })
        const all = opts.newer ?? []
        const rows = after
          ? all.filter((r) =>
              r.received_at > after.receivedAt ||
              (r.received_at === after.receivedAt && r.signal_id > after.signalId))
          : all
        return rows.slice(0, limit)
      },
      async readByIds(ids) {
        calls.byIds.push(ids)
        return opts.byIds ?? []
      },
    },
    payments: {
      async cursor(retryLimit) {
        calls.cursorLimits.push(retryLimit)
        return { sentThrough: null, retry: [], maxAttempts: 5, ...opts.cursor }
      },
      async known(signalIds) {
        calls.known.push(signalIds)
        const already = new Set(opts.known ?? [])
        return signalIds.filter((id) => already.has(id))
      },
      async settle(batchId, signals) {
        calls.batches.push({ batchId, signals })
        calls.inFlight += 1
        calls.maxInFlight = Math.max(calls.maxInFlight, calls.inFlight)
        try {
          if (opts.onSettle) return await opts.onSettle(batchId, signals)
          return signals.map((s) => ({
            signal_id: s.signalId, status: 'PROCESSED' as const,
            error_type: null, error_code: null, error_message: null,
          }))
        } finally {
          calls.inFlight -= 1
        }
      },
    },
    config: { batchSize: 2, concurrency: 2, overlapMs: 60_000 },
    newBatchId: () => `batch-${++n}`,
  }
  return { deps, calls }
}

// ── timestamp handling: the one that silently corrupts billing windows ────────

test("ClickHouse's space-separated timestamp becomes real UTC ISO", () => {
  // Left as-is, `new Date('2026-08-26 10:00:00.000')` parses as LOCAL time, so a
  // dispatcher in IST would bill every signal 5.5h early.
  assert.equal(toIso('2026-08-26 10:00:00.000'), '2026-08-26T10:00:00.000Z')
  assert.equal(
    new Date(toIso('2026-08-26 10:00:00.000')).toISOString(),
    '2026-08-26T10:00:00.000Z',
  )
})

test('an already-ISO timestamp is left alone', () => {
  assert.equal(toIso('2026-08-26T10:00:00.000Z'), '2026-08-26T10:00:00.000Z')
  assert.equal(toIso('2026-08-26T10:00:00.000+05:30'), '2026-08-26T10:00:00.000+05:30')
})

test('the overlap window shifts the watermark backwards, and survives junk', () => {
  assert.equal(overlapFrom('2026-08-26T10:00:00.000Z', 60_000), '2026-08-26T09:59:00.000Z')
  assert.equal(overlapFrom(null, 60_000), null)
  assert.equal(overlapFrom('not a date', 60_000), null)
})

// ── payload -> settle signal ─────────────────────────────────────────────────

test('the envelope overrides anything the caller put in the body', () => {
  // A caller sending their own "signalId"/"attempt" must not be able to
  // impersonate the edge's stamp and land on another signal's money row.
  const signal = toSettleSignal(
    row({ payload: '{"customerId":"cus_1","signalId":"NOT_MINE","attempt":99,"receivedAt":"1999-01-01T00:00:00Z"}' }),
    3,
  )
  assert.equal(signal?.signalId, '01M0X91S3X2SK86WYYWVENM3N7')
  assert.equal(signal?.attempt, 3)
  assert.equal(signal?.receivedAt, '2026-08-26T10:00:00.000Z')
  assert.equal(signal?.customerId, 'cus_1')
})

test('an unparseable or non-object payload is skipped, not sent half-built', () => {
  assert.equal(toSettleSignal(row({ payload: 'not json' }), 1), null)
  assert.equal(toSettleSignal(row({ payload: '[1,2,3]' }), 1), null)
  assert.equal(toSettleSignal(row({ payload: 'null' }), 1), null)
  assert.equal(toSettleSignal(row({ payload: '"a string"' }), 1), null)
})

// ── the sweep ────────────────────────────────────────────────────────────────

test('an empty archive settles nothing and reports nothing', async () => {
  const { deps, calls } = harness()
  const out = await sweepOnce(deps)
  assert.deepEqual(out, {
    read: 0, sent: 0, processed: 0, userError: 0, serverError: 0,
    skipped: 0, alreadyKnown: 0,
    // One page WAS read — it just came back empty.
    pages: 1, saturated: false, batchIds: [],
  })
  assert.equal(calls.batches.length, 0, 'settle must not be called with an empty batch')
})

test('retries are fetched by id and carry their next attempt number', async () => {
  const { deps, calls } = harness({
    cursor: { retry: [{ signalId: 'sig_old', receivedAt: '2026-08-01T00:00:00.000Z', nextAttempt: 4 }] },
    byIds: [row({ signal_id: 'sig_old' })],
  })
  await sweepOnce(deps)
  assert.deepEqual(calls.byIds, [['sig_old']])
  assert.equal(calls.batches[0]!.signals[0]!.signalId, 'sig_old')
  assert.equal(calls.batches[0]!.signals[0]!.attempt, 4, 'the dispatcher advances the counter')
})

test('a retry is never double-sent as new work', async () => {
  // The overlap window means the retry row is very likely ALSO in the "newer"
  // read. Sending it twice in one batch would make settle price it twice.
  const dup = row({ signal_id: 'sig_dup' })
  const { deps, calls } = harness({
    cursor: {
      sentThrough: '2026-08-26T10:00:00.000Z',
      retry: [{ signalId: 'sig_dup', receivedAt: '2026-08-26T10:00:00.000Z', nextAttempt: 2 }],
    },
    byIds: [dup],
    newer: [dup, row({ signal_id: 'sig_new' })],
  })
  const out = await sweepOnce(deps)
  const sent = calls.batches.flatMap((b) => b.signals.map((s) => s.signalId))
  assert.deepEqual(sent.sort(), ['sig_dup', 'sig_new'])
  assert.equal(out.sent, 2)
  assert.equal(calls.batches.flatMap((b) => b.signals).find((s) => s.signalId === 'sig_dup')!.attempt, 2)
})

test('rows are split into batches of batchSize, at most concurrency in flight', async () => {
  const rows = Array.from({ length: 4 }, (_, i) => row({ signal_id: `sig_${i}` }))
  let releases: (() => void)[] = []
  const { deps, calls } = harness({
    newer: rows,
    async onSettle(_id, signals) {
      // Hold every batch open until they are all started, so maxInFlight is real.
      await new Promise<void>((resolve) => {
        releases.push(resolve)
        if (releases.length === 2) releases.forEach((r) => r())
      })
      return signals.map((s) => ({
        signal_id: s.signalId, status: 'PROCESSED' as const,
        error_type: null, error_code: null, error_message: null,
      }))
    },
  })
  const out = await sweepOnce(deps)
  assert.equal(calls.batches.length, 2, 'batchSize 2 over 4 rows')
  assert.deepEqual(calls.batches.map((b) => b.signals.length), [2, 2])
  assert.equal(calls.maxInFlight, 2, 'both batches ran together')
  assert.equal(out.processed, 4)
  assert.equal(out.saturated, true, 'a full sweep asks the runner to go again at once')
})

test('outcomes are counted by kind', async () => {
  const rows = Array.from({ length: 3 }, (_, i) => row({ signal_id: `sig_${i}` }))
  const { deps } = harness({
    newer: rows,
    async onSettle(_id, signals) {
      return [
        { signal_id: signals[0]!.signalId, status: 'PROCESSED', error_type: null, error_code: null, error_message: null },
        ...(signals[1]
          ? [{ signal_id: signals[1].signalId, status: 'PENDING' as const, error_type: 'USER_ERROR' as const, error_code: 'VALIDATION_FAILED', error_message: 'nope' }]
          : []),
      ]
    },
  })
  const out = await sweepOnce(deps)
  assert.equal(out.processed, 2, 'one per batch')
  assert.equal(out.userError, 1)
})

test('a batch that throws does not sink the sweep or the other batches', async () => {
  const rows = Array.from({ length: 4 }, (_, i) => row({ signal_id: `sig_${i}` }))
  const { deps, calls } = harness({
    newer: rows,
    async onSettle(batchId, signals) {
      if (batchId === 'batch-1') throw new Error('settle exploded')
      return signals.map((s) => ({
        signal_id: s.signalId, status: 'PROCESSED' as const,
        error_type: null, error_code: null, error_message: null,
      }))
    },
  })
  const out = await sweepOnce(deps)
  assert.equal(calls.batches.length, 2)
  assert.equal(out.processed, 2, 'the surviving batch still settled')
  // Nothing is recorded for the failed batch, so the next sweep re-sends it —
  // safe, because settle dedups on signalId.
  assert.equal(out.sent, 4)
})

test('an unusable row is skipped and counted, and the rest still go', async () => {
  const { deps, calls } = harness({
    newer: [row({ signal_id: 'bad', payload: '{{{' }), row({ signal_id: 'good' })],
  })
  const out = await sweepOnce(deps)
  assert.equal(out.skipped, 1)
  assert.equal(out.sent, 1)
  assert.deepEqual(calls.batches.flatMap((b) => b.signals.map((s) => s.signalId)), ['good'])
})

test('the newer read is bounded by the room left after retries', async () => {
  // batchSize 2 x concurrency 2 = 4 slots; one retry leaves 3.
  const { deps, calls } = harness({
    cursor: { retry: [{ signalId: 'sig_old', receivedAt: '2026-08-01T00:00:00.000Z', nextAttempt: 2 }] },
    byIds: [row({ signal_id: 'sig_old' })],
  })
  await sweepOnce(deps)
  assert.equal(calls.newer[0]!.limit, 3)
})

test('with nothing ever settled the sweep reads from the beginning', async () => {
  const { deps, calls } = harness({ cursor: { sentThrough: null } })
  await sweepOnce(deps)
  assert.equal(calls.newer[0]!.since, null, 'no lower bound at all')
})

// ── convergence: the overlap window must not re-settle forever ───────────────

test('a candidate that already has a status row is not sent again', async () => {
  // The overlap window deliberately re-reads below the watermark to catch late
  // arrivals. Without this filter every signal in the window would be re-priced
  // on every sweep and the pipeline would never go quiet.
  const { deps, calls } = harness({
    cursor: { sentThrough: '2026-08-26T10:00:00.000Z' },
    newer: [row({ signal_id: 'sig_done' }), row({ signal_id: 'sig_fresh' })],
    known: ['sig_done'],
  })
  const out = await sweepOnce(deps)
  assert.deepEqual(calls.known, [['sig_done', 'sig_fresh']], 'both candidates are checked')
  assert.deepEqual(calls.batches.flatMap((b) => b.signals.map((s) => s.signalId)), ['sig_fresh'])
  assert.equal(out.alreadyKnown, 1)
  assert.equal(out.sent, 1)
})

test('a sweep where everything is already known sends nothing at all', async () => {
  const { deps, calls } = harness({
    cursor: { sentThrough: '2026-08-26T10:00:00.000Z' },
    newer: [row({ signal_id: 'a' }), row({ signal_id: 'b' })],
    known: ['a', 'b'],
  })
  const out = await sweepOnce(deps)
  assert.equal(out.sent, 0, 'the pipeline goes quiet')
  assert.equal(out.alreadyKnown, 2)
  assert.equal(calls.batches.length, 0, 'settle is not called at all')
})

test('a retry is exempt from the known-filter — the cursor asked for it by name', async () => {
  // A retry ALWAYS has a status row (that is where its error is recorded), so
  // filtering it as "known" would make retries impossible.
  const { deps, calls } = harness({
    cursor: {
      sentThrough: '2026-08-26T10:00:00.000Z',
      retry: [{ signalId: 'sig_retry', receivedAt: '2026-08-26T10:00:00.000Z', nextAttempt: 3 }],
    },
    byIds: [row({ signal_id: 'sig_retry' })],
    newer: [row({ signal_id: 'sig_retry' })],
    known: ['sig_retry'],
  })
  const out = await sweepOnce(deps)
  const sent = calls.batches.flatMap((b) => b.signals)
  assert.deepEqual(sent.map((s) => s.signalId), ['sig_retry'], 'sent exactly once')
  assert.equal(sent[0]!.attempt, 3, 'and as a retry, not as new work')
  assert.equal(out.sent, 1)
})

test('no known-check is made when there is no room left after retries', async () => {
  const retry = Array.from({ length: 4 }, (_, i) => ({
    signalId: `r${i}`, receivedAt: '2026-08-01T00:00:00.000Z', nextAttempt: 2,
  }))
  const { deps, calls } = harness({
    cursor: { retry },
    byIds: retry.map((r) => row({ signal_id: r.signalId })),
  })
  await sweepOnce(deps)
  assert.equal(calls.newer.length, 0, 'the archive is not re-read with a full batch')
  assert.equal(calls.known.length, 0, 'and nothing is asked about')
})

// ── starvation: the bug the runner trace found ───────────────────────────────

test('a window fuller than one batch does not starve the new work behind it', async () => {
  // The overlap window re-reads rows that are already settled. If the SQL LIMIT
  // is reached before the known-filter runs, every sweep reads the same page of
  // known rows, filters them all out, sends nothing, and NEVER reaches the newer
  // unknown rows. The pipeline wedges permanently.
  //
  // Reproduces at batchSize 2 x concurrency 2 = 4 slots: five already-settled
  // rows sit in the window ahead of one new one.
  const settledAlready = Array.from({ length: 5 }, (_, i) =>
    row({ signal_id: `old_${i}`, received_at: `2026-08-26 10:00:0${i}.000` }))
  const fresh = row({ signal_id: 'NEW', received_at: '2026-08-26 10:00:09.000' })
  const all = [...settledAlready, fresh]

  const { deps, calls } = harness({
    cursor: { sentThrough: '2026-08-26T10:00:05.000Z' },
    known: settledAlready.map((r) => r.signal_id),
  })
  // An archive that honours the keyset cursor and the limit, like ClickHouse.
  deps.archive.readNewer = async (page) => {
    let rows = all
    if (page.after) {
      rows = rows.filter((r) =>
        r.received_at > page.after!.receivedAt ||
        (r.received_at === page.after!.receivedAt && r.signal_id > page.after!.signalId))
    }
    calls.newer.push({ since: page.sinceIso, limit: page.limit })
    return rows.slice(0, page.limit)
  }

  const out = await sweepOnce(deps)
  assert.deepEqual(
    calls.batches.flatMap((b) => b.signals.map((s) => s.signalId)),
    ['NEW'],
    'the new signal must be reached, not starved behind the known ones',
  )
  assert.equal(out.sent, 1)
  assert.equal(out.alreadyKnown, 5)
  assert.ok(calls.newer.length > 1, 'it had to page past the known rows')
})

test('paging stops once the archive is exhausted', async () => {
  const { deps, calls } = harness({
    cursor: { sentThrough: '2026-08-26T10:00:00.000Z' },
    newer: [row({ signal_id: 'only' })],
  })
  const out = await sweepOnce(deps)
  assert.equal(out.sent, 1)
  // A short page means there is nothing more; it must not keep asking.
  assert.ok(calls.newer.length <= 2, `paged ${calls.newer.length} times for one row`)
})
