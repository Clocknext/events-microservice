import assert from 'node:assert/strict'
import { test } from 'node:test'
import { runOnce, toIso, toSettleSignal, type RunDeps } from './dispatch.service.js'
import type {
  ArchiveWindow,
  SettleResult,
  SettleSignal,
  SignalLogRow,
} from './dispatch.schema.js'

/** An accepted row by default: the consumer resolved the key, so `status` is
 *  PROCESSING and `organization_id` is set. Override for a PENDING row. */
function row(over: Partial<SignalLogRow> = {}): SignalLogRow {
  return {
    signal_id: '01M0X91S3X2SK86WYYWVENM3N7',
    received_at: '2026-08-26 10:00:00.000',
    api_key_hash: 'a'.repeat(64),
    customer_id: 'cus_1',
    organization_id: 'org_1',
    status: 'PROCESSING',
    error_code: '',
    error_message: '',
    payload: '{"customerId":"cus_1","inputTokens":10,"outputTokens":5}',
    ...over,
  }
}

function rows(n: number): SignalLogRow[] {
  return Array.from({ length: n }, (_, i) =>
    row({ signal_id: `sig-${String(i).padStart(5, '0')}` }),
  )
}

/** A fake archive + payments pair that records everything it was asked. */
function harness(opts: {
  ingested?: SignalLogRow[]
  onSettle?: (batchId: string, signals: SettleSignal[]) => Promise<SettleResult[]>
  config?: Partial<RunDeps['config']>
} = {}) {
  const calls = {
    windows: [] as ArchiveWindow[],
    settles: [] as { batchId: string; signals: SettleSignal[] }[],
    logs: [] as { event: string; detail: Record<string, unknown> }[],
  }
  let n = 0
  let clock = 1_000
  const deps: RunDeps = {
    archive: {
      async readIngested(window) {
        calls.windows.push(window)
        return (opts.ingested ?? []).slice(0, window.cap)
      },
    },
    payments: {
      async settle(batchId, signals) {
        calls.settles.push({ batchId, signals })
        const results = opts.onSettle
          ? await opts.onSettle(batchId, signals)
          : signals.map((s) => ({
              signal_id: s.signalId, status: 'PROCESSED' as const,
              error_type: null, error_code: null, error_message: null,
            }))
        return { results, bytes: 1_000, gzipBytes: 100 }
      },
    },
    config: { windowMs: 180_000, maxRows: 100, ...opts.config },
    newBatchId: () => `batch-${++n}`,
    log: (event, detail) => calls.logs.push({ event, detail }),
    now: () => (clock += 5),
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

// ── payload -> settle signal ─────────────────────────────────────────────────

test('the envelope overrides anything the caller put in the body', () => {
  // A caller sending their own "signalId"/"attempt" must not be able to
  // impersonate the edge's stamp and land on another signal's money row.
  const signal = toSettleSignal(
    row({ payload: '{"customerId":"cus_1","signalId":"NOT_MINE","attempt":99,"receivedAt":"1999-01-01T00:00:00Z","apiKeyHash":"beef"}' }),
  )
  assert.equal(signal?.signalId, '01M0X91S3X2SK86WYYWVENM3N7')
  assert.equal(signal?.receivedAt, '2026-08-26T10:00:00.000Z')
  assert.equal(signal?.apiKeyHash, 'a'.repeat(64))
  assert.equal(signal?.customerId, 'cus_1')
})

test('attempt is always 1 — this side no longer counts deliveries', () => {
  // There is no state here to count them with; SignalStatus owns the count.
  assert.equal(toSettleSignal(row())?.attempt, 1)
  assert.equal(toSettleSignal(row({ payload: '{"attempt":7}' }))?.attempt, 1)
})

test('a caller-supplied organizationId is overwritten by the resolved one (BUG-1)', () => {
  // The payload is a verbatim copy of a request body, so this field is
  // caller-controlled: forwarding it let a caller bill another tenant. The
  // defence is unchanged — the caller's value never survives. What changed is
  // that the consumer now resolves a TRUSTED one against /api/internal/resolve,
  // so the field is written rather than deleted. Deleting it today would throw
  // away the attribution and force settle to re-resolve every signal.
  const signal = toSettleSignal(
    row({
      organization_id: 'org_theirs',
      payload: '{"customerId":"cus_1","organizationId":"org_someone_else"}',
    }),
  )
  assert.ok(signal)
  assert.equal(signal.organizationId, 'org_theirs')
})

test('a row the consumer rejected travels as PENDING, with its reason', () => {
  const signal = toSettleSignal(
    row({
      organization_id: 'org_1',
      status: 'PENDING',
      error_code: 'CUSTOMER_NOT_FOUND',
      error_message: 'Customer "cus_nope" not found.',
    }),
  )
  assert.ok(signal)
  assert.equal(signal.status, 'PENDING')
  assert.equal(signal.errorCode, 'CUSTOMER_NOT_FOUND')
  assert.equal(signal.errorMessage, 'Customer "cus_nope" not found.')
})

test('an accepted row carries no error — empty columns become null, not ""', () => {
  // The ClickHouse columns are not Nullable, so absence is the empty string
  // there and null on the wire. Sending "" would read as an error with no
  // message.
  const signal = toSettleSignal(row())
  assert.ok(signal)
  assert.equal(signal.status, 'PROCESSING')
  assert.equal(signal.errorCode, null)
  assert.equal(signal.errorMessage, null)
})

test('an unparseable or non-object payload is skipped, not sent half-built', () => {
  assert.equal(toSettleSignal(row({ payload: 'not json' })), null)
  assert.equal(toSettleSignal(row({ payload: '[1,2,3]' })), null)
  assert.equal(toSettleSignal(row({ payload: 'null' })), null)
  assert.equal(toSettleSignal(row({ payload: '"a string"' })), null)
})

// ── the run ──────────────────────────────────────────────────────────────────

test('an empty window settles nothing and makes no settle call at all', async () => {
  const { deps, calls } = harness({ ingested: [] })
  const outcome = await runOnce(deps)
  assert.equal(outcome.read, 0)
  assert.equal(outcome.sent, 0)
  assert.equal(calls.settles.length, 0)
  // An idle minute must cost one query and nothing else.
  assert.equal(calls.windows.length, 1)
})

test('the WHOLE window goes in ONE settle call, however big it is', async () => {
  // The point of the 1.0 design: no chunking, no concurrency, no batch size.
  const { deps, calls } = harness({ ingested: rows(90), config: { maxRows: 100 } })
  const outcome = await runOnce(deps)
  assert.equal(outcome.sent, 90)
  assert.equal(calls.settles.length, 1)
  assert.equal(calls.settles[0]?.signals.length, 90)
  // One batch id for the run, and it is reported so a log line can be traced
  // to the settle call it caused.
  assert.equal(outcome.batchId, 'batch-1')
  assert.equal(calls.settles[0]?.batchId, 'batch-1')
})

test('the configured window and cap are what the archive is asked for', async () => {
  const { deps, calls } = harness({
    ingested: [],
    config: { windowMs: 7_200_000, maxRows: 42 },
  })
  await runOnce(deps)
  assert.deepEqual(calls.windows[0], {
    windowMs: 7_200_000,
    since: undefined,
    until: undefined,
    cap: 42,
  })
})

test('an explicit replay window is passed straight through', async () => {
  const { deps, calls } = harness({
    ingested: [],
    config: { since: '2026-08-26T04:00:00Z', until: '2026-08-26T05:00:00Z' },
  })
  await runOnce(deps)
  assert.equal(calls.windows[0]?.since, '2026-08-26T04:00:00Z')
  assert.equal(calls.windows[0]?.until, '2026-08-26T05:00:00Z')
})

test('hitting the row cap sets `capped` and says so in the log', async () => {
  // This is a DATA-LOSS alarm, not a tuning hint: rows beyond the cap were not
  // sent and the next window has already moved past some of them.
  const { deps, calls } = harness({ ingested: rows(50), config: { maxRows: 10 } })
  const outcome = await runOnce(deps)
  assert.equal(outcome.read, 10)
  assert.equal(outcome.capped, true)
  assert.ok(calls.logs.some((l) => l.event === 'run.capped'))
})

test('a window under the cap is not reported as capped', async () => {
  const { deps } = harness({ ingested: rows(9), config: { maxRows: 10 } })
  assert.equal((await runOnce(deps)).capped, false)
})

test('outcomes are counted by kind', async () => {
  const { deps } = harness({
    ingested: rows(3),
    async onSettle(_batchId, signals) {
      return [
        { signal_id: signals[0]!.signalId, status: 'PROCESSED', error_type: null, error_code: null, error_message: null },
        { signal_id: signals[1]!.signalId, status: 'PENDING', error_type: 'USER_ERROR', error_code: 'BAD_MODEL', error_message: 'no such model' },
        { signal_id: signals[2]!.signalId, status: 'PENDING', error_type: 'SERVER_ERROR', error_code: 'DB', error_message: 'timeout' },
      ]
    },
  })
  const outcome = await runOnce(deps)
  assert.equal(outcome.processed, 1)
  assert.equal(outcome.userError, 1)
  assert.equal(outcome.serverError, 1)
})

test('an unusable row is skipped and counted, and the rest still go', async () => {
  const { deps, calls } = harness({
    ingested: [row({ signal_id: 'a' }), row({ signal_id: 'b', payload: 'not json' }), row({ signal_id: 'c' })],
  })
  const outcome = await runOnce(deps)
  assert.equal(outcome.read, 3)
  assert.equal(outcome.sent, 2)
  assert.equal(outcome.skipped, 1)
  assert.deepEqual(calls.settles[0]?.signals.map((s) => s.signalId), ['a', 'c'])
  assert.ok(calls.logs.some((l) => l.event === 'row.unusable'))
})

test('a window of nothing BUT unusable rows makes no settle call', async () => {
  const { deps, calls } = harness({ ingested: [row({ payload: 'not json' })] })
  const outcome = await runOnce(deps)
  assert.equal(outcome.skipped, 1)
  assert.equal(outcome.sent, 0)
  assert.equal(calls.settles.length, 0)
})

test('a failing settle call PROPAGATES — it must not look like a clean run', async () => {
  // With one call per run, swallowing this would report exit 0 having sent
  // nothing. The runner turns the throw into a non-zero exit and the next
  // overlapping window re-sends the same signals.
  const { deps } = harness({
    ingested: rows(5),
    onSettle: () => Promise.reject(new Error('settle 503')),
  })
  await assert.rejects(() => runOnce(deps), /settle 503/)
})

test('the body sizes and duration come back on the outcome', async () => {
  // `gzipBytes` is watched against Vercel's 4.5MB request-body ceiling.
  const { deps } = harness({ ingested: rows(2) })
  const outcome = await runOnce(deps)
  assert.equal(outcome.bytes, 1_000)
  assert.equal(outcome.gzipBytes, 100)
  assert.ok(outcome.ms > 0)
})
