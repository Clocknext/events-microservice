import assert from 'node:assert/strict'
import { test } from 'node:test'
import type { SQSEvent, SQSRecord } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import type { StatusEventRow } from '../modules/vent/vent.schema.js'
import { consumeAccepted } from './accepted.handler.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'
import type { SettleResult, SettleSignal } from './lib/settle.js'
import { consumePending } from './pending.handler.js'

/** A ClickHouse stand-in that records every insert. `failTable` makes a table's
 *  inserts throw; `failSignalIds` fails only inserts containing a matching id. */
function fakeClickHouse(opts: { failTable?: string; failSignalIds?: Set<string> } = {}) {
  const inserted: Record<string, object[]> = {}
  const client: ClickHouseClient = {
    async insert(table, rows) {
      if (opts.failTable === table) throw new Error(`insert into ${table} failed`)
      if (opts.failSignalIds) {
        for (const row of rows) {
          const id = (row as { signal_id?: string }).signal_id
          if (id && opts.failSignalIds.has(id)) throw new Error(`poison row ${id}`)
        }
      }
      ;(inserted[table] ??= []).push(...rows)
    },
  }
  return { client, inserted }
}

function events(inserted: Record<string, object[]>): StatusEventRow[] {
  return (inserted.signal_status_events ?? []) as StatusEventRow[]
}

/** An SQSEvent from message bodies. `receiveCount` sets ApproximateReceiveCount
 *  so a test can drive the `attempt`. */
function sqsEvent(bodies: string[], receiveCountValue = 1): SQSEvent {
  const Records = bodies.map(
    (body, i) =>
      ({
        messageId: `m${i}`,
        body,
        attributes: { ApproximateReceiveCount: String(receiveCountValue) },
      }) as unknown as SQSRecord,
  )
  return { Records } as SQSEvent
}

function pendingMessage(signalId: string, errorCode = 'INVALID_BODY') {
  return JSON.stringify({
    raw_signals: {
      signal_id: signalId,
      organization_id: '',
      customer_id: 'cus_x',
      type: 'credit',
      idempotency_key: null,
      payload: '{}',
      received_at: '2026-08-24T10:00:00.000Z',
    },
    error_code: errorCode,
  })
}

function acceptedMessage(signalId: string, body: object = { customerId: 'cus_x', type: 'credit' }) {
  return JSON.stringify({
    raw_signals: {
      signal_id: signalId,
      organization_id: 'org_x',
      customer_id: 'cus_x',
      type: 'credit',
      idempotency_key: null,
      payload: JSON.stringify(body),
      received_at: '2026-08-24T10:00:00.000Z',
    },
  })
}

/** A fake settle that returns a fixed status for every signal it is handed. */
function fakeSettle(
  make: (s: SettleSignal) => SettleResult,
): (batchId: string, signals: SettleSignal[]) => Promise<SettleResult[]> {
  return async (_batchId, signals) => signals.map(make)
}

const PROCESSED = (s: SettleSignal): SettleResult => ({
  signal_id: s.signalId,
  status: 'PROCESSED',
  error_type: null,
  error_code: null,
})

// --- insertTagged -------------------------------------------------------------

test('insertTagged: one poison row fails alone, the rest still land', async () => {
  const { client, inserted } = fakeClickHouse({ failSignalIds: new Set(['2']) })
  const rows: TaggedRow[] = [
    { messageId: 'a', row: { signal_id: '1' } },
    { messageId: 'b', row: { signal_id: '2' } },
    { messageId: 'c', row: { signal_id: '3' } },
  ]
  const failed = await insertTagged(client, 'raw_signals', rows)
  assert.deepEqual([...failed], ['b'])
  const ids = inserted.raw_signals!.map((r) => (r as { signal_id: string }).signal_id)
  assert.deepEqual(ids.sort(), ['1', '3'])
})

// --- pending consumer ---------------------------------------------------------

test('pending: writes raw_signals and one Failed event carrying the error_code', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumePending(client, sqsEvent([pendingMessage('sig_1', 'MALFORMED_JSON')]), 'batch_p')
  assert.deepEqual(res.batchItemFailures, [])
  assert.equal(inserted.raw_signals!.length, 1)

  const evs = events(inserted)
  assert.equal(evs.length, 1)
  assert.equal(evs[0]!.status, 'Failed')
  assert.equal(evs[0]!.error_code, 'MALFORMED_JSON')
  assert.equal(evs[0]!.batch_id, 'batch_p')
  assert.equal(evs[0]!.attempt, 1)
})

test('pending: attempt reflects the SQS receive count', async () => {
  const { client, inserted } = fakeClickHouse()
  await consumePending(client, sqsEvent([pendingMessage('sig_1')], 3), 'batch_p')
  assert.equal(events(inserted)[0]!.attempt, 3)
})

test('pending: an unparseable message is failed, not inserted', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumePending(client, sqsEvent([pendingMessage('sig_1'), '{bad']), 'batch_p')
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm1' }])
  assert.equal(inserted.raw_signals!.length, 1)
})

test('pending: a raw_signals failure writes no orphan event', async () => {
  const { client, inserted } = fakeClickHouse({ failTable: 'raw_signals' })
  const res = await consumePending(client, sqsEvent([pendingMessage('sig_1')]), 'batch_p')
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }])
  assert.equal(inserted.signal_status_events, undefined)
})

// --- accepted consumer: the settle flow ---------------------------------------

test('accepted: a settled signal gets raw_signals, Processing, then Processed', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumeAccepted(client, fakeSettle(PROCESSED), sqsEvent([acceptedMessage('sig_1')]), 'batch_a')
  assert.deepEqual(res.batchItemFailures, [])
  assert.equal(inserted.raw_signals!.length, 1)

  const evs = events(inserted)
  assert.deepEqual(
    evs.map((e) => e.status),
    ['Processing', 'Processed'],
    'the lifecycle is logged, Processing before Processed',
  )
  for (const e of evs) assert.equal(e.batch_id, 'batch_a')
})

test('accepted: Processed is stamped later than Processing, so latest-wins works', async () => {
  const { client, inserted } = fakeClickHouse()
  await consumeAccepted(client, fakeSettle(PROCESSED), sqsEvent([acceptedMessage('sig_1')]), 'batch_a')
  const evs = events(inserted)
  const processing = evs.find((e) => e.status === 'Processing')!
  const processed = evs.find((e) => e.status === 'Processed')!
  assert.ok(processed.timestamp >= processing.timestamp, 'Processed time >= Processing time')
})

test('accepted: settle is called with the payload spread and the named fields on top', async () => {
  const { client } = fakeClickHouse()
  let seen: SettleSignal | undefined
  const settle = async (_b: string, signals: SettleSignal[]) => {
    seen = signals[0]
    return signals.map(PROCESSED)
  }
  await consumeAccepted(
    client,
    settle,
    sqsEvent([acceptedMessage('sig_1', { customerId: 'cus_x', type: 'credit', model: 'gpt-4o', inputTokens: 5 })]),
    'batch_a',
  )
  assert.equal(seen!.signalId, 'sig_1')
  assert.equal(seen!.organizationId, 'org_x')
  assert.equal(seen!.model, 'gpt-4o', 'payload fields pass through to pricing')
  assert.equal(seen!.inputTokens, 5)
  assert.equal(seen!.attempt, 1)
})

test('accepted: a terminal refusal (USER_ERROR) writes Failed and acks', async () => {
  const { client, inserted } = fakeClickHouse()
  const settle = fakeSettle((s) => ({
    signal_id: s.signalId,
    status: 'PENDING',
    error_type: 'USER_ERROR',
    error_code: 'NO_ACTIVE_PLAN',
  }))
  const res = await consumeAccepted(client, settle, sqsEvent([acceptedMessage('sig_1')]), 'batch_a')
  assert.deepEqual(res.batchItemFailures, [], 'terminal: acked, not retried')
  const evs = events(inserted)
  assert.deepEqual(evs.map((e) => e.status), ['Processing', 'Failed'])
  assert.equal(evs.find((e) => e.status === 'Failed')!.error_code, 'NO_ACTIVE_PLAN')
})

test('accepted: a retryable refusal (SERVER_ERROR) writes no terminal event and redelivers', async () => {
  const { client, inserted } = fakeClickHouse()
  const settle = fakeSettle((s) => ({
    signal_id: s.signalId,
    status: 'PENDING',
    error_type: 'SERVER_ERROR',
    error_code: 'DB_TIMEOUT',
  }))
  const res = await consumeAccepted(client, settle, sqsEvent([acceptedMessage('sig_1')]), 'batch_a')
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }], 'sent back to SQS')
  // Only the Processing event — no terminal, because it is not resolved.
  assert.deepEqual(events(inserted).map((e) => e.status), ['Processing'])
})

test('accepted: if settle itself throws, every signal retries', async () => {
  const { client, inserted } = fakeClickHouse()
  const settle = async () => {
    throw new Error('payments unreachable')
  }
  const res = await consumeAccepted(
    client,
    settle,
    sqsEvent([acceptedMessage('sig_1'), acceptedMessage('sig_2')]),
    'batch_a',
  )
  assert.deepEqual(
    res.batchItemFailures.map((f) => f.itemIdentifier).sort(),
    ['m0', 'm1'],
  )
  // raw + Processing were still written; they replay harmlessly.
  assert.equal(inserted.raw_signals!.length, 2)
  assert.deepEqual(events(inserted).map((e) => e.status), ['Processing', 'Processing'])
})

test('accepted: a mix settles some and retries others in one batch', async () => {
  const { client, inserted } = fakeClickHouse()
  const settle = fakeSettle((s) =>
    s.signalId === 'sig_2'
      ? { signal_id: s.signalId, status: 'PENDING', error_type: 'SERVER_ERROR', error_code: 'X' }
      : PROCESSED(s),
  )
  const res = await consumeAccepted(
    client,
    settle,
    sqsEvent([acceptedMessage('sig_1'), acceptedMessage('sig_2'), acceptedMessage('sig_3')]),
    'batch_a',
  )
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm1' }], 'only sig_2 retries')
  const processed = events(inserted).filter((e) => e.status === 'Processed').map((e) => e.signal_id)
  assert.deepEqual(processed.sort(), ['sig_1', 'sig_3'])
})

test('accepted: a message missing signal_id is failed and never settled', async () => {
  const { client } = fakeClickHouse()
  let settleCalled = false
  const settle = async (_b: string, signals: SettleSignal[]) => {
    settleCalled = true
    return signals.map(PROCESSED)
  }
  const res = await consumeAccepted(
    client,
    settle,
    sqsEvent([JSON.stringify({ raw_signals: { received_at: 'x' } })]),
    'batch_a',
  )
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }])
  assert.equal(settleCalled, false, 'nothing valid to settle')
})
