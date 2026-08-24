import assert from 'node:assert/strict'
import { test } from 'node:test'
import type { SQSEvent, SQSRecord } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import { consumeAccepted } from './accepted.handler.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'
import { consumePending } from './pending.handler.js'

/** A ClickHouse stand-in that records every insert. `failTable` makes one
 *  table's inserts throw, so the partial-failure paths can be exercised without
 *  a real database. `failRows` fails only inserts that contain a matching id. */
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

/** Builds an SQSEvent from message bodies, tagging each with a synthetic id. */
function sqsEvent(bodies: string[]): SQSEvent {
  const Records = bodies.map(
    (body, i) => ({ messageId: `m${i}`, body }) as SQSRecord,
  )
  return { Records } as SQSEvent
}

function ventMessage(signalId: string, errorCode = 'INVALID_BODY') {
  return JSON.stringify({
    raw_signals: {
      signal_id: signalId,
      organization_id: '',
      customer_id: 'cus_x',
      received_at: '2026-08-24T10:00:00.000Z',
      idempotency_key: null,
      api_key_id: '',
      payload: '{}',
    },
    signal_status: {
      signal_id: signalId,
      organization_id: '',
      attempt: 1,
      status: 'PENDING',
      error_type: 'USER_ERROR',
      error_code: errorCode,
      error_message: 'x',
      signal_type: null,
      updated_at: '2026-08-24T10:00:00.000Z',
    },
  })
}

function acceptedMessage(signalId: string) {
  return JSON.stringify({ raw_signals: { signal_id: signalId, received_at: '2026-08-24T10:00:00.000Z' } })
}

// --- insertTagged -------------------------------------------------------------

test('insertTagged: a clean batch is one bulk insert, no failures', async () => {
  const { client, inserted } = fakeClickHouse()
  const rows: TaggedRow[] = [
    { messageId: 'a', row: { signal_id: '1' } },
    { messageId: 'b', row: { signal_id: '2' } },
  ]
  const failed = await insertTagged(client, 'raw_signals', rows)
  assert.equal(failed.size, 0)
  assert.equal(inserted.raw_signals!.length, 2)
})

test('insertTagged: one poison row fails alone, the rest still land', async () => {
  // The bulk insert throws because of row 2; the retry pass isolates it.
  const { client, inserted } = fakeClickHouse({ failSignalIds: new Set(['2']) })
  const rows: TaggedRow[] = [
    { messageId: 'a', row: { signal_id: '1' } },
    { messageId: 'b', row: { signal_id: '2' } },
    { messageId: 'c', row: { signal_id: '3' } },
  ]
  const failed = await insertTagged(client, 'raw_signals', rows)
  assert.deepEqual([...failed], ['b'], 'only the poison message is failed')
  // 1 and 3 land on the per-row retry.
  const ids = inserted.raw_signals!.map((r) => (r as { signal_id: string }).signal_id)
  assert.deepEqual(ids.sort(), ['1', '3'])
})

// --- pending consumer ---------------------------------------------------------

test('pending: writes raw_signals then signal_status, no failures', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumePending(client, sqsEvent([ventMessage('sig_1'), ventMessage('sig_2')]))
  assert.deepEqual(res.batchItemFailures, [])
  assert.equal(inserted.raw_signals!.length, 2)
  assert.equal(inserted.signal_status!.length, 2)
})

test('pending: an unparseable message is failed, not inserted', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumePending(client, sqsEvent([ventMessage('sig_1'), '{not json']))
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm1' }])
  assert.equal(inserted.raw_signals!.length, 1, 'only the good message is written')
})

test('pending: a signal_status failure fails that message and skips nothing else', async () => {
  const { client, inserted } = fakeClickHouse({ failTable: 'signal_status' })
  const res = await consumePending(client, sqsEvent([ventMessage('sig_1')]))
  // raw_signals landed, signal_status did not, so the message replays (and both
  // rows re-insert, which ReplacingMergeTree collapses).
  assert.equal(inserted.raw_signals!.length, 1)
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }])
})

test('pending: a raw_signals failure does not then insert its signal_status', async () => {
  const { client, inserted } = fakeClickHouse({ failTable: 'raw_signals' })
  const res = await consumePending(client, sqsEvent([ventMessage('sig_1')]))
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }])
  assert.equal(inserted.signal_status, undefined, 'no orphan status row')
})

// --- accepted consumer --------------------------------------------------------

test('accepted: writes raw_signals stamped with the invocation batch id', async () => {
  const { client, inserted } = fakeClickHouse()
  const res = await consumeAccepted(
    client,
    sqsEvent([acceptedMessage('sig_1'), acceptedMessage('sig_2')]),
    'batch_abc',
  )
  assert.deepEqual(res.batchItemFailures, [])
  assert.equal(inserted.raw_signals!.length, 2)
  for (const row of inserted.raw_signals!) {
    assert.equal((row as { batch_id: string }).batch_id, 'batch_abc', 'one id per invocation')
  }
})

test('accepted: only signal_id, received_at and batch_id are written', async () => {
  const { client, inserted } = fakeClickHouse()
  await consumeAccepted(client, sqsEvent([acceptedMessage('sig_1')]), 'batch_abc')
  assert.deepEqual(
    Object.keys(inserted.raw_signals![0]!).sort(),
    ['batch_id', 'received_at', 'signal_id'],
  )
})

test('accepted: a message missing signal_id is failed', async () => {
  const { client } = fakeClickHouse()
  const res = await consumeAccepted(
    client,
    sqsEvent([JSON.stringify({ raw_signals: { received_at: 'x' } })]),
    'batch_abc',
  )
  assert.deepEqual(res.batchItemFailures, [{ itemIdentifier: 'm0' }])
})
