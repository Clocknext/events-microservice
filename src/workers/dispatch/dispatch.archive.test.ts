/** The archive read is one SELECT, and the WHERE clause in it is the answer to
 *  "how do we know the latest data". These tests pin that clause, because getting
 *  it wrong is silent: a window over the wrong column, or a bound computed on the
 *  wrong machine, loses rows without erroring. */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import type { ClickHouseReader } from '../../client/clickhouse.js'
import { createArchiveReader } from './dispatch.archive.js'

function spy() {
  const seen: { sql: string; params: Record<string, string | number> }[] = []
  const clickhouse: ClickHouseReader = {
    async query<T>(sql: string, params: Record<string, string | number> = {}): Promise<T[]> {
      seen.push({ sql, params })
      return [] as T[]
    },
  }
  return { clickhouse, seen, reader: createArchiveReader(clickhouse) }
}

/** Collapse whitespace so assertions read against the SQL, not its formatting. */
const flat = (sql: string) => sql.replace(/\s+/g, ' ').trim()

test('the window is over ingested_at, never received_at', async () => {
  // `received_at` is the CALLER's time, stamped by N edge instances off N clocks;
  // a row can land here minutes after it and a window over it would miss that row
  // permanently, with no watermark to come back for it.
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  const sql = flat(seen[0]!.sql)
  assert.match(sql, /WHERE ingested_at >= /)
  assert.doesNotMatch(sql, /WHERE received_at/)
  assert.doesNotMatch(sql, /received_at >=/)
})

test("the lower bound is computed by ClickHouse's clock, not this process's", async () => {
  // `ingested_at` is stamped by the ClickHouse server. A bound derived from the
  // DISPATCHER's clock would shift the window by whatever skew exists between two
  // machines — silently. One clock stamps and the same clock compares.
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  assert.match(flat(seen[0]!.sql), /now64\(3\) - \(\{windowMs:UInt64\} \/ 1000\)/)
  assert.equal(seen[0]!.params.windowMs, 180_000)
})

test('the normal window has NO upper bound, so consecutive runs overlap', async () => {
  // A gap between one run's window and the next is a permanently lost row.
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 60_000, cap: 10 })
  assert.doesNotMatch(flat(seen[0]!.sql), /ingested_at </)
})

test('an explicit replay window binds both ends and drops the relative bound', async () => {
  const { reader, seen } = spy()
  await reader.readIngested({
    windowMs: 180_000,
    since: '2026-08-26T04:00:00Z',
    until: '2026-08-26T05:00:00Z',
    cap: 100,
  })
  const { sql, params } = seen[0]!
  assert.match(flat(sql), /ingested_at >= parseDateTime64BestEffort\(\{since:String\}\)/)
  assert.match(flat(sql), /ingested_at < parseDateTime64BestEffort\(\{until:String\}\)/)
  assert.equal(params.since, '2026-08-26T04:00:00Z')
  assert.equal(params.until, '2026-08-26T05:00:00Z')
  // The relative bound must be gone, or the replay would be intersected with
  // "the last three minutes" and return nothing.
  assert.doesNotMatch(flat(sql), /now64/)
  assert.equal(params.windowMs, undefined)
})

test('a replay with no upper bound runs to now', async () => {
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, since: '2026-08-26T04:00:00Z', cap: 100 })
  assert.doesNotMatch(flat(seen[0]!.sql), /ingested_at </)
  assert.equal(seen[0]!.params.until, undefined)
})

test('rows are deduplicated within the run and bounded by the cap', async () => {
  // LIMIT 1 BY signal_id is not the same job as settle's idempotency: settle
  // dedups ACROSS runs, this stops one request body carrying a pre-merge
  // ReplacingMergeTree duplicate twice.
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  const sql = flat(seen[0]!.sql)
  assert.match(sql, /LIMIT 1 BY signal_id/)
  assert.match(sql, /LIMIT \{cap:UInt32\}/)
  assert.equal(seen[0]!.params.cap, 100)
})

test('rows come back oldest-ingested first, and payload is selected raw', async () => {
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  const sql = flat(seen[0]!.sql)
  assert.match(sql, /ORDER BY ingested_at ASC/)
  // Never `toString(received_at) AS received_at`: ClickHouse resolves a SELECT
  // alias inside WHERE, and the query dies comparing String to DateTime64.
  assert.match(
    sql,
    /SELECT signal_id, received_at, api_key_hash, customer_id, organization_id, status, error_code, error_message, payload/,
  )
  assert.doesNotMatch(sql, /toString\(/)
})

test('the newest version of a signal wins, and the cap drops the newest rows', async () => {
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  const sql = flat(seen[0]!.sql)
  // `signal_log` is ReplacingMergeTree(version): a row can be rewritten by
  // re-inserting it higher. Nothing does that yet — the daily reconciliation cron
  // will — and picking the newest has to be right BEFORE it does.
  assert.match(sql, /ORDER BY signal_id, version DESC/)
  assert.match(sql, /LIMIT 1 BY signal_id/)
  // Two different orderings, so they cannot share one level. The outer one is
  // load-bearing: the cap must drop the NEWEST rows, because those are the ones
  // the next overlapping window still covers.
  assert.ok(sql.indexOf('ORDER BY signal_id, version DESC') < sql.indexOf('ORDER BY ingested_at ASC'))
  assert.ok(sql.indexOf('ORDER BY ingested_at ASC') < sql.indexOf('LIMIT {cap:UInt32}'))
})

test('both statuses come back — a PENDING signal is never filtered out', async () => {
  const { reader, seen } = spy()
  await reader.readIngested({ windowMs: 180_000, cap: 100 })
  const sql = flat(seen[0]!.sql)
  // Rejected signals still go to settle, which records the terminal failure the
  // consumer already decided. Filtering here would leave them with no Postgres
  // row at all — invisible in the Signals UI.
  assert.doesNotMatch(sql, /status\s*=/)
})
