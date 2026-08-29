/**
 * Sends 100 real signals through the WHOLE pipeline and reports what each hop
 * did, against live infrastructure:
 *
 *   edge (:3000) → Kafka → ClickHouse → ONE dispatch run → settle (:3001) → Postgres
 *
 * Uses the same fixtures as `npm run e2e`, so it borrows a real org / plan /
 * credit / model, swaps one API key's hash for one it knows, and restores it in a
 * `finally`. Everything it writes is tagged with the run id and deleted at the
 * end — EXCEPT the credit balance it draws down, which is real consumption.
 *
 *   npm run send:100
 */
import './e2e/preload.mjs'
import {
  db, dispatch, KEY, preflight, remember, RUN, setup, teardown, type Fixture,
} from './e2e/fixtures.mjs'
import { check, clickhouse, heading, postSignal, report, waitFor } from './e2e/harness.mjs'

const N = 100

function bar(label: string, value: unknown): void {
  console.log(`        ${label.padEnd(34)} ${String(value)}`)
}

async function countIn(table: string, ids: string[]): Promise<number> {
  const row = await db.one<{ n: string }>(
    `select count(*) n from "${table}" where "signalId" = any($1)`,
    [ids],
  )
  return Number(row!.n)
}

async function main(): Promise<void> {
  let fixture: Fixture | null = null
  await db.connect()
  try {
    await preflight()
    fixture = await setup()
    const fx = fixture

    // ── 1 · the edge ───────────────────────────────────────────────────────
    heading(`1 · POST ${N} signals at the edge`)
    const t0 = Date.now()
    const posted = await Promise.all(
      Array.from({ length: N }, (_, i) =>
        postSignal(
          {
            customerId: fx.customerId,
            inputTokens: 100 + i,
            outputTokens: 50 + i,
            type: 'credit',
            model: fx.modelId,
            ...(fx.agentKey ? { agentKey: fx.agentKey } : {}),
            idempotencyKey: `send100_${RUN}_${i}`,
          },
          { key: KEY },
        ),
      ),
    )
    const accepted = posted.filter((r) => r.status === 202)
    const ids = accepted.map((r) => String(r.json.result?.signalId))
    ids.forEach(remember)
    check(`all ${N} accepted with 202`, accepted.length, N)
    check('every signalId distinct', new Set(ids).size, N)
    bar('edge wall time', `${Date.now() - t0}ms`)

    // ── 2 · the archive ────────────────────────────────────────────────────
    heading('2 · ClickHouse pulls them off Kafka by itself')
    const t1 = Date.now()
    const landed = await waitFor(async () => {
      const rows = await clickhouse.query<{ n: string }>(
        `SELECT count(DISTINCT signal_id) AS n FROM signal_log
          WHERE signal_id IN {ids:Array(String)}`,
        { ids: `['${ids.join("','")}']` },
      )
      return Number(rows[0]!.n) === N ? true : null
    }, { timeoutMs: 90_000 })
    check(`all ${N} reached the archive`, landed, true)
    bar('ingest lag (nothing pushed it)', `${Date.now() - t1}ms`)

    // How far apart the two clocks are on these rows — the whole reason the
    // dispatch window is over `ingested_at` and not `received_at`.
    const skew = await clickhouse.query<{
      max_lag_ms: string; min_recv: string; max_ing: string
    }>(
      `SELECT max(dateDiff('millisecond', received_at, ingested_at)) AS max_lag_ms,
              min(received_at) AS min_recv, max(ingested_at) AS max_ing
         FROM signal_log WHERE signal_id IN {ids:Array(String)}`,
      { ids: `['${ids.join("','")}']` },
    )
    bar('worst received_at → ingested_at lag', `${skew[0]!.max_lag_ms}ms`)

    // ── 3 · one dispatch run ───────────────────────────────────────────────
    heading('3 · ONE dispatch run — one window, one gzipped call')
    const run = await dispatch()
    bar('read from the window', run.read)
    bar('sent (one call, no batching)', run.sent)
    bar('processed', run.processed)
    bar('userError', run.userError)
    bar('serverError', run.serverError)
    bar('skipped (unusable rows)', run.skipped)
    bar('body', `${run.bytes}B json → ${run.gzipBytes}B gzip ` +
      `(${(run.bytes / Math.max(run.gzipBytes, 1)).toFixed(1)}:1)`)
    bar('vs the 4.5MB request ceiling', `${((run.gzipBytes / (4.5 * 1024 * 1024)) * 100).toFixed(2)}%`)
    bar('wall time', `${run.ms}ms`)
    bar('per signal', `${(run.ms / Math.max(run.sent, 1)).toFixed(1)}ms`)
    check(`the run carried all ${N} in a single call`, run.sent >= N, true)

    // ── 4 · Postgres ───────────────────────────────────────────────────────
    heading('4 · what landed in Postgres')
    const processed = Number((await db.one<{ n: string }>(
      `select count(*) n from "SignalStatus"
        where "signalId" = any($1) and status = 'PROCESSED'`, [ids]))!.n)
    check(`all ${N} are PROCESSED`, processed, N)
    const money = await countIn('SignalLog', ids)
    check(`exactly ${N} money rows — none lost, none duplicated`, money, N)
    const attempts = await db.one<{ mn: number; mx: number }>(
      `select min("attemptCount") mn, max("attemptCount") mx
         from "SignalStatus" where "signalId" = any($1)`, [ids])
    bar('attemptCount min/max', `${attempts!.mn} / ${attempts!.mx}`)
    check('every signal settled on its first attempt', attempts!.mx, 1)

    // ── 5 · the overlap ───────────────────────────────────────────────────
    heading('5 · run it AGAIN — the same window, and nothing should move')
    const again = await dispatch()
    bar('sent again', again.sent)
    bar('processed', again.processed)
    bar('wall time', `${again.ms}ms`)
    check('it re-sent the same signals', again.sent >= N, true)
    check(`still exactly ${N} money rows`, await countIn('SignalLog', ids), N)
    const after = await db.one<{ mx: number }>(
      `select max("attemptCount") mx from "SignalStatus" where "signalId" = any($1)`, [ids])
    check('and no attempt count was inflated by the replay', after!.mx, 1)
    bar('second run vs first', `${again.ms}ms vs ${run.ms}ms`)
  } finally {
    try {
      await teardown(fixture)
    } finally {
      await db.close()
    }
  }
  process.exit(report())
}

main().catch((err) => {
  console.error('\nSEND-100 ABORTED:', err)
  process.exit(1)
})
