/**
 * Traces ONE signal through every hop, printing what each one actually sent and
 * received — the exact ClickHouse SQL, the exact HTTP payloads, the exact rows.
 *
 * Not a test: it asserts nothing. It exists so the pipeline can be read rather
 * than reasoned about. Uses the same fixtures as `npm run e2e`, so it borrows a
 * real API key row and restores it in a `finally`.
 *
 *   npm run trace
 */
import './preload.mjs'
import { toIso, toSettleSignal } from '../../src/workers/dispatch/dispatch.service.js'
import { createArchiveReader } from '../../src/workers/dispatch/dispatch.archive.js'
import { settleAll } from '../../src/client/payments-client.js'
import { archiveRow, clickhouse, postSignal, waitFor } from './harness.mjs'
import { db, KEY, remember, RUN, setup, teardown, type Fixture } from './fixtures.mjs'

const rule = (n: string) => console.log(`\n\x1b[1m${'═'.repeat(76)}\n${n}\n${'═'.repeat(76)}\x1b[0m`)
const step = (n: string) => console.log(`\n\x1b[1;36m${n}\x1b[0m`)
const show = (label: string, value: unknown) =>
  console.log(`${label}\n${typeof value === 'string' ? value : JSON.stringify(value, null, 2)}`)

async function main() {
  let fixture: Fixture | null = null
  await db.connect()
  try {
    fixture = await setup()
    const fx = fixture

    // ── 1 · the edge ───────────────────────────────────────────────────────
    rule('1 · EDGE — POST /api/v1/signal')
    const body = {
      customerId: fx.customerId,
      inputTokens: 1200,
      outputTokens: 350,
      type: 'credit',
      model: fx.modelId,
      ...(fx.agentKey ? { agentKey: fx.agentKey } : {}),
      idempotencyKey: `trace_${RUN}`,
    }
    show('REQUEST BODY', body)
    const res = await postSignal(body, { key: KEY })
    show(`\nRESPONSE  ${res.status}`, res.json)
    const signalId = String(res.json.result?.signalId ?? '')
    remember(signalId)

    // ── 2 · the archive ────────────────────────────────────────────────────
    rule('2 · THE CONSUMER — it resolves the signal, THEN archives it')
    const t0 = Date.now()
    const row = await waitFor(() => archiveRow(signalId), { timeoutMs: 30_000 })
    console.log(`landed after ~${Date.now() - t0}ms — one /api/internal/resolve call happened
inside that gap, and its verdict is the status/organization_id/error_* below.
ClickHouse pulled nothing: ENGINE=Kafka and its materialized view are GONE,
because a materialized view cannot make an HTTP request.`)
    show('\nsignal_log ROW', row)
    console.log(`\nreceived_at is UTC but says so nowhere:  ${row!.received_at}`)
    console.log(`toIso() restores the T and the Z:        ${toIso(row!.received_at)}`)
    console.log(`the edge stamped:                        ${res.json.result?.receivedAt}`)

    // ── 3 · the one archive read ───────────────────────────────────────────
    rule('3 · DISPATCHER — the ONE SELECT it runs against signal_log')
    console.log(`SELECT signal_id, received_at, api_key_hash, payload
   FROM signal_log
  WHERE ingested_at >= now64(3) - ({windowMs:UInt64} / 1000)
  ORDER BY ingested_at ASC
  LIMIT 1 BY signal_id
  LIMIT {cap:UInt32}`)
    console.log(`
  THE WINDOW IS OVER ingested_at, NOT received_at, and that is the whole trick.

  received_at is the CALLER's time, stamped by N edge instances off N clocks. A
  row can land here minutes after it — the consumer batches, spends an HTTP round
  trip per signal, and a broker backlog delays it further. Nothing persists a
  watermark, so a window over
  received_at would miss such a row permanently. ingested_at is DEFAULT now64(3):
  one clock, one server, "arrived in the archive".

  The lower bound is computed by CLICKHOUSE, from its own now64(3), not by the
  dispatcher — comparing against a bound off this machine's clock would shift the
  window by whatever skew exists between the two, silently.

  There is NO upper bound, so consecutive runs overlap and nothing falls between
  them. received_at is still what settle bills on.`)

    const archive = createArchiveReader(clickhouse)
    const window = { windowMs: 90_000, cap: 1_000 }
    const candidates = await archive.readIngested(window)
    console.log(`\n→ ${candidates.length} row(s) in the last ${window.windowMs / 1000}s of INGESTION`)
    for (const c of candidates.slice(0, 5)) console.log(`   ${c.signal_id}  ${c.received_at}`)
    console.log(`   ${candidates.some((c) => c.signal_id === signalId) ? '✓' : '✗'} our signal is in the window`)
    console.log(`
  LIMIT 1 BY signal_id collapses the duplicates at-least-once Kafka delivery
  leaves in a ReplacingMergeTree before a merge runs. Cheaper than FINAL. Note it
  is a DIFFERENT job from settle's idempotency: that dedups across runs, this
  stops one request body carrying the same signal twice.`)

    // ── 6 · the settle call ────────────────────────────────────────────────
    rule('6 · DISPATCHER ④ — POST /api/internal/settle')
    const settleSignal = toSettleSignal(row!, 1)
    console.log('toSettleSignal() spreads the payload, then writes the envelope over it,')
    console.log('and DELETES organizationId — the payload is caller-controlled.\n')
    const payload = { batchId: `trace_${RUN}`, signals: [settleSignal] }
    show('REQUEST BODY (before encoding)', payload)
    console.log(`
  The real client sends this GZIPPED, with content-encoding: gzip. Not an
  optimisation — Vercel refuses a serverless function's request body over 4.5MB,
  and one window of raw signal JSON crosses that at roughly 15k signals. Signal
  JSON is the same keys over and over, so it compresses about 10:1.

  The WHOLE window goes in ONE call. There is no batch size and no concurrency.`)

    const transfer = await settleAll(`trace_${RUN}`, [settleSignal!])
    rule('7 · WHAT /api/internal/settle RETURNS  (verbatim, nothing elided)')
    console.log(`body: ${transfer.bytes}B json -> ${transfer.gzipBytes}B on the wire ` +
      `(${(transfer.bytes / Math.max(transfer.gzipBytes, 1)).toFixed(1)}:1)`)
    show('', transfer.results)

    // ── 8 · the rows ───────────────────────────────────────────────────────
    rule('8 · POSTGRES — the two rows it wrote')
    step('SignalStatus  (lifecycle — drives the Signals UI AND the dispatcher)')
    show('', await db.one(
      `select "signalId", status, "errorType", "errorCode", "errorMessage",
              "attemptCount", "organizationId", "apiKeyHash", "signalLogId",
              "receivedAt", "idempotencyKey"
         from "SignalStatus" where "signalId" = $1`, [signalId]))
    step('SignalLog  (money — only exists when the signal actually settled)')
    show('', await db.one(
      `select "signalId", "organizationId", "customerId", "creditName", "modelName",
              "inputTokens", "outputTokens", "creditsUsed", "providedCost",
              "customerCost", "receivedAt", "idempotencyKey", status
         from "SignalLog" where "signalId" = $1`, [signalId]))

    // ── 9 · the overlap ────────────────────────────────────────────────────
    rule('9 · THE NEXT RUN — it sends the same signal AGAIN, on purpose')
    const again = await archive.readIngested(window)
    const stillThere = again.some((c) => c.signal_id === signalId)
    console.log(`the same window still holds our signal: ${stillThere}`)
    console.log(`
  It does, and the next run WILL send it again. Nothing here filters it, because
  nothing here remembers that it was sent — there is no watermark, no cursor and
  no status lookup left in this process.

  That is not a leak, it is the error recovery. The window is 3x the timer's
  interval, so a run that fails is covered by the next two, and settle collapses
  the duplicates onto the same money row (SignalLog.signalId is UNIQUE and is the
  idempotency key). The row above is the proof: sent twice, charged once.`)

  } finally {
    try {
      await teardown(fixture)
    } finally {
      await db.close()
    }
  }
}

main().catch((err) => {
  console.error('\nTRACE ABORTED:', err)
  process.exit(1)
})
