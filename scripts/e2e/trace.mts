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
import { config } from '../../src/config.js'
import { toIso, toSettleSignal } from '../../src/workers/dispatch/dispatch.service.js'
import { overlapFrom } from '../../src/workers/dispatch/dispatch.service.js'
import { fetchCursor, fetchKnown } from '../../src/client/payments-client.js'
import { archiveRow, clickhouse, env, PAYMENTS, postSignal, waitFor } from './harness.mjs'
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
    rule('2 · CLICKHOUSE — the Kafka engine pulls it in by itself')
    const t0 = Date.now()
    const row = await waitFor(() => archiveRow(signalId), { timeoutMs: 30_000 })
    console.log(`landed after ~${Date.now() - t0}ms (nothing pushed it — ENGINE=Kafka consumes)`)
    show('\nsignal_log ROW', row)
    console.log(`\nreceived_at is UTC but says so nowhere:  ${row!.received_at}`)
    console.log(`toIso() restores the T and the Z:        ${toIso(row!.received_at)}`)
    console.log(`the edge stamped:                        ${res.json.result?.receivedAt}`)

    // ── 3 · the cursor ─────────────────────────────────────────────────────
    rule('3 · DISPATCHER ① — GET /api/internal/signals/cursor')
    console.log(`GET ${PAYMENTS}/api/internal/signals/cursor?retryLimit=${config.dispatchBatchSize}`)
    const cursor = await fetchCursor(config.dispatchBatchSize)
    show('\nRESPONSE result', cursor)
    const since = overlapFrom(cursor.sentThrough, 2_000)
    console.log(`\nsentThrough                       ${cursor.sentThrough}`)
    console.log(`minus the overlap window (2s here) ${since}`)
    console.log('  → that lower bound is what the next query reads from.')

    // ── 4 · the archive read ───────────────────────────────────────────────
    rule('4 · DISPATCHER ② — the SELECT it runs against signal_log')
    const sql = `SELECT signal_id, received_at, api_key_hash, payload
   FROM signal_log
  WHERE received_at > parseDateTime64BestEffort({since:String})
  ORDER BY received_at ASC
  LIMIT 1 BY signal_id
  LIMIT {limit:UInt32}`
    console.log(sql)
    console.log(`\nbound server-side:  since = ${since}   limit = ${config.dispatchBatchSize * config.dispatchConcurrency}`)
    const candidates = await clickhouse.query<{ signal_id: string; received_at: string }>(sql, {
      since: since ?? '1970-01-01T00:00:00.000Z',
      limit: config.dispatchBatchSize * config.dispatchConcurrency,
    })
    console.log(`\n→ ${candidates.length} candidate row(s)`)
    for (const c of candidates.slice(0, 5)) console.log(`   ${c.signal_id}  ${c.received_at}`)
    console.log(`
  LIMIT 1 BY signal_id collapses the duplicates at-least-once Kafka delivery
  leaves in a ReplacingMergeTree before a merge runs. Cheaper than FINAL, and it
  is what stops one physical duplicate being settled twice.`)

    // ── 5 · the known filter ───────────────────────────────────────────────
    rule('5 · DISPATCHER ③ — POST /api/internal/signals/known')
    const ids = candidates.map((c) => c.signal_id)
    show('REQUEST', { signalIds: ids.slice(0, 5).concat(ids.length > 5 ? ['…'] : []) })
    const known = await fetchKnown(ids)
    show('\nRESPONSE result', { known })
    console.log(`\n${ids.length} candidates − ${known.length} already recorded = ${ids.length - known.length} to send`)
    console.log(`
  Without this the overlap window would re-send every signal inside it on every
  sweep — correct (settle dedups) but the pipeline would never go quiet.`)

    // ── 6 · the settle call ────────────────────────────────────────────────
    rule('6 · DISPATCHER ④ — POST /api/internal/settle')
    const settleSignal = toSettleSignal(row!, 1)
    console.log('toSettleSignal() spreads the payload, then writes the envelope over it,')
    console.log('and DELETES organizationId — the payload is caller-controlled.\n')
    const payload = { batchId: `trace_${RUN}`, signals: [settleSignal] }
    show('REQUEST BODY', payload)

    const settleRes = await fetch(`${PAYMENTS}/api/internal/settle`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${env.internalSecret}` },
      body: JSON.stringify(payload),
    })
    const settleBody = JSON.parse(await settleRes.text()) as Record<string, unknown>
    rule('7 · WHAT /api/internal/settle RETURNS  (verbatim, nothing elided)')
    console.log(`HTTP ${settleRes.status}`)
    show('', settleBody)

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

    // ── 9 · convergence ────────────────────────────────────────────────────
    rule('9 · THE NEXT SWEEP — the pipeline goes quiet')
    const cursor2 = await fetchCursor(config.dispatchBatchSize)
    console.log(`cursor.sentThrough is now  ${cursor2.sentThrough}`)
    const known2 = await fetchKnown([signalId])
    console.log(`known([${signalId}])  →  ${JSON.stringify(known2)}`)
    console.log('\n  The signal is inside the overlap window and WILL be re-read, but it is')
    console.log('  now "known", so it is dropped before settle. That is convergence.')
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
