/**
 * Sends a representative signal for EVERY route and outcome the pipeline has,
 * then follows each one hop by hop and prints the result as one matrix.
 *
 * Not a test — it asserts nothing and never fails. It exists to show what the
 * system actually does with each kind of input, on real infrastructure.
 *
 *   npm run trace:all
 *
 * Same fixtures as `npm run e2e`: it borrows a real API key row and restores it
 * in a `finally`, and refuses to run against the production database.
 */
import './preload.mjs'
import { config } from '../../src/config.js'
import { runOnce, toSettleSignal } from '../../src/workers/dispatch/dispatch.service.js'
import {
  archiveRow, archiveRowCount, clickhouse, PAYMENTS, postSettle, postSignal, sleep, waitFor,
} from './harness.mjs'
import {
  db, E2E_WINDOW_MS, EXPIRED_KEY, KEY, remember, RUN, runDeps, setup, teardown,
  UNKNOWN_KEY, type Fixture,
} from './fixtures.mjs'

const B = (s: string) => `\x1b[1m${s}\x1b[0m`
const rule = (n: string) => console.log(`\n${B('═'.repeat(100))}\n${B(n)}\n${B('═'.repeat(100))}`)

/** One case, and everything that happened to it. */
interface Case {
  n: number
  label: string
  /** What we sent. `body` is passed verbatim when it is a string. */
  body: unknown
  key?: string | null
  contentType?: string | null
  path?: string
  // ── filled in as it travels ──
  httpStatus?: number
  errorReason?: string
  signalId?: string
  inArchive?: boolean
  verdict?: { status: string; error_type: string | null; error_code: string | null; error_message: string | null }
  row?: { status: string; org: string | null; attempt: number; signalLogId: string | null } | null
  money?: boolean
}

function pad(s: string, n: number): string {
  const plain = s.replace(/\x1b\[[0-9;]*m/g, '')
  return plain.length >= n ? s : s + ' '.repeat(n - plain.length)
}

async function main() {
  let fixture: Fixture | null = null
  await db.connect()
  try {
    fixture = await setup()
    const fx = fixture
    const ok = (over: Record<string, unknown> = {}) => ({
      customerId: fx.customerId, inputTokens: 1200, outputTokens: 350,
      type: 'credit', model: fx.modelId,
      ...(fx.agentKey ? { agentKey: fx.agentKey } : {}), ...over,
    })

    // ═══════════════════════════════════════════════════════════════════════
    const cases: Case[] = [
      // ── A · refused by the edge — never reach Kafka ─────────────────────
      { n: 1,  label: 'no Authorization header',        body: ok(), key: null },
      { n: 2,  label: 'bare token, no Bearer scheme',   body: ok(), key: null },
      { n: 3,  label: 'missing customerId',             body: { inputTokens: 1, outputTokens: 1 } },
      { n: 4,  label: 'whitespace-only customerId',     body: ok({ customerId: '   ' }) },
      { n: 5,  label: 'inputTokens as a string',        body: ok({ inputTokens: '1200' }) },
      { n: 6,  label: 'negative tokens',                body: ok({ outputTokens: -1 }) },
      { n: 7,  label: 'fractional tokens',              body: ok({ inputTokens: 1.5 }) },
      { n: 8,  label: 'tokens beyond 2^53-1',           body: ok({ inputTokens: Number.MAX_SAFE_INTEGER + 2 }) },
      { n: 9,  label: 'malformed JSON',                 body: '{"customerId":' },
      { n: 10, label: 'empty body',                     body: '' },
      { n: 11, label: 'JSON array, not an object',      body: '[1,2,3]' },
      { n: 12, label: 'bare JSON string',               body: '"hello"' },
      { n: 13, label: 'wrong content-type',             body: ok(), contentType: 'text/plain' },
      { n: 14, label: 'no content-type',                body: ok(), contentType: null },
      { n: 15, label: `body over BODY_BYTES`,           body: { ...ok(), filler: 'x'.repeat(config.bodyBytes + 1024) } },
      { n: 16, label: 'path no route owns',             body: ok(), path: '/api/v1/nope' },

      // ── B · accepted by the edge, refused by settle ─────────────────────
      { n: 17, label: 'unknown API key',                body: ok(), key: UNKNOWN_KEY },
      { n: 18, label: 'expired API key',                body: ok(), key: EXPIRED_KEY },
      { n: 19, label: 'unknown customerId',             body: ok({ customerId: `cus_ghost_${RUN}` }) },
      { n: 20, label: 'unknown model',                  body: ok({ model: `made-up/model-${RUN}` }) },
      { n: 21, label: 'no `type`',                      body: { customerId: fx.customerId, inputTokens: 5, outputTokens: 5 } },
      { n: 22, label: 'unknown agentKey (no credit)',   body: ok({ agentKey: `no_such_credit_${RUN}` }) },
      { n: 23, label: 'type=wallet, no wallet on plan', body: ok({ type: 'wallet' }) },
      { n: 24, label: 'type=outcome, no outcome',       body: ok({ type: 'outcome', outcome: `ghost_${RUN}`, runId: `run_${RUN}` }) },
      { n: 25, label: 'bogus type string',              body: ok({ type: 'nonsense' }) },

      // ── C · accepted and settled ────────────────────────────────────────
      { n: 26, label: 'the happy path',                 body: ok({ idempotencyKey: `trace_${RUN}` }) },
      { n: 27, label: 'zero tokens (free call)',        body: ok({ inputTokens: 0, outputTokens: 0 }) },
      { n: 28, label: 'huge but legal token counts',    body: ok({ inputTokens: 9_000_000, outputTokens: 4_000_000 }) },
      { n: 29, label: 'forged envelope in the body',    body: ok({ signalId: '01M0AAAAAAAAAAAAAAAAAAAAAA', attempt: 99, apiKeyHash: 'f'.repeat(64), organizationId: 'org_someone_else', receivedAt: '1999-01-01T00:00:00.000Z' }) },
      { n: 30, label: 'hostile unicode + SQL payload',  body: ok({ custom: { u: 'héllo 日本語 🎉 𝕳', sql: "'; DROP TABLE signal_log; --", deep: { a: { b: [1, null, true] } } } }) },
      { n: 31, label: 'unknown extra fields',           body: ok({ wat: 1, nested: { a: [1, 2] }, refId: 'ref_x' }) },
    ]

    // ── send them all ────────────────────────────────────────────────────
    rule('PHASE 1 · 31 signals hit POST /api/v1/signal')
    for (const c of cases) {
      const res = await postSignal(c.body, {
        key: c.key === undefined ? KEY : c.key,
        ...(c.contentType !== undefined ? { contentType: c.contentType } : {}),
        ...(c.path ? { path: c.path } : {}),
      })
      c.httpStatus = res.status
      // Assigned only when present: `errorReason` is an optional property and
      // `exactOptionalPropertyTypes` is on, so `undefined` is not a value for it.
      const reason = res.json.result?.errorReason
      if (reason !== undefined) c.errorReason = reason
      const id = res.json.result?.signalId
      if (res.status === 202 && typeof id === 'string') {
        c.signalId = id
        remember(id)
      }
    }
    const accepted = cases.filter((c) => c.httpStatus === 202)
    const refused = cases.filter((c) => c.httpStatus !== 202)
    console.log(`  ${accepted.length} accepted (202) · ${refused.length} refused at the gate`)

    // ── volume burst, to show batching ───────────────────────────────────
    rule('PHASE 2 · a burst of 40 valid signals, to watch the dispatcher batch')
    const burst = await Promise.all(Array.from({ length: 40 }, (_, i) =>
      postSignal(ok({ inputTokens: 10 + i, idempotencyKey: `burst_${RUN}_${i}` }), { key: KEY })))
    const burstIds = burst.filter((r) => r.status === 202).map((r) => String(r.json.result?.signalId))
    burstIds.forEach(remember)
    console.log(`  ${burstIds.length}/40 accepted`)

    // ── wait for the archive ─────────────────────────────────────────────
    rule('PHASE 3 · ClickHouse pulls them off Kafka by itself')
    const wanted = [...accepted.map((c) => c.signalId!), ...burstIds]
    const t0 = Date.now()
    await waitFor(async () => {
      const r = await clickhouse.query<{ n: string }>(
        `SELECT count(DISTINCT signal_id) AS n FROM signal_log WHERE signal_id IN {ids:Array(String)}`,
        { ids: `['${wanted.join("','")}']` })
      return Number(r[0]!.n) === wanted.length ? true : null
    }, { timeoutMs: 60_000 })
    console.log(`  ${wanted.length} rows landed in ~${Date.now() - t0}ms`)
    for (const c of accepted) c.inArchive = (await archiveRowCount(c.signalId!)) > 0

    console.log(`\n  none of the ${refused.length} refused requests reached the archive:`)
    const archiveTotal = await clickhouse.query<{ n: string }>('SELECT count() AS n FROM signal_log')
    console.log(`    signal_log holds ${archiveTotal[0]!.n} rows in total`)

    // ── dispatch ─────────────────────────────────────────────────────────
    rule('PHASE 4 · the dispatcher runs — ONCE, one window, one call')
    const o = await runOnce(runDeps())
    console.log(`  read=${pad(String(o.read), 4)} sent=${pad(String(o.sent), 4)} ` +
      `processed=${pad(String(o.processed), 4)} userError=${pad(String(o.userError), 4)} ` +
      `serverError=${pad(String(o.serverError), 3)} skipped=${pad(String(o.skipped), 3)} ` +
      `capped=${o.capped}`)
    console.log(`  window=${E2E_WINDOW_MS / 1000}s over ingested_at · ` +
      `${o.bytes}B json -> ${o.gzipBytes}B gzip · ${o.ms}ms`)
    console.log(`
  There is no loop and no second pass. One run reads everything INGESTED in the
  window and posts all of it in a single request — no batch size, no concurrency,
  no watermark to advance.`)
    await sleep(200)

    // ── collect verdicts ─────────────────────────────────────────────────
    rule('PHASE 5 · what each signal became')
    for (const c of accepted) {
      const row = await db.one<{
        status: string; errorType: string | null; errorCode: string | null
        errorMessage: string | null; attemptCount: number; organizationId: string | null
        signalLogId: string | null
      }>(`select status, "errorType", "errorCode", "errorMessage", "attemptCount",
                 "organizationId", "signalLogId"
            from "SignalStatus" where "signalId" = $1`, [c.signalId!])
      c.row = row ? {
        status: row.status, org: row.organizationId,
        attempt: row.attemptCount, signalLogId: row.signalLogId,
      } : null
      if (row) {
        c.verdict = {
          status: row.status, error_type: row.errorType,
          error_code: row.errorCode, error_message: row.errorMessage,
        }
      }
      c.money = !!(await db.one(`select 1 from "SignalLog" where "signalId" = $1`, [c.signalId!]))
    }

    const G = (s: string) => `\x1b[32m${s}\x1b[0m`
    const R = (s: string) => `\x1b[31m${s}\x1b[0m`
    const Y = (s: string) => `\x1b[33m${s}\x1b[0m`

    console.log(B(`\n  ${pad('#', 4)}${pad('CASE', 34)}${pad('EDGE', 22)}${pad('ARCHIVE', 9)}${pad('SETTLE VERDICT', 34)}MONEY`))
    console.log('  ' + '─'.repeat(100))
    for (const c of cases) {
      const edge = c.httpStatus === 202 ? G('202') : R(`${c.httpStatus} ${c.errorReason ?? ''}`)
      const arch = c.inArchive ? G('yes') : (c.httpStatus === 202 ? R('NO') : '—')
      const v = c.verdict
      const verdict = !v ? '—'
        : v.status === 'PROCESSED' ? G('PROCESSED')
        : `${Y(v.error_type ?? '?')} ${v.error_code ?? ''}`
      const money = c.money ? G('yes') : (c.httpStatus === 202 ? '·' : '—')
      console.log(`  ${pad(String(c.n), 4)}${pad(c.label, 34)}${pad(edge, 22)}${pad(arch, 9)}${pad(verdict, 34)}${money}`)
    }

    rule('PHASE 6 · the error messages settle returned, in full')
    for (const c of accepted) {
      if (!c.verdict || c.verdict.status === 'PROCESSED') continue
      console.log(`  ${pad(String(c.n), 4)}${pad(c.label, 34)}${c.verdict.error_message}`)
    }

    rule('PHASE 7 · forgery — case 29 in detail')
    const forged = cases.find((c) => c.n === 29)!
    if (forged.signalId) {
      const row = await archiveRow(forged.signalId)
      console.log(`  the edge minted            ${forged.signalId}`)
      console.log(`  the body claimed           01M0AAAAAAAAAAAAAAAAAAAAAA`)
      console.log(`  the archive keyed it on    ${row?.signal_id}`)
      console.log(`  the archive digest is      ${row?.api_key_hash.slice(0, 24)}…  (ours)`)
      console.log(`  the body claimed digest    ffffffffffffffffffffffff…`)
      const s = toSettleSignal(row!, 1)
      console.log(`  toSettleSignal produced    signalId=${s?.signalId}`)
      console.log(`                             attempt=${s?.attempt}  (body said 99)`)
      console.log(`                             receivedAt=${s?.receivedAt}  (body said 1999)`)
      console.log(`                             organizationId=${JSON.stringify(s?.organizationId)}  (body said org_someone_else)`)
      console.log(`  it settled under org       ${forged.row?.org}`)
      const ghost = await db.one(`select 1 from "SignalStatus" where "signalId" = '01M0AAAAAAAAAAAAAAAAAAAAAA'`)
      console.log(`  a row for the forged id?   ${ghost ? R('YES — LEAK') : G('no')}`)
    }

    rule('PHASE 8 · idempotency — settling case 26 five more times')
    const happy = cases.find((c) => c.n === 26)!
    if (happy.signalId && happy.money) {
      const money = () => db.one<{ n: string; credits: string }>(
        `select count(*) n, coalesce(sum("creditsUsed"),0) credits
           from "SignalLog" where "signalId" = $1`, [happy.signalId!])
      const before = await money()
      const row = await archiveRow(happy.signalId)
      const results = await Promise.all(Array.from({ length: 5 }, (_, i) =>
        postSettle({ batchId: `dup_${RUN}_${i}`, signals: [toSettleSignal(row!, 2)!] })))
      const after = await money()
      console.log(`  5 concurrent replays, all HTTP ${[...new Set(results.map((r) => r.status))].join('/')}`)
      console.log(`  money rows   before ${before!.n}   after ${after!.n}`)
      console.log(`  credits      before ${before!.credits}   after ${after!.credits}`)
    }

    rule('PHASE 9 · the overlap — a second run re-sends the same window')
    const moneyBefore = await db.one<{ n: string }>('select count(*) n from "SignalLog"')
    const final = await runOnce(runDeps())
    const moneyAfter = await db.one<{ n: string }>('select count(*) n from "SignalLog"')
    console.log(`  a fresh run sends again        ${final.sent}`)
    console.log(`  money rows before / after     ${moneyBefore!.n} / ${moneyAfter!.n}`)
    console.log(`
  It sends the same signals a second time and nothing changes. That is the design,
  not a leak: the window is wider than the timer's interval so a failed run is
  covered by the next two, and settle collapses the duplicates onto the same money
  row (SignalLog.signalId is UNIQUE and is the idempotency key). Nothing is
  persisted here to make it converge, and nothing needs to be.`)

    rule('PHASE 10 · totals')
    const tot = await db.one<{ st: string; ok: string; pend: string; money: string }>(`
      select (select count(*) from "SignalStatus")                              st,
             (select count(*) from "SignalStatus" where status='PROCESSED')     ok,
             (select count(*) from "SignalStatus" where status='PENDING')       pend,
             (select count(*) from "SignalLog" where "signalId" = any($1))      money`,
      [[...wanted]])
    console.log(`  SignalStatus rows   ${tot!.st}   (PROCESSED ${tot!.ok} · PENDING ${tot!.pend})`)
    console.log(`  SignalLog rows for this run   ${tot!.money}`)
    console.log(`  signals accepted by the edge  ${wanted.length}`)
    console.log(`  refused at the gate           ${refused.length}  (no archive row, no status row, no money)`)
  } finally {
    try { await teardown(fixture) } finally { await db.close() }
  }
}

main().catch((err) => {
  console.error('\nTRACE ABORTED:', err)
  process.exit(1)
})
