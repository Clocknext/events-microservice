/**
 * End-to-end run for the whole signal pipeline, against real infrastructure:
 *
 *   edge (Fastify) → Kafka → ClickHouse → dispatcher → /api/internal/settle → Postgres
 *
 * Nothing is mocked. Real HTTP into the edge, real Kafka, the real ClickHouse
 * Kafka engine, the real dispatcher sweep, the real settle route, real Postgres
 * rows — and a real org, plan, credit and model borrowed from the database.
 *
 * PREREQUISITES
 *   docker compose up -d                        kafka + clickhouse
 *   KAFKA_BROKERS=localhost:9092 npm run dev    the edge, on :3000
 *   (payments repo) npx next dev -p 3001        the payments app, on :3001
 *
 * Then:  npm run e2e
 *
 * See `scripts/e2e/fixtures.mts` for what it borrows and how it puts it back.
 */
import './e2e/preload.mjs'
import { config } from '../src/config.js'
import { toIso, toSettleSignal } from '../src/workers/dispatch/dispatch.service.js'
import { fetchCursor } from '../src/client/payments-client.js'
import {
  archiveRow, archiveRowCount, check, clickhouse, heading, postSettle, postSignal,
  report, sha256, sleep, waitFor, PAYMENTS,
} from './e2e/harness.mjs'
import {
  db, EXPIRED_KEY, KEY, KEY_HASH, preflight, remember, RUN, setup, statusOf,
  sweepUntilQuiet, teardown, UNKNOWN_KEY, waitForStatus, type Fixture,
} from './e2e/fixtures.mjs'

/** A body the borrowed org / plan / credit / model will actually settle. */
function validBody(fx: Fixture, over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    customerId: fx.customerId,
    inputTokens: 1200,
    outputTokens: 350,
    type: 'credit',
    model: fx.modelId,
    ...(fx.agentKey ? { agentKey: fx.agentKey } : {}),
    ...over,
  }
}

/** Posts a signal and waits for the archive to hold it. Returns the signal id. */
async function ingest(body: unknown, key: string = KEY): Promise<string | null> {
  const res = await postSignal(body, { key })
  if (res.status !== 202) return null
  const signalId = String(res.json.result?.signalId ?? '')
  remember(signalId)
  const row = await waitFor(() => archiveRow(signalId), { timeoutMs: 25_000 })
  return row ? signalId : null
}

const countIn = async (table: 'SignalLog' | 'SignalStatus', ids: string[]) =>
  Number(
    (await db.one<{ n: string }>(
      `select count(*) n from "${table}" where "signalId" = any($1)`, [ids],
    ))!.n,
  )

// ═════════════════════════════════════════════════════════════════════════════

async function edgeRejections(fx: Fixture): Promise<void> {
  heading('2 · the edge refuses bad requests — and refuses them before Kafka')

  const cases: [string, number, string][] = []
  const results: Awaited<ReturnType<typeof postSignal>>[] = []
  const add = async (
    label: string, body: unknown, status: number, reason: string,
    opts?: Parameters<typeof postSignal>[1],
  ) => {
    // A valid key by default, so each case exercises the reason it names rather
    // than tripping the API-key check first. `key: null` opts out explicitly.
    results.push(await postSignal(body, { key: KEY, ...opts }))
    cases.push([label, status, reason])
  }

  await add('no Authorization header', validBody(fx), 401, 'API_KEY_MISSING', { key: null })
  await add('missing customerId', { inputTokens: 1, outputTokens: 1 }, 400, 'INVALID_BODY')
  await add('whitespace-only customerId', validBody(fx, { customerId: '   ' }), 400, 'INVALID_BODY')
  await add('inputTokens as a string (AJV coercion is off)',
    validBody(fx, { inputTokens: '1200' }), 400, 'INVALID_BODY')
  await add('negative tokens', validBody(fx, { outputTokens: -1 }), 400, 'INVALID_BODY')
  await add('fractional tokens', validBody(fx, { inputTokens: 1.5 }), 400, 'INVALID_BODY')
  await add('tokens beyond 2^53-1 (would lose precision as a Postgres double)',
    validBody(fx, { inputTokens: Number.MAX_SAFE_INTEGER + 2 }), 400, 'INVALID_BODY')
  await add('malformed JSON', '{"customerId":', 400, 'MALFORMED_JSON')
  await add('an empty body', '', 400, 'EMPTY_BODY')
  await add('a JSON array instead of an object', '[1,2,3]', 400, 'INVALID_BODY')
  await add('a bare JSON string', '"hello"', 400, 'INVALID_BODY')
  await add('the wrong content-type', validBody(fx), 415, 'UNSUPPORTED_MEDIA_TYPE',
    { contentType: 'text/plain' })
  await add('no content-type at all', validBody(fx), 415, 'UNSUPPORTED_MEDIA_TYPE',
    { contentType: null })
  await add('a body over BODY_BYTES',
    { ...validBody(fx), filler: 'x'.repeat(config.bodyBytes + 1024) }, 413, 'BODY_TOO_LARGE')
  await add('a path no route owns', validBody(fx), 404, 'NOT_FOUND', { path: '/api/v1/nope' })

  cases.forEach(([label, status, reason], i) => {
    const res = results[i]!
    check(label, { status: res.status, reason: res.json.result?.errorReason }, { status, reason })
  })

  // A refused signal must produce NOTHING downstream. The edge is a gate: a
  // rejection is a plain rejection, never a queued 202.
  heading('3 · a refused request reaches neither Kafka nor the archive')
  const before = Number((await db.one<{ n: string }>(`select count(*) n from "SignalStatus"`))!.n)
  await sleep(2_000)
  const sweeps = await sweepUntilQuiet(2)
  const after = Number((await db.one<{ n: string }>(`select count(*) n from "SignalStatus"`))!.n)
  check(`${cases.length} refused requests produced 0 new status rows`, after - before, 0)
  check('and 0 signals were swept', sweeps.reduce((n, s) => n + s.sent, 0), 0)

  const quoted = await postSignal({ inputTokens: 1, outputTokens: 1 }, { key: KEY })
  check('a rejected request still returns a quotable ULID',
    /^[0-9A-HJKMNP-TV-Z]{26}$/.test(String(quoted.json.result?.signalId)), true)
}

async function happyPath(fx: Fixture): Promise<string | null> {
  heading('4 · the happy path: a real signal becomes real money rows')

  const res = await postSignal(
    validBody(fx, { idempotencyKey: `e2e_idem_${RUN}` }), { key: KEY })
  check('the edge answers 202', res.status, 202)
  const signalId = String(res.json.result?.signalId ?? '')
  remember(signalId)
  check('with a ULID', /^[0-9A-HJKMNP-TV-Z]{26}$/.test(signalId), true)

  const row = await waitFor(() => archiveRow(signalId), { timeoutMs: 25_000 })
  check('ClickHouse pulled it off Kafka on its own', row !== null, true)
  if (!row) return null
  check('the archive carries the key DIGEST', row.api_key_hash, KEY_HASH)
  check('and the raw key appears nowhere in the payload', row.payload.includes(KEY), false)
  check('the payload is the body as sent', JSON.parse(row.payload).customerId, fx.customerId)

  const sweeps = await sweepUntilQuiet()
  check('the dispatcher settled it', sweeps.reduce((n, s) => n + s.processed, 0) >= 1, true)

  const status = await waitForStatus(signalId)
  check('a SignalStatus row exists', status !== null, true)
  check('PROCESSED, attributed, no error', {
    status: status?.status, org: status?.organizationId, errorType: status?.errorType,
    errorCode: status?.errorCode, attempt: status?.attemptCount,
  }, { status: 'PROCESSED', org: fx.orgId, errorType: null, errorCode: null, attempt: 1 })
  check('it kept the api key digest', status?.apiKeyHash, KEY_HASH)
  check('it links a money row', typeof status?.signalLogId === 'string', true)

  const log = await db.one<{
    signalId: string; organizationId: string; customerId: string; creditsUsed: string
    receivedAt: Date; idempotencyKey: string | null
  }>(`select "signalId","organizationId","customerId","creditsUsed","receivedAt","idempotencyKey"
        from "SignalLog" where "signalId" = $1`, [signalId])
  check('a SignalLog money row exists for the same signal', log?.signalId, signalId)
  check('billed to the right org and customer',
    { org: log?.organizationId, customer: log?.customerId },
    { org: fx.orgId, customer: fx.customerId })
  check('money actually moved — credits were charged', Number(log?.creditsUsed) > 0, true)
  check('settle keyed idempotency on the signalId, not the caller key',
    log?.idempotencyKey, signalId)

  // The billing window must be the EDGE's stamp. This is exactly the bug `toIso`
  // exists to prevent: ClickHouse renders DateTime64 with a space and no zone,
  // which `new Date()` would read as LOCAL time and bill hours off.
  const edgeStamp = String(res.json.result?.receivedAt)
  check('the billing timestamp is the edge stamp, to the millisecond',
    log?.receivedAt.toISOString(), edgeStamp)
  check('and the archive agrees once converted', toIso(row.received_at), edgeStamp)

  return signalId
}

async function attribution(fx: Fixture): Promise<void> {
  heading('5 · attribution failures are recorded, and never retried forever')

  const unknown = await ingest(validBody(fx), UNKNOWN_KEY)
  const expired = await ingest(validBody(fx), EXPIRED_KEY)
  check('a signal with an unknown key is still accepted and archived', unknown !== null, true)
  check('so is one with an expired key', expired !== null, true)
  if (!unknown || !expired) return

  await sweepUntilQuiet()

  const u = await waitForStatus(unknown)
  check('an unknown key is a USER_ERROR', {
    status: u?.status, type: u?.errorType, code: u?.errorCode, message: u?.errorMessage,
  }, {
    status: 'PENDING', type: 'USER_ERROR', code: 'VALIDATION_FAILED', message: 'Invalid API key.',
  })
  check('unattributed means NULL org, not an empty string', u?.organizationId, null)
  check('but the digest is kept, so the sender stays traceable', u?.apiKeyHash?.length, 64)
  check('and no money row was written',
    await db.one(`select 1 from "SignalLog" where "signalId" = $1`, [unknown]), null)

  const e = await waitForStatus(expired)
  check('an expired key says so specifically', e?.errorMessage, 'This API key has expired.')
  check('and is also a USER_ERROR', e?.errorType, 'USER_ERROR')

  // A USER_ERROR cannot self-heal. If it were offered for retry, a dead key
  // would be re-sent every second forever.
  const cursor = await fetchCursor(500)
  const offered = new Set(cursor.retry.map((r) => r.signalId))
  check('a USER_ERROR is never offered for auto-retry',
    offered.has(unknown) || offered.has(expired), false)
}

async function settleRejections(fx: Fixture): Promise<void> {
  heading('6 · the settle rulebook, reached through the whole pipeline')

  const unknownCustomer = await ingest(validBody(fx, { customerId: `cus_nope_${RUN}` }))
  const unknownModel = await ingest(validBody(fx, { model: `made-up/model-${RUN}` }))
  const noType = await ingest({ customerId: fx.customerId, inputTokens: 5, outputTokens: 5 })
  check('all three passed the gate — the edge does not know the rulebook',
    [unknownCustomer, unknownModel, noType].every((s) => s !== null), true)

  await sweepUntilQuiet()

  if (unknownCustomer) {
    const c = await waitForStatus(unknownCustomer)
    // 404 rather than 403, so a key cannot probe which customer ids exist in
    // other organisations.
    check('an unknown customer is NOT_FOUND / USER_ERROR',
      { code: c?.errorCode, type: c?.errorType },
      { code: 'NOT_FOUND', type: 'USER_ERROR' })
    check('and wrote no money row',
      await db.one(`select 1 from "SignalLog" where "signalId" = $1`, [unknownCustomer]), null)
  }
  if (unknownModel) {
    const m = await waitForStatus(unknownModel)
    check('an unknown model is a USER_ERROR', m?.errorType, 'USER_ERROR')
    console.log(`        model: ${m?.errorCode} — ${String(m?.errorMessage).slice(0, 90)}`)
  }
  if (noType) {
    const t = await waitForStatus(noType)
    check('a missing `type` is a USER_ERROR', t?.errorType, 'USER_ERROR')
  }
}

async function idempotency(settledSignalId: string | null): Promise<void> {
  heading('7 · the property everything rests on: settling twice cannot double-charge')
  if (!settledSignalId) {
    check('SKIPPED — the happy path produced no signal', false, true)
    return
  }

  const money = () => db.one<{ n: string; credits: string }>(
    `select count(*) n, coalesce(sum("creditsUsed"),0) credits
       from "SignalLog" where "signalId" = $1`, [settledSignalId])

  const before = await money()
  const row = await archiveRow(settledSignalId)
  const signal = toSettleSignal(row!, 2)!

  const res = await postSettle({ batchId: `e2e_dup_${RUN}`, signals: [signal] })
  check('settle accepts the duplicate', res.status, 200)
  const body = JSON.parse(res.body) as { result?: { signals?: { status: string }[] } }
  check('and reports it PROCESSED — a replay, not a refusal',
    body.result?.signals?.[0]?.status, 'PROCESSED')

  const after = await money()
  check('still exactly ONE money row', Number(after!.n), 1)
  check('and not one extra credit charged', after!.credits, before!.credits)

  // Ten at once, to rule out a race through the unique index.
  const burst = await Promise.all(Array.from({ length: 10 }, (_, i) =>
    postSettle({ batchId: `e2e_burst_${RUN}_${i}`, signals: [toSettleSignal(row!, 3)!] })))
  check('10 concurrent duplicate settles all answer 200',
    burst.every((r) => r.status === 200), true)
  const afterBurst = await money()
  check('STILL exactly one money row after 10 concurrent replays', Number(afterBurst!.n), 1)
  check('and still exactly the same credits', afterBurst!.credits, before!.credits)
}

async function retries(fx: Fixture): Promise<void> {
  heading('8 · retry policy: SERVER_ERROR comes back, USER_ERROR does not')

  const planted = {
    server: `e2e_${RUN}_server`,
    user: `e2e_${RUN}_user`,
    exhausted: `e2e_${RUN}_exhausted`,
    gone: `e2e_${RUN}_gone_from_archive`,
  }
  const rows: [string, 'SERVER_ERROR' | 'USER_ERROR', number][] = [
    [planted.server, 'SERVER_ERROR', 1],
    [planted.user, 'USER_ERROR', 1],
    [planted.exhausted, 'SERVER_ERROR', 5],
    [planted.gone, 'SERVER_ERROR', 1],
  ]
  for (const [id, type, attempts] of rows) {
    await db.exec(
      `insert into "SignalStatus"
         (id, "signalId", status, "errorType", "errorCode", "errorMessage",
          "attemptCount", "receivedAt", "lastAttemptAt", "updatedAt")
       values ($1, $2, 'PENDING', $3::"SignalErrorType", 'INTERNAL_ERROR', 'planted',
               $4, now() - interval '1 hour', now() - interval '10 minutes', now())`,
      [`e2e_st_${id}`, id, type, attempts],
    )
  }

  const cursor = await fetchCursor(500)
  const offered = new Set(cursor.retry.map((r) => r.signalId))
  check('a SERVER_ERROR is offered for retry', offered.has(planted.server), true)
  check('a USER_ERROR is not', offered.has(planted.user), false)
  check(`one at attempt ${cursor.maxAttempts} is not`, offered.has(planted.exhausted), false)
  check('the offer carries the NEXT attempt number',
    cursor.retry.find((r) => r.signalId === planted.server)?.nextAttempt, 2)

  // A retry the archive no longer holds must not wedge the sweep.
  const swept = await sweepUntilQuiet(2)
  check('a retry with no archive row does not break the sweep',
    swept.every((o) => Number.isFinite(o.sent)), true)

  // And a real one gets its attempt advanced, end to end.
  const real = await ingest(validBody(fx))
  if (!real) {
    check('SKIPPED — could not ingest a signal to retry', false, true)
    return
  }
  await sweepUntilQuiet()
  await db.exec(
    `update "SignalStatus"
        set status = 'PENDING', "errorType" = 'SERVER_ERROR', "errorCode" = 'INTERNAL_ERROR',
            "attemptCount" = 1, "lastAttemptAt" = now() - interval '10 minutes'
      where "signalId" = $1`, [real])
  await sweepUntilQuiet()
  const again = await statusOf(real)
  check('a real retry is re-settled and its attempt advances to 2',
    { status: again?.status, attempt: again?.attemptCount },
    { status: 'PROCESSED', attempt: 2 })
  check('the retry did NOT create a second money row',
    await countIn('SignalLog', [real]), 1)
}

async function spoofing(fx: Fixture): Promise<void> {
  heading('9 · a caller cannot forge the envelope')

  const victim = '01M0AAAAAAAAAAAAAAAAAAAAAA'
  const res = await postSignal(validBody(fx, {
    signalId: victim,
    attempt: 99,
    apiKeyHash: sha256('some-other-key'),
    receivedAt: '1999-01-01T00:00:00.000Z',
    organizationId: 'org_someone_else',
  }), { key: KEY })
  check('the spoofing signal is accepted', res.status, 202)
  const real = String(res.json.result?.signalId ?? '')
  remember(real)
  check('the edge minted its OWN id, ignoring the body', real === victim, false)

  const row = await waitFor(() => archiveRow(real), { timeoutMs: 25_000 })
  check('the archive keyed it on the edge id', row?.signal_id, real)
  check('the archive digest is OUR key, not the forged one', row?.api_key_hash, KEY_HASH)
  check('the forged fields survive only INSIDE the payload',
    JSON.parse(row!.payload).signalId, victim)

  const signal = toSettleSignal(row!, 1)
  check('the dispatcher overrides the forged signalId', signal?.signalId, real)
  check('the forged attempt', signal?.attempt, 1)
  check('the forged receivedAt', signal?.receivedAt, toIso(row!.received_at))
  check('and the forged digest', signal?.apiKeyHash, KEY_HASH)

  await sweepUntilQuiet()
  const status = await waitForStatus(real)
  check('it settled under the real org, not the forged one', status?.organizationId, fx.orgId)
  check('no row exists for the forged id',
    await db.one(`select 1 from "SignalStatus" where "signalId" = $1`, [victim]), null)
}

async function payloadFidelity(fx: Fixture): Promise<void> {
  heading('10 · hostile payloads survive the round trip byte-exact')

  const nasty = {
    unicode: 'héllo wörld — 日本語 🎉🔥',
    whitespace: 'tab\there newline\nreturn\r nbsp​zwsp',
    quotes: 'he said "hi" and \'bye\' and \\backslash\\ and {"nested":"json"}',
    astral: '𝕳𝘊 👨‍👩‍👧‍👦',
    deep: { a: { b: { c: { d: { e: [1, 2, { f: null, g: true }] } } } } },
    emptyish: { zero: 0, empty: '', nul: null, arr: [], obj: {} },
    sqlish: "'; DROP TABLE signal_log; --",
    chInjection: "{id:String}) UNION ALL SELECT 1,2,3,4 FROM system.tables --",
    bigString: 'x'.repeat(4096),
  }
  const signalId = await ingest(validBody(fx, { custom: nasty }))
  check('a signal with a hostile custom blob is accepted and archived', signalId !== null, true)
  if (!signalId) return

  const row = await archiveRow(signalId)
  const parsed = JSON.parse(row!.payload) as { custom: typeof nasty }
  check('the whole blob round-tripped through Kafka and ClickHouse byte-exact',
    JSON.stringify(parsed.custom), JSON.stringify(nasty))
  check('the SQL-ish strings were data, not SQL — the archive still stands',
    (await archiveRowCount(signalId)) >= 1, true)
  const stillThere = await clickhouse.query<{ n: string }>(
    'SELECT count() AS n FROM signal_log')
  check('and signal_log was not dropped', Number(stillThere[0]!.n) > 0, true)

  await sweepUntilQuiet()
  check('and it still settles', (await waitForStatus(signalId))?.status, 'PROCESSED')

  // A NUL byte on its own: Postgres text columns cannot hold one, so if any
  // payload-derived value reached a text column this would surface as a
  // SERVER_ERROR rather than a settled signal.
  const nulId = await ingest(validBody(fx, { custom: { nul: 'before after' } }))
  if (nulId) {
    await sweepUntilQuiet()
    const s = await waitForStatus(nulId)
    check('a NUL byte in the payload does not break settlement',
      s?.status === 'PROCESSED' || s?.errorType === 'USER_ERROR', true)
    console.log(`        NUL byte outcome: ${s?.status} ${s?.errorType ?? ''} ${s?.errorCode ?? ''}`)
  }
}

async function batchLimits(fx: Fixture): Promise<void> {
  heading('11 · the settle route defends its own limits')

  const one = () => ({
    signalId: `e2e_${RUN}_cap_${Math.random().toString(36).slice(2)}`,
    receivedAt: new Date().toISOString(),
    apiKeyHash: KEY_HASH,
    customerId: fx.customerId,
    inputTokens: 1,
    outputTokens: 1,
    type: 'credit',
    ...(fx.agentKey ? { agentKey: fx.agentKey } : {}),
  })

  const over = await postSettle({
    batchId: `e2e_over_${RUN}`, signals: Array.from({ length: 501 }, one),
  })
  check('501 signals is refused, not silently truncated', over.status, 400)
  check('and the message says how many arrived', over.body.includes('501'), true)

  check('an empty batch is refused',
    (await postSettle({ batchId: `e2e_empty_${RUN}`, signals: [] })).status, 400)
  check('a missing batchId is refused', (await postSettle({ signals: [one()] })).status, 400)
  check('a blank batchId is refused',
    (await postSettle({ batchId: '   ', signals: [one()] })).status, 400)

  const junk = await postSettle({
    batchId: `e2e_junk_${RUN}`, signals: [null, 'a string', 42, []],
  })
  check('a batch of non-objects still answers 200', junk.status, 200)
  const parsed = JSON.parse(junk.body) as {
    result?: { signals?: { signal_id: string; error_code: string | null }[] }
  }
  check('with one entry per member', parsed.result?.signals?.length, 4)
  check('each unusable member identified by its position',
    parsed.result?.signals?.every((s) => s.signal_id.startsWith('__invalid_')), true)
  check('and none of them wrote a status row',
    await countIn('SignalStatus', parsed.result!.signals!.map((s) => s.signal_id)), 0)

  const noAuth = await fetch(`${PAYMENTS}/api/internal/settle`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}',
  })
  check('settle without the shared secret is 401', noAuth.status, 401)
  const badAuth = await fetch(`${PAYMENTS}/api/internal/settle`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer wrong-secret' },
    body: '{}',
  })
  check('settle with the wrong shared secret is 401', badAuth.status, 401)
  const cursorNoAuth = await fetch(`${PAYMENTS}/api/internal/signals/cursor`)
  check('the cursor route is guarded too', cursorNoAuth.status, 401)
}

async function volume(fx: Fixture): Promise<void> {
  const N = 120
  heading(`12 · volume: ${N} signals through the whole pipeline`)

  const posted = await Promise.all(Array.from({ length: N }, (_, i) =>
    postSignal(validBody(fx, {
      inputTokens: 100 + i, idempotencyKey: `e2e_vol_${RUN}_${i}`,
    }), { key: KEY })))
  const accepted = posted.filter((r) => r.status === 202)
  check(`all ${N} were accepted`, accepted.length, N)
  const ids = accepted.map((r) => String(r.json.result?.signalId))
  ids.forEach(remember)
  check('every signalId is distinct', new Set(ids).size, N)

  const landed = await waitFor(async () => {
    const rows = await clickhouse.query<{ n: string }>(
      `SELECT count(DISTINCT signal_id) AS n FROM signal_log
        WHERE signal_id IN {ids:Array(String)}`,
      { ids: `['${ids.join("','")}']` })
    return Number(rows[0]!.n) === N ? true : null
  }, { timeoutMs: 60_000 })
  check(`all ${N} reached the archive`, landed, true)

  const sweeps = await sweepUntilQuiet(20)
  check(`all ${N} settled`,
    Number((await db.one<{ n: string }>(
      `select count(*) n from "SignalStatus"
        where "signalId" = any($1) and status = 'PROCESSED'`, [ids]))!.n), N)
  check(`exactly ${N} money rows — none duplicated, none lost`,
    await countIn('SignalLog', ids), N)
  console.log(`        ${sweeps.length} sweeps; batch sizes ${sweeps.map((s) => s.sent).join(' + ')}`)
  check('no sweep exceeded the configured concurrency',
    sweeps.every((s) => s.batchIds.length <= config.dispatchConcurrency), true)
  check('no sweep exceeded the configured batch size',
    sweeps.every((s) => s.sent <= config.dispatchBatchSize * config.dispatchConcurrency), true)
}

async function convergence(): Promise<void> {
  heading('13 · the sweep converges — a quiet pipeline stays quiet')
  const first = await sweepUntilQuiet(4)
  check('a caught-up pipeline sends nothing', first.at(-1)?.sent, 0)
  const again = await sweepUntilQuiet(2)
  check('and stays that way on the next sweep', again[0]?.sent, 0)
  check('reporting no work rather than looping', again[0]?.saturated, false)
}

// ═════════════════════════════════════════════════════════════════════════════

async function main(): Promise<void> {
  let fixture: Fixture | null = null
  await db.connect()
  try {
    await preflight()
    fixture = await setup()
    await edgeRejections(fixture)
    const settled = await happyPath(fixture)
    await attribution(fixture)
    await settleRejections(fixture)
    await idempotency(settled)
    await retries(fixture)
    await spoofing(fixture)
    await payloadFidelity(fixture)
    await batchLimits(fixture)
    await volume(fixture)
    await convergence()
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
  console.error('\nE2E RUN ABORTED:', err)
  process.exit(1)
})
