/**
 * End-to-end run for the whole signal pipeline, against real infrastructure:
 *
 *   edge (Fastify) → Kafka → ClickHouse → dispatcher → /api/internal/settle → Postgres
 *
 * Nothing is mocked. Real HTTP into the edge, real Kafka, the real consumer
 * draining it, the real dispatcher run, the real settle route, real Postgres
 * rows — and a real org, plan, credit and model borrowed from the database.
 *
 * PREREQUISITES
 *   docker compose up -d                        kafka + clickhouse
 *   KAFKA_BROKERS=localhost:9092 npm run dev    the edge, on :3000
 *   (payments repo) npx next dev -p 3001        the payments app, on :3001
 *   npm run consume                             THE CONSUMER — long-lived
 *
 * The consumer is a PREREQUISITE, not an optional extra, and it is the one most
 * easily forgotten: ClickHouse no longer ingests from Kafka itself, so with the
 * consumer down nothing reaches `signal_log` and every wait on an archive row
 * times out. A signal sitting on the topic is not a failure — it is a process
 * that was never started.
 *
 * Then:  npm run e2e
 *
 * See `scripts/e2e/fixtures.mts` for what it borrows and how it puts it back.
 */
import './e2e/preload.mjs'
import { config } from '../src/config.js'
import { toIso, toSettleSignal } from '../src/workers/dispatch/dispatch.service.js'
import {
  archiveRow, archiveRowCount, check, clickhouse, heading, postSettle, postSignal,
  report, sha256, sleep, waitFor, PAYMENTS,
} from './e2e/harness.mjs'
import {
  db, dispatch, EXPIRED_KEY, KEY, KEY_HASH, preflight, remember, RUN, setup,
  statusOf, teardown, UNKNOWN_KEY, waitForStatus, type Fixture,
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
  await dispatch()
  const after = Number((await db.one<{ n: string }>(`select count(*) n from "SignalStatus"`))!.n)
  check(`${cases.length} refused requests produced 0 new status rows`, after - before, 0)
  // There is deliberately no assertion on `outcome.sent` here. It counts
  // everything in the window, re-sent on every run by design, so it says nothing
  // about whether these requests produced work. Postgres is the evidence.

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

  const run = await dispatch()
  check('the dispatcher settled it', run.processed >= 1, true)

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

  await dispatch()

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

  // A USER_ERROR cannot self-heal, and the window WILL send it again — that is
  // now unavoidable, since nothing here tracks what was already sent. So the rule
  // has to hold on the settle side: re-receiving a USER_ERROR must be a no-op, or
  // a dead key would be re-priced every minute forever.
  const before = { unknown: (await statusOf(unknown))?.attemptCount, expired: (await statusOf(expired))?.attemptCount }
  await dispatch()
  check('re-sending a USER_ERROR does not advance its attempt count',
    { unknown: (await statusOf(unknown))?.attemptCount, expired: (await statusOf(expired))?.attemptCount },
    before)
  check('and it stays a USER_ERROR rather than being retried into something else',
    (await statusOf(unknown))?.errorType, 'USER_ERROR')
}

async function settleRejections(fx: Fixture): Promise<void> {
  heading('6 · the settle rulebook, reached through the whole pipeline')

  const unknownCustomer = await ingest(validBody(fx, { customerId: `cus_nope_${RUN}` }))
  const unknownModel = await ingest(validBody(fx, { model: `made-up/model-${RUN}` }))
  const noType = await ingest({ customerId: fx.customerId, inputTokens: 5, outputTokens: 5 })
  check('all three passed the gate — the edge does not know the rulebook',
    [unknownCustomer, unknownModel, noType].every((s) => s !== null), true)

  await dispatch()

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

  // THIS SECTION CHANGED SHAPE WITH THE 1.0 DISPATCH MODEL.
  //
  // There is no retry list any more, and nothing on this side plants or reads a
  // status row: the dispatcher re-sends EVERY signal in its window on every run
  // and settle decides what to do with each one. So retry policy is no longer
  // something this repo can be tested for in isolation — it is a property of the
  // settle route, reachable only end to end. Which is what this does.
  //
  // The planted-status-row cases the old version used are gone with the cursor
  // they fed: a row that exists only in Postgres is never sent now, because the
  // dispatcher reads ClickHouse and nothing else.

  const real = await ingest(validBody(fx))
  if (!real) {
    check('SKIPPED — could not ingest a signal to retry', false, true)
    return
  }
  await dispatch()
  const settled = await waitForStatus(real)
  check('it settles once, at attempt 1',
    { status: settled?.status, attempt: settled?.attemptCount },
    { status: 'PROCESSED', attempt: 1 })

  // Force it back to a retryable failure, the way a real settle-side outage
  // would leave it.
  await db.exec(
    `update "SignalStatus"
        set status = 'PENDING', "errorType" = 'SERVER_ERROR', "errorCode" = 'INTERNAL_ERROR',
            "attemptCount" = 1, "lastAttemptAt" = now() - interval '10 minutes'
      where "signalId" = $1`, [real])

  // The next run's window still covers it, so it goes again with no prompting —
  // this is the whole retry mechanism now.
  await dispatch()
  const again = await statusOf(real)
  check('an overlapping window re-sends a SERVER_ERROR and it is re-settled',
    again?.status, 'PROCESSED')
  check('the retry did NOT create a second money row',
    await countIn('SignalLog', [real]), 1)

  // And the counterpart rule, which is what stops the same overlap re-pricing
  // everything forever: a signal that is already PROCESSED is a no-op on
  // re-receipt.
  const processedAttempt = (await statusOf(real))?.attemptCount
  await dispatch()
  check('re-sending a PROCESSED signal changes nothing',
    { attempt: (await statusOf(real))?.attemptCount, money: await countIn('SignalLog', [real]) },
    { attempt: processedAttempt, money: 1 })
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

  await dispatch()
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

  await dispatch()
  check('and it still settles', (await waitForStatus(signalId))?.status, 'PROCESSED')

  // A NUL byte on its own: Postgres text columns cannot hold one, so if any
  // payload-derived value reached a text column this would surface as a
  // SERVER_ERROR rather than a settled signal.
  const nulId = await ingest(validBody(fx, { custom: { nul: 'before after' } }))
  if (nulId) {
    await dispatch()
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

  const run = await dispatch()
  check(`all ${N} settled`,
    Number((await db.one<{ n: string }>(
      `select count(*) n from "SignalStatus"
        where "signalId" = any($1) and status = 'PROCESSED'`, [ids]))!.n), N)
  check(`exactly ${N} money rows — none duplicated, none lost`,
    await countIn('SignalLog', ids), N)
  // ONE run, ONE call. This is the 1.0 claim: the window goes in a single request
  // whatever its size, so there is no batch count to assert and no concurrency to
  // cap.
  check(`all ${N} went in a single dispatch run`, run.sent >= N, true)
  console.log(
    `        1 run · sent ${run.sent} · ${run.bytes}B json -> ${run.gzipBytes}B gzip ` +
    `(${(run.bytes / Math.max(run.gzipBytes, 1)).toFixed(1)}:1) · ${run.ms}ms`)
  check('the body stayed well inside the 4.5MB request-body ceiling',
    run.gzipBytes < 4.5 * 1024 * 1024, true)
}

async function convergence(): Promise<void> {
  heading('13 · the overlap is free — re-sending the window changes nothing')

  // The 1.0 design INVERTS the old invariant. A caught-up pipeline no longer
  // sends nothing: the window is wider than the interval on purpose, so every run
  // re-sends the same signals and that overlap is the entire error recovery.
  //
  // What has to hold instead is that the re-send is inert. If it were not, a
  // caught-up pipeline would re-price its whole window every minute.
  const moneyBefore = Number((await db.one<{ n: string }>(
    `select count(*) n from "SignalLog"`))!.n)
  const statusBefore = Number((await db.one<{ n: string }>(
    `select count(*) n from "SignalStatus"`))!.n)

  const first = await dispatch()
  const second = await dispatch()
  check('a caught-up window still sends its signals again, twice over',
    first.sent > 0 && second.sent === first.sent, true)

  check('and not one extra money row was written',
    Number((await db.one<{ n: string }>(`select count(*) n from "SignalLog"`))!.n),
    moneyBefore)
  check('nor one extra status row',
    Number((await db.one<{ n: string }>(`select count(*) n from "SignalStatus"`))!.n),
    statusBefore)
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
