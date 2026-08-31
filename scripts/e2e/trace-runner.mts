/**
 * Exercises the DISPATCHER AS A REAL PROCESS — not `runOnce` called by hand,
 * which is what every other script here does.
 *
 * It is a ONE-SHOT now, so what there is to see is different from the old loop:
 * no self-pacing nap, no backoff, no graceful drain. What matters instead is that
 * a run does the whole job and then exits, and that its EXIT CODE tells a timer
 * the truth — because with no state between runs, the exit code is the only thing
 * that distinguishes "nothing to do" from "this will never work".
 *
 *   0  sent, or nothing to send
 *   1  transient — the next tick retries
 *   2  misconfigured — every tick will fail identically until a human acts
 *
 *   npm run trace:runner
 */
import './preload.mjs'
import { spawn } from 'node:child_process'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { db, KEY, remember, RUN, setup, teardown, type Fixture } from './fixtures.mjs'
import { postSignal, sleep } from './harness.mjs'

const run = promisify(execFile)
const B = (s: string) => `\x1b[1m${s}\x1b[0m`
const rule = (n: string) => console.log(`\n${B('═'.repeat(88))}\n${B(n)}\n${B('═'.repeat(88))}`)

interface LogLine { at: string; event: string; [k: string]: unknown }
interface Run { code: number | null; lines: LogLine[]; raw: string[]; ms: number }

/** Runs the dispatcher once, to completion, and returns everything it said. */
function dispatchOnce(env: Record<string, string> = {}): Promise<Run> {
  const lines: LogLine[] = []
  const raw: string[] = []
  const startedAt = Date.now()
  const child = spawn('npx', ['tsx', 'src/workers/dispatch/dispatch.runner.ts'], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const consume = (chunk: Buffer) => {
    for (const l of chunk.toString().split('\n')) {
      if (!l.trim()) continue
      raw.push(l)
      try { lines.push(JSON.parse(l) as LogLine) } catch { /* tsx noise */ }
    }
  }
  child.stdout.on('data', consume)
  child.stderr.on('data', consume)
  return new Promise((resolve) => {
    child.once('exit', (code) => resolve({ code, lines, raw, ms: Date.now() - startedAt }))
  })
}

const show = (r: Run) => {
  for (const l of r.lines) {
    const { at: _at, event, ...rest } = l
    console.log(`  ${String(event).padEnd(26)} ${JSON.stringify(rest)}`)
  }
  console.log(`  ${B('exit')} ${r.code}   after ${r.ms}ms`)
}

async function main() {
  let fixture: Fixture | null = null
  await db.connect()
  try {
    fixture = await setup()
    const fx = fixture
    const body = (over: Record<string, unknown> = {}) => ({
      customerId: fx.customerId, inputTokens: 1200, outputTokens: 350,
      type: 'credit', model: fx.modelId,
      ...(fx.agentKey ? { agentKey: fx.agentKey } : {}), ...over,
    })

    // ── 1 · an idle run ────────────────────────────────────────────────────
    rule('1 · an IDLE run — one query, nothing to send, exit 0')
    const idle = await dispatchOnce({ DISPATCH_WINDOW_MS: '2000' })
    show(idle)
    console.log(`
  A 2s window with nothing in it. This is what 1,439 of the day's 1,440 runs cost
  when traffic is quiet: one indexed query and a process start.`)

    // ── 2 · a real run ─────────────────────────────────────────────────────
    rule('2 · post 50 signals, then ONE run to settle all of them')
    const posted = await Promise.all(Array.from({ length: 50 }, (_, i) =>
      postSignal(body({ inputTokens: 100 + i, idempotencyKey: `runner_${RUN}_${i}` }), { key: KEY })))
    const ids = posted.filter((r) => r.status === 202).map((r) => String(r.json.result?.signalId))
    ids.forEach(remember)
    console.log(`  ${ids.length}/50 accepted at the edge — nothing else was told`)

    // Wait for the CONSUMER to resolve and archive them; the run reads the
    // archive, so anything not yet ingested simply is not in the window.
    const until = Date.now() + 60_000
    while (Date.now() < until) {
      const n = Number((await db.one<{ n: string }>(
        `select count(*) n from "SignalStatus" where "signalId" = any($1)`, [ids]))!.n)
      if (n > 0) break
      const r = await dispatchOnce({ DISPATCH_WINDOW_MS: '120000' })
      if (r.lines.some((l) => l.event === 'dispatch.run' && Number(l.sent) > 0)) { show(r); break }
      await sleep(2_000)
    }
    const settled = Number((await db.one<{ n: string }>(
      `select count(*) n from "SignalStatus" where "signalId" = any($1) and status = 'PROCESSED'`,
      [ids]))!.n)
    console.log(`\n  settled ${settled}/${ids.length} — in a single call, no batching`)

    // ── 3 · the overlap ───────────────────────────────────────────────────
    rule('3 · run it AGAIN — it re-sends the same window, and nothing changes')
    const moneyBefore = await db.one<{ n: string }>('select count(*) n from "SignalLog"')
    const second = await dispatchOnce({ DISPATCH_WINDOW_MS: '120000' })
    show(second)
    const moneyAfter = await db.one<{ n: string }>('select count(*) n from "SignalLog"')
    console.log(`  money rows before / after: ${moneyBefore!.n} / ${moneyAfter!.n}`)
    console.log(`
  It sent them all a second time. That is the design: the window is wider than the
  timer's interval so a failed run is covered by the next two, and settle collapses
  the duplicates onto the same money row. Nothing is persisted to make this
  converge, and nothing needs to be.`)

    // ── 4 · transient failure ──────────────────────────────────────────────
    rule('4 · stop ClickHouse — the run must exit 1, not hang and not exit 0')
    await run('docker', ['compose', 'stop', 'clickhouse'])
    console.log('  clickhouse stopped')
    const broken = await dispatchOnce({ DISPATCH_WINDOW_MS: '120000' })
    show(broken)
    console.log(`
  Exit 1 with fatal=false. There is no backoff in here any more — the TIMER is the
  backoff, and the next tick simply tries again 60s later. A one-shot that cannot
  read the archive has nothing to wait for.`)

    rule('5 · bring it back — the very next run recovers, with no state to repair')
    await run('docker', ['compose', 'start', 'clickhouse'])
    for (let i = 0; i < 60; i += 1) {
      const { stdout } = await run('docker', ['inspect', '-f', '{{.State.Health.Status}}', 'clickhouse'])
      if (stdout.trim() === 'healthy') break
      await sleep(1_000)
    }
    console.log('  clickhouse healthy again')
    const recovered = await dispatchOnce({ DISPATCH_WINDOW_MS: '120000' })
    show(recovered)

    // ── 6 · the two fatal cases ────────────────────────────────────────────
    rule('6 · no INTERNAL_SETTLE_SECRET — exit 2, refusing to 401 every minute')
    show(await dispatchOnce({ INTERNAL_SETTLE_SECRET: '' }))

    rule('7 · a malformed replay window — exit 2, not a window nobody asked for')
    show(await dispatchOnce({ DISPATCH_SINCE: 'last tuesday' }))
    console.log(`
  A typo in DISPATCH_SINCE must never be interpreted. Silently reading the wrong
  window is how a manual replay turns into a re-price of the whole archive.`)

    // ── 8 · the replay tool ────────────────────────────────────────────────
    rule('8 · a deliberate replay — DISPATCH_SINCE/UNTIL, the gap-filling tool')
    const since = new Date(Date.now() - 10 * 60_000).toISOString()
    const replay = await dispatchOnce({ DISPATCH_SINCE: since, DISPATCH_UNTIL: new Date().toISOString() })
    show(replay)
    console.log(`
  This is how a gap left by an outage longer than the reconciliation window gets
  filled: an explicit [since, until) over ingested_at. No code change, and no
  cursor to rewind — because there is no cursor.`)

    rule('9 · totals')
    const tot = await db.one<{ ok: string; money: string }>(`
      select (select count(*) from "SignalStatus" where "signalId" = any($1) and status='PROCESSED') ok,
             (select count(*) from "SignalLog"    where "signalId" = any($1))                        money`,
      [ids])
    console.log(`  settled by the runner process: ${tot!.ok}`)
    console.log(`  money rows                   : ${tot!.money}  (never more than one per signal)`)
  } finally {
    try { await run('docker', ['compose', 'start', 'clickhouse']) } catch { /* already up */ }
    try { await teardown(fixture) } finally { await db.close() }
  }
}

main().catch((err) => {
  console.error('\nRUNNER TRACE ABORTED:', err)
  process.exit(1)
})
