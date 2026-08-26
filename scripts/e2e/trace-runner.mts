/**
 * Exercises the DISPATCHER LOOP as a real process — not `sweepOnce` called by
 * hand, which is what every other script here does.
 *
 * Spawns `dispatch.runner.ts` exactly as a supervisor would, feeds the edge while
 * it runs, and reads its own log to see: the self-pacing nap, a saturated sweep
 * going straight round again, the error backoff when ClickHouse disappears, and
 * a graceful SIGTERM that finishes the sweep in flight.
 *
 *   npm run trace:runner
 */
import './preload.mjs'
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { db, KEY, remember, RUN, setup, teardown, type Fixture } from './fixtures.mjs'
import { postSignal, sleep } from './harness.mjs'

const run = promisify(execFile)
const B = (s: string) => `\x1b[1m${s}\x1b[0m`
const rule = (n: string) => console.log(`\n${B('═'.repeat(88))}\n${B(n)}\n${B('═'.repeat(88))}`)

interface LogLine { at: string; event: string; [k: string]: unknown }

/** Spawns the runner and collects its JSON log lines as they arrive. */
function startDispatcher(env: Record<string, string>): {
  child: ChildProcessWithoutNullStreams
  lines: LogLine[]
  raw: string[]
} {
  const lines: LogLine[] = []
  const raw: string[] = []
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
  return { child, lines, raw }
}

const waitForEvent = async (lines: LogLine[], event: string, timeoutMs = 40_000) => {
  const until = Date.now() + timeoutMs
  while (Date.now() < until) {
    const hit = lines.find((l) => l.event === event)
    if (hit) return hit
    await sleep(200)
  }
  return null
}

async function main() {
  let fixture: Fixture | null = null
  let dispatcher: ReturnType<typeof startDispatcher> | null = null
  await db.connect()
  try {
    fixture = await setup()
    const fx = fixture
    const body = (over: Record<string, unknown> = {}) => ({
      customerId: fx.customerId, inputTokens: 1200, outputTokens: 350,
      type: 'credit', model: fx.modelId,
      ...(fx.agentKey ? { agentKey: fx.agentKey } : {}), ...over,
    })

    // ── 1 · start it, as a supervisor would ────────────────────────────────
    rule('1 · spawn `tsx src/workers/dispatch/dispatch.runner.ts`')
    dispatcher = startDispatcher({
      DISPATCH_OVERLAP_MS: '2000',
      DISPATCH_IDLE_MS: '1000',
      DISPATCH_BATCH_SIZE: '20',      // small, so a burst saturates and we can see it
      DISPATCH_CONCURRENCY: '2',
    })
    const started = await waitForEvent(dispatcher.lines, 'dispatcher.start')
    console.log(`  pid ${dispatcher.child.pid}`)
    console.log(`  ${JSON.stringify(started)}`)

    // ── 2 · feed it while it runs ──────────────────────────────────────────
    rule('2 · post 50 signals at the edge and let the loop find them')
    const posted = await Promise.all(Array.from({ length: 50 }, (_, i) =>
      postSignal(body({ inputTokens: 100 + i, idempotencyKey: `runner_${RUN}_${i}` }), { key: KEY })))
    const ids = posted.filter((r) => r.status === 202).map((r) => String(r.json.result?.signalId))
    ids.forEach(remember)
    console.log(`  ${ids.length}/50 accepted at the edge — nothing else was told`)

    // Wait until the loop has settled them all, purely by watching Postgres.
    const until = Date.now() + 90_000
    let settled = 0
    while (Date.now() < until) {
      settled = Number((await db.one<{ n: string }>(
        `select count(*) n from "SignalStatus" where "signalId" = any($1) and status = 'PROCESSED'`,
        [ids]))!.n)
      if (settled === ids.length) break
      await sleep(500)
    }
    console.log(`  the loop settled ${settled}/${ids.length} without being asked`)

    rule('3 · what the loop logged')
    for (const l of dispatcher.lines.filter((x) => x.event === 'sweep.done')) {
      console.log(`  sent=${String(l.sent).padEnd(4)} processed=${String(l.processed).padEnd(4)} ` +
        `userError=${String(l.userError).padEnd(3)} alreadyKnown=${String(l.alreadyKnown).padEnd(4)} ` +
        `batches=${(l.batchIds as string[]).length} saturated=${l.saturated}`)
    }
    const sweeps = dispatcher.lines.filter((x) => x.event === 'sweep.done')
    console.log(`\n  ${sweeps.length} sweep(s) reported work.`)
    console.log('  A sweep with saturated=true went straight round with no nap;')
    console.log('  the loop is silent while idle — sweep.done is only logged when it did something.')

    // ── 4 · the backoff, by taking ClickHouse away ─────────────────────────
    rule('4 · stop ClickHouse — the loop should back off, not die')
    const before = dispatcher.lines.length
    await run('docker', ['compose', 'stop', 'clickhouse'])
    console.log('  clickhouse stopped')
    const errored = await waitForEvent(dispatcher.lines.slice(before), 'sweep.error', 30_000)
      ?? await waitForEvent(dispatcher.lines, 'sweep.error', 5_000)
    const errors = dispatcher.lines.filter((l) => l.event === 'sweep.error')
    for (const e of errors.slice(0, 5)) {
      console.log(`  failures=${e.failures} retryInMs=${e.retryInMs}  ${String(e.error).slice(0, 70)}`)
    }
    console.log(`\n  alive after the outage? ${dispatcher.child.exitCode === null ? 'yes' : `NO (exit ${dispatcher.child.exitCode})`}`)
    console.log(`  backoff climbed as designed: ${errors.map((e) => e.retryInMs).join(' → ')}`)

    rule('5 · bring ClickHouse back — the loop should recover on its own')
    await run('docker', ['compose', 'start', 'clickhouse'])
    for (let i = 0; i < 60; i += 1) {
      const { stdout } = await run('docker', ['inspect', '-f', '{{.State.Health.Status}}', 'clickhouse'])
      if (stdout.trim() === 'healthy') break
      await sleep(1_000)
    }
    console.log('  clickhouse healthy again')
    const errCountBefore = dispatcher.lines.filter((l) => l.event === 'sweep.error').length
    const one = await postSignal(body({ idempotencyKey: `recover_${RUN}` }), { key: KEY })
    const recoveredId = String(one.json.result?.signalId ?? '')
    remember(recoveredId)
    const recoveredUntil = Date.now() + 60_000
    let recovered = false
    while (Date.now() < recoveredUntil) {
      const row = await db.one(`select 1 from "SignalStatus" where "signalId" = $1`, [recoveredId])
      if (row) { recovered = true; break }
      await sleep(500)
    }
    const errCountAfter = dispatcher.lines.filter((l) => l.event === 'sweep.error').length
    console.log(`  a signal posted after recovery was settled by the loop: ${recovered ? 'yes' : 'NO'}`)
    console.log(`  further errors while recovering: ${errCountAfter - errCountBefore}`)

    // ── 6 · graceful stop ──────────────────────────────────────────────────
    rule('6 · SIGTERM — it should finish the sweep in flight, then exit 0')
    const exited = new Promise<number | null>((resolve) =>
      dispatcher!.child.once('exit', (code) => resolve(code)))
    dispatcher.child.kill('SIGTERM')
    const code = await Promise.race([exited, sleep(20_000).then(() => 'timeout' as const)])
    const stopping = dispatcher.lines.find((l) => l.event === 'dispatcher.stopping')
    const stopped = dispatcher.lines.find((l) => l.event === 'dispatcher.stopped')
    console.log(`  logged dispatcher.stopping : ${stopping ? `yes (${stopping.signal})` : 'NO'}`)
    console.log(`  logged dispatcher.stopped  : ${stopped ? 'yes' : 'NO'}`)
    console.log(`  exit code                  : ${code}`)
    dispatcher = null

    // ── 7 · refuses to start without the secret ────────────────────────────
    rule('7 · no INTERNAL_SETTLE_SECRET — it must refuse to start, not 401 in a loop')
    const bad = startDispatcher({ INTERNAL_SETTLE_SECRET: '' })
    const badExit = await new Promise<number | null>((resolve) =>
      bad.child.once('exit', (c) => resolve(c)))
    const fatal = bad.lines.find((l) => l.event === 'dispatcher.fatal')
    console.log(`  exit code ${badExit}`)
    console.log(`  ${fatal ? String(fatal.error) : bad.raw.slice(-2).join(' ')}`)

    rule('8 · totals')
    const tot = await db.one<{ ok: string; money: string }>(`
      select (select count(*) from "SignalStatus" where "signalId" = any($1) and status='PROCESSED') ok,
             (select count(*) from "SignalLog"    where "signalId" = any($1))                        money`,
      [[...ids, recoveredId]])
    console.log(`  settled by the LOOP (not by a hand-driven sweep): ${tot!.ok}`)
    console.log(`  money rows                                      : ${tot!.money}`)
  } finally {
    if (dispatcher && dispatcher.child.exitCode === null) dispatcher.child.kill('SIGKILL')
    try { await run('docker', ['compose', 'start', 'clickhouse']) } catch { /* already up */ }
    try { await teardown(fixture) } finally { await db.close() }
  }
}

main().catch((err) => {
  console.error('\nRUNNER TRACE ABORTED:', err)
  process.exit(1)
})
