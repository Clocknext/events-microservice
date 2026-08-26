/** The dispatcher process: a loop around `sweepOnce`.
 *
 *  Run it as `npm run dispatch` (or `node dist/workers/dispatch/dispatch.runner.js`)
 *  under a supervisor, alongside the edge. It is NOT part of the Fastify app and
 *  imports no plugin.
 *
 *  THE PACE IS SELF-ADJUSTING, which is why this is a loop and not a cron:
 *
 *    full batch  -> go again immediately   (there is a backlog; drain it flat out)
 *    short/empty -> nap `DISPATCH_IDLE_MS` (caught up; idle cheaply)
 *
 *  So the loop runs at settle's maximum rate whenever there is work and costs
 *  almost nothing when there is not. A fixed schedule can do neither: it adds
 *  latency when idle and cannot catch up when behind. */
import { randomUUID } from 'node:crypto'
import { config } from '../../config.js'
import { createClickHouseReader, type ClickHouseReader } from '../../client/clickhouse.js'
import {
  fetchCursor,
  fetchKnown,
  MisconfiguredError,
  settleBatch,
} from '../../client/payments-client.js'
import { createArchiveReader } from './dispatch.archive.js'
import { sweepOnce, type SweepDeps } from './dispatch.service.js'
import type { SweepOutcome } from './dispatch.schema.js'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function line(event: string, detail: Record<string, unknown>): void {
  console.log(JSON.stringify({ at: new Date().toISOString(), event, ...detail }))
}

export function buildDeps(clickhouse: ClickHouseReader = createClickHouseReader()): SweepDeps {
  return {
    archive: createArchiveReader(clickhouse),
    payments: { cursor: fetchCursor, known: fetchKnown, settle: settleBatch },
    config: {
      batchSize: config.dispatchBatchSize,
      concurrency: config.dispatchConcurrency,
      overlapMs: config.dispatchOverlapMs,
    },
    newBatchId: randomUUID,
    log: line,
  }
}

/** Backoff for an error the sweep itself could not absorb (ClickHouse down, the
 *  cursor route unreachable). Capped so a long outage does not turn into an
 *  hour-long silence once it clears. */
const BACKOFF_MS = [1_000, 2_000, 5_000, 10_000, 30_000]

/** Raised when starting would backfill an archive nobody asked to backfill. */
export class ColdStartBackfillError extends Error {}

/**
 * Refuses to start when there is no watermark AND the archive is already large.
 *
 * With `SignalStatus` empty, every unsettled row is outstanding — so the first
 * run sweeps the entire archive. Wanted when adopting the dispatcher is meant to
 * backfill; a nasty surprise when the archive predates it. Neither is knowable
 * from in here, so it stops and asks instead of quietly doing the expensive one.
 *
 * The check is skipped entirely once anything has settled, so it costs one query
 * on the first sweep of a fresh deployment and nothing thereafter.
 */
async function guardColdStart(clickhouse: ClickHouseReader): Promise<void> {
  const limit = config.dispatchColdStartMax
  if (limit <= 0) return

  const { sentThrough } = await fetchCursor(0)
  if (sentThrough !== null) return

  const rows = await clickhouse.query<{ n: string }>(
    'SELECT count() AS n FROM signal_log',
  )
  const archived = Number(rows[0]?.n ?? 0)
  if (archived <= limit) return

  line('dispatcher.cold_start_refused', { archived, limit })
  throw new ColdStartBackfillError(
    `nothing has been settled yet and the archive holds ${archived} signals ` +
      `(DISPATCH_COLD_START_MAX=${limit}). Starting now would send all of them ` +
      `to settle. Either:\n` +
      `  · start from now  — insert one PROCESSED SignalStatus row dated now, ` +
      `then start again (see docs/ARCHITECTURE.md "Starting the dispatcher for ` +
      `the first time"); or\n` +
      `  · backfill on purpose — DISPATCH_COLD_START_MAX=0 npm run dispatch; or\n` +
      `  · start clean — npm run up`,
  )
}

export async function run(): Promise<void> {
  if (!config.internalSecret) {
    // Refuse to start rather than 401 in a loop: every sweep would fail
    // identically and the log would say nothing about why.
    throw new Error('INTERNAL_SETTLE_SECRET is not set — the dispatcher cannot authenticate')
  }
  const clickhouse = createClickHouseReader()
  const deps = buildDeps(clickhouse)
  line('dispatcher.start', {
    clickhouse: config.clickhouseUrl,
    payments: config.paymentsUrl,
    batchSize: config.dispatchBatchSize,
    concurrency: config.dispatchConcurrency,
    overlapMs: config.dispatchOverlapMs,
    coldStartMax: config.dispatchColdStartMax,
  })

  await guardColdStart(clickhouse)

  let stopping = false
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => {
      // Finish the sweep in flight rather than abandoning a settle call whose
      // outcome we would then never record.
      line('dispatcher.stopping', { signal })
      stopping = true
    })
  }

  let failures = 0
  while (!stopping) {
    let outcome: SweepOutcome
    try {
      outcome = await sweepOnce(deps)
      failures = 0
    } catch (err) {
      if (err instanceof MisconfiguredError || err instanceof ColdStartBackfillError) {
        // Waiting cannot fix this, and looping on it would bury the reason.
        line('dispatcher.misconfigured', { error: err.message })
        throw err
      }
      const wait = BACKOFF_MS[Math.min(failures, BACKOFF_MS.length - 1)]!
      failures += 1
      line('sweep.error', {
        error: err instanceof Error ? err.message : String(err),
        failures,
        retryInMs: wait,
      })
      await sleep(wait)
      continue
    }

    if (outcome.sent > 0 || outcome.skipped > 0) line('sweep.done', { ...outcome })
    // A full batch means there is probably more behind it.
    if (!outcome.saturated) await sleep(config.dispatchIdleMs)
  }
  line('dispatcher.stopped', {})
}

// Only run when this file is the entry point, so a test can import `run`.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    line('dispatcher.fatal', { error: err instanceof Error ? err.message : String(err) })
    process.exit(1)
  })
}
