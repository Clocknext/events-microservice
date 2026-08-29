/** The dispatcher process: ONE run of `runOnce`, then exit.
 *
 *  Run by a systemd timer every 60s (see docs/PRODUCTION.md for the units), or by
 *  hand as `npm run dispatch`. It is NOT part of the Fastify app and imports no
 *  plugin.
 *
 *  IT IS NOT A LOOP, and that is the whole design:
 *
 *    · one ClickHouse query for everything INGESTED in the last DISPATCH_WINDOW_MS
 *    · one gzipped POST to /api/internal/settle carrying all of it
 *    · one log line, then exit
 *
 *  Nothing is persisted between runs. The window is wider than the timer's
 *  interval, so a failed run is covered by the next two, and settle's idempotency
 *  on `signalId` makes the resulting duplicates free. That is why there is no
 *  watermark to advance, no backoff to schedule (the next tick IS the retry) and
 *  no state that a crash could leave inconsistent.
 *
 *  EXIT CODES — the timer, and whatever watches it, read these:
 *    0  sent, or nothing to send
 *    1  transient: ClickHouse down, settle 5xx, timeout. The next tick retries.
 *    2  misconfigured: bad secret, unparseable replay window, body refused.
 *       Waiting cannot fix it; every tick will fail identically until a human acts. */
import { randomUUID } from 'node:crypto'
import { config } from '../../config.js'
import { createClickHouseReader, type ClickHouseReader } from '../../client/clickhouse.js'
import { MisconfiguredError, PayloadTooLargeError, settleAll } from '../../client/payments-client.js'
import { createArchiveReader } from './dispatch.archive.js'
import { runOnce, type RunDeps } from './dispatch.service.js'

function line(event: string, detail: Record<string, unknown>): void {
  console.log(JSON.stringify({ at: new Date().toISOString(), event, ...detail }))
}

/** Vercel refuses a serverless function's request body above this. Warned on
 *  well before it, because crossing it is not a slow degradation — it is a hard
 *  413 for the whole window at once. */
const BODY_LIMIT_BYTES = 4.5 * 1024 * 1024
const BODY_WARN_BYTES = BODY_LIMIT_BYTES * 0.78

export function buildDeps(clickhouse: ClickHouseReader = createClickHouseReader()): RunDeps {
  return {
    archive: createArchiveReader(clickhouse),
    payments: { settle: settleAll },
    config: {
      windowMs: config.dispatchWindowMs,
      since: config.dispatchSince || undefined,
      until: config.dispatchUntil || undefined,
      maxRows: config.dispatchMaxRows,
    },
    newBatchId: randomUUID,
    log: line,
  }
}

/** Raised for a replay window that cannot be honoured. Caught as exit code 2:
 *  a typo in DISPATCH_SINCE must not read a window nobody asked for. */
export class BadReplayWindowError extends Error {}

function validateReplayWindow(): void {
  const { dispatchSince: since, dispatchUntil: until } = config
  if (!since) {
    if (until) {
      throw new BadReplayWindowError('DISPATCH_UNTIL is set without DISPATCH_SINCE')
    }
    return
  }
  const from = new Date(since).getTime()
  if (Number.isNaN(from)) {
    throw new BadReplayWindowError(`DISPATCH_SINCE is not a valid ISO-8601 instant: "${since}"`)
  }
  if (!until) return
  const to = new Date(until).getTime()
  if (Number.isNaN(to)) {
    throw new BadReplayWindowError(`DISPATCH_UNTIL is not a valid ISO-8601 instant: "${until}"`)
  }
  if (to <= from) {
    throw new BadReplayWindowError(
      `DISPATCH_UNTIL (${until}) is not after DISPATCH_SINCE (${since})`,
    )
  }
}

export async function run(): Promise<void> {
  if (!config.internalSecret) {
    // Refuse rather than 401: every tick would fail identically and the log
    // would say nothing about why.
    throw new MisconfiguredError(
      'INTERNAL_SETTLE_SECRET is not set — the dispatcher cannot authenticate',
    )
  }
  validateReplayWindow()

  const replay = config.dispatchSince
    ? { since: config.dispatchSince, until: config.dispatchUntil || 'now' }
    : undefined
  line('dispatch.start', {
    clickhouse: config.clickhouseUrl,
    payments: config.paymentsUrl,
    windowMs: config.dispatchWindowMs,
    maxRows: config.dispatchMaxRows,
    gzip: config.dispatchGzip,
    ...(replay && { replay }),
  })

  // A one-shot has nothing to drain, so a signal just says so and goes. The
  // settle call in flight may still commit upstream, which is safe — it is
  // idempotent on signalId, and the next window would re-send it anyway.
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => {
      line('dispatch.interrupted', { signal })
      process.exit(1)
    })
  }

  const outcome = await runOnce(buildDeps())
  line('dispatch.run', { ...outcome })

  if (outcome.gzipBytes > BODY_WARN_BYTES) {
    // Not an error yet. But the next window that is 20% busier is a 413 for
    // every signal in it at once, so this wants attention before then.
    line('dispatch.body_near_limit', {
      gzipBytes: outcome.gzipBytes,
      limitBytes: BODY_LIMIT_BYTES,
      sent: outcome.sent,
      hint: 'lower DISPATCH_WINDOW_MS, or the window has outgrown a single call',
    })
  }
}

// Only run when this file is the entry point, so a test can import `run`.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  run()
    .then(() => process.exit(0))
    .catch((err: unknown) => {
      const fatal = err instanceof MisconfiguredError
        || err instanceof BadReplayWindowError
        || err instanceof PayloadTooLargeError
      line('dispatch.failed', {
        error: err instanceof Error ? err.message : String(err),
        fatal,
      })
      process.exit(fatal ? 2 : 1)
    })
}
