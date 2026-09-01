/** The dispatcher's logic. A pure function over its two ports — no HTTP client,
 *  no ClickHouse driver.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  ONE RUN, ONE WINDOW, ONE CALL
 *
 *  A systemd timer runs this every 60s. It reads every row INGESTED in the last
 *  `DISPATCH_WINDOW_MS`, turns those rows into settle signals, and posts all of
 *  them in a single request. Then it exits.
 *
 *  There is no watermark, no cursor, no claim, no lease and no local state,
 *  because `/api/internal/settle` is idempotent on `signalId`: `SignalLog.signalId`
 *  is UNIQUE and settle uses it as the idempotency key, so a signal sent twice
 *  replays onto the original money row instead of charging again.
 *
 *  So the run never asks what it already sent. The window is deliberately WIDER
 *  than the timer's interval — 3x by default — and every signal is therefore sent
 *  about three times, with settle discarding all but the first. That overlap is
 *  not waste: it is the ENTIRE error recovery. A run that fails leaves nothing
 *  behind to resume from, and is simply covered by the next two.
 *
 *  What the overlap cannot cover is three consecutive failures. That is what the
 *  hourly reconciliation timer is for — the same binary with a 2-hour window.
 *  ───────────────────────────────────────────────────────────────────────────── */
import type {
  ArchiveReader,
  RunOutcome,
  SettleClient,
  SettleSignal,
  SignalLogRow,
} from './dispatch.schema.js'

export interface RunConfig {
  /** How far back the window reaches, over `ingested_at`. */
  windowMs: number
  /** Explicit `[since, until)` for a manual replay. Empty in normal operation. */
  since?: string | undefined
  until?: string | undefined
  /** Row ceiling. An OOM guard, not a batch size. */
  maxRows: number
}

export interface RunDeps {
  archive: ArchiveReader
  payments: SettleClient
  config: RunConfig
  /** Injected so a test is not at the mercy of a real UUID. */
  newBatchId: () => string
  log?: (event: string, detail: Record<string, unknown>) => void
  /** Injected so a test can assert on `ms` without a real clock. */
  now?: () => number
}

/**
 * ClickHouse renders `DateTime64(3)` as `2026-08-26 10:00:00.000` — a space
 * instead of a `T`, and no zone. It IS UTC. Settle validates `receivedAt` with
 * `new Date(...)`, which parses that string as LOCAL time, so a dispatcher in
 * IST would bill every signal 5½ hours early. Restoring the `T` and the `Z` is
 * what keeps the billing window the one the edge stamped.
 */
export function toIso(clickhouseTimestamp: string): string {
  const trimmed = clickhouseTimestamp.trim()
  if (trimmed === '') return trimmed
  // Already ISO (a fake in a test, or a future ClickHouse format change).
  if (trimmed.includes('T') && /(Z|[+-]\d{2}:?\d{2})$/.test(trimmed)) return trimmed
  return `${trimmed.replace(' ', 'T')}Z`
}

/**
 * Turns an archive row into the signal settle wants.
 *
 * The payload is spread FIRST and the envelope written over it, so a caller who
 * happened to send `"signalId"`, `"attempt"`, `"receivedAt"`, `"apiKeyHash"`,
 * `"organizationId"` or `"status"` in their own body cannot impersonate what the
 * pipeline stamped. Returns null when the payload will not parse, which for a row
 * the edge accepted should be impossible — but a row that cannot be turned into a
 * signal must be skipped loudly, not sent half-built.
 */
export function toSettleSignal(row: SignalLogRow, attempt = 1): SettleSignal | null {
  let payload: Record<string, unknown>
  try {
    const parsed = JSON.parse(row.payload) as unknown
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null
    payload = parsed as Record<string, unknown>
  } catch {
    return null
  }
  return {
    ...payload,
    signalId: row.signal_id,
    receivedAt: toIso(row.received_at),
    apiKeyHash: row.api_key_hash,
    // WRITTEN now, not deleted — and the reversal is deliberate, so do not
    // "restore" the old `delete payload.organizationId` (BUG-1 in
    // docs/FINDINGS.md).
    //
    // The archive is a verbatim copy of a request body, so a payload-supplied
    // `organizationId` is caller-controlled and once let a caller bill another
    // tenant. That defence is UNCHANGED: the caller's value is still discarded,
    // because the spread above is overwritten by this line. What changed is that
    // there is now a TRUSTED value to overwrite it with — the consumer resolved
    // it against /api/internal/resolve before the row was ever archived. Deleting
    // it today would throw away the attribution settle depends on and force settle
    // to re-resolve every signal, which is the work the consumer exists to remove.
    organizationId: row.organization_id,
    // An INSTRUCTION to settle, not a description: PROCESSING means price it,
    // PENDING means record the failure below and price nothing.
    status: row.status,
    // Empty string is the ClickHouse column's "absent" — the columns are not
    // Nullable — and null is what settle's schema wants.
    errorCode: row.error_code === '' ? null : row.error_code,
    errorMessage: row.error_message === '' ? null : row.error_message,
    // Always 1: this process keeps no state, so it cannot know which delivery
    // this is. `SignalStatus` counts attempts, on the side that can see them.
    attempt,
  }
}

/**
 * One run: read the window, send it, count what came back.
 *
 * Throws whatever the ports throw. That is deliberate — the runner turns it into
 * a non-zero exit and the next window re-sends the same signals, so there is
 * nothing to absorb here and swallowing it would hide a total failure behind a
 * successful-looking run.
 */
export async function runOnce(deps: RunDeps): Promise<RunOutcome> {
  const { archive, payments, config, newBatchId } = deps
  const log = deps.log ?? (() => {})
  const now = deps.now ?? Date.now
  const startedAt = now()
  const batchId = newBatchId()

  const rows = await archive.readIngested({
    windowMs: config.windowMs,
    since: config.since,
    until: config.until,
    cap: config.maxRows,
  })

  // Hitting the cap MEANS ROWS WERE LEFT UNSENT, and the next window will have
  // moved past some of them — this is a data-loss alarm, not a tuning hint. The
  // hourly reconciliation covers up to two hours of it; past that it needs
  // DISPATCH_SINCE and a human.
  const capped = rows.length >= config.maxRows
  if (capped) {
    log('run.capped', {
      read: rows.length,
      cap: config.maxRows,
      windowMs: config.windowMs,
    })
  }

  const empty: RunOutcome = {
    read: rows.length, sent: 0, skipped: 0,
    processed: 0, pending: 0, userError: 0, serverError: 0,
    capped, bytes: 0, gzipBytes: 0, ms: now() - startedAt, batchId,
  }
  if (rows.length === 0) return empty

  const signals: SettleSignal[] = []
  let skipped = 0
  for (const row of rows) {
    const signal = toSettleSignal(row)
    if (!signal) {
      skipped += 1
      log('row.unusable', { signalId: row.signal_id })
      continue
    }
    signals.push(signal)
  }
  if (signals.length === 0) return { ...empty, skipped, ms: now() - startedAt }

  const { results, bytes, gzipBytes, fireAndForget } = await payments.settle(batchId, signals)

  // Counted from what the ARCHIVE said, not from what settle answered: a signal
  // the consumer already rejected was never priced, so counting it as a settle
  // outcome would read as though settle had refused it.
  let pending = 0
  for (const signal of signals) if (signal.status === 'PENDING') pending += 1

  // Empty under fire-and-forget, because the reply was never read. The three
  // counters below stay 0 and the outcome carries `fireAndForget` so the log line
  // says WHY they are 0 — a run that priced nothing looks the same otherwise.
  let processed = 0
  let userError = 0
  let serverError = 0
  for (const result of results) {
    if (result.status === 'PROCESSED') processed += 1
    else if (result.error_type === 'USER_ERROR') userError += 1
    else serverError += 1
  }

  return {
    read: rows.length,
    sent: signals.length,
    skipped,
    processed,
    pending,
    userError,
    serverError,
    capped,
    bytes,
    gzipBytes,
    ms: now() - startedAt,
    batchId,
    ...(fireAndForget ? { fireAndForget } : {}),
  }
}
