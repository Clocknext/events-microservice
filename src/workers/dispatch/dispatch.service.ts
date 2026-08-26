/** The dispatcher's logic. A pure function over its two ports — no HTTP client,
 *  no ClickHouse driver, no clock of its own beyond `Date.now()`.
 *
 *  ─────────────────────────────────────────────────────────────────────────────
 *  WHY THERE IS NO CLAIM, NO LEASE, NO CURSOR TABLE
 *
 *  `/api/internal/settle` is idempotent on `signalId`: `SignalLog.signalId` is
 *  UNIQUE and settle passes it as the idempotency key, so a signal sent twice
 *  replays onto the original money row instead of charging the customer again.
 *
 *  That single fact removes the machinery a queue would need. The dispatcher only
 *  has to guarantee AT LEAST ONCE. Over-sending costs a cheap replay, so there is
 *  nothing to lock, nothing to lease, and no visibility timeout to get wrong —
 *  and two dispatchers running at once are safe rather than a corruption bug.
 *
 *  WHAT IS STILL OUTSTANDING is answered by Postgres (`SignalStatus`), not by
 *  ClickHouse, which has no row updates and therefore records no outcome. The
 *  sweep is a watermark for new work plus an explicit list of retries, so one
 *  ancient failure cannot drag the scan back across the whole archive.
 *  ───────────────────────────────────────────────────────────────────────────── */
import type {
  ArchiveReader,
  SettleClient,
  SettleSignal,
  SignalLogRow,
  SweepOutcome,
} from './dispatch.schema.js'

export interface SweepConfig {
  batchSize: number
  concurrency: number
  /** How far below the watermark to re-read, in ms. */
  overlapMs: number
}

export interface SweepDeps {
  archive: ArchiveReader
  payments: SettleClient
  config: SweepConfig
  /** Injected so a test is not at the mercy of a real clock or a real UUID. */
  newBatchId: () => string
  log?: (event: string, detail: Record<string, unknown>) => void
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

/** Shifts a watermark back by the overlap, so a row that lands with a timestamp
 *  just below one already seen is still picked up. */
export function overlapFrom(sentThrough: string | null, overlapMs: number): string | null {
  if (!sentThrough) return null
  const at = new Date(sentThrough).getTime()
  if (Number.isNaN(at)) return null
  return new Date(at - overlapMs).toISOString()
}

/**
 * Turns an archive row into the signal settle wants.
 *
 * The payload is spread FIRST and the envelope written over it, so a caller who
 * happened to send `"signalId"`, `"attempt"`, `"receivedAt"` or `"apiKeyHash"` in
 * their own body cannot impersonate the edge's stamp. `organizationId` is
 * stripped outright — see the note at the `delete`. Returns null when the payload
 * will not parse, which for a row the edge accepted should be impossible — but a
 * row that cannot be turned into a signal must be skipped loudly, not sent
 * half-built.
 */
export function toSettleSignal(row: SignalLogRow, attempt: number): SettleSignal | null {
  let payload: Record<string, unknown>
  try {
    const parsed = JSON.parse(row.payload) as unknown
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null
    payload = parsed as Record<string, unknown>
  } catch {
    return null
  }
  // The caller's `organizationId` is REMOVED, not overwritten. The archive is a
  // verbatim copy of a request body, so this field is caller-controlled; settle
  // resolves the real organisation from `apiKeyHash`, and forwarding a
  // body-supplied one would let a caller name a tenant they do not own. Deleting
  // it means settle sees no claim at all rather than one it has to distrust.
  delete payload.organizationId
  return {
    ...payload,
    signalId: row.signal_id,
    receivedAt: toIso(row.received_at),
    apiKeyHash: row.api_key_hash,
    attempt,
  }
}

/** Ceiling on how many pages one sweep will walk looking for unsettled rows.
 *  Bounds the work when the overlap window is far wider than the batch; the
 *  sweep simply resumes next time rather than scanning without limit. */
const MAX_PAGES = 20

/** Splits into chunks of at most `size`. */
function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

/** Runs the thunks with at most `limit` in flight, preserving result order. */
async function pool<T>(thunks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results = new Array<T>(thunks.length)
  let next = 0
  const runners = Array.from({ length: Math.min(limit, thunks.length) }, async () => {
    while (true) {
      const index = next
      next += 1
      const thunk = thunks[index]
      if (!thunk) return
      results[index] = await thunk()
    }
  })
  await Promise.all(runners)
  return results
}

/**
 * One sweep: ask what is left, read those rows out of the archive, settle them.
 *
 * Returns what it did so the runner can decide whether to loop again straight
 * away (a full batch means there is probably more) or nap.
 */
export async function sweepOnce(deps: SweepDeps): Promise<SweepOutcome> {
  const { archive, payments, config, newBatchId } = deps
  const log = deps.log ?? (() => {})
  const empty: SweepOutcome = {
    read: 0, sent: 0, processed: 0, userError: 0, serverError: 0,
    skipped: 0, alreadyKnown: 0, pages: 0, saturated: false, batchIds: [],
  }

  const cursor = await payments.cursor(config.batchSize)

  // Retries first — they are the oldest work and are named explicitly, so they
  // cannot be crowded out by a flood of new signals.
  const attemptOf = new Map<string, number>()
  for (const entry of cursor.retry) attemptOf.set(entry.signalId, entry.nextAttempt)

  const rows: SignalLogRow[] = []
  const seen = new Set<string>()
  if (attemptOf.size > 0) {
    for (const row of await archive.readByIds([...attemptOf.keys()])) {
      if (seen.has(row.signal_id)) continue
      seen.add(row.signal_id)
      rows.push(row)
    }
    // A retry the archive no longer holds (TTL, or a truncate-and-replay) can
    // never be settled. Say so — silently dropping it would leave it PENDING
    // forever with nobody looking.
    const missing = [...attemptOf.keys()].filter((id) => !seen.has(id))
    if (missing.length > 0) {
      log('retry.missing_from_archive', { count: missing.length, sample: missing.slice(0, 5) })
    }
  }

  // Then new work, up to whatever room is left in the batch.
  //
  // The read starts BELOW the watermark by the overlap window, because a row can
  // land with a `received_at` just under one already seen (ClickHouse's Kafka
  // engine flushes in batches, and several edge instances stamp the time from
  // their own clocks). Without that overlap a late arrival is lost for good.
  //
  // But the overlap must not mean re-settling the same signals every sweep. That
  // would be harmless — settle dedups on `signalId` — and still ruinous: the
  // pipeline would never go quiet and the whole window would be re-priced once a
  // second. So the candidates are checked against the status rows and anything
  // already recorded is dropped. Retries are exempt: the cursor named them
  // deliberately, and they are already in `rows`.
  //
  // It PAGES, and that is not an optimisation. The known-filter runs after the
  // read, so if a single page were all it looked at, an overlap window holding
  // more settled rows than one page would be read, filtered away entirely, and
  // the new work behind it would never be reached — every sweep sending nothing
  // while the backlog grows. The keyset cursor is what lets it walk past the
  // settled rows to the ones that still need doing.
  let alreadyKnown = 0
  let pages = 0
  const room = config.batchSize * config.concurrency - rows.length
  if (room > 0) {
    const since = overlapFrom(cursor.sentThrough, config.overlapMs)
    let after: { receivedAt: string; signalId: string } | null = null
    const wanted = rows.length + room

    while (rows.length < wanted && pages < MAX_PAGES) {
      pages += 1
      const page = await archive.readNewer({ sinceIso: since, after, limit: room })
      if (page.length === 0) break

      const last = page[page.length - 1]!
      after = { receivedAt: last.received_at, signalId: last.signal_id }

      const fresh = page.filter((row) => !seen.has(row.signal_id))
      if (fresh.length > 0) {
        const known = new Set(await payments.known(fresh.map((row) => row.signal_id)))
        alreadyKnown += known.size
        for (const row of fresh) {
          if (known.has(row.signal_id)) continue
          if (rows.length >= wanted) break
          seen.add(row.signal_id)
          rows.push(row)
        }
      }
      // A short page means the archive is exhausted — nothing to page towards.
      if (page.length < room) break
    }

    if (pages >= MAX_PAGES && rows.length < wanted) {
      // The window holds more settled rows than MAX_PAGES can walk. Not a
      // correctness problem — the next sweep starts again from the same place
      // and the watermark advances as work completes — but it means the overlap
      // is too wide for the throughput, so say so rather than quietly crawling.
      log('sweep.page_cap', { pages, alreadyKnown, overlapMs: config.overlapMs })
    }
  }

  if (rows.length === 0) return { ...empty, alreadyKnown, pages }

  const signals: SettleSignal[] = []
  let skipped = 0
  for (const row of rows) {
    const signal = toSettleSignal(row, attemptOf.get(row.signal_id) ?? 1)
    if (!signal) {
      skipped += 1
      log('row.unusable', { signalId: row.signal_id })
      continue
    }
    signals.push(signal)
  }
  if (signals.length === 0) {
    return { ...empty, read: rows.length, skipped, alreadyKnown, pages }
  }

  const batches = chunk(signals, config.batchSize)
  const batchIds = batches.map(() => newBatchId())
  const settled = await pool(
    batches.map((batch, i) => async () => {
      const batchId = batchIds[i]!
      try {
        return await payments.settle(batchId, batch)
      } catch (err) {
        // The whole batch is unresolved. Nothing is recorded for it, so the next
        // sweep picks the same signals up again — which is safe, because settle
        // dedups on `signalId`.
        log('batch.failed', {
          batchId,
          signals: batch.length,
          error: err instanceof Error ? err.message : String(err),
        })
        return [] as Awaited<ReturnType<SettleClient['settle']>>
      }
    }),
    config.concurrency,
  )

  let processed = 0
  let userError = 0
  let serverError = 0
  for (const result of settled.flat()) {
    if (result.status === 'PROCESSED') processed += 1
    else if (result.error_type === 'USER_ERROR') userError += 1
    else serverError += 1
  }

  return {
    read: rows.length,
    sent: signals.length,
    processed,
    userError,
    serverError,
    skipped,
    alreadyKnown,
    pages,
    // Saturated when the sweep filled every batch it was allowed — the signal to
    // loop again with no nap.
    saturated: signals.length >= config.batchSize * config.concurrency,
    batchIds,
  }
}
