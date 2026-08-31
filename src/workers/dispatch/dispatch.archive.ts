/** The ONE SELECT the dispatcher runs against the archive.
 *
 *  It goes through the read-only `ClickHouseReader` and binds its parameters
 *  server-side — nothing read out of the archive is ever spliced into SQL. */
import type { ClickHouseReader } from '../../client/clickhouse.js'
import type { ArchiveReader, SignalLogRow } from './dispatch.schema.js'

/** `LIMIT 1 BY signal_id` collapses the duplicates that at-least-once Kafka
 *  delivery puts in a `ReplacingMergeTree` before a merge runs. It is the cheap
 *  way to do it — `FINAL` would force a merge-on-read on every run — and it
 *  matters for money: without it one physical duplicate would be sent twice.
 *
 *  Note this is not the same job as settle's idempotency. Settle dedups ACROSS
 *  runs; this dedups WITHIN one, so the single request body does not carry the
 *  same signal twice. */
const DEDUPE = 'LIMIT 1 BY signal_id'

/** Which duplicate `LIMIT 1 BY` keeps.
 *
 *  `signal_log` is `ReplacingMergeTree(version)`, so a row can be REWRITTEN by
 *  re-inserting it at a higher version. Nothing does that today — the consumer
 *  always writes 1 — but the daily reconciliation cron will, and picking the
 *  newest version has to be right before it does, not after.
 *
 *  It is also why the whole thing needs a subquery: `LIMIT 1 BY` takes the first
 *  row per key in the CURRENT ordering, so choosing the newest version and
 *  returning rows oldest-first are two different ORDER BYs and cannot share one
 *  level. The outer ordering is not cosmetic either — see the LIMIT below. */
const NEWEST_VERSION = 'ORDER BY signal_id, version DESC'

/** The columns, in the shape `SignalLogRow` expects.
 *
 *  `received_at` is selected RAW, with no `toString()`. That is not a style
 *  choice: ClickHouse resolves a SELECT alias inside WHERE, so
 *  `toString(received_at) AS received_at` makes the WHERE clause compare a String
 *  against a DateTime64 and the query dies with
 *  "No operation greater between String and DateTime64(3)". Selecting the column
 *  plainly avoids the shadowing entirely — and JSONEachRow already renders a
 *  DateTime64(3) as `YYYY-MM-DD hh:mm:ss.sss`, which is what `toIso` converts.
 *
 *  `ingested_at` is not in this list because it is not part of the signal. The
 *  INNER query below still selects it — the outer ordering needs it — but it is
 *  projected away before the rows reach `SignalLogRow`. */
const COLUMNS = [
  'signal_id',
  'received_at',
  'api_key_hash',
  'customer_id',
  'organization_id',
  'status',
  'error_code',
  'error_message',
  'payload',
].join(', ')

export function createArchiveReader(clickhouse: ClickHouseReader): ArchiveReader {
  return {
    async readIngested({ windowMs, since, until, cap }) {
      const clauses: string[] = []
      const params: Record<string, string | number> = { cap }

      if (since) {
        // Manual replay: an operator named both ends deliberately.
        clauses.push('ingested_at >= parseDateTime64BestEffort({since:String})')
        params.since = since
        if (until) {
          clauses.push('ingested_at < parseDateTime64BestEffort({until:String})')
          params.until = until
        }
      } else {
        // The normal path. The lower bound is computed by CLICKHOUSE, from
        // `now64(3)`, and not by this process — deliberately. `ingested_at` is
        // stamped by the ClickHouse server's clock, so comparing it against a
        // bound derived from the DISPATCHER's clock would shift the window by
        // whatever skew exists between two machines, and shift it silently. One
        // clock stamps and the same clock compares.
        //
        // `/ 1000` because subtracting a number from a DateTime64 subtracts
        // SECONDS; ClickHouse's `/` is float division, so the milliseconds
        // survive.
        clauses.push('ingested_at >= now64(3) - ({windowMs:UInt64} / 1000)')
        params.windowMs = windowMs
        // No upper bound, on purpose: the window runs right up to this instant so
        // that consecutive runs overlap and nothing can fall between them.
      }

      // Both statuses come back. PENDING rows are NOT filtered out: they still go
      // to settle, which records the terminal failure the consumer already
      // decided. Dropping them here would leave a rejected signal with no row in
      // Postgres at all — invisible in the Signals UI, with its reason readable
      // only by querying ClickHouse directly.
      //
      // The outer `ORDER BY ingested_at ASC` before `LIMIT {cap}` is load-bearing:
      // the cap must drop the NEWEST rows, because those are the ones the next
      // overlapping window still covers. Dropping the oldest would lose them.
      return clickhouse.query<SignalLogRow>(
        `SELECT ${COLUMNS}
           FROM (
             SELECT ${COLUMNS}, ingested_at
               FROM signal_log
              WHERE ${clauses.join(' AND ')}
              ${NEWEST_VERSION}
              ${DEDUPE}
           )
          ORDER BY ingested_at ASC
          LIMIT {cap:UInt32}`,
        params,
      )
    },
  }
}
