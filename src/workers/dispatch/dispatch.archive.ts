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
 *  `ingested_at` is selected by nobody: it decides WHICH rows come back and is
 *  not part of the signal. */
const COLUMNS = 'signal_id, received_at, api_key_hash, payload'

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

      return clickhouse.query<SignalLogRow>(
        `SELECT ${COLUMNS}
           FROM signal_log
          WHERE ${clauses.join(' AND ')}
          ORDER BY ingested_at ASC
          ${DEDUPE}
          LIMIT {cap:UInt32}`,
        params,
      )
    },
  }
}
