/** The two SELECTs the dispatcher runs against the archive.
 *
 *  Both go through the read-only `ClickHouseReader` and both bind their
 *  parameters server-side — a `signal_id` read out of the archive is never
 *  spliced into SQL. */
import type { ClickHouseReader } from '../../client/clickhouse.js'
import type { ArchiveReader, SignalLogRow } from './dispatch.schema.js'

/** `LIMIT 1 BY signal_id` collapses the duplicates that at-least-once Kafka
 *  delivery puts in a `ReplacingMergeTree` before a merge runs. It is the cheap
 *  way to do it — `FINAL` would force a merge-on-read on every sweep — and it
 *  matters for money: without it one physical duplicate would be sent twice. */
const DEDUPE = 'LIMIT 1 BY signal_id'

/** The columns, in the shape `SignalLogRow` expects.
 *
 *  `received_at` is selected RAW, with no `toString()`. That is not a style
 *  choice: ClickHouse resolves a SELECT alias inside WHERE, so
 *  `toString(received_at) AS received_at` makes the WHERE clause compare a String
 *  against a DateTime64 and the query dies with
 *  "No operation greater between String and DateTime64(3)". Selecting the column
 *  plainly avoids the shadowing entirely — and JSONEachRow already renders a
 *  DateTime64(3) as `YYYY-MM-DD hh:mm:ss.sss`, which is what `toIso` converts. */
const COLUMNS = 'signal_id, received_at, api_key_hash, payload'

export function createArchiveReader(clickhouse: ClickHouseReader): ArchiveReader {
  return {
    async readNewer({ sinceIso, after, limit }) {
      const clauses: string[] = []
      const params: Record<string, string | number> = { limit }
      // `>` not `>=`: the watermark row itself has already been sent, and the
      // caller has already shifted the bound back by the overlap window.
      if (sinceIso) {
        clauses.push('received_at > parseDateTime64BestEffort({since:String})')
        params.since = sinceIso
      }
      // The keyset. Tuple comparison, so `signal_id` breaks the tie when two
      // signals share a millisecond — without it a page boundary could skip a
      // row or repeat one forever.
      if (after) {
        clauses.push(
          '(received_at, signal_id) > (parseDateTime64BestEffort({afterTs:String}), {afterId:String})',
        )
        params.afterTs = after.receivedAt
        params.afterId = after.signalId
      }
      return clickhouse.query<SignalLogRow>(
        `SELECT ${COLUMNS}
           FROM signal_log
           ${clauses.length ? `WHERE ${clauses.join(' AND ')}` : ''}
          ORDER BY received_at ASC, signal_id ASC
          ${DEDUPE}
          LIMIT {limit:UInt32}`,
        params,
      )
    },

    async readByIds(signalIds) {
      if (signalIds.length === 0) return []
      // The ids come from Postgres, which only ever learned them from this same
      // archive, so the set is small and bounded by the cursor's retry limit.
      return clickhouse.query<SignalLogRow>(
        `SELECT ${COLUMNS}
           FROM signal_log
          WHERE signal_id IN {ids:Array(String)}
          ORDER BY received_at ASC
          ${DEDUPE}`,
        { ids: `['${signalIds.map((id) => id.replace(/'/g, "''")).join("','")}']` },
      )
    },
  }
}
