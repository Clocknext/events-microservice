/** Shared insert logic for both SQS consumers.
 *
 *  The unit is a `{ messageId, row }` pair: a ClickHouse row that came off one
 *  SQS message, tagged with which message it was, so a failure can be reported
 *  back to SQS per-message rather than as a whole-batch failure. */
import type { ClickHouseClient } from '../../client/clickhouse.js'

export interface TaggedRow {
  /** The SQS message this row came from. */
  messageId: string
  row: object
}

/**
 * Inserts a batch of tagged rows into one table, and returns the ids of the
 * messages whose rows did not land.
 *
 * Happy path is one bulk insert. On failure it does NOT give up on the whole
 * batch — it retries each row alone, so one poison row (a bad type, an
 * unparseable value) is isolated and only its message is reported failed. The
 * rest succeed on the retry pass.
 *
 * A row re-inserted on the retry pass after the bulk insert partially applied
 * is harmless: both tables are `ReplacingMergeTree`, so a duplicate collapses
 * on merge. That is the same property that makes SQS at-least-once redelivery
 * safe, reused here.
 */
export async function insertTagged(
  client: ClickHouseClient,
  table: string,
  tagged: TaggedRow[],
): Promise<Set<string>> {
  const failed = new Set<string>()
  if (tagged.length === 0) return failed

  try {
    await client.insert(
      table,
      tagged.map((t) => t.row),
    )
    return failed
  } catch {
    // The bulk insert failed for at least one row. Find which by retrying each
    // alone — the successes still land, only the genuine offenders are failed.
    for (const { messageId, row } of tagged) {
      try {
        await client.insert(table, [row])
      } catch {
        failed.add(messageId)
      }
    }
    return failed
  }
}
