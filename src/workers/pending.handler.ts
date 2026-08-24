/** Lambda consumer for `signals_pending`.
 *
 *  ONE consumer, draining a 60-second batch window (the queue also holds each
 *  message 60s via DelaySeconds), so it runs rarely and inserts in large
 *  batches. Reserved concurrency 1 keeps it a single consumer — see
 *  scripts/localstack/deploy-lambdas.sh.
 *
 *  Each message is the two rows the edge built, keyed by table name, so this
 *  handler does two bulk inserts and no transformation:
 *
 *      raw_signals   then   signal_status
 *
 *  in that order, because a signal_status row references a raw_signals row. A
 *  message is only "done" when BOTH of its rows land; if either fails, the
 *  message is returned to SQS and both inserts replay, which ReplacingMergeTree
 *  makes harmless. */
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import { createClickHouseClient } from '../client/clickhouse.js'
import type { VentMessage } from '../modules/vent/vent.schema.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'

// Created at cold start, reused across warm invocations.
const clickhouse = createClickHouseClient()

/** The Lambda entry point. Thin — the client is the only thing it binds, so the
 *  logic below is testable with a fake one. */
export function handler(event: SQSEvent): Promise<SQSBatchResponse> {
  return consumePending(clickhouse, event)
}

export async function consumePending(
  clickhouse: ClickHouseClient,
  event: SQSEvent,
): Promise<SQSBatchResponse> {
  const failed = new Set<string>()
  const rawRows: TaggedRow[] = []
  const statusRows: TaggedRow[] = []

  for (const record of event.Records) {
    let message: VentMessage
    try {
      message = JSON.parse(record.body) as VentMessage
      if (!message.raw_signals || !message.signal_status) {
        throw new Error('message is missing raw_signals or signal_status')
      }
    } catch {
      // An unparseable message can never succeed, so fail it now: it redelivers
      // its five times and lands in the DLQ for a human, rather than blocking.
      failed.add(record.messageId)
      continue
    }
    rawRows.push({ messageId: record.messageId, row: message.raw_signals })
    statusRows.push({ messageId: record.messageId, row: message.signal_status })
  }

  // raw_signals first: signal_status describes a raw_signals row, so the parent
  // row should exist first even though nothing enforces a foreign key.
  for (const id of await insertTagged(clickhouse, 'raw_signals', rawRows)) failed.add(id)
  // Do not insert a signal_status row whose raw_signals row already failed —
  // that message is going back to the queue and both rows replay together.
  const statusToInsert = statusRows.filter((r) => !failed.has(r.messageId))
  for (const id of await insertTagged(clickhouse, 'signal_status', statusToInsert)) failed.add(id)

  return {
    batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })),
  }
}
