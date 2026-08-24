/** Lambda consumer for `signals_pending`.
 *
 *  ONE consumer (reserved concurrency 1), draining a 60-second batch window (the
 *  queue also holds each message 60s), so it runs rarely and inserts in large
 *  batches. Topology is in infra/lambdas.tf, not here.
 *
 *  A pending message is a signal the edge already rejected — a bad body, an
 *  unknown key, an outage. It never enters settlement, so it is terminal: the
 *  consumer writes its `raw_signals` row and one `Failed` status event, and that
 *  is the whole of its life in ClickHouse. */
import { randomUUID } from 'node:crypto'
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import { createClickHouseClient } from '../client/clickhouse.js'
import type { PendingMessage, StatusEventRow } from '../modules/vent/vent.schema.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'
import { receiveCount } from './lib/sqs.js'

const clickhouse = createClickHouseClient()

/** The Lambda entry point. Thin — binds the client and mints the batch id, so
 *  the logic below is testable with a fake client and a fixed id. */
export function handler(event: SQSEvent): Promise<SQSBatchResponse> {
  return consumePending(clickhouse, event, randomUUID())
}

export async function consumePending(
  clickhouse: ClickHouseClient,
  event: SQSEvent,
  batchId: string,
): Promise<SQSBatchResponse> {
  const failed = new Set<string>()
  const rawRows: TaggedRow[] = []
  const eventRows: TaggedRow[] = []
  const now = new Date().toISOString()

  for (const record of event.Records) {
    let message: PendingMessage
    try {
      message = JSON.parse(record.body) as PendingMessage
      if (!message.raw_signals?.signal_id) throw new Error('message is missing raw_signals')
    } catch {
      // Unparseable — fail it now so it redelivers its five times and lands in
      // the DLQ for a human, rather than blocking.
      failed.add(record.messageId)
      continue
    }
    rawRows.push({ messageId: record.messageId, row: message.raw_signals })
    const statusEvent: StatusEventRow = {
      signal_id: message.raw_signals.signal_id,
      batch_id: batchId,
      status: 'Failed',
      error_code: message.error_code || null,
      attempt: receiveCount(record),
      timestamp: now,
    }
    eventRows.push({ messageId: record.messageId, row: statusEvent })
  }

  // raw_signals first: a status event is about a raw_signals row.
  for (const id of await insertTagged(clickhouse, 'raw_signals', rawRows)) failed.add(id)
  // Skip the event for any message whose raw row failed — it is going back to
  // the queue and both rows replay together (ReplacingMergeTree dedups).
  const eventsToInsert = eventRows.filter((r) => !failed.has(r.messageId))
  for (const id of await insertTagged(clickhouse, 'signal_status_events', eventsToInsert)) {
    failed.add(id)
  }

  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) }
}
