/** Lambda consumer for `signals_accepted`.
 *
 *  N concurrent consumers (MaximumConcurrency on the event-source mapping),
 *  because an accepted signal is billable and the queue has no hold — it is
 *  drained fast and in parallel, the opposite of the pending consumer.
 *
 *  Each message carries only `raw_signals.{signal_id, received_at}` — the rest
 *  of the row is written later in the pipeline. This consumer stamps every row
 *  it writes with one `batch_id`, minted once per invocation, so a row can be
 *  traced back to the consumer run that ingested it. */
import { randomUUID } from 'node:crypto'
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import { createClickHouseClient } from '../client/clickhouse.js'
import type { AcceptedMessage } from '../modules/vent/vent.schema.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'

const clickhouse = createClickHouseClient()

/** The Lambda entry point. Thin — binds the client and mints the batch id, so
 *  the logic below is testable with a fake client and a fixed id. */
export function handler(event: SQSEvent): Promise<SQSBatchResponse> {
  return consumeAccepted(clickhouse, event, randomUUID())
}

export async function consumeAccepted(
  clickhouse: ClickHouseClient,
  event: SQSEvent,
  batchId: string,
): Promise<SQSBatchResponse> {
  const failed = new Set<string>()
  const rows: TaggedRow[] = []

  for (const record of event.Records) {
    let message: AcceptedMessage
    try {
      message = JSON.parse(record.body) as AcceptedMessage
      if (!message.raw_signals?.signal_id) {
        throw new Error('message is missing raw_signals.signal_id')
      }
    } catch {
      failed.add(record.messageId)
      continue
    }
    rows.push({
      messageId: record.messageId,
      row: { ...message.raw_signals, batch_id: batchId },
    })
  }

  for (const id of await insertTagged(clickhouse, 'raw_signals', rows)) failed.add(id)

  return {
    batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })),
  }
}
