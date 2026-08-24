/** Lambda consumer for `signals_accepted`.
 *
 *  N concurrent consumers (MaximumConcurrency on the mapping), because an
 *  accepted signal is billable and the queue has no hold — drained fast and in
 *  parallel, the opposite of the pending consumer.
 *
 *  Per invocation, for its batch of signals:
 *    1. write every `raw_signals` row
 *    2. write a `Processing` event for each
 *    3. POST the batch to `/internal/settle`
 *    4. per result: `Processed` (settled), `Failed` (terminal refusal), or
 *       nothing + a batch-item-failure (retryable refusal → SQS redelivers)
 *
 *  Every event is stamped with one `batch_id` per invocation, so a row traces
 *  back to the consumer run that wrote it. */
import { randomUUID } from 'node:crypto'
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda'
import type { ClickHouseClient } from '../client/clickhouse.js'
import { createClickHouseClient } from '../client/clickhouse.js'
import type { AcceptedMessage, StatusEventRow } from '../modules/vent/vent.schema.js'
import { insertTagged, type TaggedRow } from './lib/insert.js'
import { settleBatch, type SettleResult, type SettleSignal } from './lib/settle.js'
import { receiveCount } from './lib/sqs.js'

const clickhouse = createClickHouseClient()

/** Settling is injected so the logic is testable with a fake in place of the
 *  real HTTP call. */
export type SettleFn = (batchId: string, signals: SettleSignal[]) => Promise<SettleResult[]>

/** The Lambda entry point. Binds the client, the real settle call, and a batch
 *  id, so the logic below is a pure function of its inputs. */
export function handler(event: SQSEvent): Promise<SQSBatchResponse> {
  return consumeAccepted(clickhouse, settleBatch, event, randomUUID())
}

/** One message's bookkeeping, keyed by the signal so a settle result (which
 *  carries `signal_id`) can be mapped back to the SQS message to fail or ack. */
interface Pending {
  messageId: string
  attempt: number
}

function statusEvent(
  signalId: string,
  batchId: string,
  status: StatusEventRow['status'],
  errorCode: string | null,
  attempt: number,
  timestamp: string,
): StatusEventRow {
  return { signal_id: signalId, batch_id: batchId, status, error_code: errorCode, attempt, timestamp }
}

export async function consumeAccepted(
  clickhouse: ClickHouseClient,
  settle: SettleFn,
  event: SQSEvent,
  batchId: string,
): Promise<SQSBatchResponse> {
  const failed = new Set<string>()
  const rawRows: TaggedRow[] = []
  const processingRows: TaggedRow[] = []
  const toSettle: SettleSignal[] = []
  const bySignal = new Map<string, Pending>()
  const receivedAt = new Date().toISOString()

  for (const record of event.Records) {
    let message: AcceptedMessage
    try {
      message = JSON.parse(record.body) as AcceptedMessage
      if (!message.raw_signals?.signal_id) throw new Error('message is missing raw_signals')
    } catch {
      failed.add(record.messageId)
      continue
    }
    const raw = message.raw_signals
    const attempt = receiveCount(record)
    bySignal.set(raw.signal_id, { messageId: record.messageId, attempt })

    rawRows.push({ messageId: record.messageId, row: raw })
    processingRows.push({
      messageId: record.messageId,
      row: statusEvent(raw.signal_id, batchId, 'Processing', null, attempt, receivedAt),
    })

    // Build the settle input: the payload fields spread in, then the named
    // fields settle reads on top. A payload that will not parse (should not
    // happen for an accepted signal) settles with the named fields only.
    let payload: Record<string, unknown> = {}
    try {
      payload = JSON.parse(raw.payload) as Record<string, unknown>
    } catch {
      /* keep the empty object */
    }
    toSettle.push({
      ...payload,
      signalId: raw.signal_id,
      receivedAt: raw.received_at,
      organizationId: raw.organization_id,
      customerId: raw.customer_id,
      type: raw.type,
      attempt,
    })
  }

  // 1 + 2: the raw row, then the Processing event. A message whose raw row fails
  // is dropped from settlement — it is going back to the queue.
  for (const id of await insertTagged(clickhouse, 'raw_signals', rawRows)) failed.add(id)
  const processingToInsert = processingRows.filter((r) => !failed.has(r.messageId))
  for (const id of await insertTagged(clickhouse, 'signal_status_events', processingToInsert)) {
    failed.add(id)
  }

  const settleNow = toSettle.filter((s) => !failed.has(bySignal.get(s.signalId)!.messageId))
  if (settleNow.length === 0) {
    return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) }
  }

  // 3: settle. If the call itself fails, nothing was settled — every signal it
  // covered goes back to the queue and replays (raw + Processing are harmless
  // to re-insert).
  let results: SettleResult[]
  try {
    results = await settle(batchId, settleNow)
  } catch {
    for (const s of settleNow) failed.add(bySignal.get(s.signalId)!.messageId)
    return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) }
  }

  // 4: one terminal event per result. A retryable refusal writes no event and
  // lets SQS redeliver; a signal settle did not answer for is retried too.
  const settledAt = new Date().toISOString()
  const terminalRows: TaggedRow[] = []
  const answered = new Set<string>()
  for (const result of results) {
    const pending = bySignal.get(result.signal_id)
    if (!pending || failed.has(pending.messageId)) continue
    answered.add(result.signal_id)

    if (result.status === 'PROCESSED') {
      terminalRows.push({
        messageId: pending.messageId,
        row: statusEvent(result.signal_id, batchId, 'Processed', null, pending.attempt, settledAt),
      })
    } else if (result.error_type === 'SERVER_ERROR') {
      // Ours to fix, safe to retry as-is: no terminal event, let it redeliver.
      failed.add(pending.messageId)
    } else {
      // The caller's data is wrong — terminal. Record it and ack.
      terminalRows.push({
        messageId: pending.messageId,
        row: statusEvent(
          result.signal_id,
          batchId,
          'Failed',
          result.error_code,
          pending.attempt,
          settledAt,
        ),
      })
    }
  }
  // A signal sent to settle but absent from the results was not resolved — retry.
  for (const s of settleNow) {
    if (!answered.has(s.signalId)) failed.add(bySignal.get(s.signalId)!.messageId)
  }

  for (const id of await insertTagged(clickhouse, 'signal_status_events', terminalRows)) {
    failed.add(id)
  }

  return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) }
}
