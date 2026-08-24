import type { SQSRecord } from 'aws-lambda'

/** Which delivery of a message this is — SQS's own count, so a message being
 *  processed for the first time is attempt 1 and a redelivery is 2, 3, …. This
 *  is what a status event records as `attempt`, and what settle is told. */
export function receiveCount(record: SQSRecord): number {
  const raw = record.attributes?.ApproximateReceiveCount
  const n = Number.parseInt(raw ?? '1', 10)
  return Number.isFinite(n) && n > 0 ? n : 1
}
