// Drives the REAL accepted-consumer handler against the REAL /internal/settle
// and the REAL ClickHouse, from the host — because the LocalStack Lambda
// container cannot reach the host's payments app through this box's firewall.
//
// It runs the exact same `consumeAccepted` the Lambda runs; only the container
// wrapper is different (and that wrapper is LocalStack, not prod, anyway).
//
//   ORG=… CUSTOMER=… N=3 node --import tsx scripts/drive-accepted.mts
import { randomUUID } from 'node:crypto'
import { performance } from 'node:perf_hooks'
import type { SQSEvent, SQSRecord } from 'aws-lambda'
import { ulid } from 'ulid'
import { createClickHouseClient } from '../src/client/clickhouse.js'
import { consumeAccepted } from '../src/workers/accepted.handler.js'
import { settleBatch } from '../src/workers/lib/settle.js'

const ORG = process.env.ORG ?? 'cmt492mr000008bm6q7l0wer2'
const CUSTOMER = process.env.CUSTOMER ?? 'probe_nonexistent'
const AGENT_KEY = process.env.AGENTKEY ?? 'credit.research'
const MODEL = process.env.MODEL ?? 'openai/gpt-4o'
const N = Number.parseInt(process.env.N ?? '3', 10)

function record(i: number): SQSRecord {
  const signalId = ulid()
  const body = {
    customerId: CUSTOMER,
    type: 'credit',
    agentKey: AGENT_KEY,
    model: MODEL,
    inputTokens: 100 + i,
    outputTokens: 50,
    idempotencyKey: `drive_${signalId}`,
  }
  const raw_signals = {
    signal_id: signalId,
    organization_id: ORG,
    customer_id: CUSTOMER,
    type: 'credit',
    idempotency_key: null,
    payload: JSON.stringify(body),
    received_at: new Date().toISOString(),
  }
  return {
    messageId: `drive_${i}`,
    body: JSON.stringify({ raw_signals }),
    attributes: { ApproximateReceiveCount: '1' },
  } as unknown as SQSRecord
}

const event: SQSEvent = { Records: Array.from({ length: N }, (_, i) => record(i)) }
const clickhouse = createClickHouseClient()

console.log(`driving ${N} real signal(s): org=${ORG} customer=${CUSTOMER}`)
console.log(`  CLICKHOUSE_URL=${process.env.CLICKHOUSE_URL}  PAYMENTS_URL=${process.env.PAYMENTS_URL}`)

const t0 = performance.now()
const res = await consumeAccepted(clickhouse, settleBatch, event, randomUUID())
const elapsed = performance.now() - t0

console.log(`\nconsumeAccepted finished in ${elapsed.toFixed(0)}ms`)
console.log(`batchItemFailures: ${JSON.stringify(res.batchItemFailures)}`)
