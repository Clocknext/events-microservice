// Load generator for the signal edge. Fires a mix of realistic accepted signals
// and rejects at a chosen concurrency, and reports throughput + latency.
//
//   TOTAL=3000 CONCURRENCY=64 REJECT_RATIO=0.2 node scripts/loadtest.mjs
//
// It measures the EDGE (real Fastify on the host). The consumers behind SQS are
// LocalStack Lambda — drain them and check ClickHouse separately; their timing
// is not a prod proxy.
import { performance } from 'node:perf_hooks'
import { Pool } from 'undici'

const EDGE = process.env.EDGE_URL ?? 'http://localhost:3122'
const KEY = process.env.LOAD_KEY ?? 'cnk_load'
const TOTAL = Number.parseInt(process.env.TOTAL ?? '3000', 10)
const CONCURRENCY = Number.parseInt(process.env.CONCURRENCY ?? '64', 10)
const REJECT_RATIO = Number.parseFloat(process.env.REJECT_RATIO ?? '0.2')

// --- prod-like variety --------------------------------------------------------
const CUSTOMERS = Array.from({ length: 200 }, (_, i) => `cus_${String(i).padStart(5, '0')}`)
const MODELS = [
  'openai/gpt-4o',
  'openai/gpt-4o-mini',
  'anthropic/claude-sonnet-4-6',
  'anthropic/claude-opus-4-8',
  'google/gemini-2.5-pro',
]
const TYPES = ['credit', 'wallet', 'outcome']
const AGENT_KEYS = ['credit.research', 'credit.chat', 'wallet.default', 'outcome.onboard']

const pick = (a) => a[Math.floor(Math.random() * a.length)]
const int = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo))

/** A valid signal, varied across customers, models, types and token counts. */
function validSignal() {
  const type = pick(TYPES)
  const body = {
    customerId: pick(CUSTOMERS),
    type,
    model: pick(MODELS),
    inputTokens: int(50, 8000),
    outputTokens: int(10, 4000),
    cacheTokens: Math.random() < 0.3 ? int(0, 2000) : 0,
    member: `user${int(1, 40)}@acme.com`,
  }
  if (type === 'credit') body.agentKey = pick(AGENT_KEYS)
  if (type === 'outcome') {
    body.agentKey = pick(AGENT_KEYS)
    body.runId = `run_${int(1, 5000)}`
    if (Math.random() < 0.2) body.complete = true
  }
  if (Math.random() < 0.4) body.idempotencyKey = `idem_${int(1, 1_000_000)}`
  if (Math.random() < 0.2) body.custom = { feature: pick(['chat', 'voice', 'search']), region: 'eu' }
  return body
}

/** A body that fails validation a different way each time. */
function rejectSignal() {
  const variants = [
    () => ({ type: 'credit', model: 'm', inputTokens: 1, outputTokens: 1 }), // no customerId
    () => ({ customerId: pick(CUSTOMERS), type: 'credit', inputTokens: 1, outputTokens: 1 }), // no model
    () => ({ customerId: pick(CUSTOMERS), type: 'credit', model: 'm', inputTokens: 1 }), // no outputTokens
    () => ({ customerId: pick(CUSTOMERS), type: 'credit', model: 'm', inputTokens: '5', outputTokens: 1 }), // string
    () => ({ customerId: pick(CUSTOMERS), type: 'outcome', model: 'm', agentKey: 'k', inputTokens: 1, outputTokens: 1 }), // no runId
    () => ({ customerId: pick(CUSTOMERS), type: 'unit', model: 'm', inputTokens: 1, outputTokens: 1 }), // bad type
  ]
  return pick(variants)()
}

// --- fire ---------------------------------------------------------------------
const pool = new Pool(EDGE, { connections: CONCURRENCY, pipelining: 1 })
const headers = { 'content-type': 'application/json', authorization: `Bearer ${KEY}` }

const latencies = new Float64Array(TOTAL)
const stat = { accepted: 0, deferred: 0, other: 0, error: 0 }
const codes = new Map()

let next = 0
async function worker() {
  while (true) {
    const i = next++
    if (i >= TOTAL) break
    const isReject = Math.random() < REJECT_RATIO
    const body = isReject ? rejectSignal() : validSignal()
    const t0 = performance.now()
    try {
      const res = await pool.request({
        path: '/api/v1/signal',
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      })
      latencies[i] = performance.now() - t0
      codes.set(res.statusCode, (codes.get(res.statusCode) ?? 0) + 1)
      const json = await res.body.json().catch(() => ({}))
      const result = json.result ?? {}
      if (result.accepted === true) stat.accepted++
      else if (result.status === 'PENDING') stat.deferred++
      else stat.other++
    } catch {
      latencies[i] = performance.now() - t0
      stat.error++
    }
  }
}

const pct = (sortedMs, p) => sortedMs[Math.min(sortedMs.length - 1, Math.floor((p / 100) * sortedMs.length))]

const startedAt = performance.now()
await Promise.all(Array.from({ length: CONCURRENCY }, worker))
const wallSec = (performance.now() - startedAt) / 1000
await pool.close()

const sorted = Array.from(latencies).sort((a, b) => a - b)
console.log(`\n=== load: ${TOTAL} signals @ ${CONCURRENCY} concurrent, ${REJECT_RATIO * 100}% rejects ===`)
console.log(`wall:        ${wallSec.toFixed(2)}s`)
console.log(`throughput:  ${(TOTAL / wallSec).toFixed(0)} req/s`)
console.log(`latency ms:  p50 ${pct(sorted, 50).toFixed(1)}  p95 ${pct(sorted, 95).toFixed(1)}  p99 ${pct(sorted, 99).toFixed(1)}  max ${sorted.at(-1).toFixed(1)}`)
console.log(`status:      ${[...codes.entries()].map(([c, n]) => `${c}:${n}`).join('  ')}`)
console.log(`classified:  accepted ${stat.accepted}  deferred(reject) ${stat.deferred}  other ${stat.other}  error ${stat.error}`)
