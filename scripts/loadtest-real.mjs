// Mixed real-world load against the REAL pipeline (edge -> SQS -> Lambda ->
// real /internal/settle -> ClickHouse). Uses the seed org's actual customers,
// credits and outcomes so signals really price and bill.
//
//   EDGE_URL=… LOAD_KEY=cnk_… TOTAL=250 CONCURRENCY=16 node scripts/loadtest-real.mjs
import { performance } from 'node:perf_hooks'
import { Pool } from 'undici'

const EDGE = process.env.EDGE_URL ?? 'http://localhost:3123'
const KEY = process.env.LOAD_KEY ?? ''
const TOTAL = Number.parseInt(process.env.TOTAL ?? '250', 10)
const CONCURRENCY = Number.parseInt(process.env.CONCURRENCY ?? '16', 10)

const CREDIT_CUSTOMERS = ['seed_customer_1', 'seed_customer_2', 'seed_customer_3', 'seed_customer_4', 'seed_customer_5']
const CREDIT_KEYS = ['seed_credit_adv', 'seed_credit_arr']
const OUTCOME_CUSTOMER = 't_cust_oadv'
const OUTCOME_STEPS = ['t_oadv_start', 't_oadv_finish']
const MODEL = 'gpt-4o'

const pick = (a) => a[Math.floor(Math.random() * a.length)]
const int = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo))
const tokens = () => ({ inputTokens: int(50, 6000), outputTokens: int(10, 3000) })

// The mix. Weights sum to 1. Three success types + two failure modes.
function generate(i) {
  const r = Math.random()
  if (r < 0.25) return { kind: 'ok-credit', body: { customerId: pick(CREDIT_CUSTOMERS), type: 'credit', agentKey: pick(CREDIT_KEYS), model: MODEL, ...tokens() } }
  if (r < 0.45) return { kind: 'ok-wallet', body: { customerId: pick(CREDIT_CUSTOMERS), type: 'wallet', model: MODEL, ...tokens() } }
  if (r < 0.60) return { kind: 'ok-outcome', body: { customerId: OUTCOME_CUSTOMER, type: 'outcome', agentKey: pick(OUTCOME_STEPS), runId: `run_${i}`, model: MODEL, ...tokens() } }
  if (r < 0.80) {
    // fail at the EDGE (bad body) -> deferred 202 -> pending queue -> Failed
    const variants = [
      { customerId: pick(CREDIT_CUSTOMERS), type: 'credit', agentKey: 'seed_credit_adv', ...tokens() }, // no model
      { type: 'credit', agentKey: 'seed_credit_adv', model: MODEL, ...tokens() }, // no customerId
      { customerId: pick(CREDIT_CUSTOMERS), type: 'outcome', agentKey: 't_oadv_start', model: MODEL, ...tokens() }, // outcome, no runId
      { customerId: pick(CREDIT_CUSTOMERS), type: 'unit', model: MODEL, ...tokens() }, // bad type
    ]
    return { kind: 'fail-edge', body: pick(variants) }
  }
  // fail at SETTLE (valid shape, wrong plan/customer) -> accepted -> settle refuses -> Failed
  const variants = [
    { customerId: OUTCOME_CUSTOMER, type: 'credit', agentKey: 'seed_credit_adv', model: MODEL, ...tokens() }, // credit not in outcome customer's plan
    { customerId: 'no_such_customer', type: 'credit', agentKey: 'seed_credit_adv', model: MODEL, ...tokens() }, // NOT_FOUND
    { customerId: pick(CREDIT_CUSTOMERS), type: 'credit', agentKey: 'seed_credit_adv', model: 'claude-3-5-sonnet', ...tokens() }, // model maybe not enabled
  ]
  return { kind: 'fail-settle', body: pick(variants) }
}

const pool = new Pool(EDGE, { connections: CONCURRENCY })
const headers = { 'content-type': 'application/json', authorization: `Bearer ${KEY}` }

const sentKind = new Map()
const codes = new Map()
const cls = { accepted: 0, deferred: 0, hard: 0, error: 0 }
const latencies = new Float64Array(TOTAL)

let next = 0
async function worker() {
  while (true) {
    const i = next++
    if (i >= TOTAL) break
    const { kind, body } = generate(i)
    sentKind.set(kind, (sentKind.get(kind) ?? 0) + 1)
    const t0 = performance.now()
    try {
      const res = await pool.request({ path: '/api/v1/signal', method: 'POST', headers, body: JSON.stringify(body) })
      latencies[i] = performance.now() - t0
      codes.set(res.statusCode, (codes.get(res.statusCode) ?? 0) + 1)
      const json = await res.body.json().catch(() => ({}))
      const result = json.result ?? {}
      if (result.accepted === true) cls.accepted++
      else if (result.status === 'PENDING') cls.deferred++
      else cls.hard++
    } catch {
      latencies[i] = performance.now() - t0
      cls.error++
    }
  }
}

const pct = (s, p) => s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]
const startedAt = performance.now()
await Promise.all(Array.from({ length: CONCURRENCY }, worker))
const wall = (performance.now() - startedAt) / 1000
await pool.close()

const sorted = Array.from(latencies).sort((a, b) => a - b)
console.log(`\n=== mixed real load: ${TOTAL} signals @ ${CONCURRENCY} concurrent ===`)
console.log(`wall ${wall.toFixed(2)}s   throughput ${(TOTAL / wall).toFixed(0)} req/s`)
console.log(`latency ms: p50 ${pct(sorted, 50).toFixed(0)}  p95 ${pct(sorted, 95).toFixed(0)}  p99 ${pct(sorted, 99).toFixed(0)}`)
console.log(`http: ${[...codes].map(([c, n]) => `${c}:${n}`).join('  ')}`)
console.log(`edge class: accepted(202) ${cls.accepted}  deferred-reject(202) ${cls.deferred}  hard ${cls.hard}  err ${cls.error}`)
console.log(`sent mix: ${[...sentKind].map(([k, n]) => `${k} ${n}`).join('  ')}`)
