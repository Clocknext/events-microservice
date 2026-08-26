// Load generator for the signal edge. Fires a mix of valid signals and rejects
// at a chosen concurrency, and reports throughput + latency + status codes.
//
//   TOTAL=20000 CONCURRENCY=100 REJECT_RATIO=0.2 node scripts/loadtest.mjs
//
// Measures the EDGE (real Fastify on the host). Uses Node's built-in fetch, no
// dependencies. ClickHouse ingestion is separate — check signals.signal_log.
import { performance } from 'node:perf_hooks'

const EDGE = process.env.EDGE_URL ?? 'http://localhost:3000'
const KEY = process.env.LOAD_KEY ?? 'cnk_load'
const TOTAL = Number.parseInt(process.env.TOTAL ?? '20000', 10)
const CONCURRENCY = Number.parseInt(process.env.CONCURRENCY ?? '100', 10)
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
  }
  if (Math.random() < 0.4) body.idempotencyKey = `idem_${int(1, 1_000_000)}`
  if (Math.random() < 0.2) body.custom = { feature: pick(['chat', 'voice', 'search']), region: 'eu' }
  return body
}

/** A body missing one of the three required gate fields — a 400 each time. */
function rejectSignal() {
  const variants = [
    () => ({ type: 'credit', model: 'm', inputTokens: 1, outputTokens: 1 }), // no customerId
    () => ({ customerId: pick(CUSTOMERS), inputTokens: 1 }), // no outputTokens
    () => ({ customerId: pick(CUSTOMERS), inputTokens: '5', outputTokens: 1 }), // token as string
    () => ({ customerId: '   ', inputTokens: 1, outputTokens: 1 }), // blank customerId
  ]
  return pick(variants)()
}

// --- fire ---------------------------------------------------------------------
const headers = { 'content-type': 'application/json', authorization: `Bearer ${KEY}` }
const latencies = new Float64Array(TOTAL)
const codes = new Map()
let next = 0

async function worker() {
  while (true) {
    const i = next++
    if (i >= TOTAL) break
    const body = Math.random() < REJECT_RATIO ? rejectSignal() : validSignal()
    const t0 = performance.now()
    try {
      const res = await fetch(`${EDGE}/api/v1/signal`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      })
      await res.arrayBuffer() // drain
      latencies[i] = performance.now() - t0
      codes.set(res.status, (codes.get(res.status) ?? 0) + 1)
    } catch {
      latencies[i] = performance.now() - t0
      codes.set('error', (codes.get('error') ?? 0) + 1)
    }
  }
}

const pct = (s, p) => s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]

const startedAt = performance.now()
await Promise.all(Array.from({ length: CONCURRENCY }, worker))
const wallSec = (performance.now() - startedAt) / 1000

const sorted = Array.from(latencies).sort((a, b) => a - b)
console.log(`\n=== load: ${TOTAL} signals @ ${CONCURRENCY} concurrent, ${REJECT_RATIO * 100}% rejects ===`)
console.log(`wall:        ${wallSec.toFixed(2)}s`)
console.log(`throughput:  ${(TOTAL / wallSec).toFixed(0)} req/s`)
console.log(`latency ms:  p50 ${pct(sorted, 50).toFixed(1)}  p95 ${pct(sorted, 95).toFixed(1)}  p99 ${pct(sorted, 99).toFixed(1)}  max ${sorted.at(-1).toFixed(1)}`)
console.log(`status:      ${[...codes.entries()].map(([c, n]) => `${c}:${n}`).join('  ')}`)
console.log(`accepted(202): ${codes.get(202) ?? 0}`)
