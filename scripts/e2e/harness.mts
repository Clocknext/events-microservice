/** Shared plumbing for the end-to-end run: assertions, the payments database,
 *  the archive, and posting a signal at the edge. */
import { createHash, randomUUID } from 'node:crypto'
import pg from 'pg'

// `timestamp without time zone` (oid 1114) is parsed by `pg` as LOCAL time. Both
// `SignalLog.receivedAt` and `SignalStatus.receivedAt` hold UTC instants, so on a
// non-UTC machine every comparison against the edge's ISO stamp would be off by
// the local offset — 5.5 hours here. Prisma reads these as UTC; this makes the
// raw driver agree.
pg.types.setTypeParser(1114, (value: string) => new Date(`${value.replace(' ', 'T')}Z`))
import { createClickHouseReader } from '../../src/client/clickhouse.js'
import { loadPaymentsEnv } from './env.mjs'

export const EDGE = process.env.EDGE_URL ?? 'http://127.0.0.1:3000'
export const PAYMENTS = process.env.PAYMENTS_URL ?? 'http://127.0.0.1:3001'

export const env = loadPaymentsEnv()
export const clickhouse = createClickHouseReader()

// ── assertions ───────────────────────────────────────────────────────────────

let passed = 0
const failures: string[] = []
let section = ''

export function heading(title: string): void {
  section = title
  console.log(`\n\x1b[1m${title}\x1b[0m`)
}

export function check(label: string, actual: unknown, expected: unknown): boolean {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) {
    passed += 1
    console.log(`  \x1b[32mPASS\x1b[0m  ${label}`)
  } else {
    failures.push(`${section} :: ${label}`)
    console.log(`  \x1b[31mFAIL\x1b[0m  ${label}`)
    console.log(`        expected ${JSON.stringify(expected)}`)
    console.log(`        actual   ${JSON.stringify(actual)}`)
  }
  return ok
}

export function report(): number {
  console.log(`\n${'─'.repeat(70)}`)
  if (failures.length === 0) {
    console.log(`\x1b[32mAll ${passed} checks passed.\x1b[0m`)
    return 0
  }
  console.log(`\x1b[31m${failures.length} of ${passed + failures.length} checks FAILED:\x1b[0m`)
  for (const f of failures) console.log(`  - ${f}`)
  return 1
}

// ── helpers ──────────────────────────────────────────────────────────────────

export const sha256 = (s: string) => createHash('sha256').update(s).digest('hex')
export const uuid = () => randomUUID()
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Polls until `probe` returns something truthy, or gives up. Returns null on
 *  timeout so a caller can assert on the absence rather than throw. */
export async function waitFor<T>(
  probe: () => Promise<T | null | undefined>,
  { timeoutMs = 20_000, everyMs = 250 } = {},
): Promise<T | null> {
  const until = Date.now() + timeoutMs
  for (;;) {
    const value = await probe()
    if (value) return value
    if (Date.now() > until) return null
    await sleep(everyMs)
  }
}

// ── the edge ─────────────────────────────────────────────────────────────────

export interface EdgeResponse {
  status: number
  json: {
    statusCode?: number
    statusDetail?: { status?: string; message?: string }
    result?: Record<string, unknown> & { signalId?: string; errorReason?: string }
  }
  raw: string
}

/** POSTs to the ingest edge. `body` is sent verbatim when it is a string, so a
 *  case can send malformed JSON on purpose. */
export async function postSignal(
  body: unknown,
  opts: { key?: string | null; contentType?: string | null; path?: string } = {},
): Promise<EdgeResponse> {
  const headers: Record<string, string> = {}
  if (opts.contentType !== null) headers['content-type'] = opts.contentType ?? 'application/json'
  if (opts.key !== null && opts.key !== undefined) headers.authorization = `Bearer ${opts.key}`
  const res = await fetch(`${EDGE}${opts.path ?? '/api/v1/signal'}`, {
    method: 'POST',
    headers,
    body: typeof body === 'string' ? body : JSON.stringify(body),
  })
  const raw = await res.text()
  let json: EdgeResponse['json'] = {}
  try {
    json = JSON.parse(raw) as EdgeResponse['json']
  } catch {
    /* a non-JSON body is itself the finding */
  }
  return { status: res.status, json, raw }
}

/** POSTs straight to settle, bypassing the pipeline — for cases about the route
 *  itself (the batch cap, a spoofed envelope). */
export async function postSettle(payload: unknown): Promise<{ status: number; body: string }> {
  const res = await fetch(`${PAYMENTS}/api/internal/settle`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${env.internalSecret}` },
    body: JSON.stringify(payload),
  })
  return { status: res.status, body: await res.text() }
}

// ── the payments database ────────────────────────────────────────────────────

export class Db {
  private client: pg.Client

  constructor() {
    this.client = new pg.Client({ connectionString: env.databaseUrl })
  }

  async connect(): Promise<void> {
    await this.client.connect()
  }

  async close(): Promise<void> {
    await this.client.end()
  }

  async rows<T extends pg.QueryResultRow>(sql: string, params: unknown[] = []): Promise<T[]> {
    return (await this.client.query<T>(sql, params)).rows
  }

  async one<T extends pg.QueryResultRow>(sql: string, params: unknown[] = []): Promise<T | null> {
    const rows = await this.rows<T>(sql, params)
    return rows[0] ?? null
  }

  async exec(sql: string, params: unknown[] = []): Promise<number> {
    return (await this.client.query(sql, params)).rowCount ?? 0
  }
}

// ── the archive ──────────────────────────────────────────────────────────────

/** Deliberately the same shape as the dispatcher's `SignalLogRow` — the trace
 *  scripts hand these rows straight to `toSettleSignal`, so a column added there
 *  has to be selected here or the run fails at the type level rather than at
 *  three in the morning. */
export interface ArchiveRow {
  signal_id: string
  received_at: string
  api_key_hash: string
  customer_id: string
  organization_id: string
  status: string
  error_code: string
  error_message: string
  payload: string
}

export async function archiveRow(signalId: string): Promise<ArchiveRow | null> {
  const rows = await clickhouse.query<ArchiveRow>(
    `SELECT signal_id, received_at, api_key_hash, customer_id,
            organization_id, status, error_code, error_message, payload
       FROM signal_log WHERE signal_id = {id:String}
      ORDER BY version DESC LIMIT 1`,
    { id: signalId },
  )
  return rows[0] ?? null
}

/** How many PHYSICAL rows the archive holds for a signal — pre-merge duplicates
 *  included. `LIMIT 1 BY` in the dispatcher is what makes >1 harmless. */
export async function archiveRowCount(signalId: string): Promise<number> {
  const rows = await clickhouse.query<{ n: string }>(
    `SELECT count() AS n FROM signal_log WHERE signal_id = {id:String}`,
    { id: signalId },
  )
  return Number(rows[0]?.n ?? 0)
}
