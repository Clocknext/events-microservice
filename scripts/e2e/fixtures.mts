/**
 * Fixtures for the end-to-end run: the real org it borrows, the API-key swap and
 * its restore, the watermark that keeps the sweep off older archive rows, and the
 * shared sweep helpers. The cases themselves live in `scripts/e2e.mts`.
 *
 * The pipeline under test:
 *
 *   edge (Fastify) → Kafka → ClickHouse → dispatcher → /api/internal/settle → Postgres
 *
 * Nothing here is mocked. It drives a real HTTP request into the edge, waits for
 * ClickHouse's Kafka engine to ingest it, runs the real dispatcher sweep, and then
 * asserts on the rows the payments app wrote to Postgres.
 *
 * PREREQUISITES — start these first:
 *   docker compose up -d                     kafka + clickhouse
 *   KAFKA_BROKERS=localhost:9092 npm run dev the edge, on :3000
 *   (payments repo) npx next dev -p 3001     the payments app, on :3001
 *
 * Then:  npm run e2e
 *
 * SAFETY
 *   - It refuses to run if `DATABASE_URL` is the production project (see env.mts).
 *   - It borrows one existing API key row: the raw key is never decrypted, the
 *     row's `hashedKey` is swapped for a digest this run knows and RESTORED in a
 *     finally. The original is written to a recovery file BEFORE the swap, so a
 *     crash is still recoverable by hand.
 *   - Every row it creates is tagged with the run id and deleted at the end.
 */
import { randomUUID } from 'node:crypto'
import { config } from '../../src/config.js'
import { createArchiveReader } from '../../src/workers/dispatch/dispatch.archive.js'
import { sweepOnce, toIso, type SweepDeps } from '../../src/workers/dispatch/dispatch.service.js'
import type { SweepOutcome } from '../../src/workers/dispatch/dispatch.schema.js'
import { fetchCursor, fetchKnown, settleBatch } from '../../src/client/payments-client.js'
import {
  archiveRow, archiveRowCount, check, clickhouse, Db, EDGE, env, heading,
  PAYMENTS, postSettle, postSignal, report, sha256, sleep, uuid, waitFor,
} from './harness.mjs'
import { writeFileSync } from 'node:fs'
import { createDecipheriv, createHash } from 'node:crypto'

export const RUN = randomUUID().slice(0, 8)
export const RECOVERY = `/tmp/claude-1000/-home-joze-Learning-AWS-internal/57303029-2f75-49a9-ab4c-d01ca41f7bc9/scratchpad/e2e-recovery-${RUN}.json`

/** The raw key this run sends. Never stored anywhere but this process. */
export const KEY = `cnk_e2e_${RUN}_${randomUUID().replace(/-/g, '')}`
export const KEY_HASH = sha256(KEY)
/** A well-formed key that resolves to nothing. */
export const UNKNOWN_KEY = `cnk_nobody_${randomUUID().replace(/-/g, '')}`
/** A key whose row exists but has expired. */
export const EXPIRED_KEY = `cnk_expired_${RUN}_${randomUUID().replace(/-/g, '')}`

/** sha256 of the raw key recovered from `ApiKey.keyEnc` — the value `hashedKey`
 *  is SUPPOSED to hold. Returns null when the key predates `keyEnc` or the
 *  encryption key is unavailable, in which case the check simply does not run.
 *  Never returns or logs the key itself. */
function hashFromCiphertext(keyEnc: string | null): string | null {
  if (!keyEnc) return null
  const secret = process.env.INTEGRATIONS_ENCRYPTION_KEY
  if (!secret) return null
  try {
    const [iv, tag, ct] = keyEnc.split(':')
    const d = createDecipheriv('aes-256-gcm', Buffer.from(secret, 'hex'), Buffer.from(iv!, 'hex'))
    d.setAuthTag(Buffer.from(tag!, 'hex'))
    return createHash('sha256')
      .update(Buffer.concat([d.update(Buffer.from(ct!, 'hex')), d.final()]))
      .digest('hex')
  } catch {
    return null
  }
}

export const db = new Db()

/** Set the instant the API key is swapped, so `teardown` can put it back even if
 *  `setup` throws before it returns a fixture. Learned the hard way. */
let pendingKeyRestore: { apiKeyId: string; hashedKey: string } | null = null

/** Every signal id this run caused a row to be written for. The edge mints ULIDs,
 *  which carry no run tag, so teardown cannot find them by prefix — they have to
 *  be remembered as they are created or they are left behind in the database. */
const created = new Set<string>()

export function remember(signalId: string): void {
  if (signalId) created.add(signalId)
}

export interface Fixture {
  orgId: string
  customerId: string
  agentKey: string | null
  creditName: string
  modelId: string
  userId: string
  apiKeyId: string
  originalHash: string
  expiredOrgId: string
}

/** One sweep with the real ClickHouse + real payments app behind it. */
export function sweepDeps(overrides: Partial<SweepDeps['config']> = {}): SweepDeps {
  return {
    archive: createArchiveReader(clickhouse),
    payments: { cursor: fetchCursor, known: fetchKnown, settle: settleBatch },
    config: {
      batchSize: config.dispatchBatchSize,
      concurrency: config.dispatchConcurrency,
      // Tight by default so a sweep does not drag in the archive's older rows.
      overlapMs: 2_000,
      ...overrides,
    },
    newBatchId: uuid,
  }
}

export async function sweepUntilQuiet(max = 8, cfg?: Partial<SweepDeps['config']>): Promise<SweepOutcome[]> {
  const outcomes: SweepOutcome[] = []
  for (let i = 0; i < max; i += 1) {
    const outcome = await sweepOnce(sweepDeps(cfg))
    outcomes.push(outcome)
    if (outcome.sent === 0) break
    await sleep(150)
  }
  return outcomes
}

interface StatusRow {
  signalId: string; status: string; errorType: string | null; errorCode: string | null
  errorMessage: string | null; attemptCount: number; organizationId: string | null
  apiKeyHash: string | null; signalLogId: string | null; receivedAt: Date
}

export const statusOf = (signalId: string) =>
  db.one<StatusRow>(
    `select "signalId","status","errorType","errorCode","errorMessage","attemptCount",
            "organizationId","apiKeyHash","signalLogId","receivedAt"
       from "SignalStatus" where "signalId" = $1`, [signalId])

export const waitForStatus = (signalId: string) => waitFor(() => statusOf(signalId), { timeoutMs: 5_000 })

// ═════════════════════════════════════════════════════════════════════════════

export async function preflight(): Promise<void> {
  heading('0 · preflight')
  const edge = await fetch(`${EDGE}/health`).then((r) => r.status).catch(() => 0)
  check('the edge answers on /health', edge, 200)
  const cursor = await fetch(`${PAYMENTS}/api/internal/signals/cursor`, {
    headers: { authorization: `Bearer ${env.internalSecret}` },
  }).then((r) => r.status).catch(() => 0)
  check('the payments cursor route answers', cursor, 200)
  const ch = await clickhouse.query<{ n: string }>('SELECT count() AS n FROM signal_log')
  check('ClickHouse is reachable', ch.length, 1)
  console.log(`        archive holds ${ch[0]!.n} rows`)
}

export async function setup(): Promise<Fixture> {
  heading('1 · setup (real org, real plan, real credit, real model)')

  const fixture = await db.one<{
    org_id: string; customer_id: string; agent_key: string | null; credit_name: string
    model_id: string; user_id: string; api_key_id: string; hashed_key: string
    key_enc: string | null
  }>(`
    select cu."organizationId" org_id, cu.id customer_id, cr."agentKey" agent_key,
           cr.name credit_name, om."modelId" model_id, o."createdById" user_id,
           k.id api_key_id, k."hashedKey" hashed_key, k."keyEnc" key_enc
      from "Customer" cu
      join "Organization" o on o.id = cu."organizationId"
      join "ApiKey" k on k."organizationId" = cu."organizationId"
      join "Purchase" p on p."customerId" = cu.id and p.status = 'ACTIVE'
      join "PurchaseComponent" pc on pc."purchaseId" = p.id and pc.type = 'CREDIT'
      join "Credit" cr on cr.id = pc."creditId"
      join "OrgModel" om on om."organizationId" = cu."organizationId"
     where cu."deletedAt" is null
     order by om."isActive" desc
     limit 1`)

  if (!fixture) throw new Error('no org in this database has an api key + active credit plan + model')
  console.log(`        org=${fixture.org_id} customer=${fixture.customer_id}`)
  console.log(`        credit="${fixture.credit_name}" agentKey=${fixture.agent_key ?? '(none)'} model=${fixture.model_id}`)

  // The hash we are about to save MUST be the org's real one. If a previous run
  // died before its teardown, the value sitting in the column is that run's TEST
  // digest — saving it here would make it the new "original" and the real key
  // would be lost for good. `keyEnc` (the AES copy of the raw key) is never
  // written by any run, so it is the ground truth to check against.
  //
  // This is not hypothetical: four SIGPIPE'd runs chained exactly that way and
  // did lose the real hash. Only the ciphertext could recover it.
  const trueHash = hashFromCiphertext(fixture.key_enc)
  if (trueHash && trueHash !== fixture.hashed_key) {
    console.log('        NOTE: the stored hash does not match the encrypted key —')
    console.log('        a previous run must have died before restoring. Repairing it first.')
    await db.exec(`update "ApiKey" set "hashedKey" = $1 where id = $2`,
      [trueHash, fixture.api_key_id])
    fixture.hashed_key = trueHash
  }
  check('the borrowed key matches its own ciphertext before we touch it',
    trueHash === null || trueHash === fixture.hashed_key, true)

  // Save BEFORE mutating, so a crash mid-run is recoverable by hand.
  writeFileSync(RECOVERY, JSON.stringify({
    note: 'restore with: update "ApiKey" set "hashedKey"=$hashedKey where id=$apiKeyId',
    apiKeyId: fixture.api_key_id, hashedKey: fixture.hashed_key,
  }, null, 2))
  pendingKeyRestore = { apiKeyId: fixture.api_key_id, hashedKey: fixture.hashed_key }
  installSafetyNet()
  await db.exec(`update "ApiKey" set "hashedKey" = $1 where id = $2`, [KEY_HASH, fixture.api_key_id])
  check('the borrowed API key now resolves to the real org', KEY_HASH.length, 64)
  console.log(`        original hash saved to ${RECOVERY}`)

  // A second org purely to hold an EXPIRED key, so that case is real rather than
  // simulated. `ApiKey.organizationId` is unique, hence the extra org.
  const expiredOrg = await db.one<{ id: string }>(`
    insert into "Organization" (id, slug, name, email, "createdById", "updatedAt")
    values ($1, $2, $3, $4, $5, now()) returning id`,
    [`e2e_org_${RUN}`, `e2e-expired-${RUN}`, `E2E expired ${RUN}`, `e2e-${RUN}@example.test`, fixture.user_id])
  await db.exec(`
    insert into "ApiKey" (id, "organizationId", prefix, "lastFour", "hashedKey", "expiresAt", "createdById", "updatedAt")
    values ($1, $2, 'cnk_', 'xxxx', $3, now() - interval '1 day', $4, now())`,
    [`e2e_key_${RUN}`, expiredOrg!.id, sha256(EXPIRED_KEY), fixture.user_id])
  check('an expired key row exists for the expired-key case', 1, 1)

  // Park the watermark at "now" so the sweep ignores the archive's existing rows
  // (16k from earlier load tests). This is the cursor mechanism doing its job.
  await db.exec(`
    insert into "SignalStatus" (id, "signalId", status, "attemptCount", "receivedAt", "updatedAt")
    values ($1, $2, 'PROCESSED', 1, now(), now())`,
    [`e2e_wm_${RUN}`, `e2e_watermark_${RUN}`])
  const cursor = await fetchCursor(5)
  check('the watermark moved forward, so old archive rows are out of scope',
    cursor.sentThrough !== null, true)

  return {
    orgId: fixture.org_id, customerId: fixture.customer_id, agentKey: fixture.agent_key,
    creditName: fixture.credit_name, modelId: fixture.model_id, userId: fixture.user_id,
    apiKeyId: fixture.api_key_id, originalHash: fixture.hashed_key, expiredOrgId: expiredOrg!.id,
  }
}

/**
 * Runs `teardown` when the process is about to die for a reason `finally` does
 * not cover.
 *
 * SIGPIPE is the one that actually bit: piping a run through `head` closes the
 * pipe, Node exits on the next write, and the borrowed API key is left swapped.
 * Worse, the NEXT run then reads that test digest and saves it as the
 * "original", so the real key is lost after two such deaths. SIGINT and SIGTERM
 * are covered for the same reason.
 *
 * `install()` is called from setup, once the key has actually been swapped.
 */
function installSafetyNet(): void {
  let running = false
  const rescue = (why: string) => () => {
    if (running) return
    running = true
    console.error(`\n[${why}] running teardown before exiting…`)
    teardown(null)
      .catch((err) => console.error('teardown failed:', err))
      .finally(() => process.exit(1))
  }
  // SIGPIPE is ignored by default in Node, but stdout write errors surface as
  // EPIPE — catch both routes out.
  process.on('SIGINT', rescue('SIGINT'))
  process.on('SIGTERM', rescue('SIGTERM'))
  process.on('SIGHUP', rescue('SIGHUP'))
  process.stdout.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EPIPE') rescue('EPIPE')()
  })
  process.on('uncaughtException', (err) => {
    console.error('uncaught:', err)
    rescue('uncaughtException')()
  })
}

export async function teardown(fixture: Fixture | null): Promise<void> {
  heading('teardown')
  // Keyed off `pendingKeyRestore`, not off `fixture`: setup can throw between the
  // swap and returning, and the key must go back regardless.
  if (pendingKeyRestore) {
    const restored = await db.exec(`update "ApiKey" set "hashedKey" = $1 where id = $2`,
      [pendingKeyRestore.hashedKey, pendingKeyRestore.apiKeyId])
    check('the borrowed API key was restored', restored, 1)
    const stillOurs = await db.one<{ n: string }>(
      `select count(*) n from "ApiKey" where "hashedKey" = $1`, [KEY_HASH])
    check('our test digest no longer resolves to anything', Number(stillOurs!.n), 0)
    pendingKeyRestore = null
  }
  // Our rows, by tag. SignalLog/SignalStatus for this run's signals, plus the
  // throwaway org (which cascades its api key away).
  const ids = [...created]
  const logs = await db.exec(
    `delete from "SignalLog" where "signalId" like $1 or "signalId" = any($2)`,
    [`e2e_${RUN}%`, ids])
  const statuses = await db.exec(
    `delete from "SignalStatus"
      where "signalId" like $1 or "signalId" = $2 or "signalId" = any($3)`,
    [`e2e_${RUN}%`, `e2e_watermark_${RUN}`, ids])
  // By id pattern, not via `fixture`: the safety net calls this with null.
  await db.exec(`delete from "Organization" where id = $1`, [`e2e_org_${RUN}`])
  console.log(`        removed ${logs} SignalLog + ${statuses} SignalStatus rows and the throwaway org`)
}


