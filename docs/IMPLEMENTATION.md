# The signal pipeline — implementation and manual runbook

How a signal travels from a customer's HTTP request to a row of money in
Postgres, what each component is responsible for, and how to drive and verify
every step **by hand**, with no test scripts.

---

## 1. The flow

```
    customer
       │  POST /api/v1/signal      Authorization: Bearer cnk_…
       ▼
┌──────────────┐
│  EDGE        │  gate: 3 required fields, an API key must be PRESENT
│  (Fastify)   │  stamps  signalId (ULID) · receivedAt (ISO) · apiKeyHash (SHA-256)
└──────┬───────┘  produces to Kafka, THEN answers 202
       │
       ▼  topic `signals`, keyed by customerId
┌──────────────┐
│  KAFKA       │  the durable log. RF 3, 7-day retention.
└──────┬───────┘
       │  ClickHouse pulls it itself — no consumer process
       ▼
┌──────────────┐
│  CLICKHOUSE  │  kafka_signals (ENGINE=Kafka) ──MV──▶ signal_log
│  the archive │  ReplacingMergeTree, ONE writer (the MV), read-only to all else
└──────┬───────┘
       │  the dispatcher reads it
       ▼
┌──────────────┐   ① GET  /api/internal/signals/cursor   what is left to do?
│  DISPATCHER  │   ② SELECT … FROM signal_log            candidates
│  (Node loop) │   ③ POST /api/internal/signals/known    drop the already-settled
└──────┬───────┘   ④ POST /api/internal/settle           in batches of 500, 2 at a time
       │
       ▼
┌──────────────┐
│  PAYMENTS    │  prices the signal, writes SignalLog (money)
│  (Next.js)   │  upserts SignalStatus (lifecycle — drives the UI AND the dispatcher)
└──────────────┘
```

### Who owns what

| Component | Owns | Deliberately does NOT |
| --- | --- | --- |
| Edge | identity (`signalId`, `receivedAt`, `apiKeyHash`), the shape gate | resolve the key, know the pricing rulebook, touch ClickHouse |
| Kafka | durability and replay | anything else — it is one topic |
| ClickHouse | the verbatim archive | record outcomes (it has no row updates) |
| Dispatcher | carrying archive → settle, at least once | decide anything about money |
| Payments | pricing, Postgres, the lifecycle record | know that Kafka or ClickHouse exist |

### The one property everything rests on

`SignalLog.signalId` is `UNIQUE`, and `settleSignal` passes it as the idempotency
key:

```ts
// payments · src/lib/settle/settle-batch.ts
result = await settleSignal(db, {
  signalId: signal.signalId,
  receivedAt: signal.receivedAt,
  organizationId,
  idempotencyKey: signal.signalId,   // ← the signal IS its own idempotency key
}, signal);
```

So **sending the same signal twice cannot charge twice** — the second one replays
onto the original money row. That single fact is why the dispatcher needs no
claim, no lease, no visibility timeout and no cursor table: it only has to
guarantee *at least once*, and over-sending is free.

---

## 2. The code, step by step

### 2.1 The edge stamps an identity that a caller cannot forge

The id and time are stamped app-wide, before anything can fail, so even a 404 is
quotable:

```ts
// edge · src/plugins/identity.ts
app.addHook('onRequest', async (request) => {
  request.signalId = ulid()                    // sorts by creation time
  request.receivedAt = new Date().toISOString()
})
```

The key digest is taken in the one hook that already holds the raw token, so
nothing downstream ever sees the key:

```ts
// edge · src/modules/signal/signal.module.ts
app.addHook('onRequest', async (request) => {
  const rawKey = extractBearer(request.headers.authorization)
  if (!rawKey) throw new UnauthorizedError('Missing API key. …', 'API_KEY_MISSING')
  request.apiKeyHash = digestApiKey(rawKey)    // sha256 hex, 64 chars
})
```

`digestApiKey` produces exactly what `ApiKey.hashedKey` stores upstream, so
settle resolves the organisation with one unique-index hit.

### 2.2 The message: envelope outside, body untouched

```ts
// edge · src/modules/signal/signal.schema.ts
export interface SignalMessage {
  signalId: string
  receivedAt: string
  apiKeyHash: string
  body: SignalBody      // ← the caller's bytes, verbatim
}
```

`apiKeyHash` rides on the **envelope**, never inside `body`. `body` becomes
`signal_log.payload`, which is the archive of what the caller actually sent;
putting our own fields in it would corrupt that record.

### 2.3 Produce before acknowledging

```ts
// edge · src/modules/signal/signal.controller.ts
if (producer) {
  try {
    await producer.send(message)
  } catch (err) {
    throw new BadGatewayError('Could not accept the signal. Retry shortly.', 'QUEUE_UNAVAILABLE', …)
  }
}
return reply.status(202).send(…)
```

Order matters: a crash between the 202 and the produce would lose a billable
signal the caller was told was safe.

### 2.4 ClickHouse ingests itself

```sql
-- edge · docker/clickhouse/init/01-schema.sql
CREATE MATERIALIZED VIEW signals.signal_log_mv TO signals.signal_log AS
SELECT
  JSONExtractString(raw, 'signalId')                              AS signal_id,
  parseDateTime64BestEffort(JSONExtractString(raw, 'receivedAt'))  AS received_at,
  JSONExtractString(raw, 'apiKeyHash')                            AS api_key_hash,
  JSONExtractRaw(raw, 'body')                                     AS payload
FROM signals.kafka_signals;
```

`JSONAsString` puts each whole message into one `raw` column, so the body
survives with its nesting intact. The Kafka engine commits offsets only after
this insert lands — that is what makes ingestion at-least-once.

### 2.5 The dispatcher sweep

The whole of it, as a pure function over two ports:

```ts
// edge · src/workers/dispatch/dispatch.service.ts
export async function sweepOnce(deps: SweepDeps): Promise<SweepOutcome> {
  const cursor = await payments.cursor(config.batchSize)

  // ① retries first — named explicitly, so a flood of new work cannot crowd
  //    them out, and one ancient failure cannot drag the scan back with it
  for (const row of await archive.readByIds([...attemptOf.keys()])) { … }

  // ② then new work, from the watermark MINUS an overlap window
  const since = overlapFrom(cursor.sentThrough, config.overlapMs)
  const candidates = await archive.readNewer(since, room)

  // ③ drop anything already recorded, or the overlap would re-settle
  //    the same signals on every sweep and the pipeline would never go quiet
  const known = new Set(await payments.known(candidates.map((r) => r.signal_id)))

  // ④ settle, batchSize at a time, concurrency in flight
  await pool(batches.map((batch, i) => () => payments.settle(batchIds[i], batch)), concurrency)
}
```

**Why the overlap window.** ClickHouse's Kafka engine flushes in batches and
several edge instances stamp `receivedAt` from their own clocks, so a row can
appear with a timestamp just *below* one already seen. Reading strictly forward
from a watermark would lose it, permanently and silently. Reading back
`DISPATCH_OVERLAP_MS` catches it; step ③ is what stops that overlap becoming an
infinite re-settle loop.

**The timestamp conversion that prevents mis-billing:**

```ts
export function toIso(clickhouseTimestamp: string): string {
  const trimmed = clickhouseTimestamp.trim()
  if (trimmed === '') return trimmed
  if (trimmed.includes('T') && /(Z|[+-]\d{2}:?\d{2})$/.test(trimmed)) return trimmed
  return `${trimmed.replace(' ', 'T')}Z`
}
```

ClickHouse renders `DateTime64(3)` as `2026-08-26 10:00:00.000` — a space, no
zone. It **is** UTC, but `new Date()` parses that as *local* time, so a
dispatcher in IST would bill every signal 5½ hours early. Billing windows cut on
`receivedAt`, so this one line is load-bearing.

**The caller cannot forge the envelope:**

```ts
export function toSettleSignal(row: SignalLogRow, attempt: number): SettleSignal | null {
  …
  delete payload.organizationId          // caller-controlled — never forwarded
  return {
    ...payload,                          // spread FIRST
    signalId: row.signal_id,             // …then written over
    receivedAt: toIso(row.received_at),
    apiKeyHash: row.api_key_hash,
    attempt,
  }
}
```

### 2.6 The pace is self-adjusting

```ts
// edge · src/workers/dispatch/dispatch.runner.ts
while (!stopping) {
  const outcome = await sweepOnce(deps)
  if (!outcome.saturated) await sleep(config.dispatchIdleMs)   // caught up → nap
  // saturated → straight round again, no sleep
}
```

A full batch means there is probably more behind it, so drain flat out; a short
one means idle cheaply. A cron can do neither — it adds latency when idle and
cannot catch up when behind.

**Batch size buys nothing; concurrency does.** Settle splits a batch across
`INTERNAL_WORKER` workers that each walk their chunk *one signal at a time*, so
a 5,000-signal batch is not faster than ten 500s — it is the same rate with ten
times the blast radius on a timeout. Hence `DISPATCH_BATCH_SIZE=500` (settle's
own enforced cap) and `DISPATCH_CONCURRENCY=2`.

### 2.7 Settle: attribute → price → record

```ts
// payments · src/app/api/internal/settle/route.ts
const { attributed, refused } = await attributeBatch(incoming)   // one query for the batch
const settled = await withSettlePool((db) => settleBatch(db, chunks))
// …reassembled into arrival order…
const recorded = await recordSignalStatuses(results.map((result, index) => ({
  result, facts: factsFrom(incoming[index]),
})))
```

Attribution resolves every distinct `apiKeyHash` in **one** query, and the key
always wins over anything the body claims:

```ts
// payments · src/lib/settle/settle-attribution.ts
// THE KEY WINS — checked BEFORE any `organizationId` the signal carries.
const hash = typeof signal.apiKeyHash === 'string' ? signal.apiKeyHash.trim().toLowerCase() : ''
if (hash === '') { attributed.push({ index, signal }); return }   // no key: trusted caller only
const outcome = resolved.get(hash)
if (!outcome?.ok) { refused.push({ index, signal, error: outcome?.error ?? 'Invalid API key.' }); return }
attributed.push({ index, signal: { ...signal, organizationId: outcome.organizationId } })
```

### 2.8 `SignalStatus` — one table, two jobs

It is the signals-pipeline twin of `RawUsageLog`:

| `/api/v1/usage` | signals pipeline |
| --- | --- |
| `RawUsageLog.payload` | ClickHouse `signal_log.payload` |
| `RawUsageLog.status / errorType / attemptCount` | **`SignalStatus`** |
| `UsageLog` | `SignalLog` |

`RawUsageLog` did two jobs — hold the body and track the lifecycle. The body half
moved to ClickHouse; the lifecycle half cannot, because ClickHouse has no row
updates. So it lives in Postgres, where it serves the Signals UI **and** tells
the dispatcher what is still outstanding, off the same
`@@index([status, attemptCount, receivedAt])` the existing cron sweeper uses.

**FAILED is never written by settle** — mirroring `RawUsageLog`'s documented
rule. A signal that could not settle stays `PENDING` with its reason;
`attemptCount` is what stops the retries, and `FAILED` is reached only by an
explicit admin action.

**Retry policy**, copied from `/api/cron/usage-logs/process`:

| Outcome | Retried? |
| --- | --- |
| never attempted (`errorType` null) | yes — the crash case |
| `SERVER_ERROR` | yes — ours, safe to repeat as-is |
| `USER_ERROR` | **no** — cannot self-heal; waits for a fix and a manual retry |
| `attemptCount >= 5` | no — keeps its last error for inspection |

---

## 3. Configuration

**Edge** (`.env` / environment — nothing loads a file, see `src/config.ts`):

| Var | Default | Notes |
| --- | --- | --- |
| `KAFKA_BROKERS` | *(empty)* | empty = do not produce; a 202 then means only "passed the gate" |
| `KAFKA_TOPIC` | `signals` | |
| `KAFKA_USE_IAM` | `false` | MSK Serverless; token signed from the AWS default chain |
| `BODY_BYTES` | `65536` | over this is a 413 |

**Dispatcher** (same process family, separate entry point):

| Var | Default | Notes |
| --- | --- | --- |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | read-only |
| `PAYMENTS_URL` | `http://127.0.0.1:3001` | |
| `INTERNAL_SETTLE_SECRET` | *(required)* | refuses to start without it |
| `DISPATCH_BATCH_SIZE` | `500` | settle's enforced cap |
| `DISPATCH_CONCURRENCY` | `2` | throughput lever; keep ≤ `INTERNAL_BATCH_CONCURRENCY` |
| `DISPATCH_IDLE_MS` | `1000` | nap only when caught up |
| `DISPATCH_OVERLAP_MS` | `300000` | how far below the watermark to re-read |

---

## 4. Manual runbook — no scripts

Every command below was run to produce the output shown. Substitute your own
values where marked.

### Step 0 · Terminal setup

```bash
EDGE=http://127.0.0.1:3000
PAY=http://127.0.0.1:3001

# The shared secret and the database URL both live in the payments repo's .env.
cd /path/to/Clocknext-Payment-Saas && set -a && . ./.env && set +a && cd -

# psql rejects Prisma's ?pgbouncer=true, so strip the query string.
PG="${DATABASE_URL%%\?*}"
```

### Step 1 · Bring up Kafka and ClickHouse

```bash
docker compose up -d
docker compose ps          # both must read "healthy"
```

### Step 2 · Apply the ClickHouse schema

On a **fresh** volume `docker/clickhouse/init/01-schema.sql` runs automatically.
On one that already holds rows, the init script is skipped — run the migration
instead:

```bash
docker exec -i clickhouse clickhouse-client --multiquery \
  < docker/clickhouse/migrations/001_api_key_hash.sql

docker exec clickhouse clickhouse-client -q "DESCRIBE TABLE signals.signal_log"
```

Expect `signal_id · received_at · api_key_hash · payload · ingested_at`.

### Step 3 · Start the edge and send a signal

```bash
KAFKA_BROKERS=localhost:9092 npm run dev     # in another terminal

KEY=cnk_manual_demo_key
curl -s -X POST $EDGE/api/v1/signal \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $KEY" \
  -d '{"customerId":"cus_demo","inputTokens":1200,"outputTokens":350,
       "type":"credit","model":"anthropic/claude-sonnet-4.5"}' | jq .
```

```json
{
  "statusCode": 202,
  "statusDetail": { "status": "SUCCESS", "message": "Signal accepted for processing." },
  "result": {
    "signalId": "01M0XCANFDVKBSB3R319GDBZMR",
    "receivedAt": "2026-08-25T21:13:26.253Z",
    "accepted": true
  }
}
```

Keep the id:

```bash
SIG=01M0XCANFDVKBSB3R319GDBZMR
```

### Step 4 · Watch ClickHouse pull it off Kafka

Nothing pushes it — the Kafka table engine consumes on its own, typically within
a second or two.

```bash
docker exec clickhouse clickhouse-client -q "
  SELECT signal_id, received_at, api_key_hash, payload
    FROM signals.signal_log WHERE signal_id = '$SIG' FORMAT Vertical"
```

```
signal_id:    01M0XCANFDVKBSB3R319GDBZMR
received_at:  2026-08-25 21:13:26.253
api_key_hash: d098bc2edf4e53c7c44d9215b57a273b82e933f06ec6334a7a6b13e3f49e5c1e
payload:      {"customerId":"cus_demo","inputTokens":1200,"outputTokens":350,…}
```

Two things to confirm by eye:

```bash
# the digest is sha256 of the raw key — and the raw key is nowhere in the row
printf '%s' "$KEY" | sha256sum
docker exec clickhouse clickhouse-client -q "
  SELECT count() FROM signals.signal_log WHERE position(payload, '$KEY') > 0"   # → 0
```

### Step 5 · Start payments and ask what is outstanding

```bash
cd /path/to/Clocknext-Payment-Saas && npx next dev -p 3001    # another terminal

curl -s -H "authorization: Bearer $INTERNAL_SETTLE_SECRET" \
  "$PAY/api/internal/signals/cursor?retryLimit=5" | jq -c .result
```

```json
{"sentThrough":null,"retry":[],"maxAttempts":5}
```

`sentThrough: null` means nothing has ever settled, so the dispatcher will sweep
the archive from the beginning.

### Step 6 · Settle it — either way

**6a · Let the dispatcher do it** (this is the production path):

```bash
CLICKHOUSE_URL=http://127.0.0.1:8123 \
PAYMENTS_URL=$PAY \
INTERNAL_SETTLE_SECRET=$INTERNAL_SETTLE_SECRET \
npm run dispatch
```

It logs one JSON line per sweep. Ctrl-C when the sweeps go quiet.

**6b · Or do it by hand**, to see each call. Build the batch straight out of
ClickHouse — this is exactly what `toSettleSignal` does:

```bash
BODY=$(docker exec clickhouse clickhouse-client -q "
  SELECT payload, signal_id,
         formatDateTime(received_at,'%Y-%m-%dT%H:%i:%S','UTC') AS ts,
         toString(toUnixTimestamp64Milli(received_at) % 1000)  AS ms,
         api_key_hash
    FROM signals.signal_log WHERE signal_id='$SIG' FORMAT JSONEachRow" \
 | jq -c '{batchId:"manual-1", signals:[
     (.payload|fromjson|del(.organizationId))
     + {signalId:.signal_id,
        receivedAt:(.ts+"."+(("00"+(.ms))[-3:])+"Z"),
        apiKeyHash:.api_key_hash,
        attempt:1} ]}')

echo "$BODY" | jq .
```

Note the `del(.organizationId)` — the payload is caller-controlled, and a
body-supplied organisation id must never be forwarded.

```bash
curl -s -X POST $PAY/api/internal/settle \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $INTERNAL_SETTLE_SECRET" \
  -d "$BODY" | jq '{total:.result.total, processed:.result.processed,
                    recorded:.result.recorded,
                    first:(.result.signals[0]|{signal_id,status,error_type,error_code,error_message})}'
```

```json
{
  "total": 1,
  "processed": 0,
  "recorded": { "created": 1, "updated": 0, "skipped": 0, "failed": 0 },
  "first": {
    "signal_id": "01M0XCANFDVKBSB3R319GDBZMR",
    "status": "PENDING",
    "error_type": "USER_ERROR",
    "error_code": "VALIDATION_FAILED",
    "error_message": "Invalid API key."
  }
}
```

`cnk_manual_demo_key` is not a real key, so it is refused at attribution — which
is the correct outcome and a good demonstration. For a `PROCESSED` result, send a
key whose SHA-256 exists in `ApiKey.hashedKey`, for a customer with an active
plan that meters the credit named by `agentKey`.

`recorded` is the bookkeeping outcome. A non-zero `failed` means those signals
settled but their status row did not land — harmless (they will be re-sent) but
worth alerting on.

### Step 7 · Verify in Postgres

```bash
psql "$PG" -x -c "select \"signalId\", status, \"errorType\", \"errorCode\",
                         \"errorMessage\", \"attemptCount\", \"organizationId\",
                         \"signalLogId\"
                    from \"SignalStatus\" where \"signalId\" = '$SIG'"
```

```
signalId       | 01M0XCANFDVKBSB3R319GDBZMR
status         | PENDING
errorType      | USER_ERROR
errorCode      | VALIDATION_FAILED
errorMessage   | Invalid API key.
attemptCount   | 1
organizationId |                      ← NULL: never attributed
signalLogId    |                      ← NULL: no money row was written
```

For a settled signal, `status = PROCESSED`, `signalLogId` points at the money
row, and:

```bash
psql "$PG" -x -c "select \"signalId\", \"organizationId\", \"customerId\",
                         \"creditsUsed\", \"customerCost\", \"receivedAt\"
                    from \"SignalLog\" where \"signalId\" = '$SIG'"
```

`receivedAt` must equal the edge's stamp to the millisecond. If it is off by your
local UTC offset, the ClickHouse timestamp conversion has regressed.

### Step 8 · Confirm the pipeline goes quiet

```bash
curl -s -X POST $PAY/api/internal/signals/known \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $INTERNAL_SETTLE_SECRET" \
  -d "{\"signalIds\":[\"$SIG\"]}" | jq -c .result
```

```json
{"known":["01M0XCANFDVKBSB3R319GDBZMR"]}
```

Once a signal is `known`, the dispatcher stops re-sending it even though the
overlap window still re-reads it. That is what makes a caught-up pipeline idle.

### Step 9 · Prove idempotency by hand

Re-POST the exact same `$BODY` from step 6b, then count:

```bash
curl -s -X POST $PAY/api/internal/settle -H 'content-type: application/json' \
  -H "authorization: Bearer $INTERNAL_SETTLE_SECRET" -d "$BODY" > /dev/null

psql "$PG" -t -c "select count(*), coalesce(sum(\"creditsUsed\"),0)
                    from \"SignalLog\" where \"signalId\" = '$SIG'"
```

The count must stay at 1 and the credits must not move, however many times you
send it.

### Step 10 · The rejections, by hand

```bash
# 401 — no key
curl -s -X POST $EDGE/api/v1/signal -H 'content-type: application/json' \
  -d '{"customerId":"c","inputTokens":1,"outputTokens":1}' | jq -c .result.errorReason

# 400 — AJV coercion is OFF, so a stringified number is refused
curl -s -X POST $EDGE/api/v1/signal -H 'content-type: application/json' \
  -H "authorization: Bearer $KEY" \
  -d '{"customerId":"c","inputTokens":"1200","outputTokens":1}' | jq -c .result

# 415 — wrong content type      413 — body over BODY_BYTES
# 404 — unknown path            400 — malformed JSON / empty body
```

Then confirm none of them reached the archive:

```bash
docker exec clickhouse clickhouse-client -q "SELECT count() FROM signals.signal_log"
```

The count must not have moved. A refused signal is a plain rejection, never a
queued 202.

### Step 11 · Clean up a manual experiment

```bash
psql "$PG" -c "delete from \"SignalStatus\" where \"signalId\" = '$SIG'"
psql "$PG" -c "delete from \"SignalLog\"    where \"signalId\" = '$SIG'"
docker exec clickhouse clickhouse-client -q "
  ALTER TABLE signals.signal_log DELETE WHERE signal_id = '$SIG'"   # async mutation
```

---

## 5. Automated equivalents

| Command | Where | What |
| --- | --- | --- |
| `npm test` | edge | 47 unit tests — the gate, the envelope, the sweep |
| `npm run e2e` | edge | 110 checks, real Kafka + ClickHouse + Postgres |
| `npm run verify:signal-status` | payments | 15 checks on attribution + the status writer |
| `npm run dispatch` | edge | the dispatcher itself |

`npm run e2e` needs the three services from steps 1-5 already running. It
borrows a real API key row, swaps its hash for one it knows, and restores it in a
`finally`; the original is written to a recovery file *before* the swap. It
refuses to run against `PRODUCTION_DATABASE_URL`.
