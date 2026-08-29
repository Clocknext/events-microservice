# Bugs found and changes made

Work log for the signals pipeline: what was built, what broke, and what the
breakage taught. Every bug below was found by running the thing, not by reading
it — most only appeared once real Kafka, real ClickHouse and a real Postgres were
in the loop, one only once the dispatcher ran as an actual process rather than a
function call, and the last one only when someone else followed the setup
instructions on a machine that already had data.

Verification at the end: **49** unit tests, **111** end-to-end checks against
live infrastructure, **15** payments-side checks, and a live run of the
dispatcher loop itself. All green, with the database left as it was found.

> **Read this first if you are here for the current design.** This is a work log,
> not a description of the system as it stands. The dispatcher was rewritten to
> the 1.0 model — a one-shot on a 60s timer, reading a time window over
> `ingested_at`, with no watermark, no cursor route, no `known` filter and no
> paging. **BUG-3, BUG-7 and BUG-8 are bugs in machinery that no longer exists.**
> They are kept because each one records *why* a shape was chosen, and the reasons
> outlived the code: BUG-3 is why the current design needs settle to dedup rather
> than the dispatcher to filter, BUG-7 is why nothing here pages, and BUG-8 is why
> a backfill is now an explicit bounded request. FINDING-9 is the one that governs
> the current model. Current design: [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Part 1 — Bugs

### BUG-1 · Cross-tenant billing through a body-supplied `organizationId`
**Severity: critical.** Introduced by me, in the first cut of attribution.

A signal's fields come from the caller's own request body, which the edge
archives verbatim. Attribution trusted a body-supplied `organizationId` and used
it *instead of* looking up the API key:

```ts
// BEFORE — settle-attribution.ts
if (typeof signal.organizationId === "string" && signal.organizationId !== "") {
  attributed.push({ index, signal });   // ← trusted, key never consulted
  return;
}
```

And the dispatcher forwarded it, because `toSettleSignal` overrode `signalId`,
`receivedAt`, `apiKeyHash` and `attempt` — but not `organizationId`:

```ts
// BEFORE — dispatch.service.ts
return { ...payload, signalId: row.signal_id, receivedAt: …, apiKeyHash: …, attempt }
//        ^^^^^^^^^ organizationId came through untouched
```

So any customer could add one line to their request body:

```json
{ "customerId": "…", "inputTokens": 1200, "organizationId": "<another workspace>" }
```

and have their usage recorded against a workspace they do not own.

**How it surfaced.** The E2E "a caller cannot forge the envelope" case sent
`organizationId: "org_someone_else"`. That id does not exist, so it tripped the
foreign key rather than silently cross-billing — which is the only reason it was
visible at all. With a *real* org id it would have succeeded.

**Fix.** Two layers.

```ts
// settle-attribution.ts — THE KEY WINS, checked before any organizationId
const hash = typeof signal.apiKeyHash === "string" ? signal.apiKeyHash.trim().toLowerCase() : "";
if (hash === "") {
  // No key at all: a directly-supplied organizationId is honoured ONLY here —
  // the pre-attribution contract, for a trusted caller that is not the edge.
  attributed.push({ index, signal });
  return;
}
```

```ts
// dispatch.service.ts — the dispatcher never forwards the claim at all
delete payload.organizationId
```

`apiKeyHash` is stamped by the edge from the `Authorization` header and cannot be
written from the body, which makes it the only trustworthy claim of identity on
this path.

**Regression test.** `npm run verify:signal-status`:
`a body-supplied organizationId NEVER overrides the API key`, and
`a dead key is refused even when the body names a real org`.

---

### BUG-2 · One bad row silently cost a whole batch its bookkeeping
**Severity: high — a liveness failure that never stops.**

`createMany` is all-or-nothing. One row with a stale `organizationId` failed the
insert for all 124 signals in the batch:

```
Foreign key constraint violated on: `SignalStatus_organizationId_fkey`
```

Because `recordSignalStatuses` fails soft (deliberately — the money is already
committed, so a bookkeeping error must not be reported as a settlement failure),
the batch answered 200 with correct money rows and **zero status rows**. Then:

> no status rows → the watermark never advances → the dispatcher re-reads the
> same window → re-sends the same 124 signals → forever.

The E2E run showed it plainly: `20 sweeps; batch sizes 124 + 124 + 124 …`. The
money was right every time; the pipeline just never finished.

**Fix.** Fall back to per-row inserts so one bad row is isolated:

```ts
} catch (error) {
  // `createMany` is ALL OR NOTHING … with no status rows the dispatcher's
  // watermark never advances, so it re-reads and re-sends the same signals
  // forever. Falling back to one insert per row lands the good ones.
  const each = await Promise.allSettled(
    toCreate.map((row) => prisma.signalStatus.create({ data: row })),
  );
  each.forEach((outcome, i) => { … });
}
```

**Lesson.** A fail-soft path needs a blast radius as small as the failure. Fail
soft *per row*, not per batch.

---

### BUG-3 · The overlap window re-settled everything, forever
**Severity: high — correct, and yet unusable in production.**

The dispatcher deliberately re-reads *below* its watermark, because ClickHouse's
Kafka engine flushes in batches and several edge instances stamp `receivedAt`
from their own clocks — so a row can appear with a timestamp just under one
already seen. Without that overlap, a late arrival is lost silently and
permanently.

But nothing filtered the re-read. Every signal inside the window was re-sent on
every sweep. Settle dedups on `signalId`, so no money moved twice — it was
*correct*. It was also ruinous: at one sweep a second, with a five-minute
production overlap and 10k signals per five minutes, that is 10k signals
re-priced every second, and a caught-up pipeline that never goes idle.

It also corrupted the retry counter: a signal re-settled through the "new work"
path with `attempt: 1` overwrote the `attempt: 2` a retry had just written.

**Fix.** A third endpoint, and a filter step:

```
POST /api/internal/signals/known   { signalIds: [ … ] }  →  { known: [ … ] }
```

```ts
const known = new Set(await payments.known(candidates.map((row) => row.signal_id)))
for (const row of candidates) {
  if (known.has(row.signal_id)) continue   // already recorded — do not re-price
  rows.push(row)
}
```

Bounded by the batch size, not by traffic, so the answer never grows with load.
Retries are exempt — the cursor named them deliberately, and a retry *always* has
a status row, so filtering it would make retries impossible.

**Result.** `20 sweeps; batch sizes 124 + 124 + …` became `2 sweeps; batch sizes
120 + 0`.

---

### BUG-4 · ClickHouse resolved a SELECT alias inside WHERE
**Severity: medium — a hard 500 on every sweep.**

```sql
-- BEFORE
SELECT signal_id, toString(received_at) AS received_at, api_key_hash, payload
  FROM signal_log
 WHERE received_at > parseDateTime64BestEffort({since:String})
```

ClickHouse binds `received_at` in the WHERE clause to the **alias**, not the
column, so the comparison became `String > DateTime64`:

```
Code: 43. DB::Exception: No operation greater between String and DateTime64(3).
```

**Fix.** Drop the `toString()` entirely — it was never needed. `JSONEachRow`
already renders a `DateTime64(3)` as `YYYY-MM-DD hh:mm:ss.sss`, which is exactly
what `toIso` converts:

```ts
const COLUMNS = 'signal_id, received_at, api_key_hash, payload'
```

**Lesson.** In ClickHouse, never alias a column to its own name.

---

### BUG-5 · `skipDuplicates` reports how many, never which
**Severity: medium — found by reading, before it could bite.**

The first version assumed the skipped rows were the last N:

```ts
// BEFORE — wrong
const inserted = new Set(toCreate.slice(0, res.count).map((r) => r.signalId));
```

`createMany({ skipDuplicates: true })` returns a count and nothing else, and the
skipped rows are not in any particular position. Any row it guessed wrong about
would have had its verdict silently dropped.

**Fix.** Re-drive the whole set through the update pass — writing the same values
twice is harmless, guessing is not.

---

### BUG-6 · Test-side: `pg` parses `timestamp without time zone` as local
**Severity: low — a false failure, but it hid a real class of bug.**

`SignalLog.receivedAt` is `timestamp without time zone` holding a UTC instant.
The `pg` driver parses that as **local** time, so on an IST machine the E2E
"billing timestamp matches the edge stamp" check failed by 5½ hours — while
Prisma, which normalises to UTC, was perfectly correct.

**Fix** (in the test harness):

```ts
pg.types.setTypeParser(1114, (value: string) => new Date(`${value.replace(' ', 'T')}Z`))
```

Worth recording because the *production* form of this bug — `toIso` in the
dispatcher — is real and is guarded by its own test. Billing windows cut on
`receivedAt`; a timezone slip there mis-bills every signal.

---

### BUG-7 · The overlap window starved new work behind already-settled rows
**Severity: high — a permanent stall, and only found by running the loop for real.**

Every earlier test drove `sweepOnce` by hand. The loop around it —
`dispatch.runner.ts` — had only ever been unit-tested. Running it as an actual
process immediately exposed a bug none of the 110 E2E checks could see.

The known-filter runs AFTER the SQL `LIMIT`:

```ts
// BEFORE
const candidates = (await archive.readNewer(since, room))   // LIMIT room
const known = new Set(await payments.known(candidates.map((r) => r.signal_id)))
for (const row of candidates) { if (known.has(row.signal_id)) continue; rows.push(row) }
```

So when the overlap window holds more settled rows than `batchSize × concurrency`,
every sweep reads the same page of known rows, discards all of them, sends
nothing — and never reaches the newer rows behind them. **The pipeline wedges
permanently while the backlog grows.**

Observed at `batchSize 20 × concurrency 2`: 50 signals posted, 40 settled, then
silence. A signal posted afterwards was never picked up either.

Production reachability: 500 × 2 = 1000 slots against a 5-minute overlap. More
than 1000 signals in any 5-minute window — about 3.3/sec — and it stalls.

**Fix.** Keyset pagination, so the sweep can walk *past* the settled rows:

```sql
WHERE received_at > parseDateTime64BestEffort({since:String})
  AND (received_at, signal_id) > (parseDateTime64BestEffort({afterTs:String}), {afterId:String})
ORDER BY received_at ASC, signal_id ASC
```

```ts
while (rows.length < wanted && pages < MAX_PAGES) {
  const page = await archive.readNewer({ sinceIso: since, after, limit: room })
  if (page.length === 0) break
  after = { receivedAt: last.received_at, signalId: last.signal_id }
  const known = new Set(await payments.known(fresh.map((r) => r.signal_id)))
  …
  if (page.length < room) break     // archive exhausted
}
```

The tuple comparison matters: `signal_id` breaks the tie when two signals share a
millisecond, without which a page boundary could skip a row or repeat one
forever. `MAX_PAGES = 20` bounds the work, and `sweep.page_cap` is logged when it
is hit — that means the overlap is too wide for the throughput, not that anything
is lost.

**Also changed:** `DISPATCH_OVERLAP_MS` default 5 min → **60 s**. The wider the
window, the more settled rows every sweep must page past. 60 s covers a 7.5 s
Kafka flush interval and any NTP-synced clock skew.

**After:** 50/50 settled, the log showing the fix at work —

```
sent=40  processed=40  alreadyKnown=0   batches=2  saturated=true
sent=10  processed=10  alreadyKnown=40  batches=1  saturated=false
```

**Lesson.** A pure function with good ports is easy to test and easy to trust too
much. `sweepOnce` was correct in isolation every single time; the bug lived in
what happens when you call it *repeatedly*, which only running the process shows.

### BUG-8 · A cold start silently backfilled the entire archive
**Severity: medium operationally, and it happened to a real person.**

`npm run up` preserved existing containers, so a developer following the setup
ended up with a 17k-row archive from old load tests and an empty `SignalStatus`.
The watermark is `max(receivedAt)` over that table — empty means no watermark,
which means every unsettled row is outstanding. The dispatcher started and did
exactly what it was told: swept all 17,033.

```
{"event":"sweep.done","read":1000,"sent":1000,"userError":1000,"alreadyKnown":0,"pages":1,...}
{"event":"sweep.done","read":1000,"sent":1000,"userError":1000,"alreadyKnown":998,"pages":2,...}
{"event":"sweep.done","read":1000,"sent":1000,"userError":1000,"alreadyKnown":1998,"pages":3,...}
…
```

No money moved — those rows predate `api_key_hash`, so every one came back
`USER_ERROR` — but it wrote 17k junk status rows and hammered settle for minutes.
A warning had been added for this and it was simply not enough: the default
behaviour was still the destructive one.

**Fix, in two places.**

`npm run up` now gives a **clean slate by default** — `docker compose down -v`
then up, so ClickHouse comes back with the tables and no rows and Kafka with no
messages. `--keep` preserves. Local containers holding derived data; nothing
precious is at risk, and the surprising default is the one that was wrong.

And the dispatcher refuses the situation outright rather than trusting a warning
in a script it may never have been run through:

```ts
async function guardColdStart(clickhouse: ClickHouseReader): Promise<void> {
  const limit = config.dispatchColdStartMax          // DISPATCH_COLD_START_MAX, default 1000
  if (limit <= 0) return                             // 0 = deliberate backfill
  const { sentThrough } = await fetchCursor(0)
  if (sentThrough !== null) return                   // there is a watermark; never fires again
  const archived = Number((await clickhouse.query(…))[0]?.n ?? 0)
  if (archived <= limit) return
  throw new ColdStartBackfillError(…)                // names all three ways forward
}
```

Verified: with a 1,200-row archive and no watermark it exits **1 in 260 ms**
instead of backfilling —

```
{"event":"dispatcher.cold_start_refused","archived":1200,"limit":1000}
{"event":"dispatcher.fatal","error":"nothing has been settled yet and the archive
 holds 1200 signals … Either: · start from now … · backfill on purpose —
 DISPATCH_COLD_START_MAX=0 · start clean — npm run up"}
```

Both escape hatches confirmed: `DISPATCH_COLD_START_MAX=0` backfills on request,
and an under-limit archive is never blocked.

**Lesson.** A warning printed by a setup script is not a safeguard — it protects
only the person who read it, in the one flow that prints it. The guard belongs in
the thing that would do the damage.

### FINDING-9 · The window has to be over `ingested_at`, or the 1.0 model loses rows
**Severity: critical — and found by reasoning, before it could bite.**

The 1.0 dispatcher keeps no state at all: no watermark, no cursor, no local file.
It selects a time window and sends everything in it, and settle discards the
duplicates. Which raises the only hard question in the design — **what does "the
latest data" mean when nothing remembers what was already sent?**

The obvious answer is wrong. `signal_log` is `ORDER BY (received_at, signal_id)`
and `PARTITION BY toYYYYMM(received_at)`, so `received_at` is the column everything
about the table invites you to filter on:

```sql
-- WRONG, and silent
WHERE received_at >= now64(3) - 180
```

`received_at` is stamped by the **edge**, at the moment the customer's request
arrived — by N instances, off N clocks. The row reaches the archive some time
later: ClickHouse's Kafka engine flushes in batches, and a broker backlog or a
ClickHouse restart stretches that to minutes. So a signal with
`received_at = 10:00:00` can be inserted at `10:04:00`.

Under the *old* model that row was still recoverable — the overlap window read
back below the watermark for exactly this reason (BUG-3). Under this model there
is no watermark and no second look. By the time the row exists, the window that
would have contained it has moved on. **It is never sent, no error is raised, and
nothing anywhere records that a signal was dropped.** It is the worst shape of
billing bug: silent, permanent, and invisible to every check that only looks at
what *did* arrive.

**Fix.** Window over `ingested_at`, which the schema already had as an ops
column — `DEFAULT now64(3)`, written by ClickHouse when the materialized view's
insert lands. One clock, one server, and it means precisely "arrived in the
archive":

```sql
WHERE ingested_at >= now64(3) - ({windowMs:UInt64} / 1000)
```

A signal stuck in Kafka for ten minutes lands in whichever window covers the
minute it *actually arrived*. Ingestion lag stops being a correctness problem and
becomes latency.

Two details that came with it:

- **`received_at` still travels to settle.** Billing windows cut on it. The
  selection clock and the billing clock are different columns, and conflating them
  is how you bill a signal into the wrong period.
- **The bound is computed by ClickHouse, not by the dispatcher.** `ingested_at` is
  stamped by the ClickHouse server's clock, so a bound derived from `Date.now()` on
  the EC2 box would shift the window by whatever skew exists between two machines —
  silently, again. `now64(3)` in the SQL keeps one clock on both sides of the
  comparison.
- **It needs an index** (migration `002`). `ingested_at` is neither the sort key
  nor the partition key, so the window would otherwise scan every partition and
  read the `payload` column to do it. `minmax` works because inserts arrive in
  roughly `ingested_at` order.

**Lesson.** When you delete the thing that remembers, every remaining question
becomes a question about clocks — and the column that *looks* like the right one
is the one the table is sorted by, not the one that answers "what is new".

---

### Also found, not mine, not fixed

- **`npm run verify:credit` is broken** in the payments repo. It imports
  `@/lib/credit-pricing`; the file is at `src/utils/credit-pricing.ts`. Pre-dates
  this work — confirmed by stashing my changes and re-running. One-word fix, left
  alone as out of scope.
- **Two tables carry schema drift**: `AgentConversation` and `DynamicDashboard`
  have an `updatedAt DEFAULT CURRENT_TIMESTAMP` their Prisma schema does not
  declare, so `migrate diff` reports a change on every run. I avoided the same
  mistake in `SignalStatus` (see below).
- **The 500-signal batch cap was documented but never enforced.**
  `settle/route.ts:44` had an orphaned doc comment for a deleted constant, and
  the only length check was `length === 0`. A 50,000-signal POST was accepted and
  would time out. Fixed as part of this work.

---

## Part 2 — Changes

### Payments repo (`Clocknext-Payment-Saas`)

| File | Change |
| --- | --- |
| `prisma/schema.prisma` | `SignalStatus` model, `SignalLifecycle` + `SignalErrorType` enums, back-relations on `Organization` and `SignalLog` |
| `prisma/migrations/20260826000000_signal_status/` | **new** — hand-written idempotent SQL in the repo's existing style |
| `src/lib/settle/settle-attribution.ts` | **new** — batch `apiKeyHash` → organisation in one query; the key-wins security rule |
| `src/lib/settle/settle-status.ts` | **new** — the only writer of `SignalStatus`; the per-row fallback |
| `src/lib/settle/settle-batch.ts` | hoisted the null-money block to one `EMPTY_MONEY`, extracted `signalBase`, exported `refusedStatus` |
| `src/app/api/internal/settle/route.ts` | attribute → price → reassemble in arrival order → record; **the 500 cap enforced** |
| `src/app/api/internal/signals/cursor/route.ts` | **new** — the watermark and the retry list |
| `src/app/api/internal/signals/known/route.ts` | **new** — the convergence filter (BUG-3) |
| `scripts/verify-signal-status.mts` | **new** — 15 checks, incl. the BUG-1 regression |
| `scripts/alias-hooks.mjs` | taught it extensionless relative imports, so a verify script can load the Prisma client |

#### Why `SignalStatus` and not a column on `SignalLog`

`SignalLog` replaces `UsageLog`. But `RawUsageLog` was never the same table as
`UsageLog`, and the split is load-bearing — the code says so in three places:

1. **A failed signal cannot be a `SignalLog` row.** `SignalLog.customerId` is a
   *required* FK (`schema.prisma:1686`). A signal that failed *because* the
   customer id was wrong cannot satisfy it. `RawUsageLog` has only an org FK,
   which is why `queries.ts:640` finds unprocessed rows via
   `payload: { path: ["customerId"] }` instead of a join.
2. **Money aggregates carry no status filter, on purpose.**
   `/api/signals/route.ts:53-58`: *"Financial totals deliberately stay on
   UsageLog above so unprocessed rows never count toward cost / revenue /
   credits / allowance."*
3. **Transaction boundary.** A settled signal writes `SignalLog` inside the
   transaction that also debits the wallet. A failed signal's status must be
   written *after that transaction rolled back*.

So `SignalStatus` is `RawUsageLog`'s twin, not a third table. Table count is
unchanged: 2 before, 2 after, plus the ClickHouse archive.

#### Where it diverges from `RawUsageLog`, and why

| Field | `RawUsageLog` | `SignalStatus` | Reason |
| --- | --- | --- | --- |
| `signalId` | — | `String @unique` | the upsert key; the edge's ULID is the identity |
| `organizationId` | required FK | **nullable** FK | the edge accepts on key *presence*; an unresolvable key still needs a terminal row or it is re-sent forever |
| `apiKeyHash` | — | `String?` | how settle resolves the org |
| `errorCode` | — | `String?` | settle already derives it in `errorCodeFor` |
| `payload` | `Json` | **omitted** | it is in ClickHouse — the duplication the archive exists to remove |
| `usageLogId` | `@unique` | `signalLogId @unique` | same structural guard, new target |
| `(org, idempotencyKey)` | `@@unique` | **no unique** | `settleSignal` overwrites the caller's key with `signalId`, so enforcing it would reject signals settle accepts |

`updatedAt` deliberately has **no** database default, matching Prisma's own DDL —
the two pre-existing tables that added one register as drift forever.
`prisma migrate diff` against a fully-migrated scratch database reports **zero
drift** for `SignalStatus`.

### Edge repo (`AWS-internal`)

| File | Change |
| --- | --- |
| `src/modules/auth/auth.service.ts` | `digestApiKey()` |
| `src/modules/signal/signal.module.ts` | stamps `request.apiKeyHash` in the hook that already holds the token |
| `signal.schema.ts` / `.service.ts` / `.controller.ts` | `apiKeyHash` on the message envelope, never in `body`, never echoed in the 202 |
| `docker/clickhouse/init/01-schema.sql` | `api_key_hash` column + MV extraction |
| `docker/clickhouse/migrations/001_api_key_hash.sql` | **new** — the live-database path (the init script is first-start-only) |
| `src/client/clickhouse.ts` | **new** — read-only reader, server-side bound params |
| `src/client/payments-client.ts` | **new** — `cursor`, `known`, `settle`; `MisconfiguredError` for a bad shared secret |
| `src/workers/dispatch/` | **new** — `dispatch.schema.ts` (ports), `dispatch.service.ts` (pure `sweepOnce`), `dispatch.archive.ts` (the two SELECTs), `dispatch.runner.ts` (the self-pacing loop) |
| `src/config.ts` | ClickHouse, payments and dispatcher settings |
| `scripts/e2e.mts` + `scripts/e2e/` | **new** — the 110-check end-to-end run |
| `docs/IMPLEMENTATION.md` | **new** — flow, code, and the manual runbook |

The edge itself still never touches ClickHouse. The new client is the
*dispatcher's*, and it is read-only: `signal_log` has exactly one writer, the
materialized view, which is what keeps the archive rebuildable by replaying the
topic.

---

## Part 3 — Verification

### Unit — `npm test` (edge), 47 tests

Covers the gate's rejections, the envelope contract (digest present, raw key
absent, envelope outside `body`, not echoed in the 202), and the sweep: the
ClickHouse timestamp conversion, the overlap arithmetic, batching and
concurrency, retry exemption from the known-filter, unusable rows, and a batch
that throws not sinking its siblings.

### End-to-end — `npm run e2e` (edge), 110 checks

Real edge → real Kafka → real ClickHouse → real dispatcher → real settle → real
Postgres, on a real organisation with a real plan, credit and model.

| # | Section | What it proves |
| --- | --- | --- |
| 0-1 | preflight, setup | all four services live; a real org borrowed and restored |
| 2-3 | 15 edge rejections | each refused for the right reason, and **none** reached Kafka or the archive |
| 4 | happy path | 202 → archive → settle → `SignalLog` + `SignalStatus`, credits actually charged, billing timestamp exact to the millisecond |
| 5 | attribution | unknown and expired keys recorded as `USER_ERROR` with NULL org, digest kept, never auto-retried |
| 6 | settle rulebook | unknown customer is `NOT_FOUND` (404, not 403 — a key cannot probe other orgs), unknown model, missing type |
| 7 | idempotency | one duplicate, then **10 concurrent** duplicates: still one money row, not one extra credit |
| 8 | retry policy | `SERVER_ERROR` offered, `USER_ERROR` not, attempt-5 not, attempt advances 1 → 2, no second money row |
| 9 | forgery | a body carrying `signalId` / `attempt` / `apiKeyHash` / `receivedAt` / `organizationId` changes nothing (**BUG-1**) |
| 10 | payload fidelity | unicode, astral pairs, control characters, a NUL byte, SQL injection, ClickHouse parameter injection, 4KB strings — byte-exact and still settles |
| 11 | route limits | 501 refused (not truncated), empty/blank batchId refused, non-object members answered per position, all three routes 401 without the secret |
| 12 | volume | 120 signals: all accepted, all archived, all settled, exactly 120 money rows |
| 13 | convergence | a caught-up pipeline sends nothing and stays quiet (**BUG-3**) |

Run twice consecutively: 110/110 both times, and the audit afterwards showed 0
orphan rows, 22 organisations and 7 API keys — exactly as found.

### Payments — `npm run verify:signal-status`, 15 checks

Attribution (valid, unknown, expired, malformed, upper-case digest, the BUG-1
regression) and the status writer (create path, update path, org `""` → NULL,
synthetic ids skipped, the dispatcher's sweep query).

### Static

Both repos: **0** TypeScript errors, ESLint clean. `prisma migrate diff` against
a fully-migrated scratch database: **no drift** for `SignalStatus`.

---

## Part 4 — Still open

1. ~~**Partition count.**~~ **RESOLVED 2026-08-27 — start at 1, grow to 3 on a
   symptom.** The triggers are ClickHouse ingestion lagging or head-of-line
   blocking ([AWS-SETUP.md](AWS-SETUP.md) Step 0).

   The interesting part is *why this is not the irreversible decision it looks
   like*. Adding partitions rehashes `customerId`→partition and splits a
   customer's ordered stream — the standard reason to pick the number up front.
   It costs nothing here: nothing reads the topic in order. `signal_log` is
   `ORDER BY (received_at, signal_id)` with dedup on `signal_id`, the dispatcher
   windows on `ingested_at` and bills on `received_at`, and settle is idempotent
   on `signalId`. There is no stateful per-customer aggregation to corrupt.

   What *is* real, and was missing: a newly added partition has no committed
   offset, so librdkafka's default `auto.offset.reset = latest` would skip every
   message the producer wrote to it before the consumer rebalanced onto it. With
   no watermark, those signals are gone. Fixed by setting
   `kafka_auto_offset_reset = 'earliest'` on `kafka_signals` — migration `003`,
   plus `01-schema.sql`. It is inert until the topic grows, and load-bearing on
   that day.

   Note 3 partitions would **not** be HA — they would all sit on the single
   broker.
2. **Nothing is committed.** Both repos have my changes in the working tree only.
   Both also had unrelated uncommitted changes before I started (a Prisma version
   bump, `.worktrees/`, the `resolve` route) — untouched.
3. **The Signals UI is not repointed** — excluded by request. When it is,
   `loadSignalsLogsPage` and `loadCustomerSignalsLogsPage` become
   `prisma.signalStatus.findMany({ orderBy: { receivedAt: 'desc' }, include: { signalLog: … } })`,
   the three stat-card counts move to `signalStatus`, and
   `/api/usage-logs/[id]/retry` and `/fail` need twins. Retry re-reads the
   payload from ClickHouse instead of `RawUsageLog.payload`.
4. **`CLAUDE.md` in the edge repo is stale** — it still describes the deleted
   SQS + Lambda architecture (`signals_pending`, `signals_accepted`, the vent,
   two Lambda consumers). `docs/IMPLEMENTATION.md` and `docs/PRODUCTION.md` are
   current; `CLAUDE.md` should be rewritten against them.
