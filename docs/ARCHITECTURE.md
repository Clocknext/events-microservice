# End-to-end architecture

Every component, every route, and every outcome a signal can have — with the
real numbers from a live run of `npm run trace:all` (71 signals: 31 covering one
case each, plus a 40-signal burst).

Companion documents: [IMPLEMENTATION.md](IMPLEMENTATION.md) is the code
walkthrough and the manual runbook; [FINDINGS.md](FINDINGS.md) is what broke
while building it; [PRODUCTION.md](PRODUCTION.md) is the AWS wiring.

---

## 1 · The shape

```
                        ┌─────────────────────────────────────────────┐
   customer's app       │  THIS REPOSITORY (AWS-internal)             │
        │               │                                             │
        │  POST /api/v1/signal                                        │
        ▼               │                                             │
  ┌───────────┐         │  gate · stamp · produce · 202               │
  │   EDGE    │─────────┼──▶ Kafka topic `signals`                    │
  │ (Fastify) │         │         │                                   │
  └───────────┘         │         │  ClickHouse pulls it ITSELF       │
        ▲               │         ▼                                   │
        │ 202/4xx/502   │   kafka_signals (ENGINE=Kafka)              │
                        │         │  materialized view                │
                        │         ▼                                   │
                        │   signal_log  ──── the archive              │
                        │         │                                   │
                        │         │ read-only                         │
                        │  ┌──────────────┐                           │
                        │  │  DISPATCHER  │  one-shot, 60s timer:     │
                        │  │  (one-shot)  │  read window → settle     │
                        │  └──────────────┘  → exit                   │
                        └─────────┼───────────────────────────────────┘
                                  │  HTTPS + shared secret, gzipped
                        ┌─────────▼───────────────────────────────────┐
                        │  PAYMENTS APP (Clocknext-Payment-Saas)      │
                        │                                             │
                        │  /api/internal/settle           price it    │
                        │                    │                        │
                        │                    ▼                        │
                        │  Postgres  SignalLog    ← money             │
                        │            SignalStatus ← lifecycle + UI    │
                        └─────────────────────────────────────────────┘
```

### Boundaries that are load-bearing

| Boundary | Rule | If broken |
| --- | --- | --- |
| edge ↛ ClickHouse | the edge produces to Kafka and nothing else | ingest latency becomes a customer-facing latency |
| dispatcher ↛ Postgres | it asks the payments app over HTTP | two services owning one schema |
| anything ↛ `signal_log` writes | one writer: the materialized view | the archive stops being reproducible by replaying the topic |
| payload ↛ envelope | `apiKeyHash`/`signalId`/`receivedAt` live outside `body` | the archive stops being a verbatim copy of the request |
| body ↛ identity | `organizationId` from a payload is deleted, never forwarded | **cross-tenant billing** (this happened — FINDINGS BUG-1) |

---

## 2 · Every route

### Public — `/api/v1/*`, authenticated by the customer's `cnk_…` key

| Route | Auth | Answers |
| --- | --- | --- |
| `POST /api/v1/signal` | `Authorization: Bearer cnk_…`, **presence only** | 202 · 400 · 401 · 413 · 415 · 502 |
| `GET /health` | none | 200 |
| anything else | — | 404 in the v1 envelope |

The edge never resolves the key. It checks the header is there, digests it, and
lets settle decide whose it is. That is why an unknown key still gets a 202 — the
signal is archived, then refused downstream with a recorded reason.

### Internal — `/api/internal/*`, authenticated by `INTERNAL_SETTLE_SECRET`

| Route | Method | Purpose | Returns |
| --- | --- | --- | --- |
| `/api/internal/settle` | POST | price one window, whatever its size | `{ batchId, total, processed, pending, unattributed, workers, workerCount, batchConcurrency, wallMs, recorded, signals[] }` |
| `/api/internal/resolve` | POST | single-key resolution (pre-existing) | `{ apiKey }` |

Both answer `401 Unauthorized.` without the shared secret. The `Bearer` scheme is
required — a bare token is refused.

**`/api/internal/signals/cursor` and `/api/internal/signals/known` are gone from
this side.** They existed to answer "what have I already sent?", which the 1.0
dispatcher never asks: it keeps no state, re-sends a whole time window every run,
and lets settle discard the duplicates.

---

## 3 · The signal's journey, hop by hop

### Hop 1 · The edge

```
onRequest  identity.ts      signalId = ulid()          ← before anything can fail
                            receivedAt = ISO-8601
onRequest  signal.module    Bearer present?  no → 401 API_KEY_MISSING
                            apiKeyHash = sha256(token)
           route schema     customerId, inputTokens, outputTokens
                            (additionalProperties OPEN)
handler    controller       produce to Kafka → 202,  or throw → 502
```

The gate requires **three fields**. Everything else rides through untouched,
because the full rulebook lives in settle and refusing a signal here would lose
it rather than record it.

The 202 comes **after** the Kafka ack, never before — acknowledging a billable
signal that reached no topic loses it silently while telling the caller it is
safe.

### Hop 2 · Kafka → ClickHouse

Nothing pushes. `kafka_signals` is `ENGINE = Kafka` with `JSONAsString`; a
materialized view lifts three envelope fields and keeps the body verbatim:

```sql
SELECT JSONExtractString(raw,'signalId')                             AS signal_id,
       parseDateTime64BestEffort(JSONExtractString(raw,'receivedAt')) AS received_at,
       JSONExtractString(raw,'apiKeyHash')                           AS api_key_hash,
       JSONExtractRaw(raw,'body')                                    AS payload
FROM signals.kafka_signals
```

Offsets commit only after the insert lands → **at-least-once**, so duplicates are
expected and `ReplacingMergeTree` collapses them on merge. Because merges are not
immediate, every read uses `LIMIT 1 BY signal_id`.

*Measured: 55 signals landed in ~518 ms; a single signal in ~263 ms.*

### Hop 3 · The dispatch run

```
① SELECT … FROM signal_log WHERE ingested_at >= now64(3) − DISPATCH_WINDOW_MS
                           ORDER BY ingested_at ASC LIMIT 1 BY signal_id
② map rows → settle signals (envelope written over the payload)
③ POST settle   ONCE, gzipped, carrying all of them
④ exit 0 / 1 / 2
```

**Why `ingested_at` and not `received_at`.** `received_at` is the caller's time,
stamped by N edge instances off N clocks, and ClickHouse's Kafka engine flushes in
batches — so a row can land in the archive minutes after its `received_at`. With
no watermark anywhere in this design, a window over `received_at` skips that row
**permanently and silently**. `ingested_at` is `DEFAULT now64(3)`: one clock, one
server, "arrived in the archive". `received_at` is still what settle bills on.

**Why the bound is `now64(3)` and not the dispatcher's clock.** `ingested_at` is
stamped by ClickHouse, so the comparison has to be made against ClickHouse's own
clock or the window shifts by whatever skew exists between two machines.

**Why there is no upper bound, and no filter.** The window runs to this instant
and is 3× the timer's interval, so consecutive runs overlap heavily and every
signal is sent about three times. That is the *entire* error-recovery mechanism:
nothing is persisted, so a failed run has nothing to resume — and needs nothing,
because the next two read exactly the same rows. Settle collapses the duplicates
onto the same money row.

**Why `LIMIT 1 BY signal_id` is still there.** It dedups *within* one run, so a
pre-merge `ReplacingMergeTree` duplicate does not put the same signal in one
request body twice. Settle's idempotency is the across-runs half of the job.

**Why `ingested_at` needs an index.** It is neither the `ORDER BY` key
(`received_at, signal_id`) nor the partition key, so the window would otherwise be
a full scan of every partition reading the fat `payload` column — hence the
`minmax` skip index in migration `002`.

*Two consecutive runs, same 55 signals:*

```
dispatch.run  read=55  sent=55  processed=46  userError=9  capped=false
dispatch.run  read=55  sent=55  processed=0   userError=9  capped=false
```

The second run re-sends everything and moves no money. That is convergence here —
not an idle pipeline, but an inert re-send.

### Hop 4 · Settle

```
attributeBatch()    one query resolves every distinct apiKeyHash → organizationId
                    THE KEY WINS over any organizationId in the body
splitWorker()       across INTERNAL_WORKER (5) workers
settleBatch()       each worker walks its chunk ONE signal at a time,
                    one transaction per signal
reassemble          into arrival order — one entry per signal sent
recordSignalStatuses()  upsert SignalStatus (after the money, never fails the request)
```

---

## 4 · Every outcome — the live matrix

71 signals, one per case plus a 40-signal burst. `MONEY` = a `SignalLog` row exists.

### A · Refused by the edge — never reach Kafka

| # | Case | Edge | Archive | Settle | Money |
| --- | --- | --- | --- | --- | --- |
| 1 | no `Authorization` header | 401 `API_KEY_MISSING` | — | — | — |
| 2 | bare token, no `Bearer` scheme | 401 `API_KEY_MISSING` | — | — | — |
| 3 | missing `customerId` | 400 `INVALID_BODY` | — | — | — |
| 4 | whitespace-only `customerId` | 400 `INVALID_BODY` | — | — | — |
| 5 | `inputTokens` as a string | 400 `INVALID_BODY` | — | — | — |
| 6 | negative tokens | 400 `INVALID_BODY` | — | — | — |
| 7 | fractional tokens | 400 `INVALID_BODY` | — | — | — |
| 8 | tokens beyond 2⁵³−1 | 400 `INVALID_BODY` | — | — | — |
| 9 | malformed JSON | 400 `MALFORMED_JSON` | — | — | — |
| 10 | empty body | 400 `EMPTY_BODY` | — | — | — |
| 11 | JSON array, not an object | 400 `INVALID_BODY` | — | — | — |
| 12 | bare JSON string | 400 `INVALID_BODY` | — | — | — |
| 13 | wrong content-type | 415 `UNSUPPORTED_MEDIA_TYPE` | — | — | — |
| 14 | no content-type | 415 `UNSUPPORTED_MEDIA_TYPE` | — | — | — |
| 15 | body over `BODY_BYTES` | 413 `BODY_TOO_LARGE` | — | — | — |
| 16 | path no route owns | 404 `NOT_FOUND` | — | — | — |

**All 16 produced zero archive rows, zero status rows and zero money.** A refused
signal is a plain rejection, never a queued 202. Each still carries a quotable
`signalId` and `receivedAt`, stamped before the failure.

Case 5 is the one worth remembering: AJV coercion is **off**, so `"1200"` is
refused here rather than accepted and later rejected by settle's Zod rulebook.

### B · Accepted by the edge, refused by settle — archived, recorded, no money

| # | Case | Edge | Settle verdict | `error_message` |
| --- | --- | --- | --- | --- |
| 17 | unknown API key | 202 | `USER_ERROR` `VALIDATION_FAILED` | `Invalid API key.` |
| 18 | expired API key | 202 | `USER_ERROR` `VALIDATION_FAILED` | `This API key has expired.` |
| 19 | unknown `customerId` | 202 | `USER_ERROR` `NOT_FOUND` | `No customer with id "cus_ghost_…".` |
| 20 | unknown model | 202 | `USER_ERROR` `UNPROCESSABLE` | `Model "made-up/model-…" is not a valid model.` |
| 21 | no `type` | 202 | `USER_ERROR` `VALIDATION_FAILED` | `type is required — one of "wallet", "credit", "outcome".` |
| 22 | unknown `agentKey` | 202 | `USER_ERROR` `UNPROCESSABLE` | `No credit with agent key "…" exists in this workspace.` |
| 23 | `type=wallet`, no wallet component | 202 | `USER_ERROR` `UNPROCESSABLE` | `This plan has no wallet component. Use \`type: "credit"\` / \`"outcome"\`.` |
| 24 | `type=outcome`, no outcome | 202 | `USER_ERROR` `UNPROCESSABLE` | `No outcome step with agent key "…" exists in this workspace.` |
| 25 | bogus `type` string | 202 | `USER_ERROR` `VALIDATION_FAILED` | `type: Invalid option: expected one of "wallet"\|"credit"\|"outcome"` |

Two properties hold across all nine:

- **`USER_ERROR` is never auto-retried.** It cannot self-heal, so it waits for the
  caller's data to be fixed and an explicit retry. A dead key would otherwise be
  re-sent every second forever.
- **Case 19 answers 404, not 403** — deliberately, so a key cannot probe which
  customer ids exist in other organisations.
- **Cases 17 and 18 store `organizationId = NULL`**, not `''`. The signal was
  never attributed. The digest is kept, so the sender stays traceable even after
  the key is revoked.

### C · Accepted and settled

| # | Case | Edge | Settle | Money |
| --- | --- | --- | --- | --- |
| 26 | the happy path | 202 | `PROCESSED` | yes |
| 27 | zero tokens (a free call) | 202 | `PROCESSED` | yes |
| 28 | 9M input / 4M output tokens | 202 | `PROCESSED` | yes |
| 29 | forged envelope in the body | 202 | `PROCESSED` **under the real org** | yes |
| 30 | hostile unicode + SQL injection payload | 202 | `PROCESSED` | yes |
| 31 | unknown extra fields | 202 | `PROCESSED` | yes |

Plus the 40-signal burst, all `PROCESSED`. Totals: **55 accepted → 55 archived →
46 `PROCESSED` + 9 `USER_ERROR` → 46 money rows.** No losses, no duplicates.

---

## 5 · The adversarial cases in detail

### Envelope forgery (case 29)

Sent a body claiming someone else's identity on every field:

```
the edge minted            01M0Y4DXRYM4KQXR58VZ3PHGED
the body claimed           01M0AAAAAAAAAAAAAAAAAAAAAA
the archive keyed it on    01M0Y4DXRYM4KQXR58VZ3PHGED
the archive digest is      9fcb1821f557cb90d647e56f…   (ours)
the body claimed digest    ffffffffffffffffffffffff…

toSettleSignal produced    signalId       = 01M0Y4DXRYM4KQXR58VZ3PHGED
                           attempt        = 1              (body said 99)
                           receivedAt     = 2026-08-26T04:14:38.878Z  (body said 1999)
                           organizationId = undefined      (body said org_someone_else)

it settled under org       cmqryc4tt0001tim6tvf8fl74   ← the key's real org
a row for the forged id?   no
```

Three independent defences, because this is the one that already went wrong once:

1. The edge stamps the envelope **outside** `body`, so a caller cannot write it.
2. `toSettleSignal` spreads the payload first and writes the envelope over it —
   and **deletes** `organizationId` rather than overwriting it.
3. `attributeBatch` consults `apiKeyHash` **before** any `organizationId`, so
   even a signal that reached settle with one is attributed from the key.

### Payload fidelity (case 30)

Unicode, astral pairs, emoji, control characters, a NUL byte, 4 KB strings,
`'; DROP TABLE signal_log; --`, and a ClickHouse parameter-injection attempt —
all round-tripped through Kafka and ClickHouse **byte-exact**, and still settled.
Parameters are bound server-side (`{name:Type}`), never interpolated.

### Idempotency

Five concurrent replays of an already-settled signal:

```
5 concurrent replays, all HTTP 200
money rows   before 1                    after 1
credits      before 0.668914016205103    after 0.668914016205103
```

`SignalLog.signalId` is `UNIQUE` and settle passes it as the idempotency key, so
a replay lands on the original money row. `settleSignal` also catches the unique
violation from a lost race and returns the winner's result.

**This is the property the whole dispatcher design rests on.** Because
over-sending is free, the dispatcher needs no claim, no lease, no visibility
timeout and no cursor table — it only has to guarantee *at least once*. Two
dispatchers running at once are safe rather than a corruption bug.

### Convergence

```
a fresh run sends again       55
money rows before / after     55 / 55
```

Convergence in this design is not a pipeline that goes idle — it cannot, since the
window is wider than the interval and nothing filters it. It is a re-send that
moves no money.

---

## 6 · Failure modes and what happens

| What fails | Caller sees | Data | Recovery |
| --- | --- | --- | --- |
| Kafka unreachable at produce | **502** `QUEUE_UNAVAILABLE` | nothing written | caller retries |
| `KAFKA_BROKERS` unset | 202 | nothing produced | 202 means only "passed the gate" |
| ClickHouse behind on ingest | 202 already sent | rows arrive late | harmless — the window is over `ingested_at`, so a late row is simply in a later window |
| ClickHouse down | 202 already sent | Kafka retains 7 days | run exits 1; the next tick retries. No backoff needed — the timer *is* the backoff |
| ClickHouse rebuilt from scratch | — | replay the topic | offsets reset; `signal_log` is derived, never authoritative |
| settle 5xx / timeout | — | **no status rows for that window** | run exits 1; the next window overlaps it and re-sends. Safe because settle dedups |
| settle returns `SERVER_ERROR` | — | `PENDING` + reason | re-sent by every overlapping run, then hourly by reconciliation. Settle decides whether to re-process |
| settle returns `USER_ERROR` | — | `PENDING` + reason | **never** auto-retried — waits for a fix |
| a status row fails to write | — | money is committed, lifecycle is not | `recorded.failed > 0`; signal is re-sent and re-recorded |
| shared secret wrong | — | nothing | **exit 2**, so a timer's failure count shows it instead of a 401 every minute |
| dispatcher killed mid-call | — | that window's outcomes unrecorded | next window re-sends them |
| three consecutive runs fail | — | those rows leave the 3-min window | the **hourly reconciliation** (2-hour window) is the only thing that covers this |
| body over 4.5MB | — | nothing settled | **exit 2** on 413; lower `DISPATCH_WINDOW_MS` |
| window over `DISPATCH_MAX_ROWS` | — | **rows not sent** | `run.capped` logged; reconciliation covers ≤2h, else `DISPATCH_SINCE` |
| two dispatchers running | — | some duplicate sends | harmless; settle dedups |

The asymmetry is deliberate: **the ingest path fails closed** (a signal that
cannot be produced is refused with a 502), and **the settle path fails open**
(bookkeeping never fails a request, because the money is already committed and
reporting a settled signal as failed would have it re-sent).

---

## 7 · Data model

| Store | Table | Role | Written by |
| --- | --- | --- | --- |
| Kafka | `signals` | the durable log; source for replay | the edge |
| ClickHouse | `signal_log` | verbatim archive, `ReplacingMergeTree` | the MV, and only the MV |
| Postgres | `SignalLog` | **money** — only exists for a settled signal | settle, in a transaction |
| Postgres | `SignalStatus` | **lifecycle** — every signal, settled or not | settle, after the transaction |

`SignalStatus` is the signals-pipeline twin of `RawUsageLog`:

```
/api/v1/usage                              signals pipeline
─────────────────────────────────────      ──────────────────────────────
RawUsageLog.payload                    →   ClickHouse signal_log.payload
RawUsageLog.status/errorType/attempt   →   SignalStatus
UsageLog                               →   SignalLog
```

`RawUsageLog` did two jobs. The body half moved to ClickHouse; the lifecycle half
could not (no row updates there), so it stayed in Postgres — where it serves the
Signals UI *and* tells the dispatcher what is outstanding, off the same
`(status, attemptCount, receivedAt)` index the existing cron sweeper uses.

A failed signal **cannot** be a `SignalLog` row: `customerId` is a required FK, so
a signal that failed *because* the customer was wrong is unstorable there. That
is why the split exists, not style.

---

## 8 · Scaling

| Lever | Now | Effect |
| --- | --- | --- |
| Kafka partitions | **1** | consumer ceiling 1. Grow to 3 on a measured symptom — §8 below |
| `DISPATCH_WINDOW_MS` | 180 000 | 3× the timer's interval. Wider = more duplicate sends; narrower = less recovery |
| `DISPATCH_MAX_ROWS` | 100 000 | memory ceiling. Hitting it **loses rows** — it is not a throughput knob |
| `INTERNAL_WORKER` | 5 | workers inside one settle call |
| `INTERNAL_BATCH_CONCURRENCY` | 4 | concurrent batches settle will accept |

Settle splits a batch across `INTERNAL_WORKER` workers that each walk their chunk
**one signal at a time**, so wall time grows linearly with batch size while the
rate stays flat. A 5,000-signal batch is not faster than ten 500s — it is the
same rate with ten times the blast radius on a timeout, and a batch too large to
finish inside the function budget can never succeed at all.

Connection ceiling: `INTERNAL_BATCH_CONCURRENCY × INTERNAL_WORKER` = 20.

**One partition, growing to 3 on a symptom.** The two triggers, both measurable:
ClickHouse ingestion falling behind the produce rate (one partition caps you at
one consumer), or head-of-line blocking from a slow/oversized message. Neither is
speculative work to do up front — at peak (~4 MB/s for ~2 s) one partition is
ample.

**Growing is cheap in this pipeline**, which is worth stating because the usual
Kafka advice says otherwise. Adding partitions does rehash
`customerId`→partition and does split a customer's ordered stream; it costs
nothing here because nothing reads the topic in order. The MV inserts into
`signal_log` (`ORDER BY (received_at, signal_id)`, dedup on `signal_id`), the
dispatcher windows on `ingested_at` and bills on `received_at`, and settle is
idempotent on `signalId`. No stateful per-customer aggregation exists.

**The one real precondition** is `kafka_auto_offset_reset = 'earliest'` on
`kafka_signals` (migration `003`). A new partition has no committed offset, so
librdkafka's default `latest` would skip whatever the producer wrote to it before
the consumer rebalanced onto it — permanently, with no watermark to recover from.
Set before growing, it costs nothing.

Growing is then one command:
`kafka-topics.sh --alter --topic signals --partitions 3`. Lowering the count
remains impossible.

---

## 9 · Starting the dispatcher for the first time

**There is nothing to do, and that is the point.** The old model derived its
starting position from `max(receivedAt)` over `SignalStatus`, so an empty table
meant *every* archived row was outstanding and a first start against a
17,033-row archive swept all of them — safely, since they carried no
`api_key_hash` and every one came back `USER_ERROR` with no money moved, but it
wrote 17,033 status rows nobody asked for. That is why a cold-start guard and a
"park the watermark at now" incantation existed.

A time window has no such failure mode. The first run reads the last
`DISPATCH_WINDOW_MS` of *ingestion* and nothing older, whatever Postgres does or
does not contain. So:

- the cold-start guard is gone, along with `DISPATCH_COLD_START_MAX`
- there is no watermark row to plant before starting
- a deliberate backfill is now an explicit, bounded request rather than an
  accident of an empty table:

```bash
DISPATCH_SINCE=2026-08-01T00:00:00Z DISPATCH_UNTIL=2026-08-02T00:00:00Z \
DISPATCH_MAX_ROWS=1000000 npm run dispatch
```

A malformed bound exits 2 rather than reading a window nobody asked for.

## 10 · Reproducing this

| Command | What |
| --- | --- |
| `npm test` | 47 unit tests, no infrastructure |
| `npm run e2e` | 111 checks against live Kafka + ClickHouse + Postgres |
| `npm run trace:runner` | the dispatcher LOOP as a real process: pacing, backoff, SIGTERM |
| `npm run trace` | one signal, every hop, every payload printed |
| `npm run trace:all` | this document's matrix — 71 signals, every case |
| `npm run dispatch` | the dispatcher itself |

The trace scripts borrow a real API key row and restore it in a `finally`. Setup
also verifies the borrowed hash against `ApiKey.keyEnc` — the AES copy no run
ever writes — and repairs it if a previous run died before restoring. **Do not
pipe them through `head`**: SIGPIPE can kill the process before teardown finishes
(the self-heal covers the credential; stray rows may still need clearing).
