# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

THREE processes that share one codebase:

- **The edge** (`src/app.ts`, `src/modules/`, `src/plugins/`) — a Fastify service.
  `POST /api/v1/signal` is a gate: it checks the shape of a signal, stamps it with
  an identity, produces it to Kafka, and answers 202.
- **The consumer** (`src/workers/consume/`) — a LONG-LIVED Kafka consumer, the only
  always-on process here besides the edge. It drains the topic, calls the payments
  app's `/api/internal/resolve` **once per signal** (whose key is this, and is this
  body acceptable?), and writes the row **with that verdict** into ClickHouse. It
  replaced ClickHouse's own Kafka ingestion, which could not make an HTTP call.
- **The dispatcher** (`src/workers/dispatch/`) — a separate ONE-SHOT process on a
  60-second systemd timer, not a loop and not long-lived. It reads one window of
  the ClickHouse archive and posts all of it to the payments app's
  `/api/internal/settle` in a single call, then exits.

Neither worker is part of the Fastify app and neither imports a plugin.

```
customer ──▶ EDGE ──produce──▶ Kafka `signals`
             (202)                    │
                                      ▼
                                 CONSUMER ──▶ payments /api/internal/resolve
                                      │            (one call per signal:
                                      │             whose key, and is it valid?)
                                      ▼
                          ClickHouse signal_log  ── the archive, row + VERDICT
                                      │
                                      ▼
                          DISPATCHER ──▶ payments /api/internal/settle
                                                 │
                                                 ▼
                                        Postgres: SignalLog (money)
                                                  SignalStatus (lifecycle)
```

The payments app lives in a **separate repository** —
`/home/joze/Documents/Work/Clocknext-Payment-Saas`, default branch `develop`, with
the signals work on `microservices/events` — and
owns all pricing and all Postgres writes. No production code here touches Postgres.
The e2e/trace scripts do, and they do more than read: they swap an `ApiKey` hash,
insert a throwaway `Organization`, force a `SignalStatus` row back to
`SERVER_ERROR` to prove the retry path, and delete what they made. See
**The e2e harness** below for what protects that.

[README.md](README.md) is the front door; [AGENTS.md](AGENTS.md) is the
architecture contract (below). Beyond those, seven documents, in the order worth
reading them:

| Doc | What |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | every route and every outcome, as a matrix from a live 71-signal run |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | the code that matters, the config, and a manual runbook using only `curl`/`jq`/`psql` |
| [docs/FINDINGS.md](docs/FINDINGS.md) | the bugs this design already hit, and why each fix is shaped the way it is |
| [docs/PRODUCTION.md](docs/PRODUCTION.md) | the remaining AWS wiring |
| [docs/COSTS.md](docs/COSTS.md) | what that wiring costs — free tier, on-demand and committed rates for MSK, EC2, ClickHouse and the ALB |
| [docs/setup.md](docs/setup.md) | the narrow first goal: `/api/v1/signal` reachable on real AWS, producing to a real topic, TLS and the dispatcher deferred |
| [docs/AWS-SETUP.md](docs/AWS-SETUP.md) | the same ground click by click, assuming no prior AWS |

Read ARCHITECTURE before changing the pipeline.

## Architecture contract

[AGENTS.md](AGENTS.md) holds the module layout, the five-file module pattern, and
the layering rules: `src/modules/<name>/<name>.{module,routes,controller,service,schema}.ts`,
a one-way dependency direction `module → routes → controller → service`, and
`schema` as a shared leaf. `src/modules/health/` is the working reference.

It also covers the dispatcher, which follows the same shape under different
names. The tree below is a summary; AGENTS.md has the full one.

```
src/
├── server.ts                  listen + graceful shutdown
├── app.ts                     buildApp(): plugins, then modules
├── config.ts                  the ONLY place that reads process.env
├── plugins/
│   ├── core.ts                sensible, cors
│   ├── error-handler.ts       AppError -> the v1 envelope
│   ├── identity.ts            stamps signalId + receivedAt at onRequest
│   └── kafka.ts               decorates app.producer (SignalProducer | null)
├── client/                    outbound, THE WORKERS ONLY — the edge uses none
│   ├── clickhouse.ts          reader (dispatcher) + writer (consumer), over HTTP
│   ├── resolve-client.ts      resolveSignal() — ok | rejected | transient
│   └── payments-client.ts     settleAll() — one gzipped POST, nothing else
├── workers/consume/           the consumer process (LONG-LIVED, a systemd service)
│   ├── consume.schema.ts      ports + row types (a leaf)
│   ├── consume.service.ts     processBatch() — a pure function over the ports
│   ├── consume.archive.ts     the ONE INSERT into signal_log
│   └── consume.runner.ts      kafkajs wiring + entry point
├── workers/dispatch/          the dispatcher process (a ONE-SHOT, on a timer)
│   ├── dispatch.schema.ts     ports + row types (a leaf)
│   ├── dispatch.service.ts    runOnce() — a pure function over the ports
│   ├── dispatch.archive.ts    the ONE SELECT against signal_log
│   └── dispatch.runner.ts     one run, then exit + entry point
├── utils/
│   ├── envelope.ts            the public v1 envelope + the ErrorReason union
│   └── errors.ts              AppError + subclasses, each carrying a reason
└── modules/
    ├── auth/                  support module — no routes
    │   └── auth.service.ts    extractBearer + digestApiKey
    ├── signal/                POST /api/v1/signal
    └── health/                the reference implementation
```

## Commands

| Command | Notes |
| --- | --- |
| `npm run up` | one-shot setup: `.env`, containers, topic, ClickHouse schema |
| `npm run dev` | `tsx watch` — the edge, pretty logs |
| `npm run consume` | the consumer, in the foreground. LONG-LIVED — Ctrl-C to stop. Needs `INTERNAL_SETTLE_SECRET` and `KAFKA_BROKERS`, and exits 2 without either |
| `npm run dispatch` | ONE dispatch run, then exits; needs `INTERNAL_SETTLE_SECRET` |
| `npm run typecheck` | uses `tsconfig.json`, which **includes** test files |
| `npm run typecheck:scripts` | `scripts/**/*.mts` too — they are NOT in `tsconfig.json` |
| `npm run build` | uses `tsconfig.build.json`, which **excludes** `*.test.ts` |
| `npm start` | runs compiled `dist/server.js` |
| `npm test` | `node --import tsx --test` — 95 tests, no infrastructure needed |
| `npm run e2e` | the full pipeline against live Kafka + ClickHouse + Postgres |
| `npm run loadtest` | the generator in `scripts/loadtest.mjs` — `TOTAL`, `CONCURRENCY`, `REJECT_RATIO`, `EDGE_URL`, `LOAD_KEY`. Hits the EDGE only; reports throughput and p50/p95/p99 |
| `npm run down` | stop the containers (`-v` also wipes the ClickHouse volume) |

Four scripts read the live pipeline rather than asserting against it. They **assert
nothing and never fail** — they exist so the pipeline can be read instead of
reasoned about. All four borrow a real API key row the way `npm run e2e` does, and
restore it in a `finally`:

| Command | What it shows |
| --- | --- |
| `npm run trace` | ONE signal through every hop — the exact SQL, the exact HTTP payloads, the exact rows |
| `npm run trace:all` | one signal per route and outcome, printed as the matrix in ARCHITECTURE.md |
| `npm run trace:runner` | the dispatcher **as a real process**, not `runOnce` called by hand — what a one-shot does and what exit code it leaves. **It stops and restarts the ClickHouse container** to show exit 1 and recovery, and restarts it in a `finally` |
| `npm run send:100` | 100 real signals end to end, reporting what each hop did |

`send:100` deletes everything it writes **except the credit balance it draws
down** — that is real consumption against a real org.

Run a single test file:

```bash
node --import tsx --test src/workers/dispatch/dispatch.service.test.ts
```

`LOG_LEVEL=silent` suppresses Fastify's request logs, which otherwise interleave
with test output.

Two tsconfigs exist for one reason: tests must be type-checked but must not land
in `dist/`. Adding a compiler option means editing `tsconfig.json` (the build
config extends it).

`npm run e2e` needs the edge on :3000, the payments app on :3001, and
`docker compose up -d`. It borrows a real API key row, swaps its hash for one it
knows, and restores it in a `finally` — writing the original to a recovery file
*before* the swap. It refuses to run against `PRODUCTION_DATABASE_URL`.

## Gotchas that cost time

- **ESM with `module: nodenext`.** Relative imports need the `.js` extension even
  though the source is `.ts` (`./health.service.js`). Omitting it fails at
  runtime, not at typecheck. In `scripts/`, `.mts` files import each other as
  `.mjs`.
- **`verbatimModuleSyntax` is on.** Type-only imports must use
  `import type { X }` or `import { type X }`.
- **`setErrorHandler` needs an explicit generic.** Without it the `error`
  parameter is `unknown`. Use `app.setErrorHandler<FastifyError>(...)` — see
  [src/plugins/error-handler.ts](src/plugins/error-handler.ts).
- **`exactOptionalPropertyTypes` and `noUncheckedIndexedAccess` are on.** Indexed
  access yields `T | undefined`; `{ x: undefined }` is not assignable to `{ x?: T }`.
- **TypeScript 7 (native compiler).** Not all TS 5.x-era config options behave
  identically; verify rather than assuming.
- **`.env` is loaded by the npm scripts, not by the code.** `dev`, `start` and
  `dispatch` pass `--env-file-if-exists=.env`; `process.env` is still read only in
  [src/config.ts](src/config.ts). Run a binary directly and you get no `.env` —
  which is why `npm run dispatch` works and `tsx src/workers/…/dispatch.runner.ts`
  does not.
- **AJV type coercion is OFF**, against Fastify's default — set in
  [src/app.ts](src/app.ts). With it on, `"inputTokens": "1200"` would validate as
  1200 while the payments app's rulebook rejects a string, so the edge would
  accept a signal settlement later refuses. `allErrors` is on so one response
  names every broken field.
- **`signalId` is a ULID, minted in `onRequest`, not by the service.** ULID
  because it sorts by creation time, which is what `signal_log` is ordered by.
  In `onRequest` because a 415 never reaches a handler and a 404 never reaches a
  route, and both still need an id to quote back.
- **AWS credentials are not in [src/config.ts](src/config.ts)** — deliberately.
  The SDK's default chain reads them, so production is the instance role with no
  config key in between.
- **The ClickHouse init script runs on FIRST START ONLY.** It is skipped once the
  data volume exists. `docker/clickhouse/init/01-schema.sql` describes the schema
  for a *new* database; `docker/clickhouse/migrations/` carries the same changes
  for one that already has rows, and is what production runs. **Edit both.**
- **The ClickHouse client sets `date_time_input_format=best_effort` on every
  request.** Every timestamp this pipeline sends is ISO-8601 with a `T` and a `Z`,
  and ClickHouse's default `basic` format rejects both characters. It is set once
  in [src/client/clickhouse.ts](src/client/clickhouse.ts) rather than per query.
  That client also authenticates with HTTP Basic, not `?user=&password=`, which
  would land the password in the server's query log.
- **Kafka messages are keyed by `customerId`.** A no-op on today's single
  partition; it is there so growing the topic keeps a customer's signals together
  without a code change. Nothing downstream depends on that ordering.
- **`removeContentTypeParser('text/plain')` is module-scoped to signal.** That is
  what turns a wrong content type into the 415 it actually is instead of
  "body must be object". `/health/echo` still parses `text/plain`, and a test
  asserts exactly that.

### The ones that have already bitten

- **Never alias a ClickHouse column to its own name.** ClickHouse resolves a
  SELECT alias inside `WHERE`, so `toString(received_at) AS received_at` makes
  the WHERE compare a String against a DateTime64 and the query dies with
  *"No operation greater between String and DateTime64(3)"*. `JSONEachRow`
  already renders `DateTime64(3)` as a string, so the cast was never needed.
- **ClickHouse timestamps are UTC but do not say so.** It renders
  `DateTime64(3)` as `2026-08-26 10:00:00.000` — a space, no zone — and
  `new Date()` parses that as *local* time. Billing windows cut on `receivedAt`,
  so `toIso()` in `dispatch.service.ts` is load-bearing: a dispatcher in IST
  would otherwise bill every signal 5½ hours early.
- **Never window over `received_at`.** It is the caller's time, stamped by N edge
  instances off N clocks, and a row can land in ClickHouse minutes later — the
  Kafka engine flushes in batches. With no watermark anywhere in the pipeline, a
  window over `received_at` drops such a row permanently and silently. Window over
  `ingested_at`; bill on `received_at`.
- **`createMany` is all-or-nothing** (a payments-side rule this repo depends on).
  One bad row losing a whole batch's bookkeeping is not cosmetic: with no
  `SignalStatus` rows there is no record that a signal was ever settled, and the
  overlapping windows keep re-presenting it. Fail soft *per row*, never per batch.
- **Exit codes are the dispatcher's whole interface, and three error classes pick
  them.** `MisconfiguredError` (a refused shared secret, or no
  `INTERNAL_SETTLE_SECRET`), `BadReplayWindowError` (a malformed
  `DISPATCH_SINCE`/`DISPATCH_UNTIL`) and `PayloadTooLargeError` (settle answered
  413) all exit **2**; everything else exits **1**. Throwing a plain `Error` for a
  condition that will never self-heal turns a page-a-human failure into a silent
  retry loop.
- **`runOnce` deliberately does not catch.** The ports' failures propagate so the
  runner exits non-zero and the next window re-sends; swallowing one would hide a
  total failure behind a successful-looking run. A test asserts the propagation.

## Testing approach

`buildApp()` in [src/app.ts](src/app.ts) is deliberately separate from
[src/server.ts](src/server.ts) so tests call `app.inject()` with no port binding.
Test services as plain functions; test routes through `inject`. Always
`await app.close()` in a test that builds an app.

The dispatcher follows the same shape: `runOnce(deps)` is a pure function over
two narrow ports (`ArchiveReader`, `SettleClient`) declared in
`dispatch.schema.ts`, so a test hands it fakes and the runner binds the real
clients. Never reach for a driver inside the service.

`scripts/**/*.mts` are **not** in `tsconfig.json`, so `npm run typecheck` does not
see them — `npm run typecheck:scripts` does. Run it after changing anything the
e2e harness imports, or a stale reference sits there until it fails at runtime.

## The wire contract

Every response on `/api/v1/*` — success and failure — uses the public v1
envelope, ported from the payments repo to [src/utils/envelope.ts](src/utils/envelope.ts):

```json
{ "statusCode": 202,
  "statusDetail": { "status": "SUCCESS", "message": "Signal accepted for processing." },
  "result": { "accepted": true, "signalId": "01M0XCA…", "receivedAt": "2026-08-25T21:13:26.253Z" } }
```

`statusDetail.status` is binary `SUCCESS`/`ERROR`, not the four-value enum from
the payments app's internal envelope — `/api/v1/signal` is a public API-key
route, the same class as `/api/v1/usage`.

`result.errorReason` is a **wire contract**: a caller may branch on it, so
renaming a member of the `ErrorReason` union is a breaking change. Add, don't
rename.

| Where it fails | Status | `errorReason` |
| --- | --- | --- |
| path not matched (also a wrong method) | 404 | `NOT_FOUND` |
| content-type is not JSON, or absent | 415 | `UNSUPPORTED_MEDIA_TYPE` |
| body over `BODY_BYTES` | 413 | `BODY_TOO_LARGE` |
| body is not parseable JSON | 400 | `MALFORMED_JSON` |
| body is empty | 400 | `EMPTY_BODY` |
| body breaks a gate rule | 400 | `INVALID_BODY` + `issues[]` |
| no key, or no `Bearer` scheme | 401 | `API_KEY_MISSING` |
| the produce to Kafka failed | 502 | `QUEUE_UNAVAILABLE` |
| anything unhandled | 500 | `INTERNAL_ERROR` |

Two things this table encodes that are easy to undo by accident:

- **A 5xx never carries its cause.** `AppError.detail` ("connect ECONNREFUSED
  10.0.3.7:9092") goes to the log line, never to `result` — it names our hosts.
  `errorReason` is all a caller can act on anyway.
- **Every rejection still carries `signalId` and `receivedAt`**, stamped before
  anything could fail, so a caller can quote a hard 404 back and we can find it.

**Reserved but unreachable today**, kept because removing a union member a
caller might branch on is a breaking change:

| Member | Why it is dormant |
| --- | --- |
| `METHOD_NOT_ALLOWED` | Fastify answers a wrong method on a known path with 404 |
| `API_KEY_REJECTED` | only a status-derived fallback — the edge no longer resolves keys |
| `CUSTOM_TOO_LARGE` | the `custom` size check went with the old thick edge |
| `UPSTREAM_UNAVAILABLE` | `BadGatewayError`'s default; nothing passes it now |
| `EDGE_MISCONFIGURED` | belonged to the deleted `/api/internal/resolve` call |
| `BAD_REQUEST` | the generic fallback in `defaultReasonFor` |

## The signal path

`POST /api/v1/signal`. One signal, in order:

1. `plugins/identity.ts` (`onRequest`) stamps a ULID `signalId` and an ISO
   `receivedAt` — before anything can fail.
2. `signal.module.ts` (`onRequest`) requires an `Authorization: Bearer` header
   and stamps `request.apiKeyHash` = SHA-256 of the raw token. **Presence only —
   the edge does not resolve the key.**
3. The route's JSON schema judges the body.
4. `signal.controller.ts` produces to Kafka, then answers 202.

**The gate is thin on purpose.** `signal.schema.ts` requires exactly three
fields — `customerId`, `inputTokens`, `outputTokens` — and leaves
`additionalProperties` open so the whole body reaches the topic verbatim. The
full rulebook (types, models, agent keys, plans, allowances) is settle's job. A
signal that breaks a deeper rule is still archived and recorded as a
`USER_ERROR`; refusing it here would only lose it.

Two rules the envelope depends on:

- **The digest travels, never the key.** `digestApiKey` produces exactly what
  `ApiKey.hashedKey` stores upstream, so settle resolves the organisation with
  one unique-index hit and the raw `cnk_…` key never leaves this process. It is
  also not echoed back in the 202.
- **`apiKeyHash` rides on the envelope, never inside `body`.** `body` becomes
  `signal_log.payload`, which is the archive of what the caller actually sent.

**Produce BEFORE the 202, and 502 if it fails.** An accepted signal is billable,
so acknowledging one that reached no topic would lose it in silence with the
caller told it was safe. `app.producer` is null when `KAFKA_BROKERS` is unset, in
which case a 202 means only "passed the gate".

## The archive

**ONE table**, in `docker/clickhouse/init/01-schema.sql`: `signal_log`,
`ReplacingMergeTree(version)`, `ORDER BY (received_at, signal_id)`.

ClickHouse used to ingest from Kafka **itself** — an `ENGINE = Kafka` table
(`kafka_signals`) and a materialized view (`signal_log_mv`). **Both are gone**, and
`git show` is where they live now. A materialized view cannot make an HTTP request,
and every signal now needs one before it is archived, so `src/workers/consume/`
drains the topic instead. Do not reintroduce them from a stale doc.

- **`signal_log` still has exactly ONE writer** — the consumer now, not the MV.
  Everything else reads.
- **The archive is NO LONGER a pure function of the topic**, and that is the price
  of the redesign. A row carries a verdict that came from an HTTP call; a replay
  re-runs those calls, and a key that has since expired answers differently. The
  old "rebuildable by replaying the topic" guarantee is weaker than it was — say
  so rather than repeating the old claim.
- **Ingestion is at-least-once.** The consumer commits offsets only AFTER the
  insert lands, so a crash in between redelivers and the re-inserted row collapses
  on merge. Because merges are not immediate, every read that matters uses
  `LIMIT 1 BY signal_id` (cheaper than `FINAL`) or `count(DISTINCT signal_id)`.
- **Five columns hold the verdict**: `organization_id` (ours, resolved — `''` when
  the key never resolved), `status`, `error_code`, `error_message`, and
  `customer_id` (a lifted COPY, like `api_key_hash`). `status` is an INSTRUCTION to
  settle: `PROCESSING` price it, `PENDING` record the failure and price nothing,
  `SUCCESS` already settled — written only by the daily reconciliation cron, which
  owns the mapping from payments' own `PROCESSED`.
- **`version` is an ENGINE ARGUMENT, not a column that happens to exist.** It makes
  a row rewritable — re-inserted higher, newest wins — which the daily cron needs.
  Nothing writes anything but `1` today. It is in place early because a table's
  ENGINE cannot be `ALTER`ed: retrofitting it means a second table, a full copy and
  an `EXCHANGE`. Any process that rewrites a row **must carry the original
  `ingested_at` forward**, or the row drops back into the dispatcher's window and is
  re-presented to the money path.
- **`api_key_hash` is lifted into its own column** rather than left in the
  payload, because it is ours, not the caller's — `payload` must stay
  byte-for-byte what was sent.
- **`ingested_at` carries a `minmax` skip index**, because the dispatcher's window
  is over it and it is neither the `ORDER BY` key nor the partition key — without
  the index that window is a full scan of every partition, reading the fat
  `payload` column. Inserts arrive in roughly `ingested_at` order, so granule
  ranges stay tight.

## The consumer

`src/workers/consume/`. **A long-lived Kafka consumer group member** — the only
always-on process here besides the edge, and a systemd *service*, not a timer. One
started and exited every 60s would spend its life rebalancing the group.

**What drives it: kafkajs's fetch loop, not a timer.** The consumer long-polls the
broker continuously. With kafkajs's defaults (`minBytes: 1`, `maxWaitTimeInMs:
5000`, `maxBytesPerPartition: 1MB`) a fetch returns as soon as *any* message is
available, or empty after 5s. So "one batch" is not a configured size — it is
whatever accumulated on the partition since the last fetch, capped at 1MB. At
~250 bytes per signal that is up to roughly 4,000 messages in a single batch,
which is why `heartbeat()` between resolve calls is load-bearing rather than
defensive.

One batch is:

1. parse the messages the edge produced
2. one `/api/internal/resolve` call **per signal**, bounded concurrency
3. ONE ClickHouse insert for the resolved prefix
4. commit offsets — **last**, and only for what was actually written

**Order 3-before-4 is the whole safety property.** Commit first and a crash in
between loses the signal outright with the broker believing it was handled. Commit
last and a crash redelivers it, and `ReplacingMergeTree` collapses the duplicate.
At-least-once is the safe direction; an accepted signal is billable.

**Offsets are committed with `consumer.commitOffsets()`, never
`commitOffsetsIfNecessary()`.** That helper honours the autoCommit config, and the
consumer sets `autoCommit: false` deliberately so nothing commits behind its back —
which makes the helper a silent no-op. It was caught against a live broker: batches
logged `committed: 7` while the group's offset never moved, so every restart
re-read and re-resolved the whole backlog at one HTTP call per signal.
`resolveOffset` is separate bookkeeping — it tells kafkajs what not to refetch, and
commits nothing.

**A `MisconfiguredError` is recorded where it is THROWN.** kafkajs retries an
`eachBatch` throw and then raises `KafkaJSNumberOfRetriesExceeded`, which copies the
message but is a different object. Relying on `instanceof` after that trip made a
refused shared secret exit 1 instead of 2 — systemd restarting forever instead of
stopping with the reason on screen.

**`heartbeat()` runs between resolve calls.** A batch of 500 signals is 500 HTTP
calls — long enough that the broker decides the process is dead and rebalances the
group mid-batch without it. This is the most common way a consumer like this
breaks.

**A batch commits its resolved PREFIX, not all-or-nothing.** `stoppedAt` is the
index of the first message that could not be resolved; everything before it is
written and committed, the rest is redelivered. Failing the whole batch would
re-resolve 500 signals because #487 timed out — 500 HTTP calls already paid for.

**Poison vs outage needs BOTH conditions, and the second is easy to drop.** A
signal is quarantined (archived `PENDING`/`RESOLVE_FAILED` and stepped over) only
when it has failed `RESOLVE_POISON_AFTER` times in a row **and** some other call
has succeeded **since that streak began** (`succeededSinceStreak`).

The second condition is the definition, not a safety margin, and an elapsed-time
proxy is NOT equivalent — that was observed failing against a live broker. Five
failures accumulate in about five seconds; a two-minute outage window does not
expire for another two minutes. So a success from 70 seconds ago made a route that
was demonstrably down look alive, and a good signal was archived as a caller error
during a real outage. It compares a monotonic success COUNT, not timestamps:
batches complete inside one millisecond and a timestamp compare would tie.

With nothing succeeding there is nothing to call a signal poison against, so the
batch stalls and the runner exits once `RESOLVE_OUTAGE_MS` passes — systemd backs
off and every signal stays safe on the topic.

**A message that will not parse is DROPPED, not quarantined**, and logged at the
point of loss — a row in `signal_log` needs a `signal_id` to exist at all. The edge
is the only producer and always writes one, so this is corruption, not a case.

**Exit codes are the interface**: `0` clean shutdown on SIGTERM, `1` transient
(payments unreachable, ClickHouse down, broker gone — systemd restarts it), `2`
misconfigured. The unit sets `RestartPreventExitStatus=2` so a bad secret **stops
the unit** instead of hiding behind a restart loop.

**Consumer downtime becomes dispatcher row loss.** Kafka retains everything while
it is off — but the catch-up stamps every one of those rows with
`ingested_at = now`, so they all land in ONE dispatcher window and can blow past
`DISPATCH_MAX_ROWS`. Alarm on `run.capped`.

## The dispatcher

`src/workers/dispatch/`. **A one-shot on a 60-second systemd timer, not a loop
and not a long-lived process.** One run is:

1. one ClickHouse query for everything **ingested** in the last `DISPATCH_WINDOW_MS`
2. one gzipped POST of all of it to `/api/internal/settle`
3. one log line, then exit

Its whole contract with the payments app is that one route, guarded by
`INTERNAL_SETTLE_SECRET`. There is no cursor route and no known route any more.

**There is NO state of any kind** — no watermark, no cursor, no claim, no lease,
no local file. `SignalLog.signalId` is `UNIQUE` and settle uses it as the
idempotency key, so a signal sent twice replays onto the original money row
instead of charging again. That one fact is what pays for everything below.

Four things that are load-bearing:

- **The window is over `ingested_at`, NEVER `received_at`.** `received_at` is the
  caller's time, stamped by N edge instances off N clocks, and a row can land in
  ClickHouse minutes after it — the consumer batches, spends an HTTP round trip per
  signal, and a broker backlog delays it further. With no watermark, a window over
  `received_at` misses
  such a row **permanently**. `ingested_at` is `DEFAULT now64(3)`: one clock, one
  server, "arrived in the archive". `received_at` is still what settle bills on.
- **The lower bound is computed by ClickHouse**, from its own `now64(3)`, not by
  the dispatcher. `ingested_at` is stamped by the ClickHouse clock, so comparing
  it against a bound off the EC2 box's clock would shift the window by whatever
  skew exists between two machines — silently.
- **The overlap IS the error recovery.** The window is 3× the timer's interval, so
  every signal is sent about three times and settle discards the duplicates. A run
  that fails leaves nothing behind to resume from; it is simply covered by the next
  two. What that cannot cover is three consecutive failures, which is what the
  hourly reconciliation timer (a 2-hour window, same binary) is for.
- **`run.capped` means rows were lost.** `DISPATCH_MAX_ROWS` is an OOM guard, not
  a batch size. Hitting it means the window held more than one run will carry and
  the next window has already moved past some of them. Alarm on it.

**One call, whatever the size.** No batch size, no concurrency, no chunking. The
body is **gzipped**, which is not an optimisation: Vercel caps a serverless
function's request body at 4.5MB and raw signal JSON crosses that at roughly 15k
signals. Signal JSON compresses about 10:1. `dispatch.body_near_limit` warns
before the ceiling, because crossing it is a hard 413 for the whole window at
once.

**A caller cannot forge the envelope.** `toSettleSignal` spreads the payload
first and writes `signalId` / `receivedAt` / `apiKeyHash` / `organizationId` /
`status` / `errorCode` / `errorMessage` / `attempt` over it — the payload is
caller-controlled, and forwarding a body-supplied organisation id let a caller bill
another tenant (BUG-1 in [docs/FINDINGS.md](docs/FINDINGS.md)).

**`organizationId` is now WRITTEN, where BUG-1's fix DELETED it — do not "restore"
the delete.** The defence is unchanged: the caller's value never survives, because
the spread is overwritten. What changed is that there is now a *trusted* value to
overwrite it with, resolved by the consumer before the row was ever archived.
Deleting it today would throw away the attribution and force settle to re-resolve
every signal — the exact work the consumer exists to remove.

**Exit codes are the interface**: `0` sent or nothing to send, `1` transient (the
next tick is the retry — there is no backoff in here), `2` misconfigured (every
tick will fail identically until a human acts). Units and alerts:
[deploy/systemd/README.md](deploy/systemd/README.md).

**Retry policy now belongs to settle**, not here. The window re-sends everything
regardless of outcome, so settle must: re-process a `SERVER_ERROR`, no-op a
`PROCESSED`, and **never** retry a `USER_ERROR` (it cannot self-heal and waits for
a fix). Get that predicate wrong and either every transient failure becomes
permanent, or the whole window is re-priced every minute.

**Filling a gap**: `DISPATCH_SINCE` / `DISPATCH_UNTIL` read an explicit
`[since, until)` over `ingested_at` instead of the relative window. That is the
manual replay tool for an outage longer than the reconciliation window — no code
change, and no cursor to rewind.

## Configuration

Read only in [src/config.ts](src/config.ts). See `.env.example`, and the table in
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) §3.

Edge: `KAFKA_BROKERS` (empty = do not produce), `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`,
`KAFKA_USE_IAM`, `AWS_REGION`, `BODY_BYTES`, `HOST`, `PORT`, `LOG_LEVEL`.

Both workers: `CLICKHOUSE_URL`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, `PAYMENTS_URL`, `INTERNAL_SETTLE_SECRET` (required — each
exits 2 without one).

Consumer: `CONSUME_GROUP_ID` (changing it makes a NEW group with no offsets, which
then re-resolves the topic one HTTP call at a time — change it deliberately),
`CONSUME_CONCURRENCY` (the throughput ceiling), `CONSUME_FROM_BEGINNING` (a full
replay), `RESOLVE_TIMEOUT_MS`, `RESOLVE_POISON_AFTER` and `RESOLVE_OUTAGE_MS` (the
two halves of one rule — see the consumer section). It also reads the edge's
`KAFKA_*` keys.

Dispatcher: `DISPATCH_WINDOW_MS` (how far back, over `ingested_at`; keep it a
multiple of the timer's interval), `DISPATCH_MAX_ROWS` (an OOM guard — hitting it
loses rows), `DISPATCH_TIMEOUT_MS` (must exceed settle's `maxDuration`),
`DISPATCH_GZIP`, and `DISPATCH_SINCE` / `DISPATCH_UNTIL` for a manual replay.

## Repository state

- Branch `dev`. **The working tree is clean and everything in the
  source tree is committed** — the dispatcher's 1.0 rewrite, `deploy/systemd/`,
  `docker/clickhouse/migrations/` (001–003), `infra/aws/`, every doc, and the
  e2e/trace scripts all landed. Do not assume anything here is uncommitted, and do
  not trust a file count in a note like this one — check `git status`.
- **The Kafka→ClickHouse rewrite is IN THE WORKING TREE, not yet committed**:
  `src/workers/consume/`, `src/client/resolve-client.ts`, the ClickHouse writer,
  migrations `004`/`005`, the rewritten `01-schema.sql`, and
  `deploy/systemd/signal-consumer.service`. 95 tests pass and both typechecks are
  clean, but **`npm run e2e` cannot pass until the payments side lands** — see the
  next bullet.
- What is on disk but **gitignored**, and therefore invisible to `git ls-files`:
  `.env`, `dist/`, `dist-lambda/` (the deleted Lambda pipeline's build output),
  `volume/` (LocalStack runtime state — a generated CA and server keys),
  `signal-edge.tar.gz` (the build artifact `infra/aws` uploads to S3),
  `**/.terraform/`, `**/.terraform.lock.hcl` and `*.tfstate*`.
- **No SQS, no Lambda, no Redis, no Prisma, and no reject vent.** Earlier versions
  had all of them; `git log`/`git show` still has the files if you need the
  history. Do not reintroduce them from AGENTS.md's stale tree.
- **`infra/` is retired and `infra/aws/` replaces it.** The old root provisioned
  the LocalStack SQS/Lambda pipeline; only `infra/README.md` survives from it in
  git. Its two `terraform.tfstate` files still sit on disk describing nine
  LocalStack resources — untracked, unread, and never to be reused against a real
  account. `infra/aws/` is a SEPARATE root pointed at real AWS (see below).
  `terraform validate` passes; it has never been applied.
- **`Environment=` in `dispatch-reconcile.service` beats `EnvironmentFile=`**,
  which is how the hourly run gets its 2-hour window and 500k cap without a second
  `.env`. `EnvironmentFile=` itself is not optional in either unit: `.env` is read
  by the npm scripts, never by the code, so a direct `node dist/...` invocation
  gets no configuration at all and exits 2 every minute.
- **`deploy/systemd/`** holds three units: `signal-consumer.service` (always on)
  plus the two dispatch timers — the 1-minute pipeline run and the hourly
  reconciliation. The dispatcher still has no supervised long-lived process; the
  consumer is one, and is the only one.
- **Payments-side changes this repo now assumes, none of them landed yet:**
  - `/api/internal/resolve` must resolve the key **before** validating the body (so
    a rejection still names an org), accept `customerId` and check it against that
    org in the same call, answer **`403`** for a bad customer key with `401`
    reserved for OUR shared secret, and keep `5xx` for its own faults. The `401`
    split is the dangerous one: read as a customer problem it archives every signal
    on the topic as a caller error, silently.
  - `/api/internal/settle` must accept one call of arbitrary size (its 500 cap
    gone, its per-signal walk replaced by a bulk insert), decompress a gzipped
    body, and **trust the verdict** — use the supplied `organizationId` rather than
    re-resolving, and for a `PENDING` signal record the terminal `SignalStatus`
    without pricing anything.
  - Note the shared secret now protects tenant **attribution**, not just access: a
    leak lets a caller name any organisation, not merely replay signals.
- **`scripts/e2e.mts` §11 still asserts the OLD cap** — `501 signals is refused,
  not silently truncated` expects a 400 whose message names 501. That section
  contradicts the bullet above and must be rewritten in the same change that lifts
  the cap upstream, or a correct settle will fail the e2e. It is the only place
  the two halves of this transition are pinned against each other.
- **The topic is on ONE partition**, and grows to 3 only on a measured symptom
  (ClickHouse ingestion lagging, or head-of-line blocking) — see
  [docs/AWS-SETUP.md](docs/AWS-SETUP.md) Step 0. Growing is **cheap here**, which
  is unusual: adding partitions rehashes `customerId`→partition and splits a
  customer's ordered stream, but nothing in this pipeline reads the topic in
  order — `signal_log` is `ORDER BY (received_at, signal_id)` and dedups on
  `signal_id`, the dispatcher windows on `ingested_at`, settle is idempotent on
  `signalId`. Do not repeat the usual "ordering breaks" warning as if it applied.
  The one real hazard is a new partition starting at `latest` and silently
  skipping whatever the producer wrote before the consumer rebalanced onto it.
  That used to be handled by `kafka_auto_offset_reset = 'earliest'` on
  `kafka_signals`; **that table no longer exists**, so the guard now belongs to
  kafkajs — a new partition inherits the `signal-resolver` group's
  `auto.offset.reset`, which must be `earliest` *before* the topic grows.
  `CONSUME_FROM_BEGINNING` sets `fromBeginning` on subscribe, which is a
  different knob (it is about a group with NO offsets at all), so do not assume
  the two are the same lever — verify before adding partitions. Partition count
  can never be lowered.
- **Head-of-line blocking is now a real reason to add a partition**, where it was
  once hypothetical. One HTTP call per signal, in topic order, on one partition is
  this pipeline's throughput ceiling; watch `signal-resolver`'s lag.
- No linter or formatter is configured. `npm run typecheck` is the only static
  gate. As of the last sweep: 59 tests pass, both typechecks are clean.

## The AWS root (`infra/aws/`)

Six `.tf` files plus `user-data.sh.tpl`. It **reads** the pre-existing VPC,
subnets, `event-tasks` SG and MSK Serverless cluster as data sources and **owns**
only: the edge's IAM role/profile/policy, the deploy bucket, the instance, the
ALB with its SG and target group, and ONE ingress rule.

- **It owns exactly one rule on someone else's security group.** The edge joins
  the pre-existing `event-tasks` SG (MSK already allows 9098 from precisely that
  group), and Terraform takes `aws_vpc_security_group_ingress_rule.edge_from_alb`
  and nothing else. Importing the whole SG would put its MSK wiring at the mercy
  of this config.
- **The health check is `/health`, not `/api/v1/health`.** Only the signal module
  is mounted under `/api/v1`; the health module owns its own prefix. Point the
  target group at the wrong path and every target fails its check.
- **The artifact's `etag` is what makes a redeploy a redeploy.** `aws_s3_object.artifact`
  sets `etag = filemd5(...)`, which is what makes a rebuilt tarball a real diff and
  trips the instance's `user_data_replace_on_change`. Without it a redeploy is a
  no-op. A `precondition` fails the plan when the tarball is missing, and
  `aws_instance.edge` `depends_on` it so the box cannot boot with nothing to serve.
- **`nonsensitive()` on the AMI SSM parameter** is deliberate: `aws_ssm_parameter`
  marks every value sensitive, which would blank the AMI id out of `terraform plan`
  — the one line a reviewer most needs before launching an instance.
- **No credentials anywhere.** Access is Session Manager (`AmazonSSMManagedInstanceCore`),
  not a key pair; IMDSv2 is required; the edge's policy is narrower than the
  bastion's — `Connect` + `WriteData`/`WriteDataIdempotently`/`DescribeTopic` on
  the `signals` topic + `s3:GetObject` on its own bucket. No read, no CreateTopic,
  no consumer-group actions.
- **`user-data.sh.tpl` writes `/etc/signal-edge.env` and passes it with `--env-file`,**
  because nothing in the code loads a `.env` (see the gotcha below). It installs
  `nodejs22`, falling back to 20 then unversioned, because the project needs
  `--env-file` (Node ≥ 20.6) and AL2023's unversioned `nodejs` has been 18.

## The e2e harness

`scripts/e2e/` is shared by `e2e`, `trace`, `trace:all`, `trace:runner` and
`send:100`. Four things in it are load-bearing and easy to break:

- **`preload.mts` must stay the FIRST import of every runner.** `src/config.ts`
  snapshots `process.env` at import time, so the payments secrets have to be in
  place before anything imports config. ESM evaluates imports depth-first in
  source order, which is what makes "first import" mean "before config".
  It also keeps the shared secret off the command line and out of shell history.
- **The borrowed API key is restored through three separate paths**: a `finally`,
  a recovery file written to `$TMPDIR/events-microservice-e2e/` *before* the swap,
  and a signal safety net (`SIGINT`/`SIGTERM`/`SIGHUP`/`EPIPE`/`uncaughtException`).
  SIGPIPE is the one that actually bit — piping a run through `head` killed it
  mid-run, and the *next* run then saved that run's test digest as the "original".
  `setup()` therefore verifies the stored `hashedKey` against `ApiKey.keyEnc`
  (which no run ever writes) and repairs it before touching anything.
- **`pg.types.setTypeParser(1114, …)` in `harness.mts`** makes the raw driver read
  `timestamp without time zone` as UTC, the way Prisma does. Without it every
  comparison against the edge's ISO stamp is off by the local offset.
- **Scope is the ingestion window, not a watermark.** `E2E_WINDOW_MS` is 90s —
  wide enough to cover post→dispatch, tight enough to exclude the archive's older
  rows. `outcome.sent` is NOT "new work"; the evidence is always the Postgres rows.
