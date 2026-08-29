# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

Two processes that share one codebase:

- **The edge** (`src/app.ts`, `src/modules/`, `src/plugins/`) — a Fastify service.
  `POST /api/v1/signal` is a gate: it checks the shape of a signal, stamps it with
  an identity, produces it to Kafka, and answers 202.
- **The dispatcher** (`src/workers/dispatch/`) — a separate long-lived process. It
  reads the ClickHouse archive and posts batches to the payments app's
  `/api/internal/settle`. It is NOT part of the Fastify app and imports no plugin.

```
customer ──▶ EDGE ──produce──▶ Kafka `signals` ──pulled by──▶ ClickHouse signal_log
             (202)                                                     │  the archive
                                                                       ▼
                                        DISPATCHER ──▶ payments /api/internal/settle
                                                              │
                                                              ▼
                                                     Postgres: SignalLog (money)
                                                               SignalStatus (lifecycle)
```

The payments app lives in a **separate repository** (`Clocknext-Payment-Saas`) and
owns all pricing and all Postgres writes. This repo never talks to Postgres in
production code — only the E2E script does, to assert.

Seven documents, in the order worth reading them:

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
├── client/                    outbound, DISPATCHER ONLY — the edge uses neither
│   ├── clickhouse.ts          read-only reader over HTTP + JSONEachRow
│   └── payments-client.ts     settleAll() — one gzipped POST, nothing else
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
| `npm run dispatch` | ONE dispatch run, then exits; needs `INTERNAL_SETTLE_SECRET` |
| `npm run typecheck` | uses `tsconfig.json`, which **includes** test files |
| `npm run typecheck:scripts` | `scripts/**/*.mts` too — they are NOT in `tsconfig.json` |
| `npm run build` | uses `tsconfig.build.json`, which **excludes** `*.test.ts` |
| `npm start` | runs compiled `dist/server.js` |
| `npm test` | `node --import tsx --test` — 59 tests, no infrastructure needed |
| `npm run e2e` | the full pipeline against live Kafka + ClickHouse + Postgres |
| `npm run loadtest` | the generator in `scripts/loadtest.mjs` |
| `npm run down` | stop the containers (`-v` also wipes the ClickHouse volume) |

Four scripts read the live pipeline rather than asserting against it. They **assert
nothing and never fail** — they exist so the pipeline can be read instead of
reasoned about. All four borrow a real API key row the way `npm run e2e` does, and
restore it in a `finally`:

| Command | What it shows |
| --- | --- |
| `npm run trace` | ONE signal through every hop — the exact SQL, the exact HTTP payloads, the exact rows |
| `npm run trace:all` | one signal per route and outcome, printed as the matrix in ARCHITECTURE.md |
| `npm run trace:runner` | the dispatcher **as a real process**, not `runOnce` called by hand — what a one-shot does and what exit code it leaves |
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

### Three that have already bitten

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
- **`createMany` is all-or-nothing.** One bad row losing a whole batch's
  bookkeeping is not cosmetic here: with no status rows the dispatcher's
  watermark never advances, so it re-sends the same signals forever. Fail soft
  *per row*, never per batch.

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

ClickHouse ingests from Kafka **itself** — there is no consumer process. Three
objects, in `docker/clickhouse/init/01-schema.sql`:

| Object | Role |
| --- | --- |
| `kafka_signals` | `ENGINE = Kafka` over the topic, `JSONAsString` into one `raw` column |
| `signal_log_mv` | the materialized view that drains it |
| `signal_log` | `ReplacingMergeTree`, `ORDER BY (received_at, signal_id)` |

- **`signal_log` has exactly ONE writer**, the materialized view. Everything else
  reads. That is what keeps the archive rebuildable by replaying the topic — a
  second writer would put rows in it that no replay could reproduce.
- **Ingestion is at-least-once.** The Kafka engine commits offsets only after the
  MV insert lands, so a redelivery re-inserts a row; `ReplacingMergeTree`
  collapses it on merge. Because merges are not immediate, every read that
  matters uses `LIMIT 1 BY signal_id` (cheaper than `FINAL`) or
  `count(DISTINCT signal_id)`.
- **`api_key_hash` is lifted into its own column** rather than left in the
  payload, because it is ours, not the caller's — `payload` must stay
  byte-for-byte what was sent.
- **`ingested_at` carries a `minmax` skip index**, because the dispatcher's window
  is over it and it is neither the `ORDER BY` key nor the partition key — without
  the index that window is a full scan of every partition, reading the fat
  `payload` column. Inserts arrive in roughly `ingested_at` order, so granule
  ranges stay tight.

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
  ClickHouse minutes after it — the Kafka engine flushes in batches, and a broker
  backlog delays it further. With no watermark, a window over `received_at` misses
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
first and writes `signalId` / `receivedAt` / `apiKeyHash` / `attempt` over it, and
**deletes `organizationId` outright** — the payload is caller-controlled, and
forwarding a body-supplied organisation id let a caller bill another tenant
(see BUG-1 in [docs/FINDINGS.md](docs/FINDINGS.md)).

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

Dispatcher: `CLICKHOUSE_URL`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, `PAYMENTS_URL`, `INTERNAL_SETTLE_SECRET` (required — it
exits 2 without one), `DISPATCH_WINDOW_MS` (how far back, over `ingested_at`;
keep it a multiple of the timer's interval), `DISPATCH_MAX_ROWS` (an OOM guard —
hitting it loses rows), `DISPATCH_TIMEOUT_MS` (must exceed settle's
`maxDuration`), `DISPATCH_GZIP`, and `DISPATCH_SINCE` / `DISPATCH_UNTIL` for a
manual replay.

## Repository state

- Branch `Simple-version`, last commit `84db9ca`. The dispatcher **is** committed;
  what is still uncommitted is its 1.0 rewrite plus everything around it. Untracked
  and therefore invisible to `git show HEAD:`: `deploy/systemd/`,
  `docker/clickhouse/migrations/` (both `002` and `003`), `docs/AWS-SETUP.md`,
  `docs/COSTS.md`, `docs/setup.md`, `scripts/send-100.mts`, `tsconfig.scripts.json`,
  and the `payments-client` / `dispatch.archive` test files. Modified on top of
  HEAD: the whole `src/workers/dispatch/` tree, `src/config.ts`,
  `src/plugins/kafka.ts`, `src/client/payments-client.ts`, every doc, and the e2e
  scripts.
- **No SQS, no Lambda, no Redis, no Prisma, and no reject vent.** Earlier versions
  had all of them; `git show HEAD:` still has the files if you need the history.
  Do not reintroduce them from AGENTS.md's stale tree.
- **`infra/` is retired and `infra/aws/` replaces it.** The old root provisioned
  the LocalStack SQS/Lambda pipeline; its `queues.tf`/`lambdas.tf`/`variables.tf`
  went with that pipeline, and its `provider.tf` is deleted too (it referenced
  variables that no longer existed, so `terraform init` failed on it). Its two
  `terraform.tfstate` files still describe nine LocalStack resources and are left
  in place, untracked and unread — never reuse that lineage against a real
  account. `infra/aws/` is a SEPARATE root pointed at real AWS: it *reads* the
  existing VPC, subnets, `event-tasks` SG and MSK cluster as data sources and
  owns only the edge's IAM role, deploy bucket, instance, ALB and one ingress
  rule. `terraform validate` passes; it has never been applied. See
  [infra/aws/README.md](infra/aws/README.md) and
  [docs/setup.md](docs/setup.md).
- **`deploy/systemd/`** holds the two timer units that run the dispatcher — the
  1-minute pipeline run and the hourly reconciliation. They are the deployment;
  there is no supervised long-lived dispatcher process any more.
- **The dispatcher's 1.0 rewrite assumes two payments-side changes**: settle must
  accept one call of arbitrary size (its 500 cap gone, its per-signal walk
  replaced by a bulk insert), and it must decompress a gzipped request body. Until
  both land, `npm run e2e` cannot pass.
- **The topic is on ONE partition**, and grows to 3 only on a measured symptom
  (ClickHouse ingestion lagging, or head-of-line blocking) — see
  [docs/AWS-SETUP.md](docs/AWS-SETUP.md) Step 0. Growing is **cheap here**, which
  is unusual: adding partitions rehashes `customerId`→partition and splits a
  customer's ordered stream, but nothing in this pipeline reads the topic in
  order — `signal_log` is `ORDER BY (received_at, signal_id)` and dedups on
  `signal_id`, the dispatcher windows on `ingested_at`, settle is idempotent on
  `signalId`. Do not repeat the usual "ordering breaks" warning as if it applied.
  The one real hazard is a new partition starting at `latest` and silently
  skipping whatever the producer wrote before the consumer rebalanced onto it,
  which is why **`kafka_auto_offset_reset = 'earliest'`** is set on
  `kafka_signals` (migration `003`) — it must be in place *before* the topic
  grows. Partition count can never be lowered.
- No linter or formatter is configured. `npm run typecheck` is the only static
  gate.
