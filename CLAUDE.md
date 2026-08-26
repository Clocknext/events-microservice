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

Four documents, in the order worth reading them:

| Doc | What |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | every route and every outcome, as a matrix from a live 71-signal run |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | the code that matters, the config, and a manual runbook using only `curl`/`jq`/`psql` |
| [docs/FINDINGS.md](docs/FINDINGS.md) | the bugs this design already hit, and why each fix is shaped the way it is |
| [docs/PRODUCTION.md](docs/PRODUCTION.md) | the remaining AWS wiring |

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
│   └── payments-client.ts     cursor / known / settle
├── workers/dispatch/          the dispatcher process
│   ├── dispatch.schema.ts     ports + row types (a leaf)
│   ├── dispatch.service.ts    sweepOnce() — a pure function over the ports
│   ├── dispatch.archive.ts    the two SELECTs against signal_log
│   └── dispatch.runner.ts     the self-pacing loop + entry point
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
| `npm run dispatch` | the dispatcher loop; needs `INTERNAL_SETTLE_SECRET` |
| `npm run typecheck` | uses `tsconfig.json`, which **includes** test files |
| `npm run build` | uses `tsconfig.build.json`, which **excludes** `*.test.ts` |
| `npm start` | runs compiled `dist/server.js` |
| `npm test` | `node --import tsx --test` — 47 tests, no infrastructure needed |
| `npm run e2e` | 110 checks against live Kafka + ClickHouse + Postgres |
| `npm run loadtest` | the generator in `scripts/loadtest.mjs` |
| `npm run down` | stop the containers (`-v` also wipes the ClickHouse volume) |

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
- **`createMany` is all-or-nothing.** One bad row losing a whole batch's
  bookkeeping is not cosmetic here: with no status rows the dispatcher's
  watermark never advances, so it re-sends the same signals forever. Fail soft
  *per row*, never per batch.

## Testing approach

`buildApp()` in [src/app.ts](src/app.ts) is deliberately separate from
[src/server.ts](src/server.ts) so tests call `app.inject()` with no port binding.
Test services as plain functions; test routes through `inject`. Always
`await app.close()` in a test that builds an app.

The dispatcher follows the same shape: `sweepOnce(deps)` is a pure function over
two narrow ports (`ArchiveReader`, `SettleClient`) declared in
`dispatch.schema.ts`, so a test hands it fakes and the runner binds the real
clients. Never reach for a driver inside the service.

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

## The dispatcher

`src/workers/dispatch/`. Reads the archive, posts to settle. Its contract with
the payments app is three routes, all guarded by `INTERNAL_SETTLE_SECRET`:

| Call | Purpose |
| --- | --- |
| `GET /api/internal/signals/cursor` | the watermark, plus the signals due for a retry |
| `POST /api/internal/signals/known` | of these ids, which already have a status row |
| `POST /api/internal/settle` | settle a batch of ≤ 500 |

**Why there is no claim, no lease and no cursor table.** `SignalLog.signalId` is
`UNIQUE` and settle passes it as the idempotency key, so a signal sent twice
replays onto the original money row instead of charging again. The dispatcher
therefore only has to guarantee **at least once** — over-sending costs a cheap
replay. Two dispatchers running at once are safe rather than a corruption bug.

**What is still outstanding lives in Postgres, not ClickHouse**, which has no row
updates and so records no outcome. `SignalStatus` is the ledger and the Signals
UI's read model at the same time.

Three details that are load-bearing:

- **The overlap window.** The sweep reads *below* the watermark by
  `DISPATCH_OVERLAP_MS`, because the Kafka engine flushes in batches and several
  edge instances stamp `receivedAt` from their own clocks — a row can appear with
  a timestamp just under one already seen. Reading strictly forward loses it,
  silently and permanently.
- **…and the filter that stops it looping.** Without `known`, every signal in
  that window is re-sent on every sweep. Correct (settle dedups) and ruinous: a
  caught-up pipeline would never go idle. Retries are exempt from the filter —
  they always have a status row, so filtering them would make retries impossible.
- **Batch size buys nothing; concurrency does.** Settle splits a batch across
  `INTERNAL_WORKER` workers that each walk their chunk one signal at a time, so a
  5,000-signal batch is the same rate with ten times the blast radius on a
  timeout. `DISPATCH_BATCH_SIZE=500` (settle's own enforced cap),
  `DISPATCH_CONCURRENCY=2`.

**A caller cannot forge the envelope.** `toSettleSignal` spreads the payload
first and writes `signalId` / `receivedAt` / `apiKeyHash` / `attempt` over it, and
**deletes `organizationId` outright** — the payload is caller-controlled, and
forwarding a body-supplied organisation id let a caller bill another tenant
(see BUG-1 in [docs/FINDINGS.md](docs/FINDINGS.md)).

**The loop is self-pacing** (`dispatch.runner.ts`): a full batch goes round again
immediately, a short one naps `DISPATCH_IDLE_MS`. A cron can do neither — it adds
latency when idle and cannot catch up when behind.

**Retry policy**, copied from the payments app's existing
`/api/cron/usage-logs/process`: retry a signal that never errored (the crash
case) or that last failed with `SERVER_ERROR`; **never** a `USER_ERROR`, which
cannot self-heal and waits for a fix and a manual retry; stop at 5 attempts.

## Configuration

Read only in [src/config.ts](src/config.ts). See `.env.example`, and the table in
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) §3.

Edge: `KAFKA_BROKERS` (empty = do not produce), `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`,
`KAFKA_USE_IAM`, `AWS_REGION`, `BODY_BYTES`, `HOST`, `PORT`, `LOG_LEVEL`.

Dispatcher: `CLICKHOUSE_URL`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, `PAYMENTS_URL`, `INTERNAL_SETTLE_SECRET` (required — it
refuses to start without one), `DISPATCH_BATCH_SIZE`, `DISPATCH_CONCURRENCY`,
`DISPATCH_IDLE_MS`, `DISPATCH_OVERLAP_MS`.

## Repository state

- Branch `Simple-version`. The last commit predates the dispatcher — the pipeline
  described here is **uncommitted working-tree state**.
- **No SQS, no Lambda, no Redis, no Prisma, and no reject vent.** Earlier versions
  had all of them; `git show HEAD:` still has the files if you need the history.
  Do not reintroduce them from AGENTS.md's stale tree.
- **`infra/`** holds only `provider.tf`, a README and Terraform state. The queue
  and Lambda definitions were deleted with the SQS pipeline; the AWS wiring that
  replaces them is described, not yet provisioned — see
  [docs/PRODUCTION.md](docs/PRODUCTION.md).
- **The topic is on ONE partition.** That means one consumer of order, no
  consumer-side HA, and head-of-line blocking. Repartitioning later moves
  key→partition placement, so it should be decided before production.
- No linter or formatter is configured. `npm run typecheck` is the only static
  gate.
