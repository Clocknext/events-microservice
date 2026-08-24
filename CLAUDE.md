# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture contract

[AGENTS.md](AGENTS.md) is the authoritative architecture spec — the module layout,
the five-file module pattern, and the layering rules. **Read it before adding or
changing a module.** It is not duplicated here.

The short version: `src/modules/<name>/<name>.{module,routes,controller,service,schema}.ts`,
with a one-way dependency direction `module → routes → controller → service` and
`schema` as a shared leaf. `src/modules/health/` is the working reference.

## Commands

| Command | Notes |
| --- | --- |
| `npm run dev` | `tsx watch src/server.ts` — pretty logs |
| `npm run typecheck` | Uses `tsconfig.json`, which **includes** test files |
| `npm run build` | Uses `tsconfig.build.json`, which **excludes** `*.test.ts` |
| `npm start` | Runs compiled `dist/server.js` |
| `npm test` | `node --import tsx --test` — discovers `src/**/*.test.ts` |
| `npm run test:watch` | Same, with `--watch` |

Run a single test file:

```bash
node --import tsx --test src/modules/health/health.service.test.ts
```

Filter by test name within a file:

```bash
node --import tsx --test --test-name-pattern='inject' src/modules/health/health.service.test.ts
```

`LOG_LEVEL=silent` suppresses Fastify's request logs, which otherwise interleave
with test output.

Two tsconfigs exist for one reason: tests must be type-checked but must not land
in `dist/`. Adding a compiler option means editing `tsconfig.json` (the build
config extends it).

## Gotchas that cost time

- **ESM with `module: nodenext`.** Relative imports need the `.js` extension even
  though the source is `.ts` (`./health.service.js`). Omitting it fails at
  runtime, not at typecheck.
- **`verbatimModuleSyntax` is on.** Type-only imports must use
  `import type { X }` or `import { type X }`. A plain `import` of a type is an error.
- **`setErrorHandler` needs an explicit generic.** Without it the `error`
  parameter is `unknown` and property access fails to compile. Use
  `app.setErrorHandler<FastifyError>(...)` — see
  [src/plugins/error-handler.ts](src/plugins/error-handler.ts).
- **`exactOptionalPropertyTypes` and `noUncheckedIndexedAccess` are on.** Indexed
  access yields `T | undefined`; `{ x: undefined }` is not assignable to `{ x?: T }`.
- **TypeScript 7 (native compiler).** Not all TS 5.x-era config options behave
  identically; verify rather than assuming.
- **Nothing loads `.env`.** `process.env` is read only in
  [src/config.ts](src/config.ts). Pass vars via the environment, or wire up
  `node --env-file` / `@fastify/env`.
- **AJV type coercion is OFF**, against Fastify's default — set in
  [src/app.ts](src/app.ts). With it on, `"inputTokens": "1200"` validated as
  1200 while the payments app's Zod rulebook rejects a string, so the edge would
  accept a signal settlement later refuses. `allErrors` is on for the same
  reason the resolve route returns an `issues` list.
- **`ioredis` needs a named import** (`import { Redis } from 'ioredis'`). It is
  CJS, and under `module: nodenext` its default export is the namespace — not
  constructable. Fails at typecheck, which is the good case.
- **Replacing the body stream in `preParsing` hides its size from Fastify.**
  `plugins/vent.ts` tees the raw bytes so an unparseable body is still recorded,
  and re-exposes `receivedEncodedLength` on the transform. Drop that line and
  Fastify's content-length check has nothing to read, so an oversized body stops
  being a 413. Note a declared oversized `content-length` is refused *before*
  the hook runs, so `BODY_TOO_LARGE` vents with an empty `payload` — correct,
  since buffering 80KB only to reject it would make an oversized body cheaper to
  send than to refuse.
- **`signalId` is a ULID, minted in `onRequest`, not by the service.** ULID
  because it sorts by creation time, which is what both ClickHouse tables are
  ordered by. In `onRequest` because a 415 never reaches a handler and a 404
  never reaches a route, and both still need an id.
- **AWS credentials are not in [src/config.ts](src/config.ts)** — deliberately.
  The SDK's default chain reads them, so local development is
  `AWS_PROFILE=localstack` in the environment and production is the instance
  role, with no config key in between.
- **The LocalStack init script must be executable on the host.** It is
  bind-mounted into `/etc/localstack/init/ready.d`, and a non-`+x` file fails
  with a bare `Permission denied` in the container log while everything else
  looks healthy.
- **The signal body schema is a hand-kept copy.**
  [signal.schema.ts](src/modules/signal/signal.schema.ts) mirrors
  `validateSignalBody` in the payments repo (`src/lib/settle/resolve-signal.ts`),
  which is itself a copy of settlement's `recordSchema`. Three copies, no
  automatic sync. Change a signal rule in one and change it in all of them, or
  the edge accepts signals settlement rejects.

## Testing approach

`buildApp()` in [src/app.ts](src/app.ts) is deliberately separate from
[src/server.ts](src/server.ts) so tests call `app.inject()` with no port binding.
Test services as plain functions; test routes through `inject`. Always
`await app.close()` in a test that builds an app.

## The wire contract

Every response on `/api/v1/*` — success and failure — uses the public v1
envelope, ported from `v1-response.ts` in the payments repo to
[src/utils/envelope.ts](src/utils/envelope.ts):

```json
{ "statusCode": 202,
  "statusDetail": { "status": "SUCCESS", "message": "Signal accepted for processing." },
  "result": { "accepted": true, "signalId": "…", "organizationId": "…", "cached": true } }
```

**Not** the internal envelope in the payments app's `envelope.ts` — that one's
own docstring excludes public API-key routes like `/api/v1/usage`, and
`/api/v1/signal` is the same class of route. So `statusDetail.status` here is
binary `SUCCESS`/`ERROR`, not the four-value enum.

`v1Error` upstream leaves `result` empty, so this edge puts `errorReason` there.
It is a **wire contract**: a caller may branch on it, so renaming a member of
the `ErrorReason` union is a breaking change. Add, don't rename.

| Where it fails | Status | `errorReason` |
| --- | --- | --- |
| path not matched (also a wrong method) | 404 | `NOT_FOUND` |
| content-type is not JSON | 415 | `UNSUPPORTED_MEDIA_TYPE` |
| body over `BODY_BYTES` | 413 | `BODY_TOO_LARGE` |
| body is not parseable JSON | 400 | `MALFORMED_JSON` |
| body is empty | 400 | `EMPTY_BODY` |
| body breaks a signal rule (**here or upstream**) | 400 | `INVALID_BODY` + `issues[]` |
| `custom` over `CUSTOM_BYTES` | 413 | `CUSTOM_TOO_LARGE` |
| no key, or no `Bearer` scheme | 401 | `API_KEY_MISSING` |
| key unknown / malformed / expired | 401 | `API_KEY_REJECTED` |
| payments unreachable or 5xx | 502 | `UPSTREAM_UNAVAILABLE` |
| **our** `INTERNAL_SETTLE_SECRET` refused | 502 | `EDGE_MISCONFIGURED` |
| anything unhandled | 500 | `INTERNAL_ERROR` |

Three things this table encodes that are easy to undo by accident:

- **`INVALID_BODY` is the same reason whether the edge or the payments app
  refused the body**, because it is the same rulebook. A caller must not have to
  care which one answered.
- **The two 502s are different reasons.** `UPSTREAM_UNAVAILABLE` waits itself
  out; `EDGE_MISCONFIGURED` never will, and needs a page. They must not share a
  code.
- **A 5xx never carries its cause.** `AppError.detail` ("connect ECONNREFUSED
  10.0.3.7:443") goes to the log line, never to `result` — it names our hosts.
  `errorReason` is all a caller can act on anyway.

`METHOD_NOT_ALLOWED` is reserved but unreachable today: Fastify answers a wrong
method on an existing path with 404, not 405.

## The signal path

`POST /api/v1/signal` is the ingest edge. One signal, in order:

1. the route's JSON schema judges the body (mirrored from the payments rulebook)
2. `signal.service` size-checks the `custom` blob
3. `auth.service` resolves the `cnk_…` key — Redis first, the payments app's
   `POST /api/internal/resolve` on a miss
4. the accepted signal is minted a `signalId` and returned as 202

Two normalisations happen in the route's `preValidation`, both mirroring how the
payments rulebook *reads* a body rather than changing what it means: `type` is
lowercased (JSON Schema cannot match an enum case-insensitively), and the
deprecated `key` alias is folded into `agentKey` (upstream reads
`agentKey ?? key`). Without the second, AJV's `anyOf` named *both* fields in the
`issues` list and told the caller to send `key` — the one field they should not.

**Step 4 stops there on purpose.** An *accepted* signal is not queued or settled
yet. A *refused* one already is — see the reject vent below.

Step 0, before any of this, is `plugins/vent.ts` stamping the request with a
ULID `signalId` and a `receivedAt`. The service does not mint its own: a refused
signal needs an identity too, and it must be the identity it *would* have had if
it were accepted.

The customer's raw key never leaves this process: only its SHA-256 digest
travels, in `X-Api-Key-Hash`. Two credentials ride on that upstream call and
they are not the same thing — `Authorization: Bearer <INTERNAL_SETTLE_SECRET>`
proves the *caller* is us, the digest names the *customer*.

What gets cached, and what must not:

| Outcome | Cached | Why |
| --- | --- | --- |
| key resolved | yes, 60s | clamped to the key's own `expiresAt` |
| key unknown / expired | yes, 30s | one dead key must not hammer a Vercel function |
| body rejected (400) | **no** | describes one request, not the key |
| upstream unreachable (502) | **no** | no verdict was reached; retry |

Two traps this path is built around:

- **A wrong `INTERNAL_SETTLE_SECRET` also answers 401.** Caching it as a key
  rejection would 401 every customer at once. The route answers a bad shared
  secret with exactly `"Unauthorized."` and every customer-key rejection with a
  specific sentence, so `payments-client` reads that one message as our fault
  and raises a 502 instead. A narrow string match — if the upstream message
  changes, this breaks quietly.
- **Redis fails open.** A dead cache is a slow path, never a broken one, so
  `enableOfflineQueue: false` makes a lookup fail in microseconds and the
  service treats it as a miss. Boot also survives Redis being unreachable.

Upstream validates the **body before the key**, so a bad body teaches us nothing
about the key — which is exactly why the edge mirrors the body rules.

## The reject vent

Every response with a status of **400 or above** — from any route, including a
404 for a path no route owns — is published to **`signals_pending`** as one
message.

There are two pipelines, one per outcome of a signal, and both queues exist:

| Queue | Carries | Delay | Publisher |
| --- | --- | --- | --- |
| `signals_pending` | every response ≥ 400, as two rows with `status: PENDING` | 60s | `plugins/vent.ts` |
| `signals_accepted` | every 202 | none | **not built yet** |

All four queues (each has a `<name>_dlq` behind it at `maxReceiveCount: 5`) are
**Standard, not FIFO**. Exactly-once would buy nothing — duplicates already
collapse on `signal_id` in `ReplacingMergeTree`, which at-least-once redelivery
requires anyway — while FIFO's 300/sec ceiling and mandatory `MessageGroupId`
would both cost. The reasoning is in
[scripts/localstack/ready.d/01-queues.sh](scripts/localstack/ready.d/01-queues.sh).

The message is **two ClickHouse rows and nothing else**:

```json
{ "raw_signals":   { "signal_id": "01M0SE…", "organization_id": "", "customer_id": "cus_e2e",
                     "received_at": "…", "idempotency_key": "idem_e2e", "api_key_id": "",
                     "payload": "{\"customerId\":\"cus_e2e\",…}" },
  "signal_status": { "signal_id": "01M0SE…", "attempt": 1, "status": "PENDING",
                     "error_type": "USER_ERROR", "error_code": "INVALID_BODY",
                     "error_message": "model: must have required property 'model'",
                     "signal_type": "credit", "…22 null columns…": null, "updated_at": "…" } }
```

The top-level keys are **table names**, so a worker draining the queue does two
bulk inserts and no transformation:

```js
insert('raw_signals',   batch.map((m) => m.raw_signals))
insert('signal_status', batch.map((m) => m.signal_status))
```

That is the whole reason the message may not grow a field. Anything the edge
knows that has no column — the HTTP status, the api-key digest, AJV's full
`issues` list — is dropped in `vent.service.ts` rather than smuggled alongside.
`error_message` therefore holds the **first** problem only, the same sentence
`statusDetail.message` gave the caller.

Four things here are forced by ClickHouse, not by taste:

- **`organization_id` is `''` on every reject, never null.** It is a non-Nullable
  `String`, and a JSON `null` into one is an *insert error*, not a default. It is
  empty rather than absent because every 4xx on the signal route is decided
  *before* `resolveApiKeyAndBody` returns — the edge never learns whose signal it
  was. Same for `customer_id` and `api_key_id`. Every `| null` field in
  `SignalStatusRow` must be declared `Nullable(...)` in the DDL for the same
  reason, in the other direction.
- **All 31 status columns are written, most of them null.** A rejected signal was
  never priced, so there is no money, credit or outcome data. Omitting them would
  work (JSONEachRow fills defaults) but the message would stop being a record of
  what the edge did and did not know.
- **`received_at` is ISO-8601**, which ClickHouse's default
  `date_time_input_format=basic` *rejects* — the `T` and the `Z`. The worker must
  insert with `date_time_input_format=best_effort`, or reformat.
- **`payload` is capped by its ENCODED size, not by `BODY_BYTES`.** A 64KB body
  is legal, but 64KB of control characters becomes 384KB of `\u00xx` escapes and
  SQS refuses anything over 256KB — so the send would throw, the vent would fail
  open, and the row would vanish. Since a body full of control bytes is not
  valid JSON, that is precisely the `MALFORMED_JSON` reject the raw capture
  exists to preserve. `serialiseVent` trims the payload to fit instead, iterating
  by code point so a surrogate pair is never split, and logs `droppedBytes`. The
  column has no room to record that it was cut, so that log line is the only
  trace.
- **Both tables must be `ReplacingMergeTree`.** SQS is at-least-once, and the
  worker's order is `raw_signals` → `signal_status` → delete. A failure between
  the two inserts redelivers the batch and re-inserts the first table. The engine
  is what makes that harmless.

Failure posture, and why it is the opposite of the accepted path's:

- **The vent fails open.** `app.queue` is null when `SQS_PENDING_QUEUE_URL` is
  unset, and a publish that throws is logged and swallowed. Venting is
  observability; it must never turn a 400 into a 500.
- **It publishes from `onResponse`**, after the response is flushed, so SQS is
  never on the caller's critical path. That is also the only hook that sees the
  status actually sent.
- When the *accepted* queue lands it gets the reverse treatment: publish
  **before** the 202, and 502 if it fails. Losing a billable signal after
  acknowledging it is not survivable; losing an analytics row is.

`DelaySeconds=60` on the queue is the hold — it is a **queue attribute**, set in
`scripts/localstack/ready.d/01-queues.sh`, so messages pile up for a batching
consumer. The publisher sets no per-message delay and does not know the number.

**Not built yet:** the worker, the ClickHouse client, the two `CREATE TABLE`s,
and the publisher for `signals_accepted` — the queue exists, nothing sends to it.

## Repository state

- **Not a git repository.** No commits exist. Do not assume git history is available.
- `prisma/` does not exist yet — no schema, no client, no database chosen. AGENTS.md
  records the intended wiring (a `src/plugins/prisma.ts` decorating `app.prisma`).
- `modules/auth/` has resolution + caching, but no route and no session handling.
- No queue yet. Kafka/ClickHouse config keys existed in a since-deleted second
  config file with no code behind them; they were dropped rather than carried.
- No linter or formatter is configured. `npm run typecheck` is the only static gate.
