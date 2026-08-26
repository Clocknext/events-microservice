# AGENTS.md

Fastify 5 + TypeScript. This file is the architecture contract — the layout and
the layering rules. Follow it for every new feature. TypeScript, so files are
`.ts`, not `.js`.

For what the pipeline *does* — the signal path, the archive, the dispatcher, the
wire contract, and every outcome a signal can have — see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), then
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md). This file is about where code
goes, not what it means.

## Two processes, one codebase

| | Entry point | What it is |
| --- | --- | --- |
| **the edge** | `src/server.ts` | a Fastify service. `POST /api/v1/signal` — gate, stamp, produce to Kafka, 202 |
| **the dispatcher** | `src/workers/dispatch/dispatch.runner.ts` | a long-lived loop. Reads the ClickHouse archive, posts batches to the payments app |

They share `config.ts` and nothing else. The dispatcher imports **no plugin and
no Fastify**; the edge imports **nothing under `client/`**. Keep it that way — the
moment the edge opens a ClickHouse connection, or the dispatcher reaches for
`app.*`, the split stops being real.

## File structure

```
root
├── docker/clickhouse/
│   ├── init/                    first-start-only schema (new databases)
│   └── migrations/              the same changes for a database with rows
├── docs/                        ARCHITECTURE · IMPLEMENTATION · FINDINGS · PRODUCTION
├── scripts/
│   ├── up.sh                    one-shot local setup (npm run up)
│   ├── loadtest.mjs
│   └── e2e.mts + e2e/           end-to-end run, harness, fixtures, the two traces
└── src/
    ├── server.ts                listen + graceful shutdown. Nothing else.
    ├── app.ts                   buildApp(): registers plugins, then modules
    ├── config.ts                env parsing, validated once at boot
    ├── plugins/                 app-wide concerns, wrapped in fastify-plugin
    │   ├── core.ts              sensible, cors
    │   ├── error-handler.ts     AppError -> the v1 envelope
    │   ├── identity.ts          stamps signalId + receivedAt at onRequest
    │   └── kafka.ts             decorates app.producer (SignalProducer | null)
    ├── client/                  outbound HTTP — DISPATCHER ONLY, never the edge
    │   ├── clickhouse.ts        read-only reader (HTTP + JSONEachRow)
    │   └── payments-client.ts   cursor / known / settle
    ├── workers/dispatch/        the dispatcher process
    │   ├── dispatch.schema.ts   ports + row types (a leaf)
    │   ├── dispatch.service.ts  sweepOnce() — a pure function over the ports
    │   ├── dispatch.archive.ts  the two SELECTs against signal_log
    │   └── dispatch.runner.ts   the self-pacing loop + entry point
    ├── utils/                   framework-agnostic helpers
    │   ├── envelope.ts          the public v1 envelope + the ErrorReason union
    │   └── errors.ts            AppError + subclasses, each carrying a reason
    └── modules/
        ├── auth/                support module — no routes, so one file
        │   └── auth.service.ts  extractBearer + digestApiKey
        ├── signal/              POST /api/v1/signal
        │   ├── signal.module.ts
        │   ├── signal.routes.ts
        │   ├── signal.service.ts
        │   ├── signal.controller.ts
        │   └── signal.schema.ts
        └── health/              reference implementation — copy this shape
            ├── health.module.ts
            ├── health.routes.ts
            ├── health.service.ts
            ├── health.controller.ts
            └── health.schema.ts
```

A module with no HTTP surface (`auth/`) carries only the files it needs. The
five-file shape is the rule for a module that owns routes; it is not a quota.

`workers/` is **not** part of the Fastify app. `dispatch.runner.ts` is its own
entry point, run under a supervisor next to the edge. It shares `config.ts` and
the two clients, and imports no plugin.

## The five files in a module

Every module is exactly these five files, named `<module>.<role>.ts`. Add a
sixth only for a genuinely new role (e.g. `auth.repository.ts`).

| File | Responsibility | Must not |
| --- | --- | --- |
| `*.module.ts` | Entry point. Owns the URL prefix, registers routes and module-scoped plugins. | Contain business logic |
| `*.routes.ts` | Binds method + path + schema + controller handler. | Contain logic |
| `*.controller.ts` | HTTP adapter: reads `request`, returns a payload. | Contain business rules or reach a driver |
| `*.service.ts` | Business logic. Plain functions over plain values. | Import `FastifyRequest`/`FastifyReply` or reference HTTP at all |
| `*.service.ts` deps | Infrastructure a service needs arrives as a **function argument**, typed as a narrow port the module declares in its own `*.schema.ts`. | Import a driver or reach for `app.*` itself |
| `*.schema.ts` | JSON schemas for validation/serialization + the TS types routes are generic over. | Import from the other four |

Dependency direction is one-way: `module → routes → controller → service`.
`schema` is a leaf everyone may import. A service never imports upward.

**The dispatcher follows the same shape** under different names:
`dispatch.schema.ts` declares the ports (`ArchiveReader`, `SettleClient`),
`dispatch.service.ts` is a pure function over them, `dispatch.archive.ts` and
`client/` are the adapters, and `dispatch.runner.ts` binds them. That is why
`sweepOnce` is tested with fakes and no infrastructure.

## Rules

- **ESM.** Relative imports carry the `.js` extension (`./health.service.js`),
  even though the source is `.ts`. Required by `module: nodenext`. In
  `scripts/`, `.mts` files import each other as `.mjs`.
- **Register a module in one place**: one `app.register(xModule)` line in
  [src/app.ts](src/app.ts). Plugins first, modules after.
- **Prefix belongs to the module**, set in `*.module.ts`. Routes use paths
  relative to it (`'/'`, `'/signal'`), never a hardcoded full path.
- **Validate at the edge.** Every route with input declares a body/params/query
  schema, and the route is generic over the matching type
  (`app.post<{ Body: SignalBody }>`). No manual input checking in controllers.
- **Add a response schema** for each status you return — it is the serializer,
  so it also stops accidental field leaks. It is why the api-key digest cannot
  escape in a 202 even if someone adds it to the result object.
- **Errors**: services throw `AppError` subclasses from
  [src/utils/errors.ts](src/utils/errors.ts). Controllers do not catch them;
  [src/plugins/error-handler.ts](src/plugins/error-handler.ts) maps them to
  responses. Never build an error response by hand in a controller.
- **`ErrorReason` is a wire contract.** A caller may branch on it, so renaming a
  member is a breaking change. Add, don't rename — and don't delete a dormant
  one. See the reserved table in CLAUDE.md.
- **Nothing app-wide in a module.** Cross-cutting behavior goes in `src/plugins/`
  and must be wrapped in `fastify-plugin` so it escapes encapsulation.
- **`utils/` stays framework-agnostic.** If it imports `fastify`, it is a plugin,
  not a util.
- **Config only from [src/config.ts](src/config.ts).** No `process.env` reads
  anywhere else. Nothing loads a `.env` file — pass vars via the environment.
- **`client/` is the dispatcher's.** The edge must not import from it. The edge's
  only outbound dependency is Kafka, via `plugins/kafka.ts`.
- **`signal_log` is read-only to this codebase.** It has exactly one writer, the
  ClickHouse materialized view. A second writer would put rows in the archive
  that replaying the topic could not reproduce.

## Adding a module

1. `mkdir src/modules/<name>` and create the five `<name>.*.ts` files.
2. Export a `FastifyPluginAsync` named `<name>Module` from `<name>.module.ts`.
3. Add one `await app.register(<name>Module)` in `src/app.ts`.
4. `npm run typecheck` must pass before the work is considered done.

## Commands

| Command | What it does |
| --- | --- |
| `npm run up` | sets up `.env`, containers, the topic and the ClickHouse schema |
| `npm run dev` | `tsx watch` — the edge, reload on save |
| `npm run dispatch` | the dispatcher loop; needs `INTERNAL_SETTLE_SECRET` |
| `npm run typecheck` | type-check, no emit (includes tests) |
| `npm run build` | `src/` → `dist/` (excludes tests) |
| `npm start` | run compiled `dist/server.js` |
| `npm test` | `node --test` — no infrastructure needed |
| `npm run e2e` | end to end against live Kafka + ClickHouse + Postgres |
| `npm run trace` | walks one signal through every hop, printing each payload |
| `npm run trace:all` | one signal per case — the matrix in docs/ARCHITECTURE.md |
| `npm run trace:runner` | the dispatcher LOOP as a process: pacing, backoff, SIGTERM |
| `npm run loadtest` | the generator in `scripts/loadtest.mjs` |

## Testing

`buildApp()` is deliberately separate from `server.ts` so tests use
`app.inject()` with no port binding. Test services as plain functions; test
routes through `inject`. Always `await app.close()` in a test that builds an app.

The same rule gives the dispatcher its tests: because `sweepOnce` takes its two
ports as arguments, a test hands it an array-backed archive and a fake settle
client, and covers batching, concurrency, retries and the known-filter without a
container in sight. If a new piece of the pipeline is hard to test, the port is
probably missing.

## Not in this repository

- **Postgres, Prisma and all pricing.** They belong to the payments app
  (`Clocknext-Payment-Saas`), which this repo reaches only over
  `/api/internal/*`. There is no `prisma/` directory and there should not be
  one. `pg` is a **devDependency**, used solely by the E2E script to assert on
  rows.
- **SQS, Lambda, Redis, and the reject vent.** Earlier versions had all of them
  — `plugins/redis.ts`, `plugins/sqs.ts`, `plugins/vent.ts`, `modules/vent/`,
  `client/types.ts`, `workers/pending.handler.ts`, `workers/accepted.handler.ts`.
  All deleted. `git show HEAD:<path>` still has them if you need the history; do
  not reintroduce them.
- **API-key resolution at the edge.** The edge checks a key is *present* and
  digests it. Who the key belongs to is settled by `/api/internal/settle`. There
  is no cache and no upstream call on the ingest path.
- **A linter or formatter.** `npm run typecheck` is the only static gate.
