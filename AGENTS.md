# AGENTS.md

Fastify 5 + TypeScript service. This file is the architecture contract — follow it
for every new feature. TypeScript, so files are `.ts`, not `.js`.

## File structure

```
root
├── prisma/
│   ├── schema.prisma            data model
│   └── migrations/              generated, committed
└── src/
    ├── server.ts                listen + graceful shutdown. Nothing else.
    ├── app.ts                   buildApp(): registers plugins, then modules
    ├── config.ts                env parsing, validated once at boot
    ├── plugins/                 app-wide concerns, wrapped in fastify-plugin
    │   ├── core.ts              sensible, cors
    │   ├── error-handler.ts     AppError -> HTTP response
    │   ├── redis.ts             decorates app.cache (KeyCache | null)
    │   ├── sqs.ts               decorates app.queue (SignalQueue | null)
    │   └── vent.ts              stamps every request; publishes every 4xx/5xx
    ├── client/                  outbound HTTP to the payments app
    │   ├── payments-client.ts   /api/internal/resolve, /api/internal/settle
    │   └── types.ts             wire types for those endpoints
    ├── utils/                   framework-agnostic helpers
    │   ├── envelope.ts          the public v1 response envelope + ErrorReason
    │   └── errors.ts            AppError + subclasses, each carrying a reason
    └── modules/
        ├── auth/                support module — no routes, so only two files
        │   ├── auth.service.ts  api-key resolution + the Redis caches
        │   └── auth.schema.ts   ResolvedApiKey, KeyResolution, KeyCache port
        ├── vent/                support module — the reject queue's payload
        │   ├── vent.service.ts  builds the two ClickHouse rows; publishes them
        │   └── vent.schema.ts   RawSignalRow, SignalStatusRow, SignalQueue port
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

A module with no HTTP surface (`auth/`, `vent/`) carries only the files it needs.
The five-file shape is the rule for a module that owns routes; it is not a quota.

`vent/` is a module rather than a util because it owns a contract — two
ClickHouse row shapes and a queue port. Its *adapter* is a plugin (`vent.ts`)
because the rule it implements is app-wide: everything that is not an accepted
signal, including 404s no module owns. A hook, not a controller, is its caller.

## The five files in a module

Every module is exactly these five files, named `<module>.<role>.ts`. Add a
sixth only for a genuinely new role (e.g. `auth.repository.ts`).

| File | Responsibility | Must not |
| --- | --- | --- |
| `*.module.ts` | Entry point. Owns the URL prefix, registers routes and module-scoped plugins. | Contain logic |
| `*.routes.ts` | Binds method + path + schema + controller handler. | Contain logic |
| `*.controller.ts` | HTTP adapter: reads `request`, returns a payload. | Contain business rules or touch the DB |
| `*.service.ts` | Business logic and data access. Plain functions over plain values. | Import `FastifyRequest`/`FastifyReply` or reference HTTP at all |
| `*.service.ts` deps | Infrastructure a service needs (Redis, later Prisma) arrives as a **function argument**, typed as a narrow port the module declares in its own `*.schema.ts` — see `KeyCache`. | Import a driver or reach for `app.*` itself |
| `*.schema.ts` | JSON schemas for validation/serialization + the TS types routes are generic over. | Import from the other four |

Dependency direction is one-way: `module → routes → controller → service`.
`schema` is a leaf everyone may import. A service never imports upward.

## Rules

- **ESM.** Relative imports carry the `.js` extension (`./health.service.js`),
  even though the source is `.ts`. This is required by `module: nodenext`.
- **Register a module in one place**: one `app.register(xModule)` line in
  [src/app.ts](src/app.ts). Plugins first, modules after.
- **Prefix belongs to the module**, set in `*.module.ts`. Routes use paths
  relative to it (`'/'`, `'/echo'`), never a hardcoded full path.
- **Validate at the edge.** Every route with input declares a body/params/query
  schema, and the route is generic over the matching type
  (`app.post<{ Body: EchoBody }>`). No manual input checking in controllers.
- **Add a response schema** for each status you return — it is the serializer,
  so it also stops accidental field leaks.
- **Errors**: services throw `AppError` subclasses from
  [src/utils/errors.ts](src/utils/errors.ts). Controllers do not catch them;
  [src/plugins/error-handler.ts](src/plugins/error-handler.ts) maps them to
  responses. Never build an error response by hand in a controller.
- **Nothing app-wide in a module.** Cross-cutting behavior goes in `src/plugins/`
  and must be wrapped in `fastify-plugin` so it escapes encapsulation.
- **`utils/` stays framework-agnostic.** If it imports `fastify`, it is a plugin,
  not a util.
- **Config only from [src/config.ts](src/config.ts).** No `process.env` reads
  anywhere else.

## Adding a module

1. `mkdir src/modules/<name>` and create the five `<name>.*.ts` files.
2. Export a `FastifyPluginAsync` named `<name>Module` from `<name>.module.ts`.
3. Add one `await app.register(<name>Module)` in `src/app.ts`.
4. `npm run typecheck` must pass before the work is considered done.

## Commands

| Command | What it does |
| --- | --- |
| `npm run dev` | `tsx watch` — reload on save, pretty logs |
| `npm run typecheck` | Type-check, no emit |
| `npm run build` | `src/` → `dist/` |
| `npm start` | Run compiled `dist/server.js` |
| `npm test` | `node --test` |

## Testing

`buildApp()` is deliberately separate from `server.ts` so tests use
`app.inject()` with no port binding. Test services as plain functions; test
routes through `inject`.

## Not yet set up

- `prisma/` — no schema, no client, no DB chosen yet. Adding it means
  `npm i prisma @prisma/client`, a `prisma/schema.prisma`, and a
  `src/plugins/prisma.ts` that decorates the instance (`app.prisma`) and closes
  the client in `onClose`. Services import that decorator, never their own client.
- `modules/auth/` — resolution + caching are in place; there is no auth *route*
  and no session handling.
- **Queue hand-off, accepted half.** The `signals_accepted` queue exists but
  nothing publishes to it — `signal.service.ts` accepts a signal and returns it.
  When the publisher lands it must send **before** the 202 and 502 on failure,
  the reverse of the vent, which fails open. The *pending* half is done: see
  "The reject vent" in CLAUDE.md.
- **The ClickHouse side.** No client, no worker, no DDL. The vent's messages are
  already shaped as `raw_signals` and `signal_status` rows, so what is missing is
  a consumer that batches off SQS and bulk-inserts. Both tables must be
  `ReplacingMergeTree` — the reasoning is in CLAUDE.md and is not optional.
