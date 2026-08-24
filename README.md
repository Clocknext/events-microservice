# aws-internal

Fastify 5 + TypeScript service.

## Commands

| Command | What it does |
| --- | --- |
| `npm run dev` | Run with `tsx watch` — reloads on save, pretty logs |
| `npm run typecheck` | Type-check without emitting |
| `npm run build` | Compile `src/` → `dist/` |
| `npm start` | Run the compiled server from `dist/` |
| `npm test` | `node --test` (no tests yet) |

## Layout

```
src/
  server.ts   listen + graceful shutdown
  app.ts      buildApp() — registers plugins, then modules
  config.ts   env parsing, validated once at boot
  plugins/    app-wide concerns (core, error-handler)
  utils/      framework-agnostic helpers (errors)
  modules/
    health/   health.{module,routes,controller,service,schema}.ts
```

Architecture is a hard convention — see [AGENTS.md](AGENTS.md) before adding a
module. `buildApp()` is separate from `server.ts` so tests can use `app.inject()`
without binding a port.

## Routes

| Method | Path | |
| --- | --- | --- |
| `GET` | `/health` | status + uptime |
| `POST` | `/health/echo` | body `{ "message": string }` — schema-validation example |

## Config

Copy `.env.example` and set as needed: `NODE_ENV`, `HOST`, `PORT`, `LOG_LEVEL`.
Nothing loads `.env` automatically — pass vars via the environment, or add
`node --env-file=.env` / `@fastify/env` when you need it.

Logs are pretty-printed in development and raw JSON in production;
`trustProxy` turns on only when `NODE_ENV=production`.
