# events-microservice

Two processes, one codebase.

- **The edge** (`src/app.ts`, `src/modules/`, `src/plugins/`) — a Fastify 5
  service. `POST /api/v1/signal` is a gate: it checks the shape of a signal,
  stamps it with an identity, produces it to Kafka, and answers 202.
- **The dispatcher** (`src/workers/dispatch/`) — a separate one-shot process on a
  60-second systemd timer. It reads one window of the ClickHouse archive and
  posts all of it to the payments app's `/api/internal/settle`, then exits.

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

ClickHouse ingests from Kafka itself — a `Kafka` table engine plus a
materialized view, so there is no consumer process to run. Pricing and every
Postgres write belong to the payments app, which lives in a separate repository
(`Clocknext-Payment-Saas`). Nothing here talks to Postgres in production code;
only the E2E script does, to assert.

## Getting started

```bash
npm run up      # one-shot: writes .env, starts containers, creates the topic and schema
npm run dev     # the edge on :3000, pretty logs
npm test        # 59 tests, no infrastructure needed
```

## Commands

| Command | What it does |
| --- | --- |
| `npm run up` | one-shot setup: `.env`, containers, topic, ClickHouse schema |
| `npm run dev` | `tsx watch` — the edge, pretty logs |
| `npm run dispatch` | ONE dispatch run, then exits; needs `INTERNAL_SETTLE_SECRET` |
| `npm test` | `node --import tsx --test` — no infrastructure needed |
| `npm run typecheck` | `tsconfig.json`, which **includes** test files |
| `npm run typecheck:scripts` | `scripts/**/*.mts`, which are NOT in `tsconfig.json` |
| `npm run build` | `tsconfig.build.json`, which **excludes** `*.test.ts` |
| `npm start` | runs compiled `dist/server.js` |
| `npm run e2e` | the full pipeline against live Kafka + ClickHouse + Postgres |
| `npm run loadtest` | the generator in `scripts/loadtest.mjs` |
| `npm run down` | stop the containers (`-v` also wipes the ClickHouse volume) |

Four more scripts **read** the live pipeline instead of asserting against it —
they assert nothing and never fail, so the pipeline can be read rather than
reasoned about: `npm run trace` (one signal through every hop),
`npm run trace:all` (one signal per route and outcome), `npm run trace:runner`
(the dispatcher as a real process), and `npm run send:100`.

Run a single test file:

```bash
node --import tsx --test src/workers/dispatch/dispatch.service.test.ts
```

`LOG_LEVEL=silent` suppresses Fastify's request logs, which otherwise interleave
with test output.

## Routes

| Method | Path | |
| --- | --- | --- |
| `GET` | `/health` | status + uptime |
| `POST` | `/health/echo` | body `{ "message": string }` — schema-validation example |
| `POST` | `/api/v1/signal` | the gate — `Bearer` key, three required fields, 202 |

Note the health routes are **not** under `/api/v1`. A load balancer's health
check must point at `/health`.

Every response on `/api/v1/*` — success and failure — uses the public v1
envelope in [src/utils/envelope.ts](src/utils/envelope.ts). `result.errorReason`
is a wire contract: a caller may branch on it, so renaming a member of the
`ErrorReason` union is a breaking change. Add, don't rename. The full outcome
table is in [CLAUDE.md](CLAUDE.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Layout

```
src/
├── server.ts                  listen + graceful shutdown
├── app.ts                     buildApp(): plugins, then modules
├── config.ts                  the ONLY place that reads process.env
├── plugins/                   core, error-handler, identity, kafka
├── client/                    outbound, DISPATCHER ONLY — clickhouse, payments-client
├── workers/dispatch/          the dispatcher process (a one-shot, on a timer)
├── utils/                     envelope + errors
└── modules/
    ├── auth/                  support module — no routes
    ├── signal/                POST /api/v1/signal
    └── health/                the reference implementation
```

Architecture is a hard convention — see [AGENTS.md](AGENTS.md) before adding a
module: `<name>.{module,routes,controller,service,schema}.ts`, a one-way
dependency direction `module → routes → controller → service`, and `schema` as a
shared leaf. `buildApp()` is separate from `server.ts` so tests can call
`app.inject()` without binding a port.

## Config

Copy `.env.example`. Every variable is read in exactly one place,
[src/config.ts](src/config.ts).

`.env` is loaded by the **npm scripts**, not by the code — `dev`, `start` and
`dispatch` pass `--env-file-if-exists=.env`. Run a binary directly and you get no
`.env`, which is why `npm run dispatch` works and
`tsx src/workers/dispatch/dispatch.runner.ts` does not. A systemd unit that runs
`node` directly must set `EnvironmentFile=`.

Edge: `KAFKA_BROKERS` (empty = do not produce), `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`,
`KAFKA_USE_IAM`, `AWS_REGION`, `BODY_BYTES`, `HOST`, `PORT`, `LOG_LEVEL`.

Dispatcher: `CLICKHOUSE_*`, `PAYMENTS_URL`, `INTERNAL_SETTLE_SECRET` (required —
it exits 2 without one), `DISPATCH_WINDOW_MS`, `DISPATCH_MAX_ROWS`,
`DISPATCH_TIMEOUT_MS`, `DISPATCH_GZIP`, and `DISPATCH_SINCE` / `DISPATCH_UNTIL`
for a manual replay.

AWS credentials are deliberately **not** in `config.ts` — the SDK's default chain
reads them, so production is the instance role with no config key in between.

## Docs

| Doc | What |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | every route and every outcome, as a matrix from a live 71-signal run |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | the code that matters, the config, and a manual runbook |
| [docs/FINDINGS.md](docs/FINDINGS.md) | the bugs this design already hit, and why each fix is shaped the way it is |
| [docs/PRODUCTION.md](docs/PRODUCTION.md) | the remaining AWS wiring |
| [docs/COSTS.md](docs/COSTS.md) | what that wiring costs |
| [docs/setup.md](docs/setup.md) | the narrow first goal: the edge reachable on real AWS |
| [docs/AWS-SETUP.md](docs/AWS-SETUP.md) | the same ground click by click, assuming no prior AWS |

Read ARCHITECTURE before changing the pipeline. [CLAUDE.md](CLAUDE.md) collects
the gotchas that have already cost time.

## Deployment

The edge runs on EC2 under `edge.service`; the dispatcher runs as two systemd
timers in [deploy/systemd/](deploy/systemd/) — the 1-minute pipeline run and the
hourly reconciliation. Both expect the repository at `/srv/events-microservice`.
There is no supervised long-lived dispatcher process.

Terraform for the edge's own AWS footprint — IAM role, deploy bucket, EC2
instance, ALB — lives in [infra/aws/](infra/aws/README.md). It validates but has
never been applied. The layers beyond it are described in
[docs/PRODUCTION.md](docs/PRODUCTION.md).

## Status

The edge, the dispatcher, the archive schema, the systemd units and the
`infra/aws/` Terraform are built. Outstanding: nothing is provisioned on AWS yet
(`infra/aws/` validates but has never been applied), ClickHouse and the domain/TLS
layer are still console-only in [docs/AWS-SETUP.md](docs/AWS-SETUP.md), and
`npm run e2e` waits on two payments-side changes — settle accepting one call of
arbitrary size, and decompressing a gzipped body.

No linter or formatter is configured. `npm run typecheck` is the only static
gate.
