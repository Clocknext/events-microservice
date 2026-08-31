# Graph Report - .  (2026-08-29)

## Corpus Check
- 64 files · ~56,497 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 248 nodes · 399 edges · 13 communities (12 shown, 1 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 9 edges (avg confidence: 0.73)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Signal Edge and Kafka Producer|Signal Edge and Kafka Producer]]
- [[_COMMUNITY_Error Envelope and Failure Mapping|Error Envelope and Failure Mapping]]
- [[_COMMUNITY_Dispatcher IO ClickHouse and Settle|Dispatcher IO: ClickHouse and Settle]]
- [[_COMMUNITY_Runtime Package Dependencies|Runtime Package Dependencies]]
- [[_COMMUNITY_TypeScript Compiler Config|TypeScript Compiler Config]]
- [[_COMMUNITY_Dispatch Windowing Logic|Dispatch Windowing Logic]]
- [[_COMMUNITY_Edge Load Generator|Edge Load Generator]]
- [[_COMMUNITY_npm Script Commands|npm Script Commands]]
- [[_COMMUNITY_Health Module Reference|Health Module Reference]]
- [[_COMMUNITY_Local Stack Bootstrap|Local Stack Bootstrap]]
- [[_COMMUNITY_API Key Gate and Digest|API Key Gate and Digest]]
- [[_COMMUNITY_Scripts Typecheck Config|Scripts Typecheck Config]]
- [[_COMMUNITY_Build Typecheck Config|Build Typecheck Config]]

## God Nodes (most connected - your core abstractions)
1. `compilerOptions` - 20 edges
2. `scripts` - 17 edges
3. `ErrorReason` - 11 edges
4. `AppError` - 11 edges
5. `buildApp()` - 8 edges
6. `Config` - 8 edges
7. `up.sh script` - 7 edges
8. `settleAll()` - 6 edges
9. `SignalMessage` - 6 edges
10. `run()` - 6 edges

## Surprising Connections (you probably didn't know these)
- `buildApp()` --indirect_call--> `healthModule()`  [INFERRED]
  src/app.ts → src/modules/health/health.module.ts
- `buildApp()` --indirect_call--> `signalModule()`  [INFERRED]
  src/app.ts → src/modules/signal/signal.module.ts
- `capture()` --calls--> `run()`  [INFERRED]
  src/client/payments-client.test.ts → src/workers/dispatch/dispatch.runner.ts
- `runOnce()` --indirect_call--> `row()`  [INFERRED]
  src/workers/dispatch/dispatch.service.ts → src/workers/dispatch/dispatch.service.test.ts
- `post()` --calls--> `buildApp()`  [EXTRACTED]
  src/modules/signal/signal.service.test.ts → src/app.ts

## Import Cycles
- None detected.

## Communities (13 total, 1 thin omitted)

### Community 0 - "Signal Edge and Kafka Producer"
Cohesion: 0.09
Nodes (20): buildApp(), Config, VALID, postSignal(), errorResponse, AcceptedSignalResult, signalAcceptedResultSchema, SignalBody (+12 more)

### Community 1 - "Error Envelope and Failure Mapping"
Cohesion: 0.12
Nodes (17): FASTIFY_CODE_REASONS, send(), STRUCTURAL_KEYWORDS, ApiEnvelope, ApiStatus, errorEnvelope(), ErrorReason, ErrorResult (+9 more)

### Community 2 - "Dispatcher IO: ClickHouse and Settle"
Cohesion: 0.14
Nodes (17): ClickHouseReader, createClickHouseReader(), Envelope, gzip, MisconfiguredError, PayloadTooLargeError, readEnvelope(), settleAll() (+9 more)

### Community 3 - "Runtime Package Dependencies"
Cohesion: 0.08
Nodes (23): author, dependencies, aws-msk-iam-sasl-signer-js, fastify, @fastify/cors, fastify-plugin, @fastify/sensible, kafkajs (+15 more)

### Community 4 - "TypeScript Compiler Config"
Cohesion: 0.09
Nodes (22): compilerOptions, declaration, exactOptionalPropertyTypes, forceConsistentCasingInFileNames, isolatedModules, lib, module, moduleResolution (+14 more)

### Community 5 - "Dispatch Windowing Logic"
Cohesion: 0.19
Nodes (13): ArchiveReader, ArchiveWindow, RunOutcome, SettleClient, SettleResult, SettleSignal, SignalLogRow, RunConfig (+5 more)

### Community 6 - "Edge Load Generator"
Cohesion: 0.13
Nodes (16): AGENT_KEYS, codes, CONCURRENCY, CUSTOMERS, headers, latencies, MODELS, pick() (+8 more)

### Community 7 - "npm Script Commands"
Cohesion: 0.12
Nodes (17): scripts, build, dev, dispatch, down, e2e, loadtest, send:100 (+9 more)

### Community 8 - "Health Module Reference"
Cohesion: 0.18
Nodes (8): healthModule(), healthRoutes(), EchoBody, echoBodySchema, echoResponseSchema, healthResponseSchema, getHealth(), HealthStatus

### Community 9 - "Local Stack Bootstrap"
Cohesion: 0.31
Nodes (8): ch(), die(), env_set(), fail(), ok(), up.sh script, step(), warn()

### Community 10 - "API Key Gate and Digest"
Cohesion: 0.36
Nodes (6): digestApiKey(), extractBearer(), FastifyRequest, signalModule(), signalRoutes(), envelopeSchema()

### Community 11 - "Scripts Typecheck Config"
Cohesion: 0.29
Nodes (6): compilerOptions, declaration, noEmit, rootDir, extends, include

## Knowledge Gaps
- **93 isolated node(s):** `name`, `version`, `description`, `main`, `test` (+88 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **1 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Config` connect `Signal Edge and Kafka Producer` to `Dispatcher IO: ClickHouse and Settle`?**
  _High betweenness centrality (0.075) - this node is a cross-community bridge._
- **Why does `scripts` connect `npm Script Commands` to `Runtime Package Dependencies`?**
  _High betweenness centrality (0.017) - this node is a cross-community bridge._
- **What connects `name`, `version`, `description` to the rest of the system?**
  _93 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Signal Edge and Kafka Producer` be split into smaller, more focused modules?**
  _Cohesion score 0.08780487804878048 - nodes in this community are weakly interconnected._
- **Should `Error Envelope and Failure Mapping` be split into smaller, more focused modules?**
  _Cohesion score 0.12413793103448276 - nodes in this community are weakly interconnected._
- **Should `Dispatcher IO: ClickHouse and Settle` be split into smaller, more focused modules?**
  _Cohesion score 0.14245014245014245 - nodes in this community are weakly interconnected._
- **Should `Runtime Package Dependencies` be split into smaller, more focused modules?**
  _Cohesion score 0.08333333333333333 - nodes in this community are weakly interconnected._