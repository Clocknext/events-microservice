# Production setup — signal ingest → Kafka → ClickHouse

How to finish taking the current local setup to production. Local dev already
works end to end (edge → Kafka container → ClickHouse Kafka engine → `signal_log`);
this doc is the remaining AWS wiring.

## Architecture

```
API clients ──▶ edge (Fastify, box-edge) ──produce──▶ Kafka (box-kafka) topic `signals` (1 partition)
                                                          │  ClickHouse pulls it itself
                                                          ▼
                                                 kafka_signals (ENGINE=Kafka)
                                                          │  materialized view
                                                          ▼
                                                 signal_log (ReplacingMergeTree)
```

- **No consumer/worker process for INGEST.** ClickHouse's Kafka table engine does
  it. A separate dispatcher one-shot, on a 60s timer, reads one window of
  `signal_log` and posts all of it to the payments app's `/api/internal/settle`
  in a single call — see "The dispatcher" below.
- The Node app is **only the edge producer** (`src/`), run as a long-lived process.
- **One partition**, keyed by `customerId`. Grows to 3 on a measured symptom, and
  growing is cheap because nothing reads the topic in order — but only with
  `kafka_auto_offset_reset = 'earliest'` already set (migration `003`). See
  [AWS-SETUP.md](AWS-SETUP.md) Step 0.
- Billing data: at-least-once (ClickHouse commits offsets only after the MV
  insert) + dedup on `signal_id` (`ReplacingMergeTree`); money reads use
  `count(DISTINCT signal_id)` / `FINAL`.

## Status

| Piece | State |
| --- | --- |
| Edge producer (kafkajs, IAM-ready via `KAFKA_USE_IAM`) | ✅ built |
| Local Kafka + ClickHouse Kafka engine + schema | ✅ built |
| MSK cluster + topic | ⬜ provision |
| ClickHouse host + prod schema (with broker auth) | ⬜ provision |
| EC2 for the edge + IAM role + service supervision | ⬜ provision |
| Kafka ↔ ClickHouse auth decision (see below) | ⬜ **decide first** |

## ⚠️ Decide first: how ClickHouse authenticates to Kafka

This is the one place the "ClickHouse pulls directly" model constrains the AWS
choice, so settle it before provisioning:

- **MSK Serverless is IAM-only** (SASL/OAUTHBEARER with `AWS_MSK_IAM`). The **edge
  handles this fine** — kafkajs signs an IAM token (`aws-msk-iam-sasl-signer-js`,
  already wired; set `KAFKA_USE_IAM=true`). Note that **MSK provisioned also
  supports IAM**, so choosing provisioned does not mean giving up IAM — it only
  adds SCRAM and mTLS as options.
- **ClickHouse's Kafka engine (librdkafka) has no built-in AWS MSK IAM token
  provider.** So ClickHouse **cannot** natively consume from MSK Serverless.

Pick one path:

1. **MSK provisioned + SASL/SCRAM (recommended for self-hosted ClickHouse).**
   MSK provisioned supports SCRAM; ClickHouse's Kafka engine speaks
   `SASL_SSL` + `SCRAM-SHA-512` out of the box. The edge can use SCRAM too, or
   keep IAM if you enable both. Simplest way to keep "ClickHouse pulls directly".
2. **ClickHouse Cloud + ClickPipes, on MSK provisioned with IAM.** ClickPipes is a
   managed Kafka connector that *does* speak MSK IAM, which removes the librdkafka
   limitation entirely — so the edge and the ingest both keep IAM. ClickPipes
   replaces the `kafka_signals`/MV pair (the destination `signal_log` table is
   unchanged) and runs inside ClickHouse Cloud, so there is still no consumer
   process of ours. Choose this if you want managed ClickHouse.

   ⚠️ **Not on MSK Serverless.** ClickPipes reaches a private cluster through a
   *reverse private endpoint*, whose documented setup requires MSK **multi-VPC
   private connectivity** — you enable it on the cluster and grant the ClickPipes
   account (`arn:aws:iam::072088201116:root`) `kafka:CreateVpcConnection`,
   `GetBootstrapBrokers`, `DescribeCluster`, `DescribeClusterV2` in the cluster
   policy. That feature belongs to **provisioned** clusters. MSK Serverless does
   not expose it and has no public endpoint either, so reaching it from ClickHouse
   Cloud means VPC peering, Transit Gateway, or a self-managed NLB plus your own
   PrivateLink endpoint service, with Route 53 private hosted zones or resolver
   rules for the broker DNS. Provisioned + IAM is also **cheaper** than the
   Serverless floor — see [COSTS.md](COSTS.md).
3. **mTLS (TLS client certs)** on MSK provisioned — also supported by librdkafka;
   viable but more cert plumbing than SCRAM.

The rest of this doc assumes **path 1 (MSK provisioned + SCRAM)** for the
self-hosted ClickHouse case, and notes ClickPipes deltas where relevant.

## 1. MSK (Kafka)

- Create an **MSK provisioned** cluster (path 1) in the app's VPC/subnets. Enable
  **SASL/SCRAM** auth; store the broker user/password in **Secrets Manager**
  (MSK requires the secret name to start with `AmazonMSK_`) and associate it with
  the cluster. Enable TLS in transit.
- Topic **`signals`**: **1 partition** (Step 0 of AWS-SETUP.md), replication
  factor **3**, `min.insync.replicas=2`, and a generous **retention**
  (e.g. 7 days) so ClickHouse can be rebuilt by replay after any incident.
  Replication factor 3 assumes a **3-broker** cluster; the single-broker plan in
  AWS-SETUP.md §4 uses factor 1, since asking for more replicas than brokers
  fails outright. Partitions and replicas are independent knobs — 3 partitions on
  one broker is not replicated.
- Create the topic explicitly (don't rely on auto-create):
  `kafka-topics.sh --create --topic signals --partitions 1 --replication-factor 3 ...`
  (run from a bastion/admin box that can reach the brokers, authenticated with the
  SCRAM creds).
- Security group: allow the edge SG and the ClickHouse SG inbound on the SASL_SSL
  port (9096 for SCRAM on MSK).

> **The provisioning runbook is [AWS-SETUP.md](AWS-SETUP.md)** and it is the
> authority on the deployed shape: **two** EC2 boxes — `box-edge` (edge + dispatch
> timers) and `box-kafka` (self-hosted Kafka, public `SASL_SSL` on 9094) — plus
> ClickHouse Cloud, one ALB and one CloudFront distribution. **No MSK.** The MSK
> path below is kept as the comparison that decision was made against; where the
> two disagree, AWS-SETUP.md wins.

## 2. The edge on EC2

- **Instance role** (if using IAM to Kafka; optional under SCRAM): attach a policy
  granting `kafka-cluster:Connect`, `kafka-cluster:WriteData`,
  `kafka-cluster:DescribeTopic`, `kafka-cluster:WriteDataIdempotently` scoped to
  the cluster/topic ARNs. Under SCRAM instead, put the SCRAM creds in the env/secret.
- **Env** (see `.env.example`):
  - `KAFKA_BROKERS` = MSK bootstrap brokers (SASL_SSL endpoint).
  - `KAFKA_USE_IAM=true` + `AWS_REGION` if using IAM; otherwise wire SCRAM (see
    note below).
  - `KAFKA_TOPIC=signals`, `KAFKA_CLIENT_ID=signal-edge`.
- **Build & run:** `npm ci && npm run build`, then run `node dist/server.js` under
  a supervisor (systemd unit or pm2) with auto-restart. Front it with an ALB/API
  gateway terminating TLS; the edge trusts the proxy (`trustProxy` in prod).
- SCRAM note: the current producer supports PLAINTEXT and IAM. If you choose SCRAM
  for the edge too, add a SASL/SCRAM branch in `src/plugins/kafka.ts`
  (`sasl: { mechanism: 'scram-sha-512', username, password }`, `ssl: true`) — a
  small addition mirroring the existing IAM branch.

## 3. ClickHouse

- **Deploy** ClickHouse (self-managed on EC2, or ClickHouse Cloud for path 2) in
  a subnet that can reach the MSK brokers; open the ClickHouse SG → MSK SG.
- **Broker auth for the Kafka engine (path 1, SCRAM).** Put credentials in a
  server-side **named collection** (never inline in SQL) via a config file, e.g.
  `/etc/clickhouse-server/config.d/kafka.xml`:

  ```xml
  <clickhouse>
    <named_collections>
      <kafka_signals_creds>
        <kafka_security_protocol>SASL_SSL</kafka_security_protocol>
        <kafka_sasl_mechanism>SCRAM-SHA-512</kafka_sasl_mechanism>
        <kafka_sasl_username>__from_secrets_manager__</kafka_sasl_username>
        <kafka_sasl_password>__from_secrets_manager__</kafka_sasl_password>
      </kafka_signals_creds>
    </named_collections>
  </clickhouse>
  ```

- **Apply the schema** from `docker/clickhouse/init/01-schema.sql` on a NEW
  database. On one that already has rows, run the scripts in
  `docker/clickhouse/migrations/` instead — the init script is first-start-only,
  so the two are edited together and the migrations are what production runs.
  Two prod edits to the `kafka_signals` table:
  - `kafka_broker_list` = the MSK **bootstrap brokers** (not `kafka:29092`).
  - add the auth settings (or reference the named collection), e.g.
    `kafka_security_protocol = 'SASL_SSL'`, `kafka_sasl_mechanism = 'SCRAM-SHA-512'`,
    and the username/password from the named collection.
  `signal_log` and `signal_log_mv` are unchanged from local.
- **Path 2 (ClickPipes):** skip `kafka_signals` + `signal_log_mv`; create a
  ClickPipe from the MSK topic into `signal_log` (map `signalId`/`receivedAt`/`body`
  in the pipe). Keep the `signal_log` DDL.
- Back up ClickHouse (or rely on Kafka replay for recovery within the retention
  window). Set a TTL on `signal_log` if the log should age out.

## 4. Verify in prod

1. `SHOW TABLES FROM signals` → `kafka_signals`, `signal_log`, `signal_log_mv`
   (or just `signal_log` under ClickPipes).
2. POST a signal to the edge → 202.
3. `SELECT signal_id, payload FROM signals.signal_log ORDER BY received_at DESC LIMIT 1`
   → the signal ClickHouse pulled on its own.
4. `SELECT count(DISTINCT signal_id) FROM signals.signal_log` for reconciliation.
5. Watch consumer-group lag on the `clickhouse-signal-log` group; alert on it.

## The dispatcher

ClickHouse is the archive, not the biller. `/api/internal/settle` (in the payments
app) owns the pricing and the Postgres writes, so something has to carry signals
from one to the other.

**It is a one-shot on a systemd timer, not a supervised long-lived process.** One
run reads everything ingested in the last `DISPATCH_WINDOW_MS`, posts all of it in
a single gzipped call, and exits. The units, the install and the alerts are in
[deploy/systemd/README.md](../deploy/systemd/README.md); the rest of this section
is why it is shaped that way.

- **`signal_log.api_key_hash`** is what makes a signal attributable. The edge
  stamps the SHA-256 digest of the caller's `cnk_…` key onto every message
  envelope; the raw key never leaves the edge process. Settle resolves the
  organisation from that digest in one query — `ApiKey.hashedKey` stores the same
  value — so the dispatcher forwards the digest and nothing else.
- **What to send is answered by the clock, not by Postgres.** The run selects on
  `signal_log.ingested_at` — ClickHouse's own insert timestamp — and asks nothing
  about what it sent before. Selecting on `received_at` instead would be a silent
  data-loss bug: that is the caller's time, stamped by N edge instances off N
  clocks, and a row can land in the archive minutes after it. With no watermark
  anywhere, such a row would never be picked up. `ingested_at` needs its `minmax`
  skip index (migration `002`) or the window is a full partition scan.
- **Over-sending is free, and is the error recovery.** `SignalLog.signalId` is
  UNIQUE and settle uses it as the idempotency key, so a re-sent signal replays
  onto the original money row instead of charging twice. The window is 3x the
  timer's interval deliberately: every signal goes about three times, a failed run
  is covered by the next two, and nothing has to be persisted for that to work.
- **The hourly reconciliation is the outage net.** The 3-minute window covers any
  single failed tick but not three in a row — those rows fall out of it and are
  never sent again. A second timer runs the same binary hourly over a 2-hour
  window, re-sending signals settle then discards.
- **One call, gzipped.** No batch size and no concurrency. Vercel caps a
  serverless function's request body at 4.5MB, which raw signal JSON crosses at
  roughly 15k signals; signal JSON compresses about 10:1, and
  `dispatch.body_near_limit` warns before the ceiling. This assumes settle accepts
  one large call — its 500 cap removed, and its `INTERNAL_WORKER` per-signal walk
  replaced by a bulk insert. **Both are prerequisites, not optional tuning.**
- **Retry policy lives in settle now**, and it is what stops the overlap being
  ruinous. On re-receipt: re-process a `SERVER_ERROR`, no-op a `PROCESSED`, never
  retry a `USER_ERROR` (it cannot self-heal and waits for a manual fix). Get it
  wrong in one direction and every transient failure becomes permanent silence; in
  the other, the whole window is re-priced every minute.

## Operational notes

- **Reconciliation / replay:** Kafka is the source of truth. To rebuild the log,
  truncate `signal_log` and reset the ClickHouse consumer group to the earliest
  offset (detach/re-attach `kafka_signals`, or a new group name).
- **Money reads always use `FINAL` or `GROUP BY signal_id`** — pre-merge
  duplicates from at-least-once delivery are expected and collapse on merge.
- **Scaling later:** one partition caps ingest parallelism at one consumer, so
  `kafka_num_consumers` cannot help until the topic grows. Grow it with
  `--alter --topic signals --partitions 3`; then raise `kafka_num_consumers`
  toward 3 *with* `kafka_thread_per_consumer = 1`, or the extra consumers share
  one stream thread. Precondition either way:
  `kafka_auto_offset_reset = 'earliest'` must already be set, or new partitions
  start at `latest` and silently skip messages.
