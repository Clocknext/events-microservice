# Costs — MSK, EC2, ClickHouse, ALB

What the production pipeline in [PRODUCTION.md](PRODUCTION.md) costs to run: the
free tier you get once, the on-demand rate you pay by default, and the committed
rate you pay if you keep it.

**Read this first, because it changes how you read the rest:** at this pipeline's
data volume, *nothing* is priced per signal. A signal is ~1 KB on the wire, so
10 M signals/month is 10 GB — about **$1** of MSK data-in and **$0.40** of
ClickPipes ingest. Every meaningful number below is a *floor*: broker-hours,
instance-hours, load-balancer-hours, ClickHouse compute-hours. You are buying
uptime, not throughput. Per-GB rates only start to matter above ~1 TB/month
(≈1 B signals), and even then the floors still dominate.

## Scope and assumptions

- **List prices, us-east-1 (N. Virginia) and ap-south-1 (Mumbai)**, captured
  **2026-08-26** from the AWS Price List API and the vendors' own pricing pages.
  Both regions are given because the dispatcher's timezone bug in
  [CLAUDE.md](../CLAUDE.md) implies an India-adjacent team, and Mumbai is *not*
  uniformly more expensive — MSK Graviton brokers there are **29 % cheaper** than
  N. Virginia while MSK storage is **14 % dearer**.
- **730 hours = 1 month.** Every monthly figure is `hourly × 730`.
- Excludes: support plans, GST/VAT, NAT gateways, CloudWatch logs and metrics,
  Secrets Manager ($0.40/secret/month for the SCRAM creds), and any
  Enterprise Discount Program (EDP) or private-pricing agreement you already have.
- **AWS promotional credits are not a budget line here.** The Free plan's $200 is
  account-wide, not per service, expires 12 months after account creation, and
  cannot be applied to AWS Marketplace charges or to Savings Plan / RI upfront
  fees. MSK alone spends $200 in 12 days on config B.
- Rates move. Treat this file as the shape of the bill, not a quote.

## The four columns

| Service | Free tier | Pricing (on-demand, us-east-1) | Long-term pricing |
| --- | --- | --- | --- |
| **Amazon MSK — Provisioned** | **None.** No free tier of any kind. | Broker-hours + $0.10/GB-mo storage. Cheapest usable cluster (2 × `kafka.t3.small`) ≈ **$67/mo**; 3 × `kafka.m7g.large` ≈ **$447/mo** | **No Reserved Instances, no Savings Plans, no reservation model at all.** The only levers are broker size, Graviton, tiered storage ($0.06/GB-mo), and an EDP/PPA at scale |
| **Amazon MSK — Serverless** | None | **$0.75/cluster-hour = $547.50/mo floor**, + $0.0015/partition-hour, + $0.10/GB in, $0.05/GB out, $0.10/GB-mo storage | Same — no commitment discount. The floor is essentially the whole cost for a 1-partition topic |
| **Amazon EC2** (edge + dispatcher + self-hosted ClickHouse) | `t4g.small` **750 h/month free for every account through 2026-12-31** — the only EC2 free tier still obtainable. The legacy 12-month 750 h `t2/t3.micro` tier needed an account created before **2025-07-15** and runs 12 months from creation, so it has expired for every account that had it; accounts opened since get the Free plan's $200 in credits over 6 months instead | `t4g.small` $12.26/mo, `m7g.large` $59.57/mo, `r7g.xlarge` $156.37/mo | **Savings Plans / Reserved Instances: ~34 % off for 1 year, ~55 % off for 3 years** (measured on `m7g.large`: $0.0816 → $0.054 → $0.037). Compute Savings Plans also cover Lambda and Fargate |
| **Amazon EBS** (gp3, under EC2 and ClickHouse) | 30 GB + 2 M I/Os + 1 GB snapshots (legacy 12-month tier only) | **$0.08/GB-mo**, 3,000 IOPS and 125 MB/s included; $0.005/IOPS-mo and $0.06/MBps-mo beyond. Snapshots $0.05/GB-mo | No commitment discount. gp3 is **~20 % cheaper than gp2** at the same size — that is the discount. Snapshot Archive for cold copies |
| **ClickHouse (self-managed on EC2)** | **Software is free** — Apache 2.0, no licence, no seat count. AWS free tier applies to the EC2/EBS underneath | EC2 + EBS + your time. `r7g.xlarge` + 1 TB gp3 ≈ **$238/mo** | Inherits EC2's **34 %/55 %** Savings Plan discount — the *only* ClickHouse option that gets a committed AWS rate. Cheapest at steady state, dearest in operator hours |
| **ClickHouse Cloud** (direct or via AWS Marketplace) | **30-day trial with $300 credits**; $400 via AWS Activate for startups | Compute per 8 GiB unit-hour: **Basic $0.2181, Scale $0.2985, Enterprise $0.3903**. Storage **$25.30/TB-mo** compressed — **billed again for the default daily backup**. Basic 1×8 GiB 24/7 ≈ **$159/mo**; Scale 2×8 GiB ≈ **$436/mo** | **Annual committed contract**, or an **AWS Marketplace private offer** — usage draws down the commitment and lands on your AWS invoice. Ask your AWS account team whether that spend counts toward an EDP commitment. Note AWS **promotional credits do not apply to Marketplace charges**, so credits cannot subsidise a trial bought this way |
| **ClickPipes** (only on path 2) | None | XS replica **$0.0125/h = $9.13/mo** + **$0.04/GB ingested** | Folded into the ClickHouse Cloud commitment |
| **CloudFront** (in front of the ALB) | **1 TB egress + 10M requests + 2M function invocations per month, free forever** — not a 12-month tier | Beyond that: $0.0100/10k requests (US), $0.0120/10k (India); $0.085/GB (US), $0.109/GB (India). **AWS origin → CloudFront is free** | None. The free tier *is* the discount, and it does not expire |
| **MSK multi-VPC private connectivity** (needed for ClickPipes) | None | **$0.0225 per connectivity-hour per auth scheme = $16.43/mo** + $0.006/GB processed | None. Provisioned clusters only — Serverless cannot enable it |
| **Application Load Balancer** | 750 h/month + 15 LCUs/month for 12 months (legacy new-customer tier); newer accounts use the $200 credits | **$0.0225/ALB-hour = $16.43/mo** + **$0.008/LCU-hour** ($5.84/mo per sustained LCU) | **No Savings Plan, no RI.** "Reserved LCU" line items exist but bill at the *same* $0.008 — that is capacity pre-warming, not a discount |
| **Data transfer out to internet** | **100 GB/month, free forever**, across all regions and services | $0.09/GB to 10 TB, $0.085 to 50 TB, $0.07 to 150 TB, $0.05 above | Tiering is the only discount; CloudFront in front is the real one |

## MSK in detail

Broker-hours are the single biggest line in this architecture, and the one with
no escape hatch.

| Item | us-east-1 | ap-south-1 | Per broker/month (us-east-1) |
| --- | --- | --- | --- |
| `kafka.t3.small` | $0.0456/h | $0.0491/h | $33.29 |
| `kafka.m7g.large` (Graviton3) | $0.204/h | **$0.1458/h** | $148.92 |
| `kafka.m5.large` | $0.21/h | — | $153.30 |
| `kafka.m7g.xlarge` | $0.408/h | $0.2916/h | $297.84 |
| `Express.m7g.large` | $0.408/h | $0.2916/h | $297.84 |
| Broker storage | $0.10/GB-mo | $0.114/GB-mo | 500 GB = $50.00 |
| Tiered storage (long retention) | $0.06/GB-mo | $0.0652/GB-mo | 500 GB = $30.00 |
| Provisioned storage throughput (optional) | $0.08/MBps-mo | $0.0912/MBps-mo | — |
| Serverless cluster | $0.75/h | $0.79/h | **$547.50** |
| Serverless partition | $0.0015/h | $0.0016/h | $1.10 |
| Serverless data in / out | $0.10 / $0.05 per GB | $0.11 / $0.056 | 10 GB in = $1.00 |
| MSK Connect | $0.11/MCU-hour | — | — |
| MSK Replicator | $0.30/h + $0.08/GB | — | — |
| Private connectivity | $0.0225/h + $0.006/GB | — | — |

Four things this table decides:

- **Serverless is the wrong shape for a 1-partition topic.** $547.50/month buys
  elasticity across hundreds of partitions. Our topic has *one*
  ([AWS-SETUP.md](AWS-SETUP.md) Step 0), so the elasticity
  is unused and Serverless costs **8×** the smallest provisioned cluster. It also
  forces path 2 — Serverless is IAM-only and ClickHouse's Kafka engine cannot
  speak MSK IAM, so choosing it also commits you to ClickPipes or ClickHouse
  Cloud. See PRODUCTION.md "Decide first".
- **Graviton is nearly free money in Mumbai.** `m7g.large` is 3 % under `m5.large`
  in N. Virginia but **$0.1458 vs $0.204** in Mumbai — a 29 % regional gap on the
  same instance. If the cluster can live in ap-south-1, that alone is ~$126/month
  on three brokers.
- **Retention is cheap, so keep it long.** Kafka is the replay source of truth for
  the archive. 7 days of 10 GB/month traffic is ~2.5 GB per broker — under $1.
  Retention is not where the money goes; do not shorten it to save.
- **MSK never gets a commitment discount.** Every other compute line in this
  document can be bought down 34–55 %. MSK cannot, which quietly makes it the
  largest *permanent* cost even when it is not the largest on-demand one.

## EC2 in detail (edge, dispatcher, self-hosted ClickHouse)

| Instance | vCPU / RAM | us-east-1 on-demand | 1-yr RI/SP | 3-yr RI/SP | ap-south-1 on-demand |
| --- | --- | --- | --- | --- | --- |
| `t4g.small` | 2 / 2 GiB | $0.0168 ($12.26/mo) | $0.0105 | $0.0073 | $0.0112 ($8.18/mo) |
| `c7g.large` | 2 / 4 GiB | $0.0725 ($52.93/mo) | $0.0478 | $0.0319 | $0.0491 ($35.84/mo) |
| `m7g.large` | 2 / 8 GiB | $0.0816 ($59.57/mo) | $0.0540 | $0.0370 | $0.0583 ($42.56/mo) |
| `m7g.xlarge` | 4 / 16 GiB | $0.1632 ($119.14/mo) | $0.1080 | $0.0740 | $0.1166 ($85.12/mo) |
| `r7g.large` | 2 / 16 GiB | $0.1071 ($78.18/mo) | $0.0708 | $0.0486 | $0.0751 ($54.82/mo) |
| `r7g.xlarge` | 4 / 32 GiB | $0.2142 ($156.37/mo) | $0.1417 | $0.0972 | $0.1502 ($109.65/mo) |

- **The edge is not the expensive part.** It validates a body, hashes a token,
  and produces one Kafka message. `t4g.small` handles the load test in
  `scripts/loadtest.mjs` (10 k signals) without noticing; two of them behind the
  ALB is HA for $25/month.
- **The dispatcher needs no box of its own, and barely any of anyone else's.** It
  is not even a resident process: a systemd timer starts it once a minute, it makes
  ONE outbound HTTP call, and it exits. Idle minutes cost one indexed ClickHouse
  query. Put the timer beside the edge; give it its own instance only if you want a
  large window's memory (`DISPATCH_MAX_ROWS`) off the edge's box.
- **Self-hosted ClickHouse wants RAM, not cores.** `r7g.*` over `m7g.*`: the
  `ReplacingMergeTree` merges and the `LIMIT 1 BY signal_id` reads are
  memory-bound. Start at `r7g.large`, and note that the whole archive of 100 M
  signals compresses to a couple of hundred GB.
- **Buy the Savings Plan last.** A 1-year Compute Savings Plan is 34 % off and
  survives an instance-family change; a 3-year is 55 % off. Both are commitments
  in **$/hour of spend**, so buy them only once the shape has stopped moving —
  and prefer Compute Savings Plans to Reserved Instances for exactly that
  flexibility.
- **A "free" instance still has a bill.** The T4g trial discounts *instance-hours
  only*. An 8 GB gp3 root volume is $0.64/month and a **public IPv4 address is
  $0.005/h = $3.65/month** — the 750 free IPv4 hours belonged to the legacy
  12-month tier, so a new-plan account pays it. Budget ~$4.29/month per
  "free" box, and note that on a new account this is what promotional credits get
  spent on.
- **`t4g.small` is free through 2026-12-31** — 750 h/month, every account, all
  regions aggregated. Dev, staging and the bastion should all be `t4g.small`
  until then. That is one instance's worth of free hours in total, not per box —
  and it is the *only* EC2 free tier left. The 12-month tier closed to new
  accounts on 2025-07-15 and, running 12 months from account creation, has now
  lapsed for every account that qualified.

## ClickHouse: self-managed vs Cloud

| | Self-managed on EC2 | ClickHouse Cloud (Basic) | ClickHouse Cloud (Scale) |
| --- | --- | --- | --- |
| Compute | `r7g.xlarge` $156.37/mo (**$70.96 at 3-yr**) | 1 × 8 GiB, 24/7 ≈ $159/mo | 2 × 8 GiB, 24/7 ≈ $436/mo |
| Storage, 500 GB compressed | 1 TB gp3 $81.92/mo | $12.65 + $12.65 backup | same |
| Ingest from MSK | Kafka engine — **$0** | ClickPipes $9.13/mo + $0.04/GB | same |
| Auth to MSK | SCRAM or mTLS (path 1) | ClickPipes does MSK IAM (path 2) | same |
| HA | you build it | single replica | 2 replicas + SLA |
| Commitment discount | **yes, via EC2 SP** | annual contract / Marketplace private offer | same |
| Who fixes it at 3 a.m. | you | them | them |

Notes that cost real money if missed:

- **Storage bills twice.** $25.30/TB-mo is the rate, and the mandatory daily
  backup is charged at the same rate — which is why ClickHouse's own example
  shows 500 GB costing $25.30/month. Budget **~$50.60 per compressed TB**.
- **Basic can idle to zero; Scale and Enterprise are always-on.** Basic pauses
  after inactivity, so a dev service costs a fraction of $159. The moment you
  need two replicas you are on Scale's $436/month floor with no idling.
- **Compute is metered per minute in 8 GiB units.** One unit = 8 GiB RAM /
  2 vCPU. Enterprise 2 × 32 GiB = 8 units ≈ $2,279/month — that is the shape of
  the top of this curve, not a number this pipeline needs.
- **Cloud egress is dearer than AWS's.** ~$0.115/GB public egress by their own
  example, against AWS's $0.09. The dispatcher pulling batches out of ClickHouse
  Cloud is egress; keep it in the same region.
- **"ClickHouse via Amazon" is not a third product.** It is either OSS on EC2
  (rates above) or ClickHouse Cloud bought through AWS Marketplace — identical
  list rates, billed on your AWS invoice, and the only route to a *committed*
  ClickHouse Cloud rate.

## ALB in detail

| Item | us-east-1 | ap-south-1 |
| --- | --- | --- |
| Load-balancer hour | $0.0225 ($16.43/mo) | $0.0239 ($17.45/mo) |
| LCU-hour | $0.008 ($5.84/mo sustained) | $0.008 |
| Trust Store (mTLS) | $0.005/h | $0.0053/h |

One LCU covers, whichever is largest: 25 new connections/s, 3,000 active
connections/min, 1 GB/h processed, or 1,000 rule evaluations/s. `POST /api/v1/signal`
is a tiny-body, high-connection-count route, so **new connections per second is
what will bill you**, not bytes: 10 GB/month of traffic is 0.014 GB/h — three
orders of magnitude inside one LCU — while 100 signals/s on non-keep-alive
clients is 4 LCUs. Keep-alive on the client side is the cheapest optimisation in
this document. Expect **$20–35/month** in practice.

## Three configurations, costed

**The shape actually being built** ([AWS-SETUP.md](AWS-SETUP.md)) — no MSK, Kafka
self-hosted on its own box, ClickHouse Cloud:

| Line | Monthly, us-east-1 list |
| --- | --- |
| `box-edge` — edge + dispatch timers, `t4g.small` | $12.26 (**$0** through 2026-12-31) |
| `box-kafka` — self-hosted Kafka, `t4g.small` | $12.26 |
| 2 × 30 GiB gp3 | $4.80 |
| ALB + ~1 LCU | $16.43 + $5.84 |
| 1 Elastic IP (attached) | $0.00 |
| CloudFront, low volume | ~$0–1 |
| ClickHouse Cloud | billed separately by ClickHouse |
| **Total** | **≈ $52/mo** (**≈ $40** while the T4g trial lasts), + ClickHouse Cloud |

Roughly **4 months** of the $200 credit, and the largest line is the ALB — not
compute. Self-hosting Kafka instead of MSK is what makes that true: the cheapest
MSK configuration below starts at **$76/mo** for brokers alone, and MSK Serverless
at **$547/mo**.

The configurations below are the MSK-based alternatives that decision was measured
against. They are **not** what is being deployed.

**A — Dev / staging, self-hosted, us-east-1.** Path 1, no HA.

| Line | Monthly |
| --- | --- |
| MSK 2 × `kafka.t3.small` + 2 × 50 GB | $66.58 + $10.00 |
| Edge + dispatcher, 1 × `t4g.small` | $12.26 (**$0** through 2026-12-31) |
| ClickHouse 1 × `r7g.large` + 200 GB gp3 | $78.18 + $16.00 |
| ALB + ~1 LCU | $16.43 + $5.84 |
| **Total** | **≈ $205** (**≈ $193** with the T4g trial) |

**B — Production, self-hosted, path 1 (MSK provisioned + SCRAM).** 3 brokers,
2 edges, ClickHouse on its own box.

| Line | On-demand | With 1-yr Savings Plan |
| --- | --- | --- |
| MSK 3 × `kafka.m7g.large` + 3 × 500 GB | $446.76 + $150.00 | **unchanged — no SP exists** |
| Edge 2 × `m7g.large` | $119.14 | $78.84 |
| ClickHouse `r7g.xlarge` + 1 TB gp3 + snapshots | $156.37 + $81.92 + ~$25 | $103.44 + $81.92 + ~$25 |
| ALB + ~3 LCU | $16.43 + $17.52 | same |
| Secrets Manager, CloudWatch | ~$15 | ~$15 |
| **Total** | **≈ $1,028** | **≈ $935** |

The same config in **ap-south-1** lands near **$855** on-demand — 17 % less —
almost entirely because MSK Graviton brokers are 29 % cheaper there (offset a
little by 14 % dearer broker and EBS storage).

**C — Managed, path 2 (MSK Serverless + ClickHouse Cloud + ClickPipes).**

| Line | Monthly |
| --- | --- |
| MSK Serverless: cluster + 1 partition + 10 GB in | $547.50 + $1.10 + $1.00 |
| Edge 2 × `m7g.large` | $119.14 |
| ClickHouse Cloud Scale, 2 × 8 GiB, 500 GB + backup | $435.81 + $25.30 |
| ClickPipes XS + 10 GB | $9.13 + $0.40 |
| ALB + ~3 LCU | $33.95 |
| **Total** | **≈ $1,173** |

C costs more than B *and* removes the ability to buy the two largest lines down.
What it buys is that nobody on your team is on call for a ClickHouse merge.
That is a real trade — just make it deliberately, because the price gap widens
every year you keep the cluster.

## The chosen stack

CloudFront → ALB → edge on EC2 → MSK → ClickPipes → ClickHouse Cloud, with the
dispatcher beside the edge on EC2. Two notes before the numbers.

**The MSK → ClickHouse leg runs on nobody's box.** ClickPipes executes inside
ClickHouse Cloud, so there is no consumer process, no EC2 instance and no systemd
unit for it — you pay for replicas ($0.0125/h for XS) and ingested GB ($0.04). The
EC2 "worker" is the *other* leg: the dispatcher, reading `signal_log` and posting
to the payments app. The two are unrelated, and nothing of ours touches Kafka
except the edge producing to it.

**MSK Serverless cannot feed ClickPipes.** ClickPipes reaches a private cluster
via a reverse private endpoint that needs MSK **multi-VPC private connectivity**,
a provisioned-cluster feature. Serverless does not expose it and has no public
endpoint, so the alternative is VPC peering, Transit Gateway, or a self-managed
NLB + PrivateLink endpoint service plus Route 53 DNS — infrastructure you then
own. See [PRODUCTION.md](PRODUCTION.md) "Decide first". Provisioned + IAM keeps
every property Serverless was chosen for and costs less:

| Line | MSK provisioned + IAM | MSK Serverless |
| --- | --- | --- |
| MSK — 3 × `kafka.t3.small` + 300 GB, or the serverless floor | $129.86 | $549.60 |
| MSK multi-VPC private connectivity ($0.0225/h + $0.006/GB) | $16.43 | n/a — build it yourself, ~$25–40 |
| ClickPipes XS + 10 GB | $9.53 | $9.53 |
| ClickHouse Cloud Scale, 2 × 8 GiB + 500 GB + backup | $461.11 | $461.11 |
| EC2 2 × `t4g.small` — edge + dispatcher | $24.52 | $24.52 |
| ALB + 2 LCU | $28.11 | $28.11 |
| CloudFront — 10M signals is exactly the free request tier | **$0** | $0 |
| Secrets Manager, CloudWatch | ~$10 | ~$10 |
| **Total** | **≈ $680** | **≈ $1,108** + networking to maintain |

On ClickHouse Cloud **Basic** (one replica, idles when quiet) instead of Scale the
provisioned column drops to **≈ $403/month**. Basic is a fair fit while the
archive is small and a pause between dispatch runs is acceptable; Scale is the
floor once two replicas are required.

**Why provisioned still keeps IAM.** The reason to reach for Serverless was that
ClickHouse's Kafka engine cannot sign MSK IAM tokens. ClickPipes can, and it is
the consumer now — so the constraint is gone. The edge keeps `KAFKA_USE_IAM=true`
and its existing signer untouched.

### CloudFront on a POST-only route

| Item | Rate |
| --- | --- |
| Free tier, every month, no expiry | **1 TB egress + 10M requests + 2M function invocations** |
| Requests beyond the free tier | $0.0100/10k (US) · $0.0120/10k (India) |
| Egress beyond 1 TB | $0.085/GB (US) · $0.109/GB (India) |
| **AWS origin → CloudFront** | **free** — removes ALB internet egress entirely |
| Invalidations | first 1,000 paths/month free, then $0.005 each |
| Origin Shield | $0.0075/10k requests (US) · $0.0090 (India) |

10M signals/month sits exactly at the free request ceiling; 100M/month is about
$108/month in India. Be honest about what it buys on a `POST` route, though:
**no caching value at all**. What you get is edge TLS, Shield Standard, a stable
anycast entry point and somewhere to attach WAF — worth having at $0, but not a
latency win.

## Where the money actually is, in order

1. **Choose provisioned MSK over Serverless** unless partition count is genuinely
   dynamic. One partition on Serverless is $547/month of unused elasticity — the
   single largest avoidable line in the whole stack, ~$5,800/year.
2. **Pick the region before you provision.** ap-south-1 is ~17 % cheaper overall
   here and 29 % cheaper on MSK Graviton brokers. Moving a cluster later means
   re-doing the topic, the consumer group and the archive.
3. **Graviton everywhere** — `m7g`/`r7g`/`t4g` for EC2, `kafka.m7g.*` for brokers.
   The edge and the dispatcher are plain Node with no native deps; arm64 is free
   performance.
4. **`t4g.small` free hours through 2026-12-31** for every non-production box — 750 h/month
   aggregated across all regions, so one instance's worth, and a deadline rather than a
   tier. Config A's $193 becomes $205 in January.
5. **Right-size the brokers before buying anything.** A 1-partition topic at
   10 GB/month does not need `m7g.large` × 3. Two `t3.small` brokers carry this
   load and save **$380/month**; move up when retention grows or the partition
   count is raised, not before — partitions are cheap, brokers are not.
6. **Then buy a 1-year Compute Savings Plan** on the settled EC2 footprint — 34 %
   off, and it follows you across instance families. Leave the 3-year for the
   boxes you are certain about.
7. **Tiered storage on MSK** if retention ever goes past a week: $0.06 vs
   $0.10/GB-mo, and Kafka stays the replay source of truth.
8. **Keep-alive on API clients.** ALB LCUs here are driven by new connections per
   second, not bytes.
9. **Everything in one region and one AZ-set.** Cross-AZ and cross-region
   transfer is the classic surprise on a Kafka bill; the 100 GB/month free egress
   tier covers our real internet traffic entirely.

## Sources

- [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/) — broker, serverless, storage, Connect, Replicator rates
- AWS Price List API, captured 2026-08-26 — [MSK us-east-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonMSK/current/us-east-1/index.json), [MSK ap-south-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonMSK/current/ap-south-1/index.json), [ELB us-east-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSELB/current/us-east-1/index.json), [ELB ap-south-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSELB/current/ap-south-1/index.json)
- [Elastic Load Balancing pricing](https://aws.amazon.com/elasticloadbalancing/pricing/) — ALB hour, LCU definition, free tier
- [EC2 on-demand](https://aws.amazon.com/ec2/pricing/on-demand/), [Compute Savings Plans](https://aws.amazon.com/savingsplans/compute-pricing/), [Reserved Instances](https://aws.amazon.com/ec2/pricing/reserved-instances/pricing/)
- [Amazon EC2 T4g free trial extension](https://repost.aws/articles/ARi_gf6vo6TuqNtMQdiYPKyA/announcing-amazon-ec2-t4g-free-trial-extension) — 750 h/month through 2026-12-31
- [Amazon EBS pricing](https://aws.amazon.com/ebs/pricing/)
- [ClickHouse Cloud pricing](https://clickhouse.com/pricing) and [billing overview](https://clickhouse.com/docs/cloud/manage/billing/overview) — tier rates and worked examples
- [ClickPipes streaming pricing](https://clickhouse.com/docs/cloud/reference/billing/clickpipes/streaming-and-object-storage)
- [ClickHouse Cloud on AWS Marketplace — pay-as-you-go](https://aws.amazon.com/marketplace/pp/prodview-p4gwofrqpkltu) · [committed contract](https://aws.amazon.com/marketplace/pp/prodview-4qyeihstyym2s) · [AWS Activate $400 credits](https://aws.amazon.com/startups/offers/clickhouse)
- [ClickHouse Cloud backups](https://clickhouse.com/docs/cloud/manage/backups/overview)
- [AWS data transfer pricing](https://egresscost.com/aws/data-transfer-pricing/) — tiers and the 100 GB/month free allowance
- [CloudFront pay-as-you-go pricing](https://aws.amazon.com/cloudfront/pricing/pay-as-you-go/) — free tier, per-region egress and request rates
- [AWS PrivateLink for ClickPipes](https://clickhouse.com/docs/integrations/clickpipes/networking/aws-privatelink) and [the MSK reverse-private-endpoint setup](https://clickhouse.com/docs/knowledgebase/aws-privatelink-setup-for-msk-clickpipes) — why this needs a provisioned cluster
- [Secure connectivity patterns for MSK Serverless cross-account access](https://aws.amazon.com/blogs/big-data/secure-connectivity-patterns-for-amazon-msk-serverless-cross-account-access/) — peering / TGW / self-managed NLB, not multi-VPC
- [Multi-VPC private connectivity for MSK, any auth mechanism](https://aws.amazon.com/about-aws/whats-new/2024/09/multi-vpc-private-connectivity-amazon-msk-clusters-configured-authentication-mechanism)
- MSK and commitment discounts: [Redpanda's MSK cost analysis](https://www.redpanda.com/blog/amazon-msk-vs-redpanda-byoc-tco), [AutoMQ on MSK pricing](https://www.automq.com/blog/understanding-aws-msk-pricing)
