# infra/aws — the edge on real AWS

Terraform for the narrow first goal in [docs/setup.md](../../docs/setup.md):
`POST /api/v1/signal` reachable on AWS, producing to the real MSK topic. TLS, a
domain, ClickHouse and the dispatcher are all deliberately out of scope — see
[docs/PRODUCTION.md](../../docs/PRODUCTION.md) for those.

This is a **separate root** from `../` (retired, LocalStack-only). Its own state,
its own lineage.

## What it reads vs. what it owns

Read-only `data` sources — Terraform never takes ownership, so there is no import
step and no `apply` can alter them:

| Existing | Id |
| --- | --- |
| VPC | `vpc-0f1c34ce751397c5d` |
| Public subnets | `subnet-042341bb0261d3cc0` (1a), `subnet-09a7111365492a3f6` (1b) |
| SG `event-tasks` | `sg-0c5bd459dd43c87fe` |
| MSK Serverless | `clocknext-signals`, IAM auth, `:9098` |

Created and owned here:

| File | Resources |
| --- | --- |
| `iam.tf` | role + instance profile `signal-edge`, its inline policy, the SSM managed-core attachment |
| `s3.tf` | the private deploy bucket and the build artifact object |
| `edge.tf` | SG `edge-alb`, its rules, **one** ingress rule on the existing `event-tasks` SG, the EC2 instance |
| `alb.tf` | target group, ALB, HTTP:80 listener |
| `outputs.tf` | ALB DNS name, instance id, bucket, the full signal URL |

## Four things that are load-bearing

- **The edge joins the pre-existing `event-tasks` SG rather than getting its
  own.** MSK's SG already allows `9098` inbound from exactly that group, so
  joining it is what makes the producer work at all. Terraform owns precisely one
  *rule* on that group (port 3000 from the ALB) via a standalone
  `aws_vpc_security_group_ingress_rule` — importing the whole SG would put its
  existing MSK wiring at this config's mercy.
- **The target group health-checks `/health`, not `/api/v1/health`.** Only the
  signal module is mounted under `/api/v1`; the health module owns its own
  prefix. `/api/v1/health` is a 404, and pointing the check there marks every
  target unhealthy while the edge is running perfectly.
- **The MSK IAM policy wildcards only the ARN's random suffix**, never the
  cluster name. It is also narrower than the bastion's `msk-admin-temp`: no
  `ReadData`, no `CreateTopic`, no consumer-group actions. The edge only
  produces.
- **`aws_instance` depends on the artifact object.** An instance that boots
  before the tarball exists in S3 comes up healthy-looking with nothing
  listening on 3000.

There are no credentials in this config and none in `src/config.ts` either — the
SDK's default chain resolves the instance role, which is what lets
`KAFKA_USE_IAM=true` work with no secret on disk.

## One thing Terraform cannot check for you

`event-tasks` is a data source, so this config never reads or writes its
**egress** rules. The instance needs outbound access to reach S3 (the artifact),
MSK (`9098`) and SSM. A security group created through the console carries
allow-all egress by default, so this is almost certainly already fine — but if
the box boots and user-data hangs on `aws s3 cp`, that is the first thing to
check:

```bash
aws ec2 describe-security-groups --group-ids sg-0c5bd459dd43c87fe \
  --query 'SecurityGroups[0].IpPermissionsEgress'
```

## Verified so far

`terraform init`, `validate`, `fmt -check` and a read-only `terraform plan` all
pass against account `369343500638`: **18 to add, 0 to change, 0 to destroy** —
the zero-change is the part that matters, since it proves the data sources
resolve and nothing existing is being touched. The AMI alias resolves to
`ami-0cded71ff6ab7f608`. It has **never been applied**; nothing here exists in
AWS yet.

## Runbook

```bash
# 1. Build the artifact this config uploads. `npm ci --omit=dev` runs on the box,
#    so the lockfile has to travel with it.
cd ../..
npm run build
tar -czf signal-edge.tar.gz dist package.json package-lock.json

# 2. Plan, read it, then apply. These are real, billable resources.
cd infra/aws
terraform init
terraform plan
terraform apply
```

Then, once: **verify or create the `signals` topic** on the MSK cluster. The
bastion `msk-ladder` (`i-08f4928855911e723`) already has `kafka-topics.sh` and a
role that may create topics; reach it over SSM, not SSH — there is no key pair:

```bash
aws ssm start-session --target i-08f4928855911e723
```

`--create` is idempotent and no-ops if the topic exists. It is still a mutating
action against production Kafka, so read the output rather than assuming it.

Verify end to end against the ALB:

```bash
curl -i -X POST "$(terraform output -raw signal_endpoint)" \
  -H 'Authorization: Bearer test-key' \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust_1","inputTokens":10,"outputTokens":5}'
```

Expect **202** with `signalId` and `receivedAt` in the envelope. A **502
`QUEUE_UNAVAILABLE`** means the produce failed — the edge never acknowledges a
signal that reached no topic. Check, in order: the topic exists, the instance
role's policy, and that `event-tasks` is still allowed inbound on `9098` by
MSK's SG.

To reach the box itself:

```bash
aws ssm start-session --target "$(terraform output -raw edge_instance_id)"
sudo journalctl -u signal-edge -f
```

## Redeploying a new build

```bash
npm run build && tar -czf signal-edge.tar.gz dist package.json package-lock.json
terraform apply    # new etag -> new object -> user_data_replace_on_change
```

`user_data_replace_on_change = true` means this **replaces the instance**. It is
the honest behaviour for a config whose deploy mechanism is cloud-init, but it is
not a zero-downtime deploy — with one instance behind the ALB, there is a gap.
Fix that by adding a second instance before you care about it.

## Cost

EC2 `t4g.small` (free through 2026-12-31, then ~$12/mo) + 8GB gp3 (~$0.64/mo) +
a public IPv4 (~$3.65/mo) + the ALB (~$16.43/mo + LCU-hours) + a near-empty
bucket. Roughly **$20-25/month** while the free tier lasts. MSK's cost is
pre-existing, not added here. Full model: [docs/COSTS.md](../../docs/COSTS.md).

`terraform destroy` removes everything above. It cannot touch the VPC, the
subnets, the `event-tasks` SG or the MSK cluster — those are data sources.
