# Setup — getting `/api/v1/signal` reachable on real AWS (no domain yet)

Scope: be able to send a real signal to `POST /api/v1/signal` on AWS, producing
to a real Kafka topic, with domain/TLS deferred to later. ClickHouse and the
dispatcher are out of scope here too — see [PRODUCTION.md](PRODUCTION.md) for
those when you're ready.

Chosen shape: MSK Serverless, region `us-east-1`, an ALB in front now (so the
DNS name is stable once a domain is added later) instead of a bare EC2 public IP.

## What already exists in this AWS account

This environment has working AWS credentials (account `369343500638`) and
Terraform. A discovery pass turned up infrastructure that **already exists**,
provisioned outside this repo's `infra/` Terraform (that directory is now
retired: its state only ever tracked the deleted LocalStack SQS/Lambda pipeline,
never this real account):

| Resource | Id | Notes |
| --- | --- | --- |
| VPC | `vpc-0f1c34ce751397c5d` (`10.0.0.0/16`) | 2 public + 2 private subnets, 2 AZs |
| Public subnets | `subnet-042341bb0261d3cc0` (us-east-1a), `subnet-09a7111365492a3f6` (us-east-1b) | route to IGW `igw-0fc4d2ed0ea79227e` |
| MSK Serverless cluster | `clocknext-signals`, ACTIVE, IAM-auth only | bootstrap: `boot-pd2cdyql.c1.kafka-serverless.us-east-1.amazonaws.com:9098` |
| SG `msk` (`sg-045f5ccb91681d149`) | attached to the MSK ENIs | allows inbound `9098` **only from SG `event-tasks`** |
| SG `event-tasks` (`sg-0c5bd459dd43c87fe`) | description "Edge API and worker tasks" | **currently has zero inbound rules** — reserved for the edge, unused so far |
| EC2 `msk-ladder` (`i-08f4928855911e723`) | bastion, SSM-managed, no SSH key | has `kafka-topics.sh` at `/opt/kafka/kafka_2.13-3.7.0/bin/`; its role `msk-admin-temp` can create/describe topics and read/write data on this one cluster |

The SG wiring for "edge talks to MSK" is already done, and there's already an
access pattern to follow: no key pairs, SSM Session Manager only, an IAM role
instead of SSH. Everything below follows that pattern and only adds what's
missing: an IAM role for the edge, the EC2 instance itself, an S3 bucket to
hand it the build, and the ALB.

**New spend this adds** (MSK's own cost is sunk/pre-existing, not new here):
EC2 `t4g.small` (**$0** through 2026-12-31, then ~$12/mo) + 8 GB gp3 root
(~$0.64/mo) + a public IPv4 (~$3.65/mo) + ALB (~$16.43/mo + LCU-hours) + a
tiny S3 bucket. Roughly **$20-25/month** while the free EC2 tier lasts. See
[COSTS.md](COSTS.md) for the full pricing model this is drawn from.

## What to build

> **Built.** This config now exists at [`infra/aws/`](../infra/aws/README.md) —
> `terraform validate` passes and it has never been applied. The section below is
> the specification it was written against; the runbook at the end is still the
> order to run things in. Two deviations from the spec below, both deliberate:
> the artifact reaches S3 as an `aws_s3_object` rather than a `local-exec`, and
> user-data asks for `nodejs22`/`nodejs20` before falling back to plain `nodejs`
> — AL2023's unversioned package has shipped as 18 on some releases, and this
> project needs >= 20.6 for `--env-file`.

A new, self-contained Terraform config at **`infra/aws/`** — separate from
`infra/`, which is retired. That root's `queues.tf`/`lambdas.tf` went with the
deleted SQS/Lambda pipeline and its state describes LocalStack resources, not
this account, so a fresh directory avoids state confusion. `infra/aws/` points at
real AWS with the default credential chain, no LocalStack block.

`data` sources (read-only references — Terraform never takes ownership, so
there's no import step and no risk of it changing them): the existing VPC,
both public subnets, and the `event-tasks` SG.

New resources:

1. **IAM role + instance profile** `signal-edge` for the EC2 instance:
   - `kafka-cluster:Connect`, `WriteData`, `WriteDataIdempotently`,
     `DescribeTopic` scoped to the `clocknext-signals` cluster/topic ARNs
     (same shape as `msk-admin-temp`'s policy, narrower — no `ReadData`,
     `CreateTopic`, or group actions, since the edge only produces).
   - `s3:GetObject` on the new deploy bucket/prefix.
   - `AmazonSSMManagedInstanceCore` attached, so the instance is reachable via
     SSM Session Manager for debugging — no key pair, matching `msk-ladder`.
2. **S3 bucket** for the build artifact (private, no public access).
3. **Security group** `edge-alb`: inbound `80` from `0.0.0.0/0`, all egress.
4. **`aws_vpc_security_group_ingress_rule`** on the *existing* `event-tasks`
   SG: allow TCP `3000` from the new `edge-alb` SG. A standalone rule
   resource, not an import of the whole SG — Terraform only owns this one
   rule; everything else about that SG is left alone.
5. **EC2 instance** `t4g.small`, Amazon Linux 2023 arm64 (AMI resolved via the
   `aws_ssm_parameter` for `al2023-ami-kernel-default-arm64`; confirmed
   available as `ami-0cded71ff6ab7f608`), in public subnet
   `subnet-09a7111365492a3f6`, `vpc_security_group_ids = [event-tasks]`, the
   new instance profile, `associate_public_ip_address = true` (the subnet
   itself doesn't auto-assign — same as how `msk-ladder` got its public IP).
   User-data:
   - `dnf install -y nodejs npm`
   - pulls the build tarball from S3, extracts to `/opt/signal-edge`
   - `npm ci --omit=dev`
   - writes `/etc/signal-edge.env`:
     ```
     NODE_ENV=production
     HOST=0.0.0.0
     PORT=3000
     KAFKA_BROKERS=boot-pd2cdyql.c1.kafka-serverless.us-east-1.amazonaws.com:9098
     KAFKA_TOPIC=signals
     KAFKA_CLIENT_ID=signal-edge
     KAFKA_USE_IAM=true
     AWS_REGION=us-east-1
     BODY_BYTES=65536
     LOG_LEVEL=info
     ```
   - writes a `signal-edge.service` systemd unit
     (`ExecStart=/usr/bin/node --env-file=/etc/signal-edge.env /opt/signal-edge/dist/server.js`,
     `Restart=always`), `systemctl enable --now signal-edge`.
6. **Target group**: HTTP `3000`, health check `GET /health` (the existing
   route in `src/modules/health/health.routes.ts`, mounted at `/health` — no
   auth required, so the ALB can hit it directly).
7. **ALB**: internet-facing, both public subnets, `edge-alb` SG, one HTTP `80`
   listener forwarding to the target group. No HTTPS/cert yet — that's the
   domain step being deferred.
8. **Outputs**: ALB DNS name, EC2 instance id.

### Files

- `infra/aws/provider.tf` — real AWS, `region = "us-east-1"`, no LocalStack
  block.
- `infra/aws/data.tf` — the VPC/subnet/SG data sources above.
- `infra/aws/iam.tf` — role, policy, instance profile.
- `infra/aws/s3.tf` — the deploy bucket.
- `infra/aws/edge.tf` — SG, SG rule, EC2 instance, user-data template.
- `infra/aws/alb.tf` — target group, ALB, listener.
- `infra/aws/outputs.tf` — ALB DNS name, instance id.
- `infra/aws/user-data.sh.tpl` — the cloud-init script templated with the S3
  bucket/key and the env values above.

## Runbook, in order

1. `npm run build` locally → tar `dist/`, `package.json`, `package-lock.json`.
2. `terraform init && terraform apply` in `infra/aws/` — creates the S3
   bucket, IAM role, SG rule, ALB/target group **and** the EC2 instance in one
   pass. (The instance's user-data would fail to find the artifact if it booted
   before the upload, so the tarball is an `aws_s3_object` that the
   `aws_instance` `depends_on` — a real resource rather than a `local-exec`, so
   its etag also drives redeploys.)
3. **Verify/create the `signals` topic** on the MSK cluster via SSM
   `send-command` against `msk-ladder`, using its existing `kafka-topics.sh`
   + IAM auth — idempotent (`--create` no-ops if the topic already exists).
   This is a real, if low-risk, mutating action against production Kafka —
   run it deliberately and check the output rather than assuming success.
4. **Verify end-to-end**:
   ```bash
   curl -X POST http://<alb-dns-name>/api/v1/signal \
     -H 'Authorization: Bearer test-key' \
     -H 'Content-Type: application/json' \
     -d '{"customerId":"cust_1","inputTokens":10,"outputTokens":5}'
   ```
   Expect `202` with `signalId`/`receivedAt` in the envelope. Then optionally
   confirm on `msk-ladder` via `kafka-console-consumer.sh` that the message
   landed on the `signals` topic.

## Verification checklist

- [ ] `terraform plan` reviewed before `apply` — these are real, billable
      resources.
- [ ] `curl` against the ALB DNS name returns `202`.
- [ ] Topic `signals` confirmed to exist (created if missing).
- [ ] Message observed on the topic via `msk-ladder`'s console consumer.

## Explicitly out of scope here

ClickHouse, the dispatcher, and the domain/TLS layer — those are unchanged
from what [PRODUCTION.md](PRODUCTION.md) already describes, to be picked up
in a later pass.
