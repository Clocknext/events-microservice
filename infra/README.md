# infra/ — Terraform for the local AWS (LocalStack)

This provisions the AWS half of the stack — the SQS queues, the two Lambda
consumers, and the event-source mappings that wire them together — from code
instead of a bash script. Run it against LocalStack locally; the same files
would run against a real account by editing only `provider.tf`.

## What it manages

- `queues.tf` — `signals_pending`, `signals_accepted`, and a `_dlq` for each
  (Standard queues, `maxReceiveCount: 5`, 60s hold on pending, none on accepted).
- `lambdas.tf` — `signals-pending-consumer` (reserved concurrency 1, 60s window)
  and `signals-accepted-consumer` (N concurrent), their IAM role, and the
  event-source mappings. Both use partial batch failure.
- `variables.tf` — the sizing knobs (batch sizes, concurrency, windows).

## Prerequisites

- `docker compose up -d` — LocalStack + ClickHouse must be running first. This
  Terraform creates only the *AWS* resources; the containers are separate.
- `terraform` on PATH.

## Runbook

```bash
# from the repo root
npm run infra:apply     # builds the Lambda bundles, then `terraform apply`
npm run infra:plan      # preview only — shows what would change
npm run infra:destroy   # tear it all down
```

`infra:apply` is idempotent: run it again after `npm run build:lambdas` changed
a bundle and Terraform updates just the function; run it with no changes and it
reports "No changes."

## Real AWS

`provider.tf` is the only LocalStack-specific file — fake creds and the `:4566`
endpoints. Point it at a real account (or delete the block for the default
chain), give the IAM role real SQS/logs policies, and `terraform apply` builds
the same queues + Lambdas in AWS. The CloudFront / ALB / EC2 / ClickHouse layers
in front are provisioned separately (console or their own Terraform).

## Note on LocalStack quirks

- A batch size over 10 requires a batching window > 0 (`accepted_batch_window`,
  default 1s) — an AWS rule, enforced here too.
- After `terraform destroy`, SQS holds a deleted queue name for ~60s before it
  can be recreated. If an immediate re-apply fails on queue creation, wait and
  retry.
