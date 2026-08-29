# infra/ — retired

This root provisioned the **LocalStack** SQS/Lambda pipeline: `signals_pending`,
`signals_accepted`, their DLQs, and the two consumer Lambdas. That pipeline was
removed from the codebase — there is no SQS, no Lambda and no reject vent any
more — and `queues.tf`, `lambdas.tf` and `variables.tf` went with it.

Nothing here runs. `provider.tf` is gone too: it referenced `var.aws_region` and
`var.localstack_endpoint`, whose `variables.tf` no longer existed, so
`terraform init` failed on this directory. `git show HEAD:infra/provider.tf` has
it if you want the history.

**The real AWS config lives in [`aws/`](aws/README.md).** It is a separate root
with its own state, which is the point: this directory's `terraform.tfstate`
still describes nine LocalStack resources in a container that no longer exists,
and reusing that lineage against a real account would be a bad day.

Those two state files are untracked and left in place rather than deleted — they
are the only remaining record of what the old pipeline held. Nothing reads them.
To be rid of them:

```bash
rm infra/terraform.tfstate infra/terraform.tfstate.backup
rm -rf infra/.terraform infra/.terraform.lock.hcl
```
