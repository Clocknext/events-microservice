#!/bin/bash
# Point the accepted consumer at the REAL payments app instead of mock-settle.
#
# The LocalStack Lambda cannot reach a payments app running on the host (this
# box's firewall blocks container->host). The fix: run payments as a CONTAINER
# on the same docker network, so the Lambda reaches it by name — the same way it
# reaches clickhouse and mock-settle.
#
# WARNING: settle has no working dry-run — every PROCESSED signal is BILLED for
# real against whatever DATABASE_URL the payments .env points at. Use seed/test
# customers.
#
#   scripts/use-real-settle.sh            # start payments container + repoint Lambda
#   scripts/use-real-settle.sh --revert   # point the Lambda back at mock-settle
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PAYMENTS_DIR="${PAYMENTS_DIR:-/home/joze/Documents/Work/Clocknext-Payment-Saas}"
NETWORK="${LAMBDA_DOCKER_NETWORK:-aws-internal_default}"
export PATH="$HOME/.local/bin:$PATH"

if [ "${1:-}" = "--revert" ]; then
  echo "==> reverting the accepted consumer to mock-settle"
  terraform -chdir="$ROOT/infra" apply -auto-approve >/dev/null
  docker rm -f payments >/dev/null 2>&1 || true
  echo "done. accepted consumer -> http://mock-settle:3999"
  exit 0
fi

SECRET="$(grep -h '^INTERNAL_SETTLE_SECRET=' "$PAYMENTS_DIR"/.env "$PAYMENTS_DIR"/.env.local 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"')"
[ -z "$SECRET" ] && { echo "no INTERNAL_SETTLE_SECRET in $PAYMENTS_DIR/.env*"; exit 1; }

echo "==> starting payments as a container on $NETWORK (reusing host node_modules)"
docker rm -f payments >/dev/null 2>&1 || true
docker run -d --name payments --network "$NETWORK" -p 127.0.0.1:3000:3000 \
  -v "$PAYMENTS_DIR":/app -w /app \
  node:22 sh -c "exec node_modules/.bin/next dev -H 0.0.0.0 -p 3000" >/dev/null

echo -n "==> waiting for Next.js"
for _ in $(seq 1 45); do
  docker logs payments 2>&1 | grep -qiE 'ready|Local:' && { echo " ready"; break; }
  echo -n "."; sleep 2
done

echo "==> pointing the accepted consumer at http://payments:3000"
(cd "$ROOT" && npm run build:lambdas >/dev/null 2>&1)
terraform -chdir="$ROOT/infra" apply -auto-approve \
  -var "payments_url=http://payments:3000" \
  -var "internal_settle_secret=$SECRET" >/dev/null

echo "done. accepted consumer -> http://payments:3000 (REAL settle, REAL billing)."
echo "the edge (host) resolves real keys via http://localhost:3000."
echo "revert with: scripts/use-real-settle.sh --revert"
