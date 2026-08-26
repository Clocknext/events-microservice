#!/usr/bin/env bash
#
# Brings the local pipeline up from nothing:
#
#   .env  ·  Kafka  ·  the `signals` topic  ·  ClickHouse  ·  the schema
#
# Then prints the three commands to run.
#
#   npm run up              CLEAN SLATE — recreates the containers, so ClickHouse
#                           comes up with the tables and no rows, and Kafka with
#                           no messages. This is the default because a stale
#                           archive means the dispatcher's first run backfills it.
#
#   npm run up -- --keep    keep whatever is already there
#
# Local data only: the archive is derived from the topic, and both live in
# throwaway containers. Nothing here touches Postgres.
#
# It does NOT start the edge, the dispatcher or the payments app. Those are
# long-lived processes and belong in their own terminals where you can see them.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$PWD"
PAYMENTS_DIR="${PAYMENTS_DIR:-$HOME/Documents/Work/Clocknext-Payment-Saas}"

KEEP=0
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1 ;;
    -h|--help) sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $arg (try --keep)" >&2; exit 1 ;;
  esac
done

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
ok()    { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn()  { printf '  \033[33m!\033[0m %s\n' "$*"; }
fail()  { printf '  \033[31m✗\033[0m %s\n' "$*"; }
step()  { printf '\n\033[1m%s\033[0m\n' "$*"; }

die() { fail "$*"; exit 1; }

# ── 0 · prerequisites ────────────────────────────────────────────────────────
step "0 · prerequisites"
for cmd in docker node npm; do
  command -v "$cmd" >/dev/null || die "$cmd is not installed"
done
docker info >/dev/null 2>&1 || die "the docker daemon is not running"
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
ok "docker $(docker --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1) · node $(node --version)"

[ -d node_modules ] || { warn "installing dependencies"; npm ci; }
ok "dependencies present"

# ── 1 · .env ─────────────────────────────────────────────────────────────────
# `npm run dev|start|dispatch` load this with --env-file-if-exists, so it is the
# one place local configuration lives.
step "1 · .env"
if [ -f .env ]; then
  ok ".env exists (left alone)"
else
  cp .env.example .env
  ok ".env created from .env.example"
fi

# Read a key out of a dotenv-style file without sourcing it (values may contain
# characters the shell would mangle) and without ever echoing the value.
env_get() {
  [ -f "$1" ] || return 1
  sed -nE "s/^[[:space:]]*$2[[:space:]]*=[[:space:]]*(.*)$/\1/p" "$1" \
    | tail -1 | sed -E 's/^["'"'"']//; s/["'"'"']$//'
}

env_set() {
  local file=$1 key=$2 value=$3
  if grep -qE "^[[:space:]]*#?[[:space:]]*$key[[:space:]]*=" "$file"; then
    # Replace in place. `|` as the delimiter, and the value goes through a temp
    # file so a secret never lands in the process list.
    python3 - "$file" "$key" "$value" <<'PY'
import re, sys
path, key, value = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path).read()
text = re.sub(rf'^[ \t]*#?[ \t]*{re.escape(key)}[ \t]*=.*$',
              f'{key}={value}', text, count=1, flags=re.M)
open(path, 'w').write(text)
PY
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# The dispatcher authenticates to the payments app with a shared secret. It must
# be byte-identical on both sides, so take it from the payments repo rather than
# asking anyone to copy it by hand.
CURRENT_SECRET="$(env_get .env INTERNAL_SETTLE_SECRET || true)"
if [ -z "$CURRENT_SECRET" ]; then
  if PAYMENTS_SECRET="$(env_get "$PAYMENTS_DIR/.env" INTERNAL_SETTLE_SECRET)" && [ -n "$PAYMENTS_SECRET" ]; then
    env_set .env INTERNAL_SETTLE_SECRET "$PAYMENTS_SECRET"
    ok "INTERNAL_SETTLE_SECRET copied from the payments repo"
  else
    warn "INTERNAL_SETTLE_SECRET is empty and the payments repo was not found at:"
    warn "  $PAYMENTS_DIR"
    warn "  set PAYMENTS_DIR, or fill it in .env by hand — the dispatcher refuses to start without it"
  fi
else
  ok "INTERNAL_SETTLE_SECRET already set"
fi

KAFKA_TOPIC="$(env_get .env KAFKA_TOPIC || echo signals)"
KAFKA_TOPIC="${KAFKA_TOPIC:-signals}"
CH_DB="$(env_get .env CLICKHOUSE_DATABASE || echo signals)"
CH_DB="${CH_DB:-signals}"
# One partition today. See docs/ARCHITECTURE.md §8 — repartitioning later moves
# key→partition placement, so this is a decision to make before production.
PARTITIONS="${KAFKA_PARTITIONS:-1}"

# ── 2 · containers ───────────────────────────────────────────────────────────
# Clean by default. `down -v` takes the clickhouse-data volume with it, and Kafka
# keeps its log in the container's own layer, so both come back empty — which is
# the point: with rows in the archive and none in SignalStatus, the dispatcher's
# first run treats the whole archive as outstanding and backfills it.
step "2 · kafka + clickhouse"
if [ "$KEEP" = "1" ]; then
  ok "--keep: leaving existing data alone"
else
  warn "recreating the containers — the archive and the topic will be EMPTY"
  warn "(pass --keep to preserve them)"
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
fi
docker compose up -d >/dev/null
printf '  waiting for health'
for _ in $(seq 1 90); do
  k=$(docker inspect -f '{{.State.Health.Status}}' kafka       2>/dev/null || echo starting)
  c=$(docker inspect -f '{{.State.Health.Status}}' clickhouse  2>/dev/null || echo starting)
  [ "$k" = healthy ] && [ "$c" = healthy ] && break
  printf '.'; sleep 2
done
printf '\n'
[ "${k:-}" = healthy ]  || die "kafka did not become healthy — docker compose logs kafka"
[ "${c:-}" = healthy ]  || die "clickhouse did not become healthy — docker compose logs clickhouse"
ok "kafka healthy · clickhouse healthy"

# ── 3 · the topic ────────────────────────────────────────────────────────────
# Auto-create is on, so the topic would spring into being on first produce — but
# then its partition count is whatever the broker default happens to be. Create
# it explicitly so the number is ours.
step "3 · topic \`$KAFKA_TOPIC\`"
topics=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true)
if grep -qx "$KAFKA_TOPIC" <<<"$topics"; then
  have=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
          --describe --topic "$KAFKA_TOPIC" 2>/dev/null | grep -oE 'PartitionCount: [0-9]+' | grep -oE '[0-9]+')
  ok "exists with $have partition(s)"
  if [ "${have:-0}" -lt "$PARTITIONS" ]; then
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --alter --topic "$KAFKA_TOPIC" --partitions "$PARTITIONS" >/dev/null
    ok "grown to $PARTITIONS partition(s)"
  fi
else
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$KAFKA_TOPIC" \
    --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
  ok "created with $PARTITIONS partition(s)"
fi

# ── 4 · the ClickHouse schema ────────────────────────────────────────────────
# init/ runs on FIRST START ONLY — the container skips it once the data volume
# exists. So: apply init when the schema is absent, then always run the
# migrations, which are idempotent and are what a live database gets.
step "4 · clickhouse schema"
ch() { docker exec -i clickhouse clickhouse-client "$@"; }

# On a fresh volume the container entrypoint runs init/ itself, but it finishes
# shortly AFTER the healthcheck starts passing. Give it a moment before deciding
# the schema is missing, or this applies it a second time for no reason.
for _ in $(seq 1 15); do
  [ "$(ch -q "EXISTS TABLE $CH_DB.signal_log" 2>/dev/null || echo 0)" = "1" ] && break
  sleep 1
done
if [ "$(ch -q "EXISTS TABLE $CH_DB.signal_log" 2>/dev/null || echo 0)" = "1" ]; then
  ok "$CH_DB.signal_log exists"
else
  warn "$CH_DB.signal_log missing — applying init/01-schema.sql"
  ch --multiquery < docker/clickhouse/init/01-schema.sql
  ok "base schema applied"
fi

shopt -s nullglob
for m in docker/clickhouse/migrations/*.sql; do
  ch --multiquery < "$m"
  ok "migration $(basename "$m")"
done
shopt -u nullglob

missing=$(ch -q "SELECT arrayStringConcat(arrayFilter(x -> NOT has(groupArray(name), x),
                   ['signal_id','received_at','api_key_hash','payload']), ', ')
                 FROM system.columns WHERE database='$CH_DB' AND table='signal_log'" 2>/dev/null || echo '?')
[ -z "$missing" ] || die "signal_log is missing columns: $missing"
ok "columns: signal_id · received_at · api_key_hash · payload"

for obj in kafka_signals signal_log signal_log_mv; do
  [ "$(ch -q "EXISTS TABLE $CH_DB.$obj" 2>/dev/null || echo 0)" = "1" ] \
    && ok "$obj" || die "$CH_DB.$obj is missing"
done
ok "archive holds $(ch -q "SELECT count() FROM $CH_DB.signal_log") row(s)"

# ── 5 · the payments app ─────────────────────────────────────────────────────
step "5 · the payments app"
if [ -d "$PAYMENTS_DIR" ]; then
  ok "found at $PAYMENTS_DIR"
  if [ -d "$PAYMENTS_DIR/prisma/migrations/20260826000000_signal_status" ]; then
    ok "the SignalStatus migration is present"
  else
    warn "prisma/migrations/20260826000000_signal_status is missing there"
  fi
else
  warn "not found at $PAYMENTS_DIR — set PAYMENTS_DIR if it lives elsewhere"
fi

# ── 6 · the backlog check ────────────────────────────────────────────────────
# The dispatcher's watermark is `max(receivedAt)` over SignalStatus. With that
# table empty, EVERY row in the archive is outstanding, so its first run sweeps
# the lot. A clean slate makes that a non-issue; --keep does not.
step "6 · backlog check"
ARCHIVE_ROWS=$(ch -q "SELECT count() FROM $CH_DB.signal_log" 2>/dev/null || echo 0)
if [ "${ARCHIVE_ROWS:-0}" -eq 0 ]; then
  ok "archive is empty — the dispatcher starts with nothing to catch up on"
else
  warn "the archive holds $ARCHIVE_ROWS row(s)."
  warn "If SignalStatus is empty, the dispatcher's first run will sweep them ALL."
  warn "The dispatcher refuses to do that by default — see DISPATCH_COLD_START_MAX."
  echo
  echo "    npm run up               start clean instead (recreates the containers)"
  echo "    DISPATCH_COLD_START_MAX=0 npm run dispatch    backfill deliberately"
fi

# ── done ─────────────────────────────────────────────────────────────────────
PAY_URL="$(env_get .env PAYMENTS_URL || echo http://127.0.0.1:3001)"
PORT="$(env_get .env PORT || echo 3000)"

cat <<EOF

$(bold "════════════════════════════════════════════════════════════════════")
$(bold " up. now run these, one per terminal:")
$(bold "════════════════════════════════════════════════════════════════════")

  $(bold "1 · the payments app")   — owns pricing and Postgres
      cd $PAYMENTS_DIR && npx next dev -p 3001

  $(bold "2 · the edge")           — POST /api/v1/signal, on :$PORT
      cd $ROOT && npm run dev

  $(bold "3 · the dispatcher")     — archive → $PAY_URL/api/internal/settle
      cd $ROOT && npm run dispatch

  All three read $ROOT/.env — nothing needs exporting.

  $(bold "smoke test")
      curl -s -X POST http://127.0.0.1:$PORT/api/v1/signal \\
        -H 'content-type: application/json' \\
        -H 'authorization: Bearer cnk_your_key' \\
        -d '{"customerId":"cus_x","inputTokens":1200,"outputTokens":350,
             "type":"credit","model":"anthropic/claude-sonnet-4.5"}' | jq .

      docker exec clickhouse clickhouse-client -q \\
        "SELECT signal_id, received_at, api_key_hash FROM $CH_DB.signal_log
          ORDER BY received_at DESC LIMIT 1 FORMAT Vertical"

  $(bold "checks")   npm test · npm run e2e · npm run trace · npm run trace:all · npm run trace:runner
  $(bold "stop")     npm run down       ·  npm run down -- -v   (also wipes the archive)

EOF
