#!/bin/bash
set -euxo pipefail

# The project needs Node >= 20.6 — it relies on `--env-file`, which does not
# exist before then. AL2023's unversioned `nodejs` package has been 18 on some
# releases, so ask for a known-good major explicitly and fall back rather than
# silently landing on a runtime where --env-file is an unknown flag.
if ! dnf install -y nodejs22 nodejs22-npm; then
  if ! dnf install -y nodejs20 nodejs20-npm; then
    dnf install -y nodejs npm
  fi
fi

node --version

install -d -m 0755 /opt/signal-edge
aws s3 cp "s3://${bucket}/${key}" /tmp/signal-edge.tar.gz --region "${region}"
tar -xzf /tmp/signal-edge.tar.gz -C /opt/signal-edge
rm -f /tmp/signal-edge.tar.gz

cd /opt/signal-edge
npm ci --omit=dev

# `src/config.ts` is the only thing that reads process.env, and nothing in the
# code loads a .env file — so this MUST reach the process via --env-file below.
# No AWS credentials here on purpose: the instance role supplies them, which is
# what makes KAFKA_USE_IAM=true work with no secret on disk.
cat > /etc/signal-edge.env <<'ENVEOF'
NODE_ENV=production
HOST=0.0.0.0
PORT=${port}
KAFKA_BROKERS=${brokers}
KAFKA_TOPIC=${topic}
KAFKA_CLIENT_ID=signal-edge
KAFKA_USE_IAM=true
AWS_REGION=${region}
BODY_BYTES=65536
LOG_LEVEL=info
ENVEOF
chmod 600 /etc/signal-edge.env

NODE_BIN="$(command -v node)"

cat > /etc/systemd/system/signal-edge.service <<UNITEOF
[Unit]
Description=Signal edge (Fastify)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$NODE_BIN --env-file=/etc/signal-edge.env /opt/signal-edge/dist/server.js
WorkingDirectory=/opt/signal-edge
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNITEOF

systemctl daemon-reload
systemctl enable --now signal-edge
