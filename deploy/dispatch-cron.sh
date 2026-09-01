#!/usr/bin/env bash
# One dispatch run, for cron.
#
# WHY CRON AND NOT THE SYSTEMD TIMER: systemd refuses to start a unit that is
# already running, so a run lasting longer than 60s makes the next tick VANISH —
# and with a non-overlapping window (DISPATCH_WINDOW_MS == the interval) the rows
# in that skipped minute are never read by this path at all.
#
# That skip existed to stop two runs sending the SAME window. It cannot happen
# now: consecutive windows tile rather than overlap, so two concurrent runs read
# two different minutes. Overlapping runs are therefore safe here, and cron
# starting one every minute regardless is exactly what is wanted.
#
# `.env` is read by NOTHING in the code (src/config.ts only reads process.env),
# and cron supplies almost no environment, so it is sourced here. Without this
# the process gets no configuration at all and exits 2 every minute.
set -euo pipefail
cd /home/ubuntu/events-microservice
set -a
. /home/ubuntu/events-microservice/.env
set +a
exec /usr/bin/node dist/workers/dispatch/dispatch.runner.js
