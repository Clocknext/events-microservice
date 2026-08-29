# Dispatch timers

Two timers, one binary. Both run `dist/workers/dispatch/dispatch.runner.js`,
which is a **one-shot**: one window of the ClickHouse archive, one gzipped POST
to `/api/internal/settle`, one log line, exit.

| Unit | Cadence | Window | Job |
| --- | --- | --- | --- |
| `dispatch.timer` | every 60s | 3 min (`DISPATCH_WINDOW_MS`) | the pipeline |
| `dispatch-reconcile.timer` | hourly | 2 h (overridden in the unit) | the safety net |

The minute run's window is 3× its interval on purpose: that overlap is the whole
of the error recovery, since nothing is persisted between runs. A single failed
tick is covered by the next two. Three consecutive failures are not — that is
what the hourly reconciliation is for.

Overlapping runs are impossible by construction: systemd will not start a unit
that is already running. This is the reason these are timers and not crontab
entries, which would need `flock` to get the same guarantee and fail open
without it.

## Install

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dispatch
sudo chown -R dispatch:dispatch /srv/events-microservice
sudo install -m 0644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dispatch.timer dispatch-reconcile.timer
```

`.env` must be readable by the `dispatch` user and must contain
`INTERNAL_SETTLE_SECRET` — the run exits 2 without it rather than 401 every
minute. `EnvironmentFile=` in the unit is not optional: `.env` is loaded by the
npm scripts, never by the code.

## Watch it

```bash
systemctl list-timers 'dispatch*'          # next and last fire
journalctl -u dispatch -f -o cat           # one JSON line per run
journalctl -u dispatch --since -1h | jq -s 'map(select(.event=="dispatch.run")) | add'
```

## Exit codes

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | sent, or nothing to send | nothing |
| 1 | transient — ClickHouse down, settle 5xx, timeout | nothing; the next tick retries |
| 2 | misconfigured — bad secret, bad replay window, body refused | **act**; every tick will fail identically |

## Alerts worth having

- `exit 2` — ever.
- `exit 1` twice in a row — the next tick is no longer covering it.
- `"event":"run.capped"` — **rows were not sent.** The window held more than
  `DISPATCH_MAX_ROWS` and the next window has already moved past some of them.
- `"event":"dispatch.body_near_limit"` — the gzipped body is closing on Vercel's
  4.5MB request-body ceiling. Crossing it is a hard 413 for the entire window at
  once, not a gradual slowdown.
- `serverError > 0` — settle is failing on individual signals.

## Filling a gap

For an outage longer than the reconciliation window, replay an explicit window by
hand. No code change and no cursor to rewind, because there is no cursor:

```bash
sudo -u dispatch env $(grep -v '^#' /srv/events-microservice/.env | xargs) \
  DISPATCH_SINCE=2026-08-26T04:00:00Z \
  DISPATCH_UNTIL=2026-08-26T05:00:00Z \
  DISPATCH_MAX_ROWS=1000000 \
  node --max-old-space-size=4096 dist/workers/dispatch/dispatch.runner.js
```

Both bounds are over `ingested_at`, and a malformed one exits 2 rather than
reading a window nobody asked for. Re-sending signals that already settled is
safe — settle is idempotent on `signalId`.
