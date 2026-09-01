# The worker units

Three units, two binaries, and the difference in KIND is the point:

| Unit | Kind | Cadence | Job |
| --- | --- | --- | --- |
| `signal-consumer.service` | **always on** | — | drains Kafka, resolves each signal, writes `signal_log` |
| `dispatch.timer` | one-shot | every 60s | 3 min window (`DISPATCH_WINDOW_MS`) — the pipeline |
| `dispatch-reconcile.timer` | one-shot | hourly | 2 h window, set on `ExecStart` — **not** `Environment=`, which `EnvironmentFile=` overrides. The safety net |

**The consumer is a service because a Kafka consumer group member has to be.** One
that started and exited every 60 seconds would spend its life rebalancing the
group instead of draining it. It is the only always-on process in this repo
besides the edge.

**The dispatcher is a one-shot because it holds nothing worth keeping.** Both
timers run `dist/workers/dispatch/dispatch.runner.js`: one window of the ClickHouse
archive, one gzipped POST to `/api/internal/settle`, one log line, exit.

Start the consumer **first** — the dispatcher has nothing to read until it has
written something.

The minute run's window is 3× its interval on purpose: that overlap is the whole
of the error recovery, since nothing is persisted between runs. A single failed
tick is covered by the next two. Three consecutive failures are not — that is
what the hourly reconciliation is for.

Overlapping runs are impossible by construction: systemd will not start a unit
that is already running. This is the reason these are timers and not crontab
entries, which would need `flock` to get the same guarantee and fail open
without it.

## Install

All three units expect the checkout at **`/home/ubuntu/events-microservice`** and
run as **`ubuntu`** — the user that owns the tree and runs the deploy. There is no
service account to create and nothing to `chown`:

```bash
cd /home/ubuntu/events-microservice
sudo install -m 0644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signal-consumer
sudo systemctl enable --now dispatch.timer dispatch-reconcile.timer
```

Living under `/home` costs exactly one line of hardening: the units set
**`ProtectHome=read-only`, not `true`** — `true` mounts /home as an empty tmpfs
inside the unit's namespace, and node would fail to open `dist/`. Nothing in
either worker writes to disk, so read-only gives up nothing that mattered.

`.env` sits beside `dist/` at `/home/ubuntu/events-microservice/.env`, mode `600`,
owned by `ubuntu`, and must contain `INTERNAL_SETTLE_SECRET` — each worker exits 2
without it rather than 401 in a loop. The consumer additionally needs
`KAFKA_BROKERS`. `EnvironmentFile=` in the units is not optional: `.env` is loaded
by the npm scripts, never by the code.

## Exit codes, and what systemd does with them

Both binaries use the same three, and the units treat them differently on purpose:

| Code | Means | Consumer | Dispatcher |
| --- | --- | --- | --- |
| `0` | clean | shut down on SIGTERM | sent, or nothing to send |
| `1` | transient | `Restart=on-failure`, 5s later | the next tick IS the retry — no backoff |
| `2` | misconfigured | **the unit STOPS** (`RestartPreventExitStatus=2`) | the timer keeps firing and keeps failing |

`RestartPreventExitStatus=2` on the consumer is deliberate. Exit 2 means a bad
shared secret or missing `KAFKA_BROKERS` — every restart would fail identically, so
stopping puts the reason in `systemctl status` instead of hiding it behind a
restart counter. **Nothing drains the topic while it is stopped**, which is the
loud failure this is choosing over a silent one.

## Watch it

```bash
systemctl status signal-consumer           # running? or stopped on an exit 2?
journalctl -u signal-consumer -f -o cat    # one JSON line per BATCH
systemctl list-timers 'dispatch*'          # next and last fire
journalctl -u dispatch -f -o cat           # one JSON line per RUN
journalctl -u dispatch --since -1h | jq -s 'map(select(.event=="dispatch.run")) | add'
```

A consumer `batch` line reads:

```json
{"event":"batch","partition":0,"read":12,"processing":11,"pending":1,
 "quarantined":0,"stoppedAt":-1,"committed":12}
```

| Field | Means |
| --- | --- |
| `processing` | resolved and acceptable — settle will price these |
| `pending` | rejected by payments. **Not a consumer error** — archived with its reason and still dispatched, so the failure stays visible in the Signals UI |
| `quarantined` | payments could not answer about this signal `RESOLVE_POISON_AFTER` times running *while other calls succeeded*. Archived `PENDING`/`RESOLVE_FAILED` and stepped over so the topic keeps moving |
| `stoppedAt` | `-1` is clean. Otherwise the index it stopped at: everything before was written and committed, the rest is redelivered |
| `committed` | offsets moved. Always ≤ `read` |

## Alerts worth having

Dispatcher:

- `exit 2` — ever.
- `exit 1` twice in a row — the next tick is no longer covering it.
- `"event":"run.capped"` — **rows were not sent.** The window held more than
  `DISPATCH_MAX_ROWS` and the next window has already moved past some of them.
- `"event":"dispatch.body_near_limit"` — the gzipped body is closing on Vercel's
  4.5MB request-body ceiling. Crossing it is a hard 413 for the entire window at
  once, not a gradual slowdown.
- `serverError > 0` — settle is failing on individual signals.

Consumer:

- **the unit not running** — nothing is draining the topic. Kafka retains the
  signals, so nothing is lost *yet*; but the catch-up stamps them all with
  `ingested_at = now`, so they land in ONE dispatcher window and can trip
  `run.capped`. The two alarms are connected.
- `"quarantined" > 0` — a signal payments cannot answer about. Something is wrong
  with that signal or that route.
- `"event":"message.unparsable"` — a message that could not be keyed into a row and
  was **dropped**. The edge always writes a `signalId`, so this is corruption.
- **consumer lag** on `signal-resolver`
  (`kafka-consumer-groups.sh --describe --group signal-resolver`). One HTTP call
  per signal is this design's ceiling; sustained lag means raising
  `CONSUME_CONCURRENCY` or adding partitions.

## Filling a gap

For an outage longer than the reconciliation window, replay an explicit window by
hand. No code change and no cursor to rewind, because there is no cursor:

```bash
cd /home/ubuntu/events-microservice
env $(grep -v '^#' .env | xargs) \
  DISPATCH_SINCE=2026-08-26T04:00:00Z \
  DISPATCH_UNTIL=2026-08-26T05:00:00Z \
  DISPATCH_MAX_ROWS=1000000 \
  node --max-old-space-size=4096 dist/workers/dispatch/dispatch.runner.js
```

Both bounds are over `ingested_at`, and a malformed one exits 2 rather than
reading a window nobody asked for. Re-sending signals that already settled is
safe — settle is idempotent on `signalId`.
