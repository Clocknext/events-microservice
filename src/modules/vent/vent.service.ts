/** Business logic for the reject vent. Knows nothing about HTTP — no request,
 *  no reply — and the queue arrives as the narrow `SignalQueue` port.
 *
 *  Every response the service sends that is NOT an accepted signal is vented:
 *  a body the schema refused, a key nobody recognises, a path that does not
 *  exist, a payments outage. Each becomes one message carrying two ClickHouse
 *  rows, already shaped, so the worker that drains the queue does no work
 *  beyond two bulk inserts.
 *
 *  What this file must never do is invent a field. The two row types are
 *  columns (see vent.schema.ts); anything the edge knows that has no column —
 *  the HTTP status, the api-key digest, AJV's full `issues` list — is dropped
 *  here rather than smuggled into the message. */
import { ulid } from 'ulid'
import type {
  AcceptedMessage,
  RawSignalRow,
  SignalQueue,
  SignalStatusRow,
  VentMessage,
} from './vent.schema.js'

/** The three meters a signal can record against. A body naming anything else
 *  leaves `signal_type` null — the column describes what was metered, and
 *  "cerdit" metered nothing. */
const SIGNAL_TYPES = new Set(['wallet', 'credit', 'outcome'])

/**
 * A ULID, not a UUID: it sorts by creation time, which is what both tables are
 * ordered by, so ids minted in sequence land in the same part rather than
 * scattering across the whole key range.
 */
export function mintSignalId(): string {
  return ulid()
}

/** Reads a top-level string off a body that may be anything at all — a parsed
 *  object, a JSON array, `undefined` because parsing threw. Returns null unless
 *  the field is genuinely a non-empty string. */
function readString(body: unknown, field: string): string | null {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return null
  const value = (body as Record<string, unknown>)[field]
  return typeof value === 'string' && value !== '' ? value : null
}

function readSignalType(body: unknown): SignalStatusRow['signal_type'] {
  const raw = readString(body, 'type')
  if (raw === null) return null
  // Lowercased for the same reason the signal route lowercases it before
  // validation: the rulebook reads `type` case-insensitively, so `"Credit"`
  // metered a credit and the column should say so.
  const normalised = raw.trim().toLowerCase()
  return SIGNAL_TYPES.has(normalised) ? (normalised as SignalStatusRow['signal_type']) : null
}

/** Everything the HTTP layer knows about one rejected request. Plain values —
 *  the caller pulls these off the request, this file never sees it. */
export interface VentInput {
  signalId: string
  receivedAt: string
  statusCode: number
  /** The `ErrorReason` the error handler settled on. */
  errorReason: string | null
  /** Its one-line summary — the first problem only, exactly what the caller
   *  read in `statusDetail.message`. */
  errorMessage: string | null
  /** The parsed body, when there was one. `undefined` when parsing threw. */
  body: unknown
  /** The raw bytes as sent, captured before parsing so an unparseable body is
   *  still recorded word for word. */
  rawBody: string | null
}

/**
 * Builds the two rows for one rejected request.
 *
 * `attempt` is 1 and `status` is PENDING for everything vented here: nothing
 * was settled, and a rejected signal has no second delivery to count. The
 * money and outcome columns are all null because pricing never ran — they are
 * written out anyway rather than omitted, so the message stays a full record of
 * what the edge did and did not know.
 */
export function buildVentMessage(input: VentInput): VentMessage {
  const raw_signals: RawSignalRow = {
    signal_id: input.signalId,
    // '' rather than null: the column is non-Nullable, and a null would fail
    // the insert outright. Unknown on every reject — see vent.schema.ts.
    organization_id: '',
    customer_id: readString(input.body, 'customerId') ?? '',
    received_at: input.receivedAt,
    idempotency_key: readString(input.body, 'idempotencyKey'),
    api_key_id: '',
    // The raw bytes win over the parsed body: they are what the caller actually
    // sent. The fallback only matters if a 4xx is ever raised on a route whose
    // body was parsed without the capture hook running.
    payload: input.rawBody ?? (input.body === undefined ? '' : JSON.stringify(input.body)),
  }

  const signal_status: SignalStatusRow = {
    signal_id: input.signalId,
    organization_id: '',
    attempt: 1,
    status: 'PENDING',
    // 5xx is ours and safe to retry as-is; everything else is the caller's data
    // to fix. This is the only place the two are told apart.
    error_type: input.statusCode >= 500 ? 'SERVER_ERROR' : 'USER_ERROR',
    // Null only if a 4xx was answered without passing through the error
    // handler, which nothing does today. Left null rather than guessed: an
    // invented code is worse than an absent one.
    error_code: input.errorReason,
    error_message: input.errorMessage,
    signal_type: readSignalType(input.body),
    usage_log_id: null,
    credits_used: null,
    provided_cost: null,
    customer_cost: null,
    credit_id: null,
    credit_name: null,
    model_name: null,
    provider: null,
    member_name: null,
    currency_code: null,
    applied_rules: null,
    wallet_debit_usd: null,
    balance_remaining: null,
    outcome_id: null,
    outcome_name: null,
    outcome_step: null,
    outcome_steps_done: null,
    outcome_run_id: null,
    outcome_closed_run: null,
    outcome_completed: null,
    outcome_signal_count: null,
    outcome_total_steps: null,
    // Same instant as `received_at`. The edge writes the row once; a later
    // settlement pass is what moves it, and that is what the version column on
    // `signal_status` exists to order.
    updated_at: input.receivedAt,
  }

  return { raw_signals, signal_status }
}

/**
 * Builds the `signals_accepted` message for one signal.
 *
 * Takes the identity the request was stamped with rather than minting anything:
 * the id a caller is given on a 202 must be the id on the queue, or the row
 * they are told to look for is not the row that exists.
 */
export function buildAcceptedMessage(identity: {
  signalId: string
  receivedAt: string
}): AcceptedMessage {
  return {
    raw_signals: {
      signal_id: identity.signalId,
      received_at: identity.receivedAt,
    },
  }
}

/**
 * Puts an accepted signal on the queue.
 *
 * Throws on failure, and the caller MUST NOT swallow it — this is the exact
 * opposite of the vent's posture. The vent fails open because losing an
 * analytics row costs nothing; an accepted signal is billable, and a 202 sent
 * after a failed publish would lose it silently with the caller told it was
 * safe. So: publish first, answer second, and fail loudly in between.
 */
export async function publishAccepted(
  queue: SignalQueue,
  message: AcceptedMessage,
): Promise<void> {
  await queue.send(JSON.stringify(message))
}

/** SQS refuses a message over 256 KiB. */
const SQS_MAX_MESSAGE_BYTES = 256 * 1024

/** Room for everything in the message that is not `payload`: the 31 status
 *  columns, the six other raw columns, and JSON's own punctuation. Generous —
 *  the trade is a few KB of headroom against a dropped row. */
const NON_PAYLOAD_BUDGET_BYTES = 8 * 1024

/** Cuts `raw` to the longest prefix whose JSON-ENCODED form fits the budget.
 *
 *  Encoded, not raw, because the two differ by up to 6x: a control byte is one
 *  byte in the body but six characters of \u00xx escape inside a JSON string.
 *  Iterating by code point rather than by index keeps a surrogate pair whole,
 *  so an emoji is
 *  never sliced into two orphaned halves. */
function truncateToEncodedBudget(raw: string, budgetBytes: number): string {
  let used = 0
  let end = 0
  for (const char of raw) {
    // `JSON.stringify('a')` is `"a"` — the two quotes are not part of the cost.
    const cost = Buffer.byteLength(JSON.stringify(char), 'utf8') - 2
    if (used + cost > budgetBytes) break
    used += cost
    end += char.length
  }
  return raw.slice(0, end)
}

/**
 * Serialises one message, shrinking `payload` if that is what it takes to fit.
 *
 * A body is capped at `BODY_BYTES` (64 KB) but its ENCODED size is not: 64 KB
 * of control characters becomes 384 KB of `\u00xx` escapes, and SQS rejects
 * anything over 256 KB. Left alone, the send throws, the vent fails open, and
 * the row vanishes — and since a body full of control bytes is not valid JSON,
 * that is exactly the MALFORMED_JSON reject the raw capture exists to preserve.
 *
 * So the payload is trimmed rather than the message dropped. Nothing else can
 * give: every other column is either tiny or load-bearing.
 */
export function serialiseVent(message: VentMessage): { body: string; droppedBytes: number } {
  const body = JSON.stringify(message)
  if (Buffer.byteLength(body, 'utf8') <= SQS_MAX_MESSAGE_BYTES) {
    return { body, droppedBytes: 0 }
  }

  const full = message.raw_signals.payload
  const trimmed = truncateToEncodedBudget(full, SQS_MAX_MESSAGE_BYTES - NON_PAYLOAD_BUDGET_BYTES)
  return {
    body: JSON.stringify({
      ...message,
      raw_signals: { ...message.raw_signals, payload: trimmed },
    }),
    droppedBytes: Buffer.byteLength(full, 'utf8') - Buffer.byteLength(trimmed, 'utf8'),
  }
}

/**
 * Hands one message to the queue, and reports how much of the payload had to be
 * left behind — 0 for every message that was not oversized, which is nearly all
 * of them.
 *
 * Throws whatever the driver throws. The caller decides what that means, and
 * for a reject it means "log it and move on" — a dead queue must never turn a
 * 400 into a 500. That judgement belongs at the call site, not here.
 */
export async function publishVent(queue: SignalQueue, message: VentMessage): Promise<number> {
  const { body, droppedBytes } = serialiseVent(message)
  await queue.send(body)
  return droppedBytes
}
