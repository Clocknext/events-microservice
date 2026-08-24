/** Business logic for building and publishing queue messages. Knows nothing
 *  about HTTP — no request, no reply — and the queue arrives as the narrow
 *  `SignalQueue` port.
 *
 *  Two messages, one per pipeline:
 *    · a PENDING message — one rejected signal, built by the vent plugin from
 *      any response >= 400.
 *    · an ACCEPTED message — one accepted signal, built by the signal
 *      controller before it answers 202.
 *
 *  Both carry a `raw_signals` row the consumer inserts unchanged. The status
 *  events (`Processing` / `Processed` / `Failed`) are written by the CONSUMERS,
 *  not here — the edge does not know a signal's fate. */
import { ulid } from 'ulid'
import type { AcceptedMessage, PendingMessage, RawSignalRow, SignalQueue } from './vent.schema.js'

/** The three meters a signal can record against. A body naming anything else
 *  leaves `type` null — the column describes what was metered, and "cerdit"
 *  metered nothing. */
const SIGNAL_TYPES = new Set(['wallet', 'credit', 'outcome'])

/**
 * A ULID, not a UUID: it sorts by creation time, which is what `raw_signals` is
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

/** The metered type, lowercased the way the rulebook reads it, or null. */
function readSignalType(body: unknown): string | null {
  const raw = readString(body, 'type')
  if (raw === null) return null
  const normalised = raw.trim().toLowerCase()
  return SIGNAL_TYPES.has(normalised) ? normalised : null
}

/** Everything the HTTP layer knows about one rejected request. Plain values —
 *  the caller pulls these off the request, this file never sees it. */
export interface PendingInput {
  signalId: string
  receivedAt: string
  /** The `ErrorReason` the error handler settled on. */
  errorReason: string | null
  /** The parsed body, when there was one. `undefined` when parsing threw. */
  body: unknown
  /** The raw bytes as sent, captured before parsing so an unparseable body is
   *  still recorded word for word. */
  rawBody: string | null
}

/** Builds the `signals_pending` message for one rejected request. */
export function buildPendingMessage(input: PendingInput): PendingMessage {
  const raw_signals: RawSignalRow = {
    signal_id: input.signalId,
    // '' rather than null: the column is non-Nullable, and a null would fail the
    // insert. Unknown on every reject — the key never resolved.
    organization_id: '',
    customer_id: readString(input.body, 'customerId') ?? '',
    type: readSignalType(input.body),
    idempotency_key: readString(input.body, 'idempotencyKey'),
    // The raw bytes win over the parsed body: they are what the caller actually
    // sent, and on a malformed body the parsed form does not exist.
    payload: input.rawBody ?? (input.body === undefined ? '' : JSON.stringify(input.body)),
    received_at: input.receivedAt,
  }

  return {
    raw_signals,
    // Always set in practice — every 4xx passes through the error handler, which
    // stamps a reason. '' is the never-taken fallback rather than a guess.
    error_code: input.errorReason ?? '',
  }
}

/** What the controller knows about one accepted signal. */
export interface AcceptedInput {
  signalId: string
  receivedAt: string
  organizationId: string
  /** The parsed, validated body — the source of customer, type and payload. */
  body: unknown
}

/**
 * Builds the `signals_accepted` message for one signal.
 *
 * Carries the full `raw_signals` row, because the consumer both inserts it and
 * calls settle, and settle needs the customer, the type and the payload. The id
 * and time are the ones the request was stamped with — the id a caller is given
 * on a 202 must be the id on the queue.
 */
export function buildAcceptedMessage(input: AcceptedInput): AcceptedMessage {
  return {
    raw_signals: {
      signal_id: input.signalId,
      organization_id: input.organizationId,
      customer_id: readString(input.body, 'customerId') ?? '',
      type: readSignalType(input.body),
      idempotency_key: readString(input.body, 'idempotencyKey'),
      payload: input.body === undefined ? '' : JSON.stringify(input.body),
      received_at: input.receivedAt,
    },
  }
}

// --- publishing, with the SQS size ceiling ------------------------------------

/** SQS refuses a message over 256 KiB. */
const SQS_MAX_MESSAGE_BYTES = 256 * 1024

/** Room for everything in the message that is not `payload`. Generous — the
 *  trade is a few KB of headroom against a dropped row. */
const NON_PAYLOAD_BUDGET_BYTES = 8 * 1024

/** Cuts `raw` to the longest prefix whose JSON-ENCODED form fits the budget.
 *
 *  Encoded, not raw, because the two differ by up to 6x: a control byte is one
 *  byte in the body but six characters of `\u00xx` escape inside a JSON string.
 *  Iterating by code point rather than by index keeps a surrogate pair whole, so
 *  an emoji is never sliced into two orphaned halves. */
function truncateToEncodedBudget(raw: string, budgetBytes: number): string {
  let used = 0
  let end = 0
  for (const char of raw) {
    const cost = Buffer.byteLength(JSON.stringify(char), 'utf8') - 2
    if (used + cost > budgetBytes) break
    used += cost
    end += char.length
  }
  return raw.slice(0, end)
}

/**
 * Serialises a message, shrinking `raw_signals.payload` if that is what it takes
 * to fit under the SQS limit.
 *
 * A body is capped at `BODY_BYTES` (64 KB) but its ENCODED size is not: 64 KB of
 * control characters becomes 384 KB of `\u00xx` escapes. Left alone the send
 * throws, the publish fails, and the row vanishes — and a body full of control
 * bytes is exactly the MALFORMED_JSON reject the raw capture exists to preserve.
 * So the payload is trimmed rather than the message dropped. (A valid accepted
 * body never approaches the limit, so this only ever bites a pending reject.)
 */
export function serialiseMessage(message: { raw_signals: RawSignalRow }): {
  body: string
  droppedBytes: number
} {
  const body = JSON.stringify(message)
  if (Buffer.byteLength(body, 'utf8') <= SQS_MAX_MESSAGE_BYTES) {
    return { body, droppedBytes: 0 }
  }

  const full = message.raw_signals.payload
  const trimmed = truncateToEncodedBudget(full, SQS_MAX_MESSAGE_BYTES - NON_PAYLOAD_BUDGET_BYTES)
  return {
    body: JSON.stringify({ ...message, raw_signals: { ...message.raw_signals, payload: trimmed } }),
    droppedBytes: Buffer.byteLength(full, 'utf8') - Buffer.byteLength(trimmed, 'utf8'),
  }
}

/**
 * Publishes a rejected signal, and reports how much of the payload was trimmed
 * (0 for nearly every message).
 *
 * Throws whatever the driver throws. For a reject that means "log it and move
 * on" — a dead queue must never turn a 400 into a 500. That judgement is at the
 * call site, not here.
 */
export async function publishPending(queue: SignalQueue, message: PendingMessage): Promise<number> {
  const { body, droppedBytes } = serialiseMessage(message)
  await queue.send(body)
  return droppedBytes
}

/**
 * Publishes an accepted signal.
 *
 * Throws on failure, and the caller MUST NOT swallow it — the opposite of the
 * pending posture. An accepted signal is billable; a 202 sent after a failed
 * publish would lose it silently. Publish first, answer second.
 */
export async function publishAccepted(queue: SignalQueue, message: AcceptedMessage): Promise<void> {
  const { body } = serialiseMessage(message)
  await queue.send(body)
}
