/**
 * Response envelope for the public v1 API (`/api/v1/*`).
 *
 * A port of `v1-response.ts` in the payments repo, so a customer integrating
 * against `POST /api/v1/signal` here and `POST /api/v1/usage` there unwraps one
 * shape. Every response, success or error, looks like:
 *
 *   {
 *     "statusCode": 202,
 *     "statusDetail": { "status": "SUCCESS", "message": "..." },
 *     "result": { ... }
 *   }
 *
 * `statusCode` mirrors the HTTP status. `statusDetail.status` is "SUCCESS" for
 * 2xx and "ERROR" otherwise — deliberately binary, unlike the four-value enum
 * the payments app's INTERNAL routes use. `result` always carries an object.
 *
 * WHERE THE ERROR REASON LIVES: `v1Error` upstream leaves `result` as `{}`, so
 * this edge puts `errorReason` (and `issues`, when a body was judged) there. A
 * caller already reading `statusDetail.message` is unaffected; a new one can
 * branch on a stable string instead of parsing English.
 *
 * These builders return plain objects rather than replies — nothing here knows
 * about Fastify.
 */

/** Stable, machine-readable cause. Add to this union rather than inventing a
 *  string at a throw site: it is a wire contract, so a caller may branch on it
 *  and a renamed member is a breaking change. */
export type ErrorReason =
  // --- the request never got as far as being understood -----------------------
  | 'NOT_FOUND'
  | 'METHOD_NOT_ALLOWED'
  | 'UNSUPPORTED_MEDIA_TYPE'
  | 'BODY_TOO_LARGE'
  | 'MALFORMED_JSON'
  | 'EMPTY_BODY'
  | 'BAD_REQUEST'
  // --- the signal itself was judged and refused ------------------------------
  | 'INVALID_BODY'
  | 'CUSTOM_TOO_LARGE'
  // --- the caller could not be identified ------------------------------------
  | 'API_KEY_MISSING'
  | 'API_KEY_REJECTED'
  // --- our side failed; the signal was never judged, so it is retryable ------
  | 'UPSTREAM_UNAVAILABLE'
  | 'EDGE_MISCONFIGURED'
  /** The signal could not be put on the queue, so it is recorded nowhere. The
   *  one failure that must not answer 202: `Processing in the queue.` would be
   *  a promise we did not keep, and the caller would never resend. */
  | 'QUEUE_UNAVAILABLE'
  | 'INTERNAL_ERROR'

/** One rejected field, in the same shape `/api/internal/resolve` reports. */
export interface SignalIssue {
  field: string
  message: string
}

export type ApiStatus = 'SUCCESS' | 'ERROR'

export interface ApiEnvelope<T> {
  statusCode: number
  statusDetail: { status: ApiStatus; message: string }
  result: T
}

export interface ErrorResult {
  errorReason: ErrorReason
  /** Present only when a body was judged — every broken field, not just the
   *  first. Omitted rather than sent empty when nothing was judged. */
  issues?: SignalIssue[]
  /** Our id for this request, stamped before anything could fail. Present on
   *  every error the signal route answers, so even a hard rejection can be
   *  quoted back to us and found. */
  signalId?: string
  receivedAt?: string
}

/* WHAT IS DELIBERATELY NOT HERE: the underlying cause of a 5xx. `errorReason`
 * already tells the caller everything they can act on (UPSTREAM_UNAVAILABLE =
 * retry, EDGE_MISCONFIGURED = tell us), while the cause itself — "connect
 * ECONNREFUSED 10.0.3.7:443" — names our internal hosts. It rides in the log
 * line instead, keyed by the same errorReason. */

export const DEFAULT_SUCCESS_MESSAGE = 'Request processed successfully.'

export function successEnvelope<T extends object>(
  result: T,
  { statusCode = 200, message = DEFAULT_SUCCESS_MESSAGE } = {},
): ApiEnvelope<T> {
  return { statusCode, statusDetail: { status: 'SUCCESS', message }, result }
}

export function errorEnvelope(
  statusCode: number,
  message: string,
  result: ErrorResult,
): ApiEnvelope<ErrorResult> {
  return { statusCode, statusDetail: { status: 'ERROR', message }, result }
}

/** JSON schema for the envelope, given a schema for `result`. Every status a
 *  route can answer with needs one, or Fastify has no serializer for it. */
export function envelopeSchema<T>(resultSchema: T) {
  return {
    type: 'object',
    required: ['statusCode', 'statusDetail', 'result'],
    properties: {
      statusCode: { type: 'integer' },
      statusDetail: {
        type: 'object',
        required: ['status', 'message'],
        properties: {
          status: { type: 'string', enum: ['SUCCESS', 'ERROR'] },
          message: { type: 'string' },
        },
      },
      result: resultSchema,
    },
  } as const
}

/** `result` for any error response. */
export const errorResultSchema = {
  type: 'object',
  required: ['errorReason'],
  properties: {
    errorReason: { type: 'string' },
    signalId: { type: 'string' },
    receivedAt: { type: 'string' },
    issues: {
      type: 'array',
      items: {
        type: 'object',
        required: ['field', 'message'],
        properties: { field: { type: 'string' }, message: { type: 'string' } },
      },
    },
  },
} as const
