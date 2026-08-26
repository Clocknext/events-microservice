/**
 * The single exit for every failure on every route.
 *
 * Its job is that a caller never has to care WHERE a signal was refused. A
 * rejection from Fastify's body parser, from AJV, and from a service all leave
 * here in one envelope carrying `result.errorReason` -- so `BODY_TOO_LARGE`
 * reads the same whether the ceiling was hit while parsing or while measuring
 * the `custom` blob, and Fastify's internal `FST_ERR_*` codes never reach a
 * customer.
 */
import type { FastifyError, FastifyReply, FastifyRequest } from 'fastify'
import fp from 'fastify-plugin'
import {
  errorEnvelope,
  type ErrorReason,
  type ErrorResult,
  type SignalIssue,
} from '../utils/envelope.js'
import { AppError } from '../utils/errors.js'

/** Fastify's own failures, translated. Anything absent here falls back to the
 *  status-derived default below, so an unmapped code degrades to a truthful
 *  generic rather than leaking `FST_ERR_...` onto the wire. */
const FASTIFY_CODE_REASONS: Record<string, { status: number; reason: ErrorReason }> = {
  FST_ERR_CTP_BODY_TOO_LARGE: { status: 413, reason: 'BODY_TOO_LARGE' },
  FST_ERR_CTP_INVALID_MEDIA_TYPE: { status: 415, reason: 'UNSUPPORTED_MEDIA_TYPE' },
  FST_ERR_CTP_EMPTY_TYPE: { status: 415, reason: 'UNSUPPORTED_MEDIA_TYPE' },
  FST_ERR_CTP_INVALID_JSON_BODY: { status: 400, reason: 'MALFORMED_JSON' },
  FST_ERR_CTP_EMPTY_JSON_BODY: { status: 400, reason: 'EMPTY_BODY' },
  FST_ERR_CTP_INVALID_CONTENT_LENGTH: { status: 400, reason: 'MALFORMED_JSON' },
  FST_ERR_ROUTE_METHOD_NOT_SUPPORTED: { status: 405, reason: 'METHOD_NOT_ALLOWED' },
}

function defaultReasonFor(status: number): ErrorReason {
  if (status === 401 || status === 403) return 'API_KEY_REJECTED'
  if (status === 404) return 'NOT_FOUND'
  if (status === 405) return 'METHOD_NOT_ALLOWED'
  if (status === 413) return 'BODY_TOO_LARGE'
  if (status === 415) return 'UNSUPPORTED_MEDIA_TYPE'
  if (status >= 500) return 'INTERNAL_ERROR'
  return 'BAD_REQUEST'
}

/** AJV reports structure; these keywords report only that a *combinator*
 *  failed ("must match a schema in anyOf"), which tells a caller nothing the
 *  branch errors beside them do not already say. */
const STRUCTURAL_KEYWORDS = new Set(['if', 'anyOf', 'allOf', 'oneOf', 'then', 'else'])

/**
 * Turns AJV's errors into the `{ field, message }` list
 * `/api/internal/resolve` returns, so a body refused here and a body refused
 * upstream look identical to the caller.
 *
 * A `required` failure has no `instancePath` -- the missing field's name is in
 * `params.missingProperty`, which is also how the cross-field rules (`model`
 * for any type, `runId` for an outcome) name themselves.
 */
function issuesFromValidation(validation: FastifyError['validation']): SignalIssue[] {
  if (!validation) return []
  const issues: SignalIssue[] = []
  const seen = new Set<string>()

  for (const entry of validation) {
    if (STRUCTURAL_KEYWORDS.has(entry.keyword)) continue
    const missing = (entry.params as { missingProperty?: string } | undefined)?.missingProperty
    const field = missing ?? entry.instancePath.replace(/^\//, '').replace(/\//g, '.')
    const message = entry.message ?? 'is invalid'
    // The same field can fail two keywords at once (minLength and pattern on a
    // whitespace-only id). Report each distinct complaint once.
    const dedupeKey = `${field} ${message}`
    if (seen.has(dedupeKey)) continue
    seen.add(dedupeKey)
    issues.push({ field, message })
  }
  return issues
}

/** The one-line summary for `statusDetail.message`, built the way the payments
 *  rulebook builds its `error`: the first problem, prefixed by its field. */
function summarise(issues: SignalIssue[]): string {
  const first = issues[0]
  if (!first) return 'Invalid request body.'
  return first.field ? `${first.field}: ${first.message}` : first.message
}

/** Sends the error envelope.
 *
 *  Every ≥400 is answered as itself now — the edge is a gate, so a refused
 *  signal is a plain rejection, not a queued 202. The stamped `signalId` rides
 *  along so even a hard rejection is quotable back to us. */
async function send(
  request: FastifyRequest,
  reply: FastifyReply,
  status: number,
  message: string,
  result: ErrorResult,
) {
  return reply.status(status).send(
    errorEnvelope(status, message, {
      ...result,
      // Even a hard rejection is quotable: the caller can give us this id and
      // we can find the request in the logs.
      ...(request.signalId ? { signalId: request.signalId } : {}),
      ...(request.receivedAt ? { receivedAt: request.receivedAt } : {}),
    }),
  )
}

export default fp(
  async (app) => {
    app.setErrorHandler<FastifyError>((error, request, reply) => {
      // 1. Something a service decided. It already knows its status and reason.
      if (error instanceof AppError) {
        request.log.warn(
          { err: error, errorReason: error.reason, detail: error.detail },
          'signal rejected',
        )
        // `error.detail` is deliberately NOT forwarded — it went into the log
        // line above. See the note in utils/envelope.ts.
        return send(request, reply, error.statusCode, error.message, {
          errorReason: error.reason,
          ...(error.issues ? { issues: error.issues } : {}),
        })
      }

      // 2. AJV refused the body. Reported as INVALID_BODY -- the same reason an
      //    upstream body rejection gets, because it is the same rulebook.
      if (error.validation) {
        const issues = issuesFromValidation(error.validation)
        request.log.info({ issues }, 'body rejected')
        return send(request, reply, 400, summarise(issues), {
          errorReason: 'INVALID_BODY',
          issues,
        })
      }

      // 3. Fastify's own failure -- parsing, content type, routing.
      const mapped = error.code ? FASTIFY_CODE_REASONS[error.code] : undefined
      const status = mapped?.status ?? error.statusCode ?? 500
      if (status < 500) {
        request.log.info({ err: error }, 'request rejected')
        return send(request, reply, status, error.message, {
          errorReason: mapped?.reason ?? defaultReasonFor(status),
        })
      }

      // 4. Unexpected. Log everything, tell the caller nothing -- an internal
      //    message could name a host, a query or a secret.
      request.log.error({ err: error }, 'unhandled error')
      return send(request, reply, 500, 'Something went wrong on our end.', {
        errorReason: 'INTERNAL_ERROR',
      })
    })

    // A 404 is answered in the same envelope, so a caller that mistyped the
    // path gets a parseable answer rather than Fastify's bare shape.
    app.setNotFoundHandler((request: FastifyRequest, reply: FastifyReply) => {
      return send(request, reply, 404, `Route ${request.method} ${request.url} not found.`, {
        errorReason: 'NOT_FOUND',
      })
    })
  },
  { name: 'error-handler' },
)
