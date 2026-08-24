/** Business logic. Knows nothing about HTTP — no request, no reply.
 *
 *  One signal's journey through the edge, in order:
 *
 *    0. the request is stamped with an id and an arrival time (plugins/vent.ts)
 *    1. the body is judged by the route's schema (before we get here)
 *    2. the `custom` blob is size-checked  ← here
 *    3. the API key is resolved, from Redis if possible ← auth.service
 *    4. the accepted signal carries that id back to the caller and is HELD
 *
 *  Step 4 is deliberately incomplete: an ACCEPTED signal is not queued or
 *  settled yet — that hand-off is the next piece of work. A REFUSED one is
 *  already queued, from `onResponse`, by the vent (modules/vent). */
import { config } from '../../config.js'
import { PayloadTooLargeError } from '../../utils/errors.js'
import { resolveApiKeyAndBody } from '../auth/auth.service.js'
import type { KeyCache } from '../auth/auth.schema.js'
import type { AcceptedSignalResult, SignalBody, SignalIdentity } from './signal.schema.js'

/** The `custom` blob is caller-controlled and unbounded in shape, so it gets
 *  its own ceiling below the whole-body limit. Checked here rather than in the
 *  schema because JSON Schema cannot measure serialized bytes. */
function assertCustomWithinLimit(custom: unknown): void {
  if (custom === undefined || custom === null) return
  const bytes = Buffer.byteLength(JSON.stringify(custom), 'utf8')
  if (bytes > config.customBytes) {
    throw new PayloadTooLargeError(
      `\`custom\` is ${bytes} bytes, over the ${config.customBytes}-byte limit.`,
      // Distinct from BODY_TOO_LARGE: the request as a whole was acceptable, so
      // the caller should shrink this one field, not the signal.
      'CUSTOM_TOO_LARGE',
    )
  }
}

/**
 * Authenticates a signal and accepts it.
 *
 * Throws on every rejection — `UnauthorizedError`, `BadRequestError`,
 * `PayloadTooLargeError` or `BadGatewayError` — and the error-handler plugin
 * turns each into its response. Nothing is caught here on purpose: the
 * distinction between "the customer sent something wrong" (4xx, do not retry)
 * and "we could not tell" (502, retry) is the contract the caller acts on.
 */
export async function ingestSignal(
  cache: KeyCache | null,
  rawKey: string | null,
  body: SignalBody,
  identity: SignalIdentity,
): Promise<AcceptedSignalResult> {
  assertCustomWithinLimit(body.custom)

  const { key, cached } = await resolveApiKeyAndBody(cache, rawKey, body)

  return {
    accepted: true,
    // NOT minted here. The id and the arrival time are stamped on the request
    // before anything can fail (plugins/vent.ts), for one reason: a signal that
    // is REFUSED needs an identity too, and it must be the same identity it
    // would have had if it were accepted. Minting at acceptance would give the
    // two paths two different ids for the same request.
    ...identity,
    organizationId: key.organizationId,
    apiKeyId: key.id,
    cached,
  }
}
