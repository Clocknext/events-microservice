/** Adapts HTTP to the service layer: pull input off the request, hand back a
 *  payload. No business rules, and no error handling — a thrown AppError is the
 *  error-handler plugin's job. */
import type { FastifyReply, FastifyRequest } from 'fastify'
import { successEnvelope } from '../../utils/envelope.js'
import { BadGatewayError } from '../../utils/errors.js'
import { extractBearer } from '../auth/auth.service.js'
import { buildAcceptedMessage, publishAccepted } from '../vent/vent.service.js'
import type { SignalBody } from './signal.schema.js'
import * as signalService from './signal.service.js'

export async function postSignal(
  request: FastifyRequest<{ Body: SignalBody }>,
  reply: FastifyReply,
) {
  const accepted = await signalService.ingestSignal(
    // `app.cache` is null when Redis is unconfigured or was never reachable;
    // the service then resolves every signal upstream.
    request.server.cache,
    extractBearer(request.headers.authorization),
    request.body,
    // Stamped at onRequest, so this is the same id a rejection would have
    // carried onto the queue. The service does not mint its own.
    { signalId: request.signalId, receivedAt: request.receivedAt },
  )

  // BEFORE the 202, not after, and it throws rather than failing open. This is
  // the reverse of the reject vent on purpose: an accepted signal is billable,
  // so acknowledging one that reached no queue would lose it in silence with
  // the caller told it was safe. A rejected signal's row is only analytics.
  const acceptedQueue = request.server.acceptedQueue
  if (acceptedQueue) {
    try {
      await publishAccepted(acceptedQueue, buildAcceptedMessage(accepted))
    } catch (err) {
      throw new BadGatewayError('Could not accept the signal. Retry shortly.', 'QUEUE_UNAVAILABLE', {
        detail: err instanceof Error ? err.message : 'accepted queue publish failed',
      })
    }
  }

  // 202, not 200: the signal is authenticated, well-formed and queued, but
  // nothing has been settled and no money has moved.
  request.log.info(
    {
      signalId: accepted.signalId,
      organizationId: accepted.organizationId,
      customerId: request.body.customerId,
      type: request.body.type,
      cached: accepted.cached,
    },
    'signal accepted',
  )
  return reply.status(202).send(
    successEnvelope(accepted, {
      statusCode: 202,
      message: 'Signal accepted for processing.',
    }),
  )
}
