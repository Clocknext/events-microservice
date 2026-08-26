/** Adapts HTTP to the service layer: pull input off the request, hand back a
 *  payload. No business rules, and no error handling — a thrown AppError is the
 *  error-handler plugin's job. */
import type { FastifyReply, FastifyRequest } from 'fastify'
import { successEnvelope } from '../../utils/envelope.js'
import { BadGatewayError } from '../../utils/errors.js'
import type { SignalBody } from './signal.schema.js'
import * as signalService from './signal.service.js'

export async function postSignal(
  request: FastifyRequest<{ Body: SignalBody }>,
  reply: FastifyReply,
) {
  // All stamped at onRequest, so this is the id the caller is handed and the id
  // on the topic — the same signal under one identity. `apiKeyHash` comes from
  // the module's own hook (the raw key never travels).
  const identity = {
    signalId: request.signalId,
    receivedAt: request.receivedAt,
    apiKeyHash: request.apiKeyHash,
  }
  const message = signalService.buildSignalMessage(request.body, identity)

  // BEFORE the 202, not after, and it throws rather than failing open: an
  // accepted signal is billable, so acknowledging one that reached no topic
  // would lose it in silence with the caller told it was safe. `app.producer` is
  // null when no brokers are configured, in which case a 202 means only "passed
  // the gate".
  const producer = request.server.producer
  if (producer) {
    try {
      await producer.send(message)
    } catch (err) {
      throw new BadGatewayError('Could not accept the signal. Retry shortly.', 'QUEUE_UNAVAILABLE', {
        detail: err instanceof Error ? err.message : 'kafka produce failed',
      })
    }
  }

  // 202, not 200: the signal passed the gate and is on the topic, but nothing
  // has been settled and no money has moved.
  request.log.info(
    { signalId: identity.signalId, customerId: request.body.customerId },
    'signal accepted',
  )
  return reply.status(202).send(
    successEnvelope(signalService.acceptedResult(identity), {
      statusCode: 202,
      message: 'Signal accepted for processing.',
    }),
  )
}
