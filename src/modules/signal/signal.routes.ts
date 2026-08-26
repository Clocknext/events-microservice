/** Wires schemas to controllers. No logic lives here. */
import type { FastifyPluginAsync } from 'fastify'
import { config } from '../../config.js'
import { envelopeSchema, errorResultSchema } from '../../utils/envelope.js'
import * as controller from './signal.controller.js'
import {
  signalAcceptedResultSchema,
  signalBodySchema,
  type SignalBody,
} from './signal.schema.js'

const errorResponse = envelopeSchema(errorResultSchema)

export const signalRoutes: FastifyPluginAsync = async (app) => {
  // Fastify parses text/plain by default, which would reach the body schema as
  // a string and be refused as "body must be object" -- a 400 that misnames the
  // problem. Removing the parser here (encapsulated to this module, so /health
  // keeps it) makes a non-JSON content type the 415 it actually is.
  app.removeContentTypeParser('text/plain')

  app.post<{ Body: SignalBody }>(
    '/signal',
    {
      // Anything over this never reaches the schema — Fastify answers 413. The
      // body is otherwise passed through untouched: the gate reads three fields
      // and the rest rides to Kafka as sent.
      bodyLimit: config.bodyBytes,
      schema: {
        body: signalBodySchema,
        response: {
          202: envelopeSchema(signalAcceptedResultSchema),
          400: errorResponse,
          401: errorResponse,
          413: errorResponse,
          415: errorResponse,
          500: errorResponse,
          502: errorResponse,
        },
      },
    },
    controller.postSignal,
  )
}
