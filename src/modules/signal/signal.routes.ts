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
      // Anything over this never reaches the schema — Fastify answers 413.
      bodyLimit: config.bodyBytes,
      // Two normalisations, both mirroring how the payments rulebook READS the
      // body rather than changing what it means. preValidation runs before the
      // body schema, which is the whole point of doing them here.
      preValidation: async (request) => {
        const body = request.body as { type?: unknown; agentKey?: unknown; key?: unknown } | null
        if (!body) return

        // `type` is accepted case-insensitively upstream, and JSON Schema cannot
        // compare against an enum case-insensitively.
        if (typeof body.type === 'string') {
          body.type = body.type.trim().toLowerCase()
        }

        // Upstream reads `agentKey ?? key`, so folding the deprecated alias in
        // here is semantically identical -- and it means a missing agent key is
        // reported as `agentKey` alone. Left as an anyOf, AJV named BOTH fields
        // and told the caller to send `key`, which is the one they should not.
        // `key` itself is left untouched for anything downstream that reads it.
        if (
          (body.agentKey === undefined || body.agentKey === null || body.agentKey === '') &&
          typeof body.key === 'string' &&
          body.key.trim() !== ''
        ) {
          body.agentKey = body.key
        }
      },
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
