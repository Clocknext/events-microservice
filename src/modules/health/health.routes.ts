/** Wires schemas to controllers. No logic lives here. */
import type { FastifyPluginAsync } from 'fastify'
import * as controller from './health.controller.js'
import {
  echoBodySchema,
  echoResponseSchema,
  healthResponseSchema,
  type EchoBody,
} from './health.schema.js'

export const healthRoutes: FastifyPluginAsync = async (app) => {
  app.get(
    '/',
    { schema: { response: { 200: healthResponseSchema } } },
    controller.getHealth,
  )

  app.post<{ Body: EchoBody }>(
    '/echo',
    { schema: { body: echoBodySchema, response: { 200: echoResponseSchema } } },
    controller.postEcho,
  )
}
