/** Module entry point: owns the URL prefix and any module-scoped plugins. */
import type { FastifyPluginAsync } from 'fastify'
import { signalRoutes } from './signal.routes.js'

export const signalModule: FastifyPluginAsync = async (app) => {
  await app.register(signalRoutes, { prefix: '/api/v1' })
}
