/** Module entry point: owns the URL prefix and any module-scoped plugins. */
import type { FastifyPluginAsync } from 'fastify'
import { healthRoutes } from './health.routes.js'

export const healthModule: FastifyPluginAsync = async (app) => {
  await app.register(healthRoutes, { prefix: '/health' })
}
