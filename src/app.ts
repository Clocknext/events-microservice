import Fastify, { type FastifyInstance } from 'fastify'
import { config } from './config.js'
import { healthModule } from './modules/health/health.module.js'
import { signalModule } from './modules/signal/signal.module.js'
import corePlugins from './plugins/core.js'
import errorHandler from './plugins/error-handler.js'
import redis from './plugins/redis.js'
import sqs from './plugins/sqs.js'
import vent from './plugins/vent.js'

export async function buildApp(): Promise<FastifyInstance> {
  const app = Fastify({
    logger: config.isProduction
      ? { level: config.logLevel }
      : {
          level: config.logLevel,
          transport: {
            target: 'pino-pretty',
            options: { translateTime: 'HH:MM:ss Z', ignore: 'pid,hostname' },
          },
        },
    // Trust the load balancer / API gateway in front of us for client IP + proto.
    trustProxy: config.isProduction,
    ajv: {
      customOptions: {
        // OFF, against Fastify's default. Coercion would let `"inputTokens":
        // "1200"` through as 1200, but the payments app's Zod rulebook rejects a
        // string outright — so the edge would accept a signal that settlement
        // later refuses, which is the one failure this service exists to prevent.
        coerceTypes: false,
        // Report every broken field in one response, not just the first. The
        // upstream resolve route does the same with its `issues` list.
        allErrors: true,
      },
    },
  })

  // Plugins first — modules registered below depend on them.
  await app.register(corePlugins)
  await app.register(errorHandler)
  await app.register(redis)
  await app.register(sqs)
  // After the error handler on purpose: the vent reads what that plugin decided
  // about a failure, and before the modules, so its onRequest hook has minted an
  // id before any route can start failing.
  await app.register(vent)

  // One register() per module. Each module owns its own prefix.
  await app.register(healthModule)
  await app.register(signalModule)

  return app
}
