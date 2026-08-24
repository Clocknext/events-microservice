/** Redis connection for the api-key resolution cache.
 *
 *  Decorates the instance as `app.cache`, typed as the narrow `KeyCache` port
 *  (see auth.schema) rather than as ioredis — services take that port as an
 *  argument, so they stay framework- and driver-agnostic and a test can hand
 *  them a plain Map.
 *
 *  `app.cache` is null when `REDIS_URL` is unset: the cache is an optimisation,
 *  never a dependency, so the service simply resolves every signal upstream.
 *
 *  The connection deliberately does NOT queue while offline
 *  (`enableOfflineQueue: false`), so a dead Redis fails a lookup in
 *  microseconds instead of stalling ingest behind a reconnect. The service
 *  treats that failure as a cache miss. */
// Named import, not default: ioredis is CJS and under `module: nodenext` its
// default export is the namespace, which is not constructable.
import { Redis } from 'ioredis'
import fp from 'fastify-plugin'
import { config } from '../config.js'
import type { KeyCache } from '../modules/auth/auth.schema.js'

declare module 'fastify' {
  interface FastifyInstance {
    cache: KeyCache | null
  }
}

export default fp(
  async (app) => {
    if (!config.redisUrl) {
      app.log.warn('REDIS_URL is not set — api-key resolutions will not be cached')
      app.decorate('cache', null)
      return
    }

    const redis = new Redis(config.redisUrl, {
      lazyConnect: true,
      enableOfflineQueue: false,
      // One retry, then surface the error. A cache lookup must never be the
      // slowest thing on the ingest path.
      maxRetriesPerRequest: 1,
      connectTimeout: 2000,
    })

    // An 'error' listener is mandatory: without one, ioredis escalates a
    // connection error to an unhandled 'error' event and kills the process.
    redis.on('error', (err) => {
      app.log.error({ err }, 'redis error')
    })

    try {
      await redis.connect()
      app.log.info('redis connected')
    } catch (err) {
      // Boot anyway. Ingest degrades to resolving every signal upstream, which
      // is slower but correct; refusing to start would be worse.
      app.log.error({ err }, 'redis connect failed — starting without cache')
    }

    app.decorate('cache', redis)

    app.addHook('onClose', async () => {
      await redis.quit().catch(() => redis.disconnect())
    })
  },
  { name: 'redis' },
)
