/** Module entry point: owns the URL prefix and any module-scoped plugins. */
import type { FastifyPluginAsync } from 'fastify'
import { UnauthorizedError } from '../../utils/errors.js'
import { digestApiKey, extractBearer } from '../auth/auth.service.js'
import { signalRoutes } from './signal.routes.js'

declare module 'fastify' {
  interface FastifyRequest {
    /** SHA-256 digest of the caller's key, stamped at `onRequest` on the routes
     *  this module owns. Empty string elsewhere — `/health` takes no key. */
    apiKeyHash: string
  }
}

export const signalModule: FastifyPluginAsync = async (app) => {
  app.decorateRequest('apiKeyHash', '')

  // The gate's first check: an API key must be present. Presence only — the edge
  // does not resolve it (no Redis, no upstream call); `/api/internal/settle`
  // settles who the key belongs to later.
  //
  // The check runs at onRequest, BEFORE the body is parsed, and that ordering is
  // the point: a request with no key and an unparseable body is still a 401, not
  // a receipt for a signal nobody could ever claim.
  //
  // The digest is taken here, in the one place that already holds the raw token,
  // so nothing downstream ever needs to see the key itself.
  app.addHook('onRequest', async (request) => {
    const rawKey = extractBearer(request.headers.authorization)
    if (!rawKey) {
      throw new UnauthorizedError(
        'Missing API key. Send `Authorization: Bearer <your cnk_… key>`.',
        'API_KEY_MISSING',
      )
    }
    request.apiKeyHash = digestApiKey(rawKey)
  })

  await app.register(signalRoutes, { prefix: '/api/v1' })
}
