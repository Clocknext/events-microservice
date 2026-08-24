/** Module entry point: owns the URL prefix and any module-scoped plugins. */
import type { FastifyPluginAsync } from 'fastify'
import { UnauthorizedError } from '../../utils/errors.js'
import { extractBearer } from '../auth/auth.service.js'
import { signalRoutes } from './signal.routes.js'

export const signalModule: FastifyPluginAsync = async (app) => {
  // Everything this module refuses is answered 202 and queued instead, except
  // an unidentified caller. The flag is set here rather than assumed by the
  // error handler because only this module owns that policy — a 404 for a path
  // no route matches must stay a 404.
  //
  // The check runs at onRequest, BEFORE the body is parsed, and that ordering
  // is the point: a request with no key and an unparseable body is still a 401,
  // not a 202 for a signal nobody could ever claim.
  app.addHook('onRequest', async (request) => {
    request.deferToQueue = true

    if (!extractBearer(request.headers.authorization)) {
      throw new UnauthorizedError(
        'Missing API key. Send `Authorization: Bearer <your cnk_… key>`.',
        'API_KEY_MISSING',
      )
    }
  })

  await app.register(signalRoutes, { prefix: '/api/v1' })
}
