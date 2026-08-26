/** Stamps every request with an identity before anything can fail.
 *
 * A plugin rather than part of the signal module because the id must exist even
 * for responses no module owns — a 404 for an unmatched path, a 415 refused
 * before any handler runs — so a caller can quote it back and we can find the
 * request in the logs.
 *
 * The id is a ULID, not a UUID: it sorts by creation time, which is what the
 * downstream ClickHouse tables are ordered by, so ids minted in sequence land
 * near each other rather than scattering across the key range.
 */
import { ulid } from 'ulid'
import fp from 'fastify-plugin'

declare module 'fastify' {
  interface FastifyRequest {
    /** ULID stamped at `onRequest`. Every request has one, answered or not. */
    signalId: string
    /** Arrival time, ISO-8601 UTC. */
    receivedAt: string
  }
}

export default fp(
  async (app) => {
    app.decorateRequest('signalId', '')
    app.decorateRequest('receivedAt', '')

    app.addHook('onRequest', async (request) => {
      request.signalId = ulid()
      request.receivedAt = new Date().toISOString()
    })
  },
  { name: 'identity' },
)
