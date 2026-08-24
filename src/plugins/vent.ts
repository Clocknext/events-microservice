/**
 * The app-wide adapter that puts every rejected request on the queue.
 *
 * It is a plugin rather than part of the signal module because the rule is
 * "everything that is not an accepted signal", and that includes responses no
 * module owns — a 404 for a path that matched no route, a wrong method, a body
 * Fastify refused before any handler ran. Scoped to the signal module, none of
 * those would ever be seen.
 *
 * Three hooks, in the order they fire:
 *
 *   onRequest    mint the identity, before anything can fail
 *   preParsing   tee the raw bytes, so an unparseable body is still recorded
 *   onResponse   status >= 400 -> build the two rows and publish
 *
 * Minting in `onRequest` is the whole trick: a 415 never reaches a handler and
 * a 404 never reaches a route, yet both leave with a `signalId`. The service's
 * own accepted path takes the same id off the request, so the id a caller is
 * given is provably the id in the queue.
 *
 * Publishing from `onResponse` is deliberate too — that hook runs after the
 * response is flushed, so SQS is never on the caller's critical path, and it is
 * the only place that sees the status code actually sent.
 */
import { Transform } from 'node:stream'
import { StringDecoder } from 'node:string_decoder'
import type { FastifyInstance, FastifyRequest } from 'fastify'
import fp from 'fastify-plugin'
import { config } from '../config.js'
import { buildVentMessage, mintSignalId, publishVent } from '../modules/vent/vent.service.js'

declare module 'fastify' {
  interface FastifyRequest {
    /** ULID stamped at `onRequest`. Every request has one, answered or not. */
    signalId: string
    /** Arrival time, ISO-8601 UTC. Shared by both rows. */
    receivedAt: string
    /** The body as sent, before parsing. Null when the caller sent none. */
    rawBody: string | null
    /** Filled by the error handler, read here — the reason and its one-line
     *  summary are decided when the response is built, and `onResponse` cannot
     *  read a body that has already gone out. */
    errorReason: string | null
    errorMessage: string | null
    /** Set once this request's rows are on the queue, so the `onResponse` hook
     *  does not file a second copy of a signal the error handler already sent. */
    vented: boolean
    /** Set by the signal module: a rejection on this route is answered 202 and
     *  queued rather than refused. Only the module knows that policy applies. */
    deferToQueue: boolean
  }
}

export default fp(
  async (app) => {
    app.decorateRequest('signalId', '')
    app.decorateRequest('receivedAt', '')
    app.decorateRequest('rawBody', null)
    app.decorateRequest('errorReason', null)
    app.decorateRequest('errorMessage', null)
    app.decorateRequest('vented', false)
    app.decorateRequest('deferToQueue', false)

    app.addHook('onRequest', async (request) => {
      request.signalId = mintSignalId()
      request.receivedAt = new Date().toISOString()
    })

    // Fastify's JSON parser throws away the bytes when they do not parse, which
    // loses exactly the payload a customer most needs to see. Teeing the stream
    // keeps them. The copy is capped at the same ceiling as the body itself, so
    // this cannot become a second, unbounded buffer of caller-controlled data.
    app.addHook('preParsing', async (request, _reply, payload) => {
      const decoder = new StringDecoder('utf8')
      let seen = 0
      let captured = 0

      const tee = new Transform({
        transform(chunk: Buffer, _encoding, callback) {
          seen += chunk.length
          if (captured < config.bodyBytes) {
            // Decoded incrementally rather than concatenated at 'end': a body
            // Fastify aborts mid-stream never fires 'end', and its first bytes
            // are worth keeping. (A body whose declared content-length is
            // already over the ceiling is refused before this hook runs at all,
            // so BODY_TOO_LARGE is vented with an empty payload.)
            const room = config.bodyBytes - captured
            const slice = chunk.length > room ? chunk.subarray(0, room) : chunk
            captured += slice.length
            request.rawBody = (request.rawBody ?? '') + decoder.write(slice)
          }
          callback(null, chunk)
        },
      })

      // Replacing the stream hides the original's byte count from Fastify's
      // content-length check. Re-exposing it keeps `bodyLimit` enforced — drop
      // this and an oversized body stops being a 413.
      Object.defineProperty(tee, 'receivedEncodedLength', { get: () => seen })

      return payload.pipe(tee)
    })

    app.addHook('onResponse', async (request, reply) => {
      // 2xx and 3xx are not rejects. Every 4xx and 5xx is, wherever it came
      // from — a refused body, an unknown key, an unmatched path, an outage.
      if (reply.statusCode < 400) return
      // A signal answered 202 was already vented, synchronously, before that
      // 202 was sent. Venting again would file the same signal twice.
      if (request.vented) return

      try {
        await ventNow(app, request, reply.statusCode)
      } catch (err) {
        // Fail open, loudly. The response has already gone out and was a
        // truthful rejection; all that is lost is a row, and losing it must not
        // cost the caller anything. This log is the only trace, so it has the id.
        request.log.error(
          { err, signalId: request.signalId, statusCode: reply.statusCode },
          'failed to vent rejected signal',
        )
      }
    })
  },
  { name: 'vent' },
)

/**
 * Publishes one request's rows and waits for the queue to take them.
 *
 * Called two ways, and the difference is who catches:
 *
 *   · from `onResponse`, for a request already answered with an error. A
 *     failure there is swallowed — the caller's response was correct without us.
 *   · from the error handler, BEFORE it answers 202. A failure there must
 *     propagate: `Processing in the queue.` is a promise, and a signal that
 *     reached no queue and no ClickHouse row would be lost in silence.
 *
 * `statusCode` is the status the request WOULD have been refused with, not the
 * 202 that may go out instead — `error_type` is derived from it, so passing 202
 * here would file every refusal as though nothing had gone wrong.
 */
export async function ventNow(
  app: FastifyInstance,
  request: FastifyRequest,
  statusCode: number,
): Promise<void> {
  const queue = app.queue
  if (!queue) return

  const droppedBytes = await publishVent(
    queue,
    buildVentMessage({
      signalId: request.signalId,
      receivedAt: request.receivedAt,
      statusCode,
      errorReason: request.errorReason,
      errorMessage: request.errorMessage,
      body: request.body,
      rawBody: request.rawBody,
    }),
  )
  request.vented = true

  // Truncation is rare and silent in the row itself — the payload column has no
  // room to say it was cut. This line is the only place it shows.
  if (droppedBytes > 0) {
    request.log.warn(
      { signalId: request.signalId, droppedBytes },
      'vent payload truncated to fit the SQS message limit',
    )
  }
}
