/** Business logic. Knows nothing about HTTP — no request, no reply.
 *
 *  The edge is a thin gate now. By the time we get here the route's schema has
 *  already confirmed the three required fields and the module's onRequest hook
 *  has confirmed an API key is present. All that is left is to shape the message
 *  the controller produces to Kafka — the id and arrival time were stamped on
 *  the request before anything could fail (plugins/identity.ts), so an accepted
 *  signal carries the same id the caller is handed back. */
import type { AcceptedSignalResult, SignalBody, SignalIdentity, SignalMessage } from './signal.schema.js'

/** Builds the one Kafka message for a gate-passing signal: the edge-stamped
 *  envelope plus the body exactly as the caller sent it. */
export function buildSignalMessage(body: SignalBody, identity: SignalIdentity): SignalMessage {
  return {
    signalId: identity.signalId,
    receivedAt: identity.receivedAt,
    apiKeyHash: identity.apiKeyHash,
    body,
  }
}

/** What the controller answers on a 202. Deliberately drops `apiKeyHash`: the
 *  caller already holds the key it came from, and a response body is no place to
 *  echo a credential's digest back. */
export function acceptedResult(identity: SignalIdentity): AcceptedSignalResult {
  return {
    accepted: true,
    signalId: identity.signalId,
    receivedAt: identity.receivedAt,
  }
}
