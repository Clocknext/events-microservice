/** Business logic for the api-key gate. Knows nothing about HTTP.
 *
 *  The edge only checks that a key is PRESENT — it does not resolve it. Who a
 *  key belongs to, and whether it is valid, is settled downstream by
 *  `/api/internal/settle`. So this module only reads the token off the header and
 *  digests it, which is the whole of what the edge needs to make the signal
 *  attributable later. */
import { createHash } from 'node:crypto'

/** Pulls the token out of `Authorization: Bearer <token>`. The scheme is
 *  required — the same rule the payments app applies to us. */
export function extractBearer(header: string | undefined): string | null {
  const match = /^Bearer\s+(\S.*)$/i.exec(header ?? '')
  return match?.[1]?.trim() || null
}

/**
 * The SHA-256 digest of a raw `cnk_…` key, as 64 lowercase hex characters.
 *
 * This is what travels — never the key itself. It is exactly what the payments
 * app stores in `ApiKey.hashedKey`, so `/api/internal/settle` resolves the
 * signal's organisation from it with a single unique-index hit, and the customer's
 * key cannot leak through Kafka, ClickHouse or an internal request log.
 *
 * The edge does NOT resolve the digest itself: whose key this is, and whether it
 * is still valid, is settled downstream. All the edge does is make the question
 * answerable later.
 */
export function digestApiKey(rawKey: string): string {
  return createHash('sha256').update(rawKey).digest('hex')
}
