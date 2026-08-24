/** Business logic for api-key resolution. Knows nothing about HTTP — no
 *  request, no reply, and Redis arrives as the narrow `KeyCache` port.
 *
 *  This is the hot path: every signal passes through it, and the whole point is
 *  that almost none of them reach the payments app. Two answers are cached
 *  under one key, because "this key does not exist" is as worth remembering as
 *  "this key belongs to org X" — otherwise one misconfigured client turns into
 *  sustained load on a Vercel function.
 *
 *  What is NOT cached: a body rejection (it describes one request, not the key)
 *  and an outage (no verdict was reached). Caching either would be wrong in a
 *  way that lasts. */
import { createHash } from 'node:crypto'
import { resolveSignal } from '../../client/payments-client.js'
import { config } from '../../config.js'
import { BadGatewayError, BadRequestError, UnauthorizedError } from '../../utils/errors.js'
import type { AuthResult, KeyCache, KeyResolution } from './auth.schema.js'

/** Bumped when the cached JSON shape changes, so old entries are ignored rather
 *  than misread by new code. */
const CACHE_PREFIX = 'cnk:apikey:v1:'

/** Pulls the token out of `Authorization: Bearer <token>`. The scheme is
 *  required — the same rule the payments app applies to us. */
export function extractBearer(header: string | undefined): string | null {
  const match = /^Bearer\s+(\S.*)$/i.exec(header ?? '')
  return match?.[1]?.trim() || null
}

/** SHA-256 of the raw key as 64 lowercase hex chars — the exact shape stored in
 *  `ApiKey.hashedKey`, so the upstream lookup is one unique-index hit. */
export function digestApiKey(rawKey: string): string {
  return createHash('sha256').update(rawKey, 'utf8').digest('hex')
}

function hasExpired(expiresAt: string | null): boolean {
  if (!expiresAt) return false
  const at = Date.parse(expiresAt)
  return !Number.isNaN(at) && at <= Date.now()
}

async function readCache(
  cache: KeyCache | null,
  cacheKey: string,
): Promise<KeyResolution | null> {
  if (!cache) return null
  try {
    const raw = await cache.get(cacheKey)
    if (!raw) return null
    return JSON.parse(raw) as KeyResolution
  } catch {
    // A dead Redis, or an entry written by an older shape. Either way this is a
    // miss: the caller resolves upstream and the request still succeeds.
    return null
  }
}

async function writeCache(
  cache: KeyCache | null,
  cacheKey: string,
  resolution: KeyResolution,
): Promise<void> {
  if (!cache) return

  let ttl = resolution.ok ? config.keyCacheTtlSeconds : config.keyCacheMissTtlSeconds
  if (resolution.ok && resolution.key.expiresAt) {
    // Never trust a key past its own expiry. Without this clamp a key expiring
    // in 5s would keep being accepted for the full cache TTL.
    const secondsLeft = Math.floor((Date.parse(resolution.key.expiresAt) - Date.now()) / 1000)
    if (Number.isFinite(secondsLeft)) ttl = Math.min(ttl, Math.max(secondsLeft, 1))
  }

  try {
    await cache.set(cacheKey, JSON.stringify(resolution), 'EX', ttl)
  } catch {
    // Failing to cache costs a round trip next time; it is not worth failing
    // the request we already have an answer for.
  }
}

/**
 * Resolves the API key and judges the signal body in one step, answering from
 * Redis when it can.
 *
 * Returns the resolved key on success. On failure it throws, and which error it
 * throws is the whole contract — the caller downstream will fan accepted and
 * rejected signals onto different queues:
 *
 *   UnauthorizedError  the key is missing, unknown, malformed or expired
 *   BadRequestError    the body breaks a signal rule (`details.issues` lists
 *                      every broken field, not just the first)
 *   BadGatewayError    no verdict was reached — retry, do not reject
 *
 * NOTE on ordering: the payments route validates the body BEFORE it looks the
 * key up, so a request with a bad body never teaches us anything about the key.
 * That is exactly why the route in front of this mirrors the body rules in its
 * own schema: by the time we get here the body is nearly always already valid,
 * and the upstream 400 is a backstop for the rules a JSON schema cannot express.
 */
export async function resolveApiKeyAndBody(
  cache: KeyCache | null,
  rawKey: string | null,
  body: unknown,
): Promise<AuthResult> {
  if (!rawKey) {
    throw new UnauthorizedError(
      'Missing API key. Send `Authorization: Bearer <your cnk_… key>`.',
      'API_KEY_MISSING',
    )
  }

  const digest = digestApiKey(rawKey)
  const cacheKey = `${CACHE_PREFIX}${digest}`

  const cached = await readCache(cache, cacheKey)
  if (cached) {
    if (!cached.ok) throw new UnauthorizedError(cached.error, 'API_KEY_REJECTED')
    // Belt and braces alongside the TTL clamp: an entry written before a clock
    // change could otherwise outlive the key it describes.
    if (!hasExpired(cached.key.expiresAt)) {
      return { key: cached.key, cached: true }
    }
  }

  const outcome = await resolveSignal(digest, body)

  switch (outcome.outcome) {
    case 'resolved':
      await writeCache(cache, cacheKey, { ok: true, key: outcome.key })
      return { key: outcome.key, cached: false }

    case 'rejected-key':
      await writeCache(cache, cacheKey, {
        ok: false,
        status: outcome.status,
        error: outcome.message,
      })
      throw new UnauthorizedError(outcome.message, 'API_KEY_REJECTED')

    case 'rejected-body':
      // Same reason a locally-refused body gets: one rulebook, one code.
      throw new BadRequestError(outcome.message, 'INVALID_BODY', {
        issues: outcome.issues,
      })

    case 'unavailable':
      // `reason` separates "payments is down" (wait for it) from "our shared
      // secret is wrong" (nobody's signal will ever land until it is fixed) --
      // the two need different alerts, so they must not share a code.
      throw new BadGatewayError(
        'Could not verify the signal. Retry shortly.',
        outcome.misconfigured ? 'EDGE_MISCONFIGURED' : 'UPSTREAM_UNAVAILABLE',
        { detail: outcome.message },
      )
  }
}
