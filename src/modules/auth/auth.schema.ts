/** Types for api-key resolution. A leaf: imports nothing from the module. */

/** The four fields `/api/internal/resolve` hands back, and the only ones the
 *  edge caches. `expiresAt` is null for a key that never expires — read that as
 *  "no expiry", not "expired". */
export interface ResolvedApiKey {
  id: string
  organizationId: string
  createdById: string
  expiresAt: string | null
}

/** One rejected field, as the resolve route reports it. */
export interface SignalIssue {
  field: string
  message: string
}

/** What one resolution attempt concluded. The rejected branch is cached too —
 *  a key that does not exist is an answer worth remembering, otherwise a
 *  misconfigured client hammers the payments app with the same dead key. */
export type KeyResolution =
  | { ok: true; key: ResolvedApiKey }
  | { ok: false; status: number; error: string }

/** A successful resolution plus how we learned it. `cached` drives nothing but
 *  logging and the response — it is the number to watch to know the cache is
 *  doing its job. */
export interface AuthResult {
  key: ResolvedApiKey
  cached: boolean
}

/**
 * The slice of Redis this module needs — deliberately tiny so the service takes
 * a port, not a driver. ioredis satisfies it structurally; so does a Map-backed
 * fake in a test.
 */
export interface KeyCache {
  get(key: string): Promise<string | null>
  set(key: string, value: string, mode: 'EX', ttlSeconds: number): Promise<unknown>
}
