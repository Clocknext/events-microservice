import assert from 'node:assert/strict'
import { test } from 'node:test'
import { UnauthorizedError } from '../../utils/errors.js'
import type { KeyCache, KeyResolution, ResolvedApiKey } from './auth.schema.js'
import { digestApiKey, extractBearer, resolveApiKeyAndBody } from './auth.service.js'

/** A Map behind the `KeyCache` port. Every test here stays offline: a cache HIT
 *  must never call the payments app, so if one of these tests tries to make a
 *  network request it is the test that has found a bug. */
function fakeCache(): KeyCache & { store: Map<string, string>; ttls: number[] } {
  const store = new Map<string, string>()
  const ttls: number[] = []
  return {
    store,
    ttls,
    async get(key) {
      return store.get(key) ?? null
    },
    async set(key, value, _mode, ttl) {
      store.set(key, value)
      ttls.push(ttl)
      return 'OK'
    },
  }
}

const KEY = 'cnk_7a0a91e402fd2944f79ab8f7ba2f26f678b7acf0c2a08b3a5bea92ed65a811a3'
const BODY = { customerId: 'cus_abc123', inputTokens: 1, outputTokens: 1 }

function seed(cache: KeyCache & { store: Map<string, string> }, resolution: KeyResolution) {
  cache.store.set(`cnk:apikey:v1:${digestApiKey(KEY)}`, JSON.stringify(resolution))
}

const liveKey: ResolvedApiKey = {
  id: 'key_1',
  organizationId: 'org_1',
  createdById: 'usr_1',
  expiresAt: null,
}

test('digest is 64 lowercase hex chars', () => {
  assert.match(digestApiKey(KEY), /^[0-9a-f]{64}$/)
})

test('bearer scheme is required', () => {
  assert.equal(extractBearer(`Bearer ${KEY}`), KEY)
  assert.equal(extractBearer(`bearer ${KEY}`), KEY, 'scheme is case-insensitive')
  assert.equal(extractBearer(KEY), null, 'a bare token is not accepted')
  assert.equal(extractBearer(undefined), null)
})

test('a cached key resolves without touching payments', async () => {
  const cache = fakeCache()
  seed(cache, { ok: true, key: liveKey })

  const result = await resolveApiKeyAndBody(cache, KEY, BODY)
  assert.deepEqual(result, { key: liveKey, cached: true })
})

test('a cached rejection is a 401, also without touching payments', async () => {
  const cache = fakeCache()
  seed(cache, { ok: false, status: 401, error: 'Invalid API key.' })

  await assert.rejects(
    resolveApiKeyAndBody(cache, KEY, BODY),
    (err: unknown) => err instanceof UnauthorizedError && err.message === 'Invalid API key.',
  )
})

test('a key cached before its expiry is not trusted past it', async () => {
  const cache = fakeCache()
  const expired = { ...liveKey, expiresAt: new Date(Date.now() - 1000).toISOString() }
  seed(cache, { ok: true, key: expired })

  // The entry is a hit, but re-resolving is the only correct move — with no
  // PAYMENTS_URL configured that surfaces as a 502, never as an acceptance.
  await assert.rejects(resolveApiKeyAndBody(cache, KEY, BODY), (err: unknown) => {
    assert.ok(err instanceof Error)
    assert.notEqual((err as { statusCode?: number }).statusCode, 200)
    return true
  })
})

test('a missing key is rejected before any lookup', async () => {
  const cache = fakeCache()
  await assert.rejects(
    resolveApiKeyAndBody(cache, null, BODY),
    (err: unknown) => err instanceof UnauthorizedError,
  )
  assert.equal(cache.store.size, 0, 'nothing was cached for a request with no key')
})

test('no cache configured still resolves — the cache is never a dependency', async () => {
  // null cache + unreachable payments: the request fails as a 502, which proves
  // it went upstream rather than erroring on the missing cache.
  await assert.rejects(resolveApiKeyAndBody(null, KEY, BODY), (err: unknown) => {
    assert.equal((err as { statusCode?: number }).statusCode, 502)
    return true
  })
})
