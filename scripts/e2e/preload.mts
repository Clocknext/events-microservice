/** Puts the payments app's secrets into `process.env` BEFORE anything reads them.
 *
 *  `src/config.ts` snapshots `process.env` at import time, and the dispatcher's
 *  client reads `config.internalSecret` — so this has to run first. Import it as
 *  the FIRST import of the runner: ESM evaluates imports depth-first in source
 *  order, which makes "first import" the same thing as "before config".
 *
 *  Doing it here rather than on the command line keeps the shared secret out of
 *  the process list and the shell history. */
import { loadPaymentsEnv } from './env.mjs'

const env = loadPaymentsEnv()
process.env.INTERNAL_SETTLE_SECRET ??= env.internalSecret
// Needed by the fixtures' ciphertext check, which verifies the borrowed API key
// really is the org's before swapping it.
process.env.INTEGRATIONS_ENCRYPTION_KEY ??= env.encryptionKey
process.env.PAYMENTS_URL ??= 'http://127.0.0.1:3001'
process.env.CLICKHOUSE_URL ??= 'http://127.0.0.1:8123'
