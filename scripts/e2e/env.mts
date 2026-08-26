/** Reads the payments app's own `.env` so no secret is ever passed on a command
 *  line or exported into the shell. Point `PAYMENTS_ENV_FILE` elsewhere to use a
 *  different deployment. */
import { readFileSync } from 'node:fs'

const DEFAULT_ENV_FILE = '/home/joze/Documents/Work/Clocknext-Payment-Saas/.env'

export interface PaymentsEnv {
  databaseUrl: string
  internalSecret: string
  /** Only used to VERIFY a borrowed API key against its own ciphertext. */
  encryptionKey: string
}

/** Minimal dotenv: `KEY=value`, ignoring comments and blank lines, honouring
 *  surrounding quotes. Later assignments win, matching dotenv. */
function parseEnvFile(path: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const match = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(line)
    if (!match) continue
    let value = match[2]!.trim()
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1)
    }
    out[match[1]!] = value
  }
  return out
}

export function loadPaymentsEnv(): PaymentsEnv {
  const path = process.env.PAYMENTS_ENV_FILE ?? DEFAULT_ENV_FILE
  const env = parseEnvFile(path)
  const databaseUrl = env.DATABASE_URL ?? ''
  const internalSecret = env.INTERNAL_SETTLE_SECRET ?? ''
  if (!databaseUrl) throw new Error(`DATABASE_URL is not set in ${path}`)
  if (!internalSecret) throw new Error(`INTERNAL_SETTLE_SECRET is not set in ${path}`)

  // Hard stop: this harness writes and deletes rows, and swaps an API key hash.
  // It must never be aimed at the production project. `PRODUCTION_DATABASE_URL`
  // in the same file names the one to refuse.
  const production = env.PRODUCTION_DATABASE_URL ?? ''
  const ref = (url: string) => /postgres(?:ql)?:\/\/([^:]+):/.exec(url)?.[1] ?? url
  if (production && ref(production) === ref(databaseUrl)) {
    throw new Error('refusing to run: DATABASE_URL is the PRODUCTION database')
  }
  return { databaseUrl, internalSecret, encryptionKey: env.INTEGRATIONS_ENCRYPTION_KEY ?? '' }
}
