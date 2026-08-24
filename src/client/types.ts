// Wire types for the payments app's /api/internal/* endpoints.

import type { ResolvedApiKey, SignalIssue } from '../modules/auth/auth.schema.js'

/** Every `/api/*` route on the payments app answers in this envelope. */
export interface ApiEnvelope<T> {
  statusCode: number
  statusDetail: { status: string; message: string }
  result: T
}

/**
 * What one `POST /api/internal/resolve` call concluded. The route answers two
 * questions at once — is the body acceptable, and whose key is this — so its
 * failures are not interchangeable, and only one of them is worth caching:
 *
 *   resolved      the key is good (cache it)
 *   rejected-key  the key is unknown, malformed or expired (cache it)
 *   rejected-body this particular body breaks a signal rule (NEVER cache — it
 *                 says nothing about the key, and the next body differs)
 *   unavailable   the call never produced a verdict (never cache; retryable)
 */
export type ResolveOutcome =
  | { outcome: 'resolved'; key: ResolvedApiKey }
  | { outcome: 'rejected-key'; status: number; message: string }
  | { outcome: 'rejected-body'; message: string; issues: SignalIssue[] }
  | {
      outcome: 'unavailable'
      message: string
      /** True when the call failed because OUR shared secret was refused, not
       *  because payments was unreachable. Same 502 to the caller, different
       *  `errorReason` -- one waits itself out, the other never will. */
      misconfigured?: boolean
    }

export interface SettleSignal {
  signalId: string
  organizationId: string
  receivedAt: string
  idempotencyKey?: string | null
  payload: unknown
}

// One verdict per signal from /internal/settle. Which fields are present depends
// on the outcome: PROCESSED carries the money numbers, PENDING carries the error.
export type Verdict = {
  signalId: string
  status: 'PROCESSED' | 'PENDING'
  signalLogId?: string
  creditsUsed?: number
  providedCost?: number
  customerCost?: number
  creditName?: string | null
  modelName?: string | null
  outcomeCompleted?: boolean
  duplicate?: boolean
  errorType?: 'USER_ERROR' | 'SERVER_ERROR'
  errorCode?: string
  error?: string
}
