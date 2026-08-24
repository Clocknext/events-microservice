/** Errors services throw; the error-handler plugin turns them into responses.
 *
 *  Every error carries a `reason` — the stable, machine-readable string the
 *  caller branches on, surfaced as `result.errorReason`. The subclass fixes the
 *  HTTP status; the throw site picks the reason, because one status covers
 *  several distinct causes (a 413 is an oversized body OR an oversized `custom`
 *  blob, and only the thrower knows which). */
import type { ErrorReason, SignalIssue } from './envelope.js'

interface AppErrorOptions extends ErrorOptions {
  /** Every broken field, when a body was judged. */
  issues?: SignalIssue[]
  /** Operator-facing note for a 5xx. Not a customer's fault. */
  detail?: string
}

export class AppError extends Error {
  readonly statusCode: number
  readonly reason: ErrorReason
  readonly issues: SignalIssue[] | undefined
  readonly detail: string | undefined

  constructor(
    message: string,
    statusCode = 500,
    reason: ErrorReason = 'INTERNAL_ERROR',
    options?: AppErrorOptions,
  ) {
    super(message, options)
    this.name = 'AppError'
    this.statusCode = statusCode
    this.reason = reason
    this.issues = options?.issues
    this.detail = options?.detail
  }
}

export class BadRequestError extends AppError {
  constructor(message = 'Bad Request', reason: ErrorReason = 'BAD_REQUEST', options?: AppErrorOptions) {
    super(message, 400, reason, options)
    this.name = 'BadRequestError'
  }
}

export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized', reason: ErrorReason = 'API_KEY_REJECTED', options?: AppErrorOptions) {
    super(message, 401, reason, options)
    this.name = 'UnauthorizedError'
  }
}

export class NotFoundError extends AppError {
  constructor(message = 'Not Found', reason: ErrorReason = 'NOT_FOUND', options?: AppErrorOptions) {
    super(message, 404, reason, options)
    this.name = 'NotFoundError'
  }
}

export class ConflictError extends AppError {
  constructor(message = 'Conflict', reason: ErrorReason = 'BAD_REQUEST', options?: AppErrorOptions) {
    super(message, 409, reason, options)
    this.name = 'ConflictError'
  }
}

export class PayloadTooLargeError extends AppError {
  constructor(
    message = 'Payload Too Large',
    reason: ErrorReason = 'BODY_TOO_LARGE',
    options?: AppErrorOptions,
  ) {
    super(message, 413, reason, options)
    this.name = 'PayloadTooLargeError'
  }
}

/** An upstream dependency failed or was unreachable. Distinct from a 500 on
 *  purpose: it tells the caller the signal was never judged, so retrying it is
 *  the right move rather than a duplicate. */
export class BadGatewayError extends AppError {
  constructor(
    message = 'Bad Gateway',
    reason: ErrorReason = 'UPSTREAM_UNAVAILABLE',
    options?: AppErrorOptions,
  ) {
    super(message, 502, reason, options)
    this.name = 'BadGatewayError'
  }
}
