/** Business logic. Knows nothing about HTTP — no request, no reply. */

export interface HealthStatus {
  status: 'ok'
  uptime: number
}

export function getHealth(): HealthStatus {
  return { status: 'ok', uptime: process.uptime() }
}

export function echo(message: string): { echo: string } {
  return { echo: message }
}
