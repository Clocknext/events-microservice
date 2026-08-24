/** JSON schemas for validation/serialization, plus the TS types routes are generic over. */

export const healthResponseSchema = {
  type: 'object',
  required: ['status', 'uptime'],
  properties: {
    status: { type: 'string' },
    uptime: { type: 'number' },
  },
} as const

export const echoBodySchema = {
  type: 'object',
  required: ['message'],
  additionalProperties: false,
  properties: {
    message: { type: 'string', minLength: 1 },
  },
} as const

export const echoResponseSchema = {
  type: 'object',
  required: ['echo'],
  properties: {
    echo: { type: 'string' },
  },
} as const

export interface EchoBody {
  message: string
}
