/** Adapts HTTP to the service layer: pull input off the request, hand back a payload. */
import type { FastifyRequest } from 'fastify'
import type { EchoBody } from './health.schema.js'
import * as healthService from './health.service.js'

export async function getHealth() {
  return healthService.getHealth()
}

export async function postEcho(request: FastifyRequest<{ Body: EchoBody }>) {
  return healthService.echo(request.body.message)
}
