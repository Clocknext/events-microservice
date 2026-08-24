import assert from 'node:assert/strict'
import { test } from 'node:test'
import { buildApp } from '../../app.js'
import { getHealth } from './health.service.js'

test('service returns ok', () => {
  assert.equal(getHealth().status, 'ok')
})

test('route responds via inject', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'GET', url: '/health' })
  assert.equal(res.statusCode, 200)
  assert.equal(res.json().status, 'ok')
  await app.close()
})
