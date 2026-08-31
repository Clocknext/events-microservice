/** The runner's two pure pieces. The consumer loop itself needs a broker, so it
 *  is exercised by `npm run e2e`, not here. */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createResolveHealth, parseSignalMessage } from './consume.runner.js'

function buf(value: unknown): Buffer {
  return Buffer.from(typeof value === 'string' ? value : JSON.stringify(value))
}

test('a well-formed message round-trips out of the topic', () => {
  const message = parseSignalMessage(
    buf({
      signalId: '01M0X91S3X2SK86WYYWVENM3N7',
      receivedAt: '2026-08-26T10:00:00.000Z',
      apiKeyHash: 'a'.repeat(64),
      body: { customerId: 'cus_1', inputTokens: 10, outputTokens: 5 },
    }),
  )
  assert.equal(message?.signalId, '01M0X91S3X2SK86WYYWVENM3N7')
  assert.equal(message?.body.customerId, 'cus_1')
})

test('anything that cannot be keyed into a row is refused', () => {
  // A row in `signal_log` needs a `signal_id` to exist at all, so these cannot be
  // quarantined the way a resolve failure can — they are dropped, and the runner
  // logs each one at the point of loss.
  assert.equal(parseSignalMessage(null), null)
  assert.equal(parseSignalMessage(buf('not json')), null)
  assert.equal(parseSignalMessage(buf('[1,2,3]')), null)
  assert.equal(parseSignalMessage(buf({ receivedAt: 'x', body: {} })), null, 'no signalId')
  assert.equal(parseSignalMessage(buf({ signalId: '', receivedAt: 'x', body: {} })), null)
  assert.equal(parseSignalMessage(buf({ signalId: 's', body: {} })), null, 'no receivedAt')
  assert.equal(parseSignalMessage(buf({ signalId: 's', receivedAt: 'x' })), null, 'no body')
})

test('a missing apiKeyHash becomes empty rather than undefined', () => {
  // The ClickHouse column is not Nullable, and a row predating the edge stamping
  // the digest is exactly what `''` is there for.
  const message = parseSignalMessage(buf({ signalId: 's', receivedAt: 'x', body: {} }))
  assert.equal(message?.apiKeyHash, '')
})

test('a fresh tracker has NOT answered, however recently it was created', () => {
  // The whole correctness of the quarantine gate. A cold tracker reports a tiny
  // `msSinceSuccess` because it measures from creation — so if that alone decided
  // liveness, a consumer starting while payments was down would quarantine the
  // first signals it ever saw.
  const health = createResolveHealth(() => 1_000)
  assert.equal(health.hasAnswered(), false)
  assert.equal(health.msSinceSuccess(), 0)

  health.recordSuccess('sig-1')
  assert.equal(health.hasAnswered(), true)
})

test('succeededSinceStreak needs a success AFTER the streak began', () => {
  // The exact rule the quarantine turns on, and the one an elapsed-time proxy got
  // wrong against a live broker.
  let clock = 1_000
  const health = createResolveHealth(() => clock)

  health.recordSuccess('sig-earlier')
  clock += 70_000
  health.recordFailure('sig-x')
  // Note the comparison is on a monotonic success COUNT, not on these
  // timestamps: batches complete inside one millisecond, so a timestamp compare
  // would tie and a real poison message would never be caught.
  // A success that PREDATES the streak proves nothing about the route now.
  assert.equal(health.succeededSinceStreak('sig-x'), false)

  clock += 1_000
  health.recordFailure('sig-x')
  assert.equal(health.succeededSinceStreak('sig-x'), false)

  // Other traffic gets through, mid-streak. Now it is poison.
  clock += 1_000
  health.recordSuccess('sig-other')
  assert.equal(health.succeededSinceStreak('sig-x'), true)
})

test('succeededSinceStreak is false for a signal with no streak, and when cold', () => {
  const health = createResolveHealth(() => 1_000)
  assert.equal(health.succeededSinceStreak('never-seen'), false)
  health.recordFailure('sig-x')
  // Cold tracker: nothing has EVER answered, so nothing can license a quarantine.
  assert.equal(health.succeededSinceStreak('sig-x'), false)
})

test('a failure streak is per-signal, and only its OWN success clears it', () => {
  // Poison is defined as failing WHILE OTHERS SUCCEED, so a global clear on any
  // success would erase the one count the rule depends on.
  const health = createResolveHealth()
  assert.equal(health.recordFailure('sig-a'), 1)
  assert.equal(health.recordFailure('sig-a'), 2)
  assert.equal(health.recordFailure('sig-b'), 1)

  health.recordSuccess('sig-b')
  assert.equal(health.recordFailure('sig-a'), 3, "sig-b's success must not reset sig-a")

  health.recordSuccess('sig-a')
  assert.equal(health.recordFailure('sig-a'), 1)
})
