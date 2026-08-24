/** SQS connection for the reject vent.
 *
 *  Decorates the instance as `app.queue`, typed as the narrow `SignalQueue`
 *  port (see vent.schema) rather than as the AWS SDK — the service takes that
 *  port as an argument, so it stays driver-agnostic and a test can hand it an
 *  array.
 *
 *  `app.queue` is null when `SQS_PENDING_QUEUE_URL` is unset, and the vent then
 *  does nothing: venting is observability, never a dependency. This is the same
 *  posture `plugins/redis.ts` takes, for a stronger reason — a queue that
 *  refuses a rejected signal must not turn that signal's 400 into a 500.
 *
 *  Credentials are NOT read here. They come from the SDK's default chain, so
 *  local development is `AWS_PROFILE=localstack` in the environment and
 *  production is the instance role, with no config key in between. */
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs'
import fp from 'fastify-plugin'
import { config } from '../config.js'
import type { SignalQueue } from '../modules/vent/vent.schema.js'

declare module 'fastify' {
  interface FastifyInstance {
    /** `signals_pending` — every response of 400 or above. */
    queue: SignalQueue | null
    /** `signals_accepted` — every signal that passed every check. */
    acceptedQueue: SignalQueue | null
  }
}

export default fp(
  async (app) => {
    if (!config.pendingQueueUrl && !config.acceptedQueueUrl) {
      app.log.warn('no SQS queue URLs configured — signals will not be queued')
      app.decorate('queue', null)
      app.decorate('acceptedQueue', null)
      return
    }

    const client = new SQSClient({
      region: config.awsRegion,
      // Set only for LocalStack. Empty means the real endpoint for the region,
      // which is what the SDK resolves on its own.
      ...(config.awsEndpointUrl ? { endpoint: config.awsEndpointUrl } : {}),
      // One retry, then give up. A publish that retries for long is a publish
      // that outlives the request it describes.
      maxAttempts: 2,
    })

    // NOTE: the queue's own `DelaySeconds` is what holds a message before a
    // consumer can see it — 60s on `signals_pending`, none on
    // `signals_accepted`. It is a queue ATTRIBUTE, set where the queue is
    // created (scripts/localstack/ready.d/01-queues.sh in development), so
    // nothing here has to know the number.
    const sender = (queueUrl: string): SignalQueue => ({
      async send(body) {
        await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }))
      },
    })

    app.decorate('queue', config.pendingQueueUrl ? sender(config.pendingQueueUrl) : null)
    app.decorate(
      'acceptedQueue',
      config.acceptedQueueUrl ? sender(config.acceptedQueueUrl) : null,
    )

    if (!config.pendingQueueUrl) {
      app.log.warn('SQS_PENDING_QUEUE_URL is not set — rejected signals will not be vented')
    }
    if (!config.acceptedQueueUrl) {
      app.log.warn('SQS_ACCEPTED_QUEUE_URL is not set — accepted signals will not be queued')
    }
    app.log.info(
      { pending: config.pendingQueueUrl, accepted: config.acceptedQueueUrl },
      'sqs connected',
    )

    app.addHook('onClose', async () => {
      client.destroy()
    })
  },
  { name: 'sqs' },
)
