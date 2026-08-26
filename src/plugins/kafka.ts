/** Kafka producer for the ingest topic.
 *
 *  Decorates the instance as `app.producer`, typed as the narrow `SignalProducer`
 *  port rather than as kafkajs — the controller takes that port, so it stays
 *  driver-agnostic and a test can hand it an array-backed fake.
 *
 *  `app.producer` is null when `KAFKA_BROKERS` is unset, and producing then does
 *  nothing: a 202 means only "passed the gate". Unlike the api-key cache this is
 *  NOT a fail-open optimisation on the hot path — when a producer IS configured,
 *  a failed publish is a 502, because an accepted signal is billable and must not
 *  be acknowledged unless it is durably on the topic. That judgement lives at the
 *  call site (signal.controller.ts); this file only owns the connection.
 *
 *  Credentials are NOT read here. In production (`KAFKA_USE_IAM`) the IAM token
 *  is signed from the AWS default chain (the instance role); locally the broker
 *  is PLAINTEXT and needs none. */
import { Kafka, type Producer, type SASLOptions } from 'kafkajs'
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js'
import fp from 'fastify-plugin'
import { config } from '../config.js'
import type { SignalMessage } from '../modules/signal/signal.schema.js'

/** The slice of a producer the edge needs — deliberately tiny, like the old
 *  `SignalQueue` port. The kafkajs producer satisfies it through the wrapper
 *  below; a test hands it an array. */
export interface SignalProducer {
  send(message: SignalMessage): Promise<void>
}

declare module 'fastify' {
  interface FastifyInstance {
    producer: SignalProducer | null
  }
}

/** MSK Serverless auth: SASL/OAUTHBEARER whose token is an IAM-signed blob, over
 *  TLS. The provider is called by kafkajs whenever it needs a fresh token. */
function iamSasl(): SASLOptions {
  return {
    mechanism: 'oauthbearer',
    oauthBearerProvider: async () => {
      const { token } = await generateAuthToken({ region: config.awsRegion })
      return { value: token }
    },
  }
}

export default fp(
  async (app) => {
    if (config.kafkaBrokers.length === 0) {
      app.log.warn('KAFKA_BROKERS is not set — signals will not be produced')
      app.decorate('producer', null)
      return
    }

    const kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      ...(config.kafkaUseIam ? { ssl: true, sasl: iamSasl() } : {}),
    })

    const producer: Producer = kafka.producer()
    await producer.connect()
    app.log.info(
      { brokers: config.kafkaBrokers, topic: config.kafkaTopic, iam: config.kafkaUseIam },
      'kafka connected',
    )

    app.decorate('producer', {
      async send(message: SignalMessage) {
        await producer.send({
          topic: config.kafkaTopic,
          // Key by customer so one customer's signals keep their order on a
          // single partition.
          messages: [{ key: message.body.customerId, value: JSON.stringify(message) }],
        })
      },
    })

    app.addHook('onClose', async () => {
      await producer.disconnect()
    })
  },
  { name: 'kafka' },
)
