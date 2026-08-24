import { buildApp } from './app.js'
import { config } from './config.js'

const app = await buildApp()

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    app.log.info({ signal }, 'shutting down')
    app.close().then(
      () => process.exit(0),
      (err: unknown) => {
        app.log.error({ err }, 'error during shutdown')
        process.exit(1)
      },
    )
  })
}

try {
  await app.listen({ host: config.host, port: config.port })
} catch (err) {
  app.log.error({ err }, 'failed to start server')
  process.exit(1)
}
