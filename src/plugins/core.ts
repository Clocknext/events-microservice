/** App-wide plugins. `fastify-plugin` keeps them out of an encapsulation context
 *  so everything registered afterwards can see them. */
import cors from '@fastify/cors'
import sensible from '@fastify/sensible'
import fp from 'fastify-plugin'

export default fp(
  async (app) => {
    await app.register(sensible)
    await app.register(cors, { origin: true })
  },
  { name: 'core' },
)
