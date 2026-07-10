import { it, describe } from 'node:test'
import assert from 'node:assert'

import server from './server'

describe('server.run()', () => {
  it('fails to initialize the server without database', { expectFailure: 'Error while initializing' }, async () => {
    await server.run()
  })
})