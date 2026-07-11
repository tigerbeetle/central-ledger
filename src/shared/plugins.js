/*****
 License
 --------------
 Copyright © 2020-2024 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Mojaloop Foundation
 - Name Surname <name.surname@mojaloop.io>

 * Infitx
 - Vijay Kumar Guthi <vijaya.guthi@infitx.com>
 - Kevin Leyow <kevin.leyow@infitx.com>
 - Kalin Krustev <kalin.krustev@infitx.com>
 - Steven Oderayi <steven.oderayi@infitx.com>
 - Eugen Klymniuk <eugen.klymniuk@infitx.com>

 * ModusBox
 - Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 --------------

 ******/

'use strict'

const Path = require('path')
const assert = require('assert')
const fs = require('fs')
const Inert = require('@hapi/inert')
const Vision = require('@hapi/vision')
const Blipp = require('blipp')
const ErrorHandling = require('@mojaloop/central-services-error-handling')
const { APIDocumentation, loggingPlugin, HapiEventPlugin } = require('@mojaloop/central-services-shared').Util.Hapi
const Config = require('../lib/config')
const { logger } = require('./logger')

const myDocsPlugin = {
  name: 'apiDocumentation2',
  register: (server, options) => {
    assert(options.pathToSwaggerFile, 'Expected `options.pathToSwaggerFile`.')

    // Check the file exists and parses.
    try {
      const file = fs.readFileSync(options.pathToSwaggerFile)
      JSON.parse(file)
    } catch (err) {
      const errorMessage = `documentation - failed to read pathToSwaggerFile with error: ${err.message}`
      logger.error(errorMessage)
      throw new Error(errorMessage)
    }

    const page = `<!DOCTYPE html>
    <html>
    <head><title>API Docs</title></head>
    <body>
      <script id="api-reference" data-url="/swagger2.json"></script>
      <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
    </body>
    </html>`

    server.route([
      {
        method: 'GET',
        path: '/swagger2.json',
        options: {
          tags: ['api', 'documentation'],
          handler: (request, h) => {
            const file = fs.readFileSync(options.pathToSwaggerFile)
            return h.response(file)
          },
          plugins: {
            apiDocumentation: false
          }
        }
      },
      {
        method: 'GET',
        path: '/documentation2',
        options: {
          tags: ['api', 'documentation'],
          handler: (_request, h) => {
            return h.response(page)
              .type('text/html');
          },
          plugins: {
            apiDocumentation: false
          }
        }
      }
    ])
  }
}

const registerPlugins = async (server) => {
  await server.register({
    plugin: myDocsPlugin,
    options: {
      pathToSwaggerFile: Path.resolve(process.cwd(), 'src/api/interface/swagger.json')
    }
  })

  if (Config.API_DOC_ENDPOINTS_ENABLED) {
    await server.register({
      plugin: APIDocumentation,
      options: {
        documentPath: Path.resolve(process.cwd(), 'src/api/interface/swagger.json')
      }
    })
  }

  await server.register({
    plugin: require('@hapi/good'),
    options: {
      ops: {
        interval: 10000
      }
    }
  })

  await server.register({
    plugin: require('@hapi/basic')
  })

  await server.register({
    plugin: require('@now-ims/hapi-now-auth')
  })

  await server.register({
    plugin: require('hapi-auth-bearer-token')
  })

  await server.register([Inert, Vision, Blipp, ErrorHandling, HapiEventPlugin])

  await server.register({
    plugin: loggingPlugin,
    options: { log: logger }
  })
}

module.exports = {
  registerPlugins
}
