import HandlerSettlementV2 from "./handler-v2"

const HapiOpenAPI = require('hapi-openapi')
const Path = require('path')

const buildRoutes = (handler: HandlerSettlementV2) => {
  return {
    plugin: HapiOpenAPI,
    options: {
      api: Path.join(__dirname, '../settlement/interface/swagger.json'),
      handlers: {
        health: {
          get: notImplemented('GET /health')
        },
        settlementWindows: {
          get: handler.getSettlementWindowsByParams.bind(handler),
          '{id}': {
            get: notImplemented('GET /settlementWindows/{id}'),
            post: notImplemented('POST /settlementWindows/{id}')
          }
        },
        settlements: {
          get: handler.getSettlementByParams.bind(handler),
          post: handler.createSettlementEvent.bind(handler),
          '{id}': {
            get: notImplemented('GET /settlements/{id}'),
            put: notImplemented('PUT /settlements/{id}')
          },
          '{sid}': {
            participants: {
              '{pid}': {
                get: notImplemented('GET /settlements/{sid}/participants/{pid}'),
                put: notImplemented('PUT /settlements/{sid}/participants/{pid}'),
                accounts: {
                  '{aid}': {
                    get: notImplemented('GET /settlements/{sid}/participants/{pid}/accounts/{aid}'),
                    put: notImplemented('PUT /settlements/{sid}/participants/{pid}/accounts/{aid}')
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

const notImplemented = (route: string) => (_req: any, h: any) => {
  return h.response({ error: `${route} not implemented in handler-v2` }).code(501)
}

export default buildRoutes
