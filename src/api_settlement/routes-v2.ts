import assert from "node:assert"
import HandlerSettlementV2 from "./handler-v2"

const HapiOpenAPI = require('hapi-openapi')
const Path = require('path')

const buildRoutes = (
  handlerSettlement: HandlerSettlementV2
) => {
  assert(handlerSettlement)

  return {
    options: {
      api: Path.join(__dirname, '../interface/swagger.json'),
      handlers: handlerSettlement
    }
  }
}

export default buildRoutes
