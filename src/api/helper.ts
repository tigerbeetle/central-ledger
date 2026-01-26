import assert from "assert"
import { Ledger } from "src/domain/ledger-v2/Ledger"

/**
 * Helper function to resolve the Ledger saftely from Hapi context
 */
export const getLedger = (request: any): Ledger => {
  assert(request, 'request is undefined')
  assert(request.server.app, 'request.server.app is undefined')
  assert(request.server.app.ledger, 'Ledger not available in server app state')
  return request.server.app.ledger
}