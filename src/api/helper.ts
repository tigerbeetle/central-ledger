import assert from "assert"
import * as ErrorHandler from '@mojaloop/central-services-error-handling'
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

/**
 * Maps legacy settlement state format to new Ledger interface format
 */
export const mapSettlementState = (state: string): 'RECORDED' | 'RESERVED' | 'COMMITTED' | 'SETTLED' => {
  switch (state) {
    case 'PS_TRANSFERS_RECORDED': return 'RECORDED'
    case 'PS_TRANSFERS_RESERVED': return 'RESERVED'
    case 'PS_TRANSFERS_COMMITTED': return 'COMMITTED'
    case 'SETTLED': return 'SETTLED'
    default: {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
        `Unexpected account.state: ${state}. Expected [PS_TRANSFERS_RECORDED | PS_TRANSFERS_RESERVED | PS_TRANSFERS_COMMITTED | SETTLED]`
      )
    }
  }
}