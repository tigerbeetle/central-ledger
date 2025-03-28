
const ErrorHandler = require('@mojaloop/central-services-error-handling')

const ledger = require('./ledger')
const assert = require('assert')
const { logger } = require('../../shared/logger')


/**
 * TigerBeetle replacements/shims for Admin API
 */

const addLimitAndInitialPosition = async (participantName, limitAndInitialPositionObj) => {
  const log = logger.child({ participantName, limitAndInitialPositionObj })
  try {
    const fspId = participantName
    const currency = limitAndInitialPositionObj.currency
    assert(fspId)
    assert(currency)

    await ledger.onboardDfsp(fspId, currency)
  } catch (err) {
    log.error('error adding limit and initial position', err)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}


module.exports = {
  addLimitAndInitialPosition
}