

const ErrorHandler = require('@mojaloop/central-services-error-handling')
const ParticipantModel = require('../../models/participant/participantCached')

const ledger = require('./ledger')
const assert = require('assert')
const { logger } = require('../../shared/logger')
const { MLNumber } = require('@mojaloop/ml-number/src/mlnumber')


/**
 * TigerBeetle replacements/shims for Admin API
 */


// I'm not sure if the limit really makes sense here, since I'd rather we simply deposit the
// collateral first, and then specifiy the limit/reserverd portion
const addLimitAndInitialPosition = async (participantName, limitAndInitialPositionObj) => {
  const log = logger.child({ participantName, limitAndInitialPositionObj })
  try {
    const fspId = participantName
    const currency = limitAndInitialPositionObj.currency
    assert(fspId)
    assert(currency)

    await ledger.onboardDfsp(fspId, currency)
  } catch (err) {
    // TODO: catch the `UNIQUE constraint failed: accounts.fspId, accounts.currency, accounts.accountType -        {"context":"CSSh"}`
    // error?

    log.error('error adding limit and initial position', err)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

const recordFundsInOut = async (payload, params, enums) => {
  const log = logger.child({ payload, params, enums })
  log.debug('fast/admin - recording funds in/out')

  try {
    const transferId = payload.transferId
    const { name, id } = params
    assert(name)
    // AccountId doesn't really have any meaning in this context, since we lookup accounts
    // based on the dfspid + currency + account type
    assert(id)
    assert(transferId)
    const amountStr = payload.amount.amount
    
    // TODO: add BigInt function to MLNumber
    const amount = BigInt((new MLNumber(amountStr)).toNumber())
    assert(amount)

    const currency = payload.amount.currency
    assert(currency)

    const participant = await ParticipantModel.getByName(name)
    assert(participant, 'Participant does not exist')
    assert(participant.isActive, true, 'Participant is currently set inactive')

    if (payload.action !== 'recordFundsIn') {
      throw new Error('Not yet implemented - New Ledger currently only supports `action: recordFundsIn`')
    }

    await ledger.recordCollateralDeposit(name, amount, currency)
    // also make available for clearing, since the Mojaloop API is kinda backwards
    // we _could_ base this on the limit from the current limit of the DFSP to maintain compatibilty
    await ledger.makeFundsAvailableForClearing(name, amount, currency)
    
  } catch (err) {
    log.error('error recording funds in/out', err)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

const getAccounts = async (name) => {
  const fspId = name
  const accounts = await ledger.getAccountsForFspId(fspId)

  // TODO(LD): transform to look like old api - for now I'm not too worried
  // just need a way to see what the accounts look like

  return accounts
}


module.exports = {
  addLimitAndInitialPosition,
  getAccounts,
  recordFundsInOut
}