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

 * Shashikant Hirugade <shashikant.hirugade@modusbox.com>
 --------------
 ******/

'use strict'

import { Ledger } from "src/domain/ledger-v2/Ledger"

const Config = require('../../lib/config')
const Logger = require('../../shared/logger').logger
const ErrorHandler = require('@mojaloop/central-services-error-handling')
const rethrow = require('../../shared/rethrow')
const MLNumber = require('@mojaloop/ml-number')
const assert = require('assert')
const { randomUUID } = require('crypto')
const { assertString, safeStringToNumber } = require('../../shared/config/util')

const getLedger = (request) => {
  assert(request, 'request is undefined')
  assert(request.server.app, 'request.server.app is undefined')
  assert(request.server.app.ledger, 'Ledger not available in server app state')
  return request.server.app.ledger
}

/**
 * @class ParticipantAPIHandlerV2
 * @description A refactored Participant API Handler, written in Typescript
 *   and using the new `Ledger` Interface.
 */
export default class ParticipantAPIHandlerV2 {

  public async create(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.payload)
      assert(request.payload.currency)
      assert(request.payload.name)

      // startingDeposit allows us to create an opening balance for the Dfsp while onboarding.
      // We added this feature to the Admin API to help limit the breaking changes when moving to
      // TigerBeetle, as the TigerBeetleLedger doesn't allow `initialPositionAndLimits` to be 
      // called _before_ funds have been deposited for the Dfsp.
      let startingDeposit = 0
      if (request.payload.startingDeposit) {
        assertString(request.payload.startingDeposit)
        const startingDepositStr = request.payload.startingDeposit
        startingDeposit = safeStringToNumber(startingDepositStr)
        assert(startingDeposit >= 0)
      }

      const { currency, name } = request.payload
      const ledger = getLedger(request)
      const createDfspResult = await ledger.createDfsp({
        dfspId: name,
        currencies: [currency],
        startingDeposits: [startingDeposit]
      })

      if (createDfspResult.type === 'ALREADY_EXISTS') {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant currency has already been registered')
      }

      if (createDfspResult.type === 'FAILED') {
        Logger.error(`participants.create() - failed to create: ${name} with error: ${createDfspResult.error.message}`)
        throw createDfspResult.error
      }

      // now look up the participant
      const getByNameReply = await this.getByName(ledger, request.payload.name)
      return h.response(getByNameReply).code(201)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantCreate' })
    }
  }

  public async getAll(request): Promise<object> {

    const ledger = getLedger(request)

    const resultDfsps = await ledger.getAllDfsps({})
    if (resultDfsps.type === 'FAILURE') {
      throw resultDfsps.fspiopError
    }

    const resultHub = await ledger.getHubAccounts()
    if (resultHub.type === 'FAILURE') {
      throw resultHub.fspiopError
    }
    const hubLedgerAccounts = resultHub.accounts

    const reply = []

    // Map from Ledger format Dfsp to Existing API
    resultDfsps.result.dfsps.forEach(ledgerDfsp => {
      const apiMappedAccounts = ledgerDfsp.accounts.map(acc => ({
        createdBy: 'unknown',
        createdDate: null,
        currency: acc.currency,
        id: acc.id.toString(),
        isActive: acc.isActive ? 1 : 0,
        ledgerAccountType: acc.ledgerAccountType
      }))

      const url = `${Config.HOSTNAME}/participants/${ledgerDfsp.name}`
      reply.push({
        name: ledgerDfsp.name,
        id: url,
        // created: ledgerDfsp.created,
        created: null,
        isActive: ledgerDfsp.isActive ? 1 : 0,
        // TODO(LD): hardcoded for now
        isProxy: 0,
        links: {
          self: url
        },
        accounts: apiMappedAccounts
      })
    })

    // Now do the same for the Hub accounts
    const hubUrl = `${Config.HOSTNAME}/participants/Hub`
    const hubAccounts = hubLedgerAccounts.map(acc => ({
      createdBy: 'unknown',
      createdDate: null,
      currency: acc.currency,
      id: acc.id.toString(),
      isActive: acc.isActive ? 1 : 0,
      ledgerAccountType: acc.ledgerAccountType
    }))
    reply.push({
      name: 'Hub',
      id: hubUrl,
      // TODO(LD): this could be simply when the first account was created
      created: new Date(),
      // TODO(LD): Load from some hub account metadata?
      isActive: 1,
      // TODO(LD): hardcoded for now
      isProxy: 0,
      links: {
        self: hubUrl
      },
      accounts: hubAccounts
    })

    return reply
  }

  private async getByName(ledger: Ledger, dfspName: string): Promise<unknown> {
    assert(ledger)
    assert(dfspName)

    const resultGetDfsp = await ledger.getDfsp({ dfspId: dfspName })
    if (resultGetDfsp.type === 'FAILURE') {
      throw resultGetDfsp.error
    }

    // Map from Ledger format Dfsp to Existing API
    const ledgerDfsp = resultGetDfsp.result
    const apiMappedAccounts = ledgerDfsp.accounts.map(acc => ({
      createdBy: 'unknown',
      createdDate: null,
      currency: acc.currency,
      id: acc.id.toString(),
      isActive: acc.isActive ? 1 : 0,
      ledgerAccountType: acc.ledgerAccountType
    }))

    const url = `${Config.HOSTNAME}/participants/${ledgerDfsp.name}`
    return {
      name: ledgerDfsp.name,
      id: url,
      // created: ledgerDfsp.created,
      created: null,
      isActive: ledgerDfsp.isActive ? 1 : 0,
      // TODO(LD): hardcoded for now
      isProxy: 0,
      links: {
        self: url
      },
      accounts: apiMappedAccounts
    }
  }

  public async update(request): Promise<any> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.payload)
      assert(request.payload.isActive !== undefined)

      const { isActive } = request.payload
      assert.equal(typeof isActive, 'boolean')
      const ledger = getLedger(request)

      let response
      if (isActive === false) {
        response = await ledger.disableDfsp({ dfspId: request.params.name })
      } else {
        response = await ledger.enableDfsp({ dfspId: request.params.name })
      }

      if (response.type === 'FAILURE') {
        throw response.fspiopError
      }

      // now look up the participant
      const getByNameReply = await this.getByName(ledger, request.params.name)
      return getByNameReply
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantCreate' })
    }
  }

  /**
   * Unfortunately the API is mismatched here between request.payload.limit.value (a number)
   * and what we have in deposit. So recordFundsV2 must adapt from a string formatted
   * amount to a real number
   */
  public async addLimitAndInitialPosition(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.payload)
      assert(request.payload.currency)
      assert(request.payload.limit)
      assert(request.payload.limit.type)
      assert(request.payload.limit.value !== undefined)

      const ledger = getLedger(request)

      const depositCmd = {
        transferId: randomUUID(), // TODO: should be defined by the user in the API
        dfspId: request.params.name,
        currency: request.payload.currency,
        // Implicitly deposit funds here. In the new Ledger, you cannot have a limit without a
        // position
        amount: request.payload.limit.value
      }
      const result = await ledger.deposit(depositCmd)
      if (result.type === 'FAILURE') {
        throw result.error
      }
      if (result.type === 'ALREADY_EXISTS') {
        return
      }

      return h.response().code(201)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantAddLimitAndInitialPosition' })
    }
  }

  public async getLimits(request): Promise<any> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.query)
      assert(request.query.currency)
      // Only limits of type NET_DEBIT_CAP are supported
      assert.equal(request.query.type, 'NET_DEBIT_CAP')

      const ledger = getLedger(request)
      const limitResponse = await ledger.getNetDebitCap({
        dfspId: request.params.name,
        currency: request.query.currency
      })

      if (limitResponse.type !== 'SUCCESS') {
        // special case 
        if (limitResponse.fspiopError.apiErrorCode &&
          limitResponse.fspiopError.apiErrorCode.code &&
          limitResponse.fspiopError.apiErrorCode.code === '3200'
        ) {
          return []
        }

        throw limitResponse.fspiopError
      }

      return [
        {
          currency: request.query.currency,
          limit: limitResponse.limit
        }
      ]
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantGetLimits' })
    }
  }

  public async getAccounts(request): Promise<any> {
    assert(request)
    assert(request.params)
    assert(request.params.name)
    assert(request.query)
    assert(request.query.currency)

    const name = request.params.name
    const currency = request.query.currency
    const ledger = getLedger(request)
    const ledgerAccountsResponse = await ledger.getDfspAccounts({ dfspId: name, currency })

    if (ledgerAccountsResponse.type === 'FAILURE') {
      Logger.error(`getAccounts() - failed with error: ${ledgerAccountsResponse.fspiopError.message}`)
      throw ledgerAccountsResponse.fspiopError
    }

    // Map to legacy compatible API response
    return ledgerAccountsResponse.accounts.map(acc => {
      return {
        ...acc,
        id: acc.id.toString(),
        isActive: acc.isActive ? 1 : 0,
      }
    })
  }

  public async updateAccount(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.params.id)
      assert(request.payload)
      assert(request.payload.isActive !== undefined)

      const ledger = getLedger(request)
      const { name, id } = request.params
      const { isActive } = request.payload

      let result
      if (isActive) {
        result = await ledger.enableDfspAccount({ dfspId: name, accountId: id })
      } else {
        result = await ledger.disableDfspAccount({ dfspId: name, accountId: id })
      }

      if (result.type === 'FAILURE') {
        throw result.fspiopError
      }

      return h.response().code(200)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantUpdateAccount' })
    }
  }

  public async recordFunds(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.payload)
      assert(request.payload.action)

      const ledger = getLedger(request)
      const { name } = request.params
      const { action, amount } = request.payload

      switch (action) {
        case 'recordFundsIn': {
          assert(request.payload.transferId)
          assert(amount, 'amount is required')
          assert(amount.amount, 'amount.amount is required')
          assert(amount.currency, 'amount.currency is required')
          const transferId = request.payload.transferId

          const depositCmd = {
            transferId,
            dfspId: name,
            currency: amount.currency,
            amount: new MLNumber(amount.amount).toNumber(),
          }

          const result = await ledger.deposit(depositCmd)

          if (result.type === 'FAILURE') {
            throw result.error
          }

          if (result.type === 'ALREADY_EXISTS') {
            throw ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR,
              'Transfer with this ID already exists'
            )
          }
          return h.response().code(202)
        }
        case 'recordFundsOutPrepareReserve': {
          assert(request.payload.transferId)
          assert(amount, 'amount is required')
          assert(amount.amount, 'amount.amount is required')
          assert(amount.currency, 'amount.currency is required')

          const transferId = request.payload.transferId

          const withdrawPrepareCmd = {
            transferId,
            dfspId: name,
            currency: amount.currency,
            amount: new MLNumber(amount.amount).toNumber(),
          }

          const result = await ledger.withdrawPrepare(withdrawPrepareCmd)

          if (result.type === 'FAILURE') {
            throw result.error
          }

          if (result.type === 'INSUFFICIENT_FUNDS') {
            throw ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.PAYER_FSP_INSUFFICIENT_LIQUIDITY,
              `Insufficient funds for withdrawal. Available: ${result.availableBalance}, Requested: ${result.requestedAmount}`
            )
          }

          return h.response().code(202)
        }
        case 'recordFundsOutCommit': {
          const transferId = request.params.transferId
          const withdrawCommitCmd = {
            transferId
          }
          const result = await ledger.withdrawCommit(withdrawCommitCmd)

          if (result.type === 'FAILURE') {
            throw result.error
          }
          return h.response().code(202)
        }
        case 'recordFundsOutAbort': {
          throw new Error('not implemented!!!')
        }
        default: {
          throw new Error(`recordFunds() - unhandled action: ${action}`)
        }
      }
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantRecordFunds' })
    }
  }
}