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

import { FSPIOPError } from "@mojaloop/central-services-error-handling"
import { Ledger } from "src/domain/ledger-v2/Ledger"
import { CommandResult } from "src/domain/ledger-v2/types"

// TODO(LD): change to imports!
const Config = require('../../lib/config')
const Logger = require('../../shared/logger').logger
const ErrorHandler = require('@mojaloop/central-services-error-handling')
const rethrow = require('../../shared/rethrow')
const MLNumber = require('@mojaloop/ml-number')
const assert = require('assert')
const { randomUUID } = require('crypto')
const { convertBigIntToNumber } = require('../../shared/config/util')
const ParticipantService = require('../../domain/participant')

const getLedger = (request): Ledger => {
  assert(request, 'request is undefined')
  assert(request.server.app, 'request.server.app is undefined')
  assert(request.server.app.ledger, 'Ledger not available in server app state')
  return request.server.app.ledger
}

export interface IParticipantService {
  ensureExists(name: string): Promise<void>
  addEndpoint(name: string, payload: { type: string, value: string }): Promise<void>
  addEndpoints(name: string, endpoints: Array<{ type: string, value: string }>): Promise<Array<any>>
  getEndpoint(name: string, type: string): Promise<Array<{ name: string, value: string }>>
  getAllEndpoints(name: string): Promise<Array<{ name: string, value: string }>>
}

interface ParticipantAccount {
  createdBy: string
  createdDate: string | null
  currency: string
  id: string
  isActive: number
  ledgerAccountType: string
}

interface ParticipantResponse {
  name: string
  id: string
  created: Date | null
  isActive: number
  isProxy: number
  links: {
    self: string
  }
  accounts: ParticipantAccount[]
}

/**
 * @class ParticipantAPIHandlerV2
 * @description A refactored Participant API Handler, written in Typescript
 *   and using the new `Ledger` Interface.
 */
export default class ParticipantAPIHandlerV2 {
  private participantService: IParticipantService

  constructor() {
    this.participantService = ParticipantService
  }

  public async getAll(request): Promise<ParticipantResponse[]> {
    const ledger = getLedger(request)

    const resultDfsps = await ledger.getAllDfsps({})
    if (resultDfsps.type === 'FAILURE') {
      throw resultDfsps.error
    }

    const resultHub = await ledger.getHubAccounts({})
    if (resultHub.type === 'FAILURE') {
      throw resultHub.error
    }
    const hubLedgerAccounts = resultHub.accounts

    const reply = []

    // Map from Ledger format Dfsp to Existing API
    resultDfsps.result.dfsps.forEach(ledgerDfsp => {
      const apiMappedAccounts = ledgerDfsp.accounts.map(acc => ({
        createdBy: 'unknown',
        createdDate: null,
        currency: acc.currency,
        id: convertBigIntToNumber(acc.id),
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
      id: convertBigIntToNumber(acc.id),
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

  public async getByName(request): Promise<ParticipantResponse> {
    assert(request)
    assert(request.params)
    assert(request.params.name)

    const ledger = getLedger(request)
    return this._getByName(ledger, request.params.name)
  }

  private async _getByName(ledger: Ledger, dfspName: string): Promise<ParticipantResponse> {
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
      id: convertBigIntToNumber(acc.id),
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

  public async create(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.payload)
      assert(request.payload.currency)
      assert(request.payload.name)

      // Create the participant here with the participantService, then
      // do everything else in the ledger
      await this.participantService.ensureExists(request.payload.name)

      const { currency, name } = request.payload
      const ledger = getLedger(request)
      const createDfspResult = await ledger.createDfsp({
        dfspId: name,
        currencies: [currency]
      })

      if (createDfspResult.type === 'ALREADY_EXISTS') {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant currency has already been registered')
      }

      if (createDfspResult.type === 'FAILURE') {
        Logger.error(`participants.create() - failed to create: ${name} with error: ${createDfspResult.error.message}`)
        throw createDfspResult.error
      }

      const getByNameReply = await this._getByName(ledger, request.payload.name)
      return h.response(getByNameReply).code(201)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantCreate' })
    }
  }

  public async update(request): Promise<any> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.params.name !== 'Hub', 'Cannot update the Hub account.')
      assert(request.payload)
      assert(request.payload.isActive !== undefined)

      const { isActive } = request.payload
      assert.equal(typeof isActive, 'boolean')
      const ledger = getLedger(request)

      let response: CommandResult<void>
      if (isActive === false) {
        response = await ledger.disableDfsp({ dfspId: request.params.name })
      } else {
        response = await ledger.enableDfsp({ dfspId: request.params.name })
      }

      if (response.type === 'FAILURE') {
        throw response.error
      }

      // now look up the participant
      const getByNameReply = await this._getByName(ledger, request.params.name)
      return getByNameReply
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantCreate' })
    }
  }

  public async addEndpoint(request, h): Promise<unknown> {
    try {
      const participantName = request.params.name
      const payload = request.payload

      // Normalize to array (handle both single and array inputs) and use bulk method
      const endpoints = Array.isArray(payload) ? payload : [payload]
      await this.participantService.addEndpoints(participantName, endpoints)

      return h.response().code(201)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantAddEndpoint' })
    }
  }

  public async getEndpoint(request): Promise<any> {
    try {
      if (request.query.type) {
        const result = await this.participantService.getEndpoint(request.params.name, request.query.type)
        let endpoint = {}
        if (Array.isArray(result) && result.length > 0) {
          endpoint = {
            type: result[0].name,
            value: result[0].value
          }
        }
        return endpoint
      } else {
        const result = await this.participantService.getAllEndpoints(request.params.name)
        const endpoints = []
        if (Array.isArray(result) && result.length > 0) {
          result.forEach(item => {
            endpoints.push({
              type: item.name,
              value: item.value
            })
          })
        }
        return endpoints
      }
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantGetEndpoint' })
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
      assert(request.payload.limit.value >= 0)

      const ledger = getLedger(request)

      const depositCmd = {
        transferId: randomUUID(), // TODO: should be defined by the user in the API
        dfspId: request.params.name,
        currency: request.payload.currency,
        // Implicitly deposit funds here. In the new Ledger, you cannot have a limit without a
        // position
        amount: request.payload.limit.value,
        reason: 'Initial position with limit'
      }
      const depositResult = await ledger.deposit(depositCmd)
      if (depositResult.type === 'FAILURE') {
        throw depositResult.error
      }

      const setNetDebitCapResult = await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId: request.params.name,
        currency: request.payload.currency,
        amount: request.payload.limit.value
      })
      if (setNetDebitCapResult.type === 'FAILURE') {
        throw setNetDebitCapResult.error
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
        // check for fspiop error
        let maybeFspiopError = limitResponse.error as FSPIOPError
        // special case
        if (maybeFspiopError && maybeFspiopError.apiErrorCode &&
          maybeFspiopError.apiErrorCode.code &&
          maybeFspiopError.apiErrorCode.code === '3200'
        ) {
          return []
        }

        throw limitResponse.error
      }

      return [
        {
          currency: request.query.currency,
          limit: limitResponse.result
        }
      ]
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantGetLimits' })
    }
  }

  public async getLimitsForAllParticipants(request): Promise<any> {
    try {
      assert(request)
      assert(request.query)
      assert(request.query.currency)
      // Only limits of type NET_DEBIT_CAP are supported
      assert.equal(request.query.type, 'NET_DEBIT_CAP')

      const ledger = getLedger(request)
      const currency = request.query.currency


      // TODO(LD): Ideally we would implement this in the getAllDfsps() method itself
      // but for now we can stitch this together from a few other methods. The main goal here
      // is to maintain backwards compatibility while not making the surface area of the
      // Ledger interface unnessesarily large.

      const resultDfsps = await ledger.getAllDfsps({})
      if (resultDfsps.type === 'FAILURE') {
        throw resultDfsps.error
      }

      const dfspsWithCurrency = resultDfsps.result.dfsps.filter(dfsp =>
        dfsp.accounts.some(account => account.currency === currency)
      )

      const limitResponses = await Promise.all(
        dfspsWithCurrency.map(async (dfsp) => {
          const limitResponse = await ledger.getNetDebitCap({
            dfspId: dfsp.name,
            currency
          })
          return { dfsp, limitResponse }
        })
      )

      const limits = []
      for (const { dfsp, limitResponse } of limitResponses) {
        if (limitResponse.type === 'SUCCESS') {
          limits.push({
            name: dfsp.name,
            currency,
            limit: limitResponse.result
          })
        } else {
          // Check if this is the "no limit set" error (3200)
          let maybeFspiopError = limitResponse.error as FSPIOPError
          if (maybeFspiopError && maybeFspiopError.apiErrorCode &&
            maybeFspiopError.apiErrorCode.code &&
            maybeFspiopError.apiErrorCode.code === '3200'
          ) {
            // No limit set for this participant, skip
            continue
          }
          // For any other error, fail the entire operation
          throw limitResponse.error
        }
      }

      return limits
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantGetLimitsForAllParticipants' })
    }
  }

  public async adjustLimits(request, h): Promise<unknown> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)
      assert(request.payload)
      assert(request.payload.currency)
      assert(request.payload.limit)
      assert(request.payload.limit.type)
      assert(request.payload.limit.value !== undefined)
      assert(request.payload.limit.value >= 0)
      // Only limits of type NET_DEBIT_CAP are supported
      assert.equal(request.payload.limit.type, 'NET_DEBIT_CAP')
      const ledger = getLedger(request)

      const result = await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId: request.params.name,
        currency: request.payload.currency,
        amount: request.payload.limit.value
      })

      if (result.type === 'FAILURE') {
        throw result.error
      }

      // The Ledger doesn't return anything, but the API Expects a response body
      const updatedLimit = {
        currency: request.payload.currency,
        limit: {
          type: request.payload.limit.type,
          value: request.payload.limit.value
        }
      }
      return h.response(updatedLimit).code(200)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'adjustLimits' })
    }
  }

  public async createHubAccount(request, h): Promise<unknown> {
    try {
      const ledger = getLedger(request)
      const currency = request.payload.currency
      const type = request.payload.type

      Logger.warn(`createHubAccount: ignoring type parameter '${type}' - ledger.createHubAccount() creates all hub accounts for the currency, not individual account types`)

      // Create a default settlement model for the currency
      const settlementModel = {
        name: `DEFERRED_MULTILATERAL_NET_${currency}`,
        settlementGranularity: "NET",
        settlementInterchange: "MULTILATERAL",
        settlementDelay: "DEFERRED",
        currency,
        requireLiquidityCheck: true,
        ledgerAccountType: "POSITION",
        settlementAccountType: "SETTLEMENT",
        autoPositionReset: true
      }

      const result = await ledger.createHubAccount({
        currency,
        settlementModel
      })

      if (result.type === 'FAILURE') {
        throw result.error
      }

      // Return the Hub participant with all its accounts
      const hubParticipant = await this.getAll({ query: {}, payload: {}, server: request.server })
      const hub = hubParticipant.find(p => p.name === 'Hub')

      return h.response(hub).code(201)
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantCreateHubAccount' })
    }
  }

  public async getPositions(request): Promise<any> {
    try {
      assert(request)
      assert(request.params)
      assert(request.params.name)

      const name = request.params.name
      const currency = request.query?.currency
      const ledger = getLedger(request)

      let ledgerAccountsResponse
      if (currency) {
        // Get accounts for specific currency
        ledgerAccountsResponse = await ledger.getDfspAccounts({ dfspId: name, currency })
      } else {
        // Get accounts for all currencies
        ledgerAccountsResponse = await ledger.getAllDfspAccounts({ dfspId: name })
      }

      if (ledgerAccountsResponse.type === 'FAILURE') {
        throw ledgerAccountsResponse.error
      }

      // Filter for POSITION accounts only
      const positionAccounts = ledgerAccountsResponse.accounts.filter(
        acc => acc.ledgerAccountType === 'POSITION'
      )

      // Map to the expected position format
      const positions = positionAccounts.map(acc => ({
        currency: acc.currency,
        value: acc.value,
        changedDate: acc.changedDate
      }))

      // If currency was specified, return single position object
      // Otherwise return array of positions
      if (currency) {
        return positions.length > 0 ? positions[0] : null
      }

      return positions
    } catch (err) {
      rethrow.rethrowAndCountFspiopError(err, { operation: 'participantGetPositions' })
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
      Logger.error(`getAccounts() - failed with error: ${ledgerAccountsResponse.error.message}`)
      throw ledgerAccountsResponse.error
    }

    // Map to legacy compatible API response
    return ledgerAccountsResponse.accounts.map(acc => {
      return {
        ...acc,
        id: convertBigIntToNumber(acc.id),
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
        throw result.error
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
          assert(request.payload.reason, 'reason is required')
          const transferId = request.payload.transferId

          const depositCmd = {
            transferId,
            dfspId: name,
            currency: amount.currency,
            amount: new MLNumber(amount.amount).toNumber(),
            reason: request.payload.reason
          }

          const result = await ledger.deposit(depositCmd)

          if (result.type === 'FAILURE') {
            // Special case - deactivated participant
            if (result.error.message === 'Participant is currently set inactive') {
              return h.response(result.error).code(400)
            }
            throw result.error
          }

          if (result.type === 'ALREADY_EXISTS') {
            // Just log the warning, the previous implementation did this check async, so we never
            // returned an error
            Logger.warn('recordFunds() failed silently with ALREADY_EXISTS error')
          }
          return h.response().code(202)
        }
        case 'recordFundsOutPrepareReserve': {
          assert(request.payload.transferId)
          assert(amount, 'amount is required')
          assert(amount.amount, 'amount.amount is required')
          assert(amount.currency, 'amount.currency is required')
          assert(request.payload.reason, 'reason is required')

          const transferId = request.payload.transferId

          const withdrawPrepareCmd = {
            transferId,
            dfspId: name,
            currency: amount.currency,
            amount: new MLNumber(amount.amount).toNumber(),
            reason: request.payload.reason
          }

          const result = await ledger.withdrawPrepare(withdrawPrepareCmd)

          if (result.type === 'FAILURE') {
            throw result.error
          }

          if (result.type === 'INSUFFICIENT_FUNDS') {
            // Just log the warning, the previous implementation did this check async, so we never
            // returned an error
            Logger.warn('recordFunds() failed silently with INSUFFICENT_FUNDS error')
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
          const transferId = request.params.transferId
          const withdrawAbortCmd = {
            transferId
          }
          const result = await ledger.withdrawAbort(withdrawAbortCmd)

          if (result.type === 'FAILURE') {
            throw result.error
          }
          return h.response().code(202)
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