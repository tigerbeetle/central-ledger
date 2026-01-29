/*****
 License
 --------------
 Copyright © 2020-2025 Mojaloop Foundation
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

 * ModusBox
 - Deon Botha <deon.botha@modusbox.com>
 - Georgi Georgiev <georgi.georgiev@modusbox.com>
 - Miguel de Barros <miguel.debarros@modusbox.com>
 - Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 - Valentin Genev <valentin.genev@modusbox.com>
 --------------
 ******/

import * as ErrorHandler from '@mojaloop/central-services-error-handling'
import * as EventSdk from '@mojaloop/event-sdk'
import type { Request, ResponseToolkit, ResponseObject } from '@hapi/hapi'
import { Util, Enum } from '@mojaloop/central-services-shared'
import { logger } from '../../../../shared/logger'
import { getLedger, mapSettlementState } from '../../../../api/helper'
import { SettlementAbortCommand, SettlementUpdateCommand } from 'src/domain/ledger-v2/types'
import assert from 'assert'


const Settlements = require('../../../domain/settlement/index')

interface SettlementGetRequest extends Request {
  params: {
    id: number
  }
}

interface SettlementUpdatePayload {
  participants?: Array<{
    id: number
    accounts: Array<{
      id: number
      state: string
      reason: string
      externalReference: string
    }>
  }>
  state?: string
  reason?: string
  externalReference?: string
}

interface SettlementPutRequest extends Request {
  params: {
    id: number
  }
  payload: SettlementUpdatePayload
}

/**
 * summary: Returns Settlement(s) as per parameters/filter criteria.
 * description:
 * parameters: id
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function get(
  request: SettlementGetRequest,
  h: ResponseToolkit
): Promise<ResponseObject> {
  const settlementId = request.params.id
  try {
    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT,
      Enum.Events.Event.Action.GET,
      `sid=${settlementId}`,
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit({
      headers: request.headers,
      params: request.params
    }, EventSdk.AuditEventAction.start)

    const Enums = await (request.server.methods as any).enums('settlementStates')
    request.server.log('info', `get settlement by Id requested with id ${settlementId}`)
    const settlementResult = await Settlements.getById({ settlementId }, Enums)
    return h.response(settlementResult)
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * summary: Acknowledgement of settlement by updating with Settlements Id.
 * description:
 * parameters: id, settlementUpdatePayload
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function put(
  request: SettlementPutRequest
): Promise<ResponseObject> {
  try {
    assert(request)
    const ledger = getLedger(request)
    assert(request.params)
    assert(request.params.id)
    assert(request.payload)

    // shortcut
    const id = request.params.id
    const payload = request.payload

    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT,
      Enum.Events.Event.Action.PUT,
      `sid=${id}`,
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit(payload, EventSdk.AuditEventAction.start)

    let updateType: 'ABORT' | 'SETTLE'
    if (payload.participants && (payload.state || payload.reason || payload.externalReference)) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
        'No other properties are allowed when participants is provided'
      )
    }

    if (payload.participants) {
      updateType = 'SETTLE'
    } else if (payload.state) {
      if (payload.state !== 'ABORTED') {
        const error = ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
          'Invalid state value - only ABORTED is supported'
        )
        logger.error(error)
        throw error
      }

      if (!payload.reason) {
        const error = ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.MISSING_ELEMENT, 'State and reason are mandatory'
        )
        logger.error(error)
        throw error
      }

      updateType = 'ABORT'
    } else {
      const error = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Invalid request payload input'
      )
      logger.error(error)
      throw error
    }

    // Execute based on update type
    switch (updateType) {
      case 'ABORT': {
        assert(payload.reason)
        const settlementUpdateResponse = await ledger.settlementAbort({ id, reason: payload.reason })
        if (settlementUpdateResponse.type === 'FAILURE') {
          return ErrorHandler.Factory.reformatFSPIOPError(settlementUpdateResponse.error) as any
        }

        return true as any
      }
      case 'SETTLE': {
        // Flatten participants and accounts into updates array
        const updates: SettlementUpdateCommand['updates'] = []
        for (const participant of payload.participants) {
          for (const account of participant.accounts) {
            assert(participant.id)
            assert(account.id)
            assert(account.state)
            assert(account.reason)
            assert(account.externalReference)
            updates.push({
              participantId: participant.id,
              accountId: account.id,
              participantState: mapSettlementState(account.state),
              reason: account.reason,
              externalReference: account.externalReference
            })
          }
        }

        const cmd: SettlementUpdateCommand = {
          id,
          updates
        }
        const settlementUpdateResponse = await ledger.settlementUpdate(cmd)
        if (settlementUpdateResponse.type === 'FAILURE') {
          return ErrorHandler.Factory.reformatFSPIOPError(settlementUpdateResponse.error) as any
        }

        return true as any

        // Old implementation
        // const Enums = {
        //   ledgerAccountTypes: await (request.server.methods as any).enums('ledgerAccountTypes'),
        //   ledgerEntryTypes: await (request.server.methods as any).enums('ledgerEntryTypes'),
        //   participantLimitTypes: await (request.server.methods as any).enums('participantLimitTypes'),
        //   settlementStates: await (request.server.methods as any).enums('settlementStates'),
        //   settlementWindowStates: await (request.server.methods as any).enums('settlementWindowStates'),
        //   transferParticipantRoleTypes: await (request.server.methods as any).enums('transferParticipantRoleTypes'),
        //   transferStates: await (request.server.methods as any).enums('transferStates'),
        //   transferStateEnums: await (request.server.methods as any).enums('transferStateEnums')
        // }
        // return await Settlements.putById(settlementId, request.payload, Enums)
      }
    }
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * Operations on /settlements/{id}
 */
module.exports = {
  get,
  put
}
