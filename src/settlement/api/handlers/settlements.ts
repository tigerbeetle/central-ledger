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
import { getLedger } from '../../../api/helper'
import { GetSettlementQuery, GetSettlementsQuery, SettlementPrepareCommand, SettlementState } from '../../../domain/ledger-v2/types'
import assert from 'assert'

const Settlements = require('../../domain/settlement/index')

interface SettlementsGetRequest extends Request {
  query: {
    currency?: string
    participantId?: string
    settlementWindowId?: string
    accountId?: string
    state?: string
    fromDateTime?: string
    toDateTime?: string
  }
}

interface SettlementEventPayload {
  settlementWindows: Array<{
    id: number
  }>
  reason: string
  settlementModel: string
}

interface SettlementsPostRequest extends Request {
  payload: SettlementEventPayload
}

/**
 * summary: Returns Settlement(s) as per parameter(s).
 * description:
 * parameters: currency, participantId, settlementWindowId, accountId, state, fromDateTime, toDateTime
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function get(
  request: SettlementsGetRequest,
  h: ResponseToolkit
): Promise<ResponseObject> {
  try {
    assert(request)
    assert(request.query)
    const ledger = getLedger(request)

    // Validate that at least one query parameter has a truthy value
    const query = request.query
    const hasValidFilter = Object.keys(query).some(key => {
      const value = query[key as keyof typeof query]
      return !!value
    })

    if (!hasValidFilter) {
      const error = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
        'At least one valid filter parameter is required (currency, participantId, settlementWindowId, state, fromDateTime, or toDateTime)'
      )
      throw error
    }

    // Helper to parse and validate date strings
    const parseDate = (dateStr: string, fieldName: string): Date => {
      const date = new Date(dateStr)
      if (isNaN(date.getTime())) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
          `Invalid ${fieldName}: ${dateStr}`
        )
      }
      return date
    }

    // Helper to validate settlement state
    const parseState = (stateStr: string): SettlementState => {
      const validStates: SettlementState[] = ['PENDING', 'PROCESSING', 'COMMITTED', 'ABORTED']
      if (!validStates.includes(stateStr as SettlementState)) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
          `Invalid state: ${stateStr}. Must be one of: ${validStates.join(', ')}`
        )
      }
      return stateStr as SettlementState
    }

    // Build query object with validation
    const getSettlementsQuery: GetSettlementsQuery = {
      ...(query.currency && { currency: query.currency }),
      ...(query.participantId && { participantId: parseInt(query.participantId) }),
      ...(query.settlementWindowId && { settlementWindowId: parseInt(query.settlementWindowId) }),
      ...(query.state && { state: parseState(query.state) }),
      ...(query.fromDateTime && { fromDateTime: parseDate(query.fromDateTime, 'fromDateTime') }),
      ...(query.toDateTime && { toDateTime: parseDate(query.toDateTime, 'toDateTime') }),
    }

    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT,
      Enum.Events.Event.Action.GET,
      undefined,
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit({
      headers: request.headers,
      params: request.params
    }, EventSdk.AuditEventAction.start)

    const result = await ledger.getSettlements(getSettlementsQuery)
    if (result.type === 'SUCCESS') {
      return h.response(result.result)
    }
    return ErrorHandler.Factory.reformatFSPIOPError(result.error) as any
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * summary: Trigger the creation of a settlement event, that does the calculation of the net settlement position per participant and marks all transfers in the affected windows as Pending settlement. Returned dataset is the net settlement report for the settlement window
 * description:
 * parameters: settlementEventPayload
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function post(
  request: SettlementsPostRequest,
  h: ResponseToolkit
): Promise<ResponseObject> {
  try {
    assert(request)
    assert(request.payload)
    assert(request.payload.settlementModel)
    assert.equal(typeof request.payload.settlementModel, 'string')
    assert(request.payload.reason)
    assert.equal(typeof request.payload.reason, 'string')
    assert(request.payload.settlementWindows)
    assert(Array.isArray(request.payload.settlementWindows))
    assert(request.payload.settlementWindows.length > 0)
    const windowIds = request.payload.settlementWindows.map(sw => sw.id)
    assert.equal(typeof windowIds[0], 'number')

    const ledger = getLedger(request)

    const { span, payload, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT,
      Enum.Events.Event.Action.POST,
      payload.settlementWindows.map((id: any) => id.id).join(''),
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit(request.payload, EventSdk.AuditEventAction.start)

    const command: SettlementPrepareCommand = {
      windowIds,
      model: request.payload.settlementModel,
      reason: request.payload.reason,
    }
    const settlementResult = await ledger.settlementPrepare(command)
    if (settlementResult.type === 'FAILURE') {
      return settlementResult.error as any
    }

    const query: GetSettlementQuery = {
      id: settlementResult.result.id
    }
    const settlementQueryResult = await ledger.getSettlement(query)
    switch (settlementQueryResult.type) {
      case 'FOUND':
        // TODO: map from Ledger representation to dto
        return h.response(settlementQueryResult)
      case 'NOT_FOUND':
        return new Error(`settlement not found after being created`) as any
      case 'FAILED':
        return new Error(`settlement lookup failed`) as any
    }
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * Operations on /settlements
 */
module.exports = {
  get,
  post
}
