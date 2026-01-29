/*****
 License
 --------------
 Copyright � 2020-2025 Mojaloop Foundation
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
import { assert } from 'console'
import { GetSettlementWindowsQuery, SettlementWindowState } from 'src/domain/ledger-v2/types'
import { getLedger } from '../../../api/helper'

const settlementWindows = require('../../domain/settlementWindow/index')

interface SettlementWindowsGetRequest extends Request {
  query: {
    participantId?: number
    state?: string
    fromDateTime?: string
    toDateTime?: string
    currency?: string
  }
}

/**
 * summary: Returns a Settlement Window(s) as per parameter(s).
 * description:
 * parameters: participantId, state, fromDateTime, toDateTime, currency
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function get(
  request: SettlementWindowsGetRequest,
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
        'At least one valid filter parameter is required (participantId, state, fromDateTime, toDateTime, or currency)'
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

    // Helper to validate settlement window state
    const parseState = (stateStr: string): SettlementWindowState => {
      const validStates: SettlementWindowState[] = [
        'OPEN', 'CLOSED', 'PENDING_SETTLEMENT', 'SETTLED', 'ABORTED', 'PROCESSING', 'FAILED'
      ]
      if (!validStates.includes(stateStr as SettlementWindowState)) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
          `Invalid state: ${stateStr}. Must be one of: ${validStates.join(', ')}`
        )
      }
      return stateStr as SettlementWindowState
    }

    // Build settlement window query object
    const getSettlementWindowQuery: GetSettlementWindowsQuery = {
      ...(query.participantId && { participantId: query.participantId }),
      ...(query.state && { state: parseState(query.state) }),
      ...(query.fromDateTime && { fromDateTime: parseDate(query.fromDateTime, 'fromDateTime') }),
      ...(query.toDateTime && { toDateTime: parseDate(query.toDateTime, 'toDateTime') }),
      ...(query.currency && { currency: query.currency })
    }

    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT_WINDOW,
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

    const result = await ledger.getSettlementWindows(getSettlementWindowQuery)
    if (result.type === 'FAILURE') {
      request.server.log('error', result.error)
      return ErrorHandler.Factory.reformatFSPIOPError(result.error) as any
    }

    return h.response(result.result)
    // const Enums = await (request.server.methods as any).enums('settlementWindowStates')
    // const settlementWindowResult = await settlementWindows.getByParams({ query: request.query }, Enums)
    // return h.response(settlementWindowResult)
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * Operations on /settlementWindows
 */
module.exports = {
  get
}
