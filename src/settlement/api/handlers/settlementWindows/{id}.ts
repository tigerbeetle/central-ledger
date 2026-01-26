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
 - Miguel de Barros <miguel.debarros@modusbox.com>
 - Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 - Valentin Genev <valentin.genev@modusbox.com>
 --------------
 ******/

import * as settlementWindowV2 from '../../../domain/settlementWindow/index-v2'
import type { SettlementWindowEnums } from '../../../domain/settlementWindow/index-v2'
import * as ErrorHandler from '@mojaloop/central-services-error-handling'
import * as EventSdk from '@mojaloop/event-sdk'
import type { Request, ResponseToolkit, ResponseObject } from '@hapi/hapi'

// const {Util, Eu} = require('@mojaloop/central-services-shared')
// const Utility = CentralServicesShared.Util
// const Enum = CentralServicesShared.Enum

import { Util, Enum } from '@mojaloop/central-services-shared'
import { Ledger } from 'src/domain/ledger-v2/Ledger'
import assert from 'assert'
import { SettlementCloseWindowCommand } from 'src/domain/ledger-v2/types'
const settlementWindow = require('../../../domain/settlementWindow/index')

interface SettlementWindowGetRequest extends Request {
  params: {
    id: number
  }
}

interface SettlementWindowPostRequest extends Request {
  params: {
    id: number
  }
  payload: {
    reason: string
  }
}

/**
 * summary: Returns a Settlement Window as per id.
 * description:
 * parameters: id
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function get(
  request: SettlementWindowGetRequest,
  h: ResponseToolkit
): Promise<ResponseObject> {
  const settlementWindowId = request.params.id
  try {
    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT_WINDOW,
      Enum.Events.Event.Action.GET,
      `settlementWindowId=${settlementWindowId}`,
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit({
      headers: request.headers,
      params: request.params
    }, EventSdk.AuditEventAction.start)
    const Enums = await (request.server.methods as any).enums('settlementWindowStates')
    const settlementWindowResult = await settlementWindow.getById(
      { settlementWindowId },
      Enums,
      request.server.log
    )
    return h.response(settlementWindowResult)
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}

/**
 * summary: If the settlementWindow is open, it can be closed and a new window created. If it is already closed, return an error message. Returns the new settlement window.
 * description:
 * parameters: id, settlementWindowClosurePayload
 * produces: application/json
 * responses: 200, 400, 401, 404, 415, default
 */
async function post(
  request: SettlementWindowPostRequest
): Promise<ResponseObject> {
  try {
    const ledger = getLedger(request)
    assert(request)
    assert(request.payload)
    assert(request.payload.reason)
    assert(request.params)
    assert(request.params.id)

    const id = request.params.id
    const reason = request.payload.reason
    assert.strictEqual(typeof id, 'number', 'Expceted `id` to be a number')

    const { span, headers } = request as any
    const spanTags = Util.EventFramework.getSpanTags(
      Enum.Events.Event.Type.SETTLEMENT_WINDOW,
      Enum.Events.Event.Action.POST,
      `settlementWindowId=${id}`,
      headers[Enum.Http.Headers.FSPIOP.SOURCE],
      headers[Enum.Http.Headers.FSPIOP.DESTINATION]
    )
    span.setTags(spanTags)
    await span.audit(request.payload, EventSdk.AuditEventAction.start)
    const Enums = await (request.server.methods as any).enums('settlementWindowStates') as SettlementWindowEnums

    const cmd: SettlementCloseWindowCommand = {
      id, reason
    }
    const result = await ledger.closeSettlementWindow(cmd)
    if (result.type === 'FAILURE') {
      return ErrorHandler.Factory.reformatFSPIOPError(result.error) as any
    }

    // TODO(LD): Also get the old and new window. E.g. from processAndClose()
    // const closedWindow = await getById({ settlementWindowId }, enums)
    // const newWindow = await getById({ settlementWindowId: newSettlementWindowId }, enums)

    // return {
    //   closedWindow,
    //   newWindow
    // }

    return true as any
  } catch (err) {
    request.server.log('error', err)
    return ErrorHandler.Factory.reformatFSPIOPError(err) as any
  }
}


const getLedger = (request: any): Ledger => {
  assert(request, 'request is undefined')
  assert(request.server.app, 'request.server.app is undefined')
  assert(request.server.app.ledger, 'Ledger not available in server app state')
  return request.server.app.ledger
}

module.exports = {
  get,
  post
}
