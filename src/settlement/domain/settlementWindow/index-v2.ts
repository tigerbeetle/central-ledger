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
 - Lazola Lucas <lazola.lucas@modusbox.com>
--------------
 ******/

import * as ErrorHandler from '@mojaloop/central-services-error-handling'
import { logger } from '../../../shared/logger'

const SettlementWindowModel = require('../../models/settlementWindow')
const SettlementWindowContentModel = require('../../models/settlementWindowContent')
const hasFilters = require('../../utils/truthyProperty')

export interface SettlementWindowEnums {
  OPEN?: number
  PROCESSING?: number
  CLOSED?: number
}

export interface SettlementWindow {
  settlementWindowId: number
  state: number
  reason: string | null
  createdDate: Date
  changedDate: Date
  content?: any[]
}

interface GetByIdParams {
  settlementWindowId: number
}

interface GetByParamsQuery {
  participantId?: string
  state?: string
  fromDateTime?: string
  toDateTime?: string
  currency?: string
}

interface GetByParamsParams {
  query: GetByParamsQuery
}

interface ProcessAndCloseParams {
  settlementWindowId: number
  reason: string
}

interface ProcessAndCloseResult {
  closedWindow: SettlementWindow
  newWindow: SettlementWindow
}

/**
 * Get settlement window by ID with content
 */
export async function getById(params: GetByIdParams, enums: SettlementWindowEnums): Promise<SettlementWindow> {
  const settlementWindow = await SettlementWindowModel.getById(params)

  if (!settlementWindow) {
    const error = ErrorHandler.Factory.createFSPIOPError(
      ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
      `No record for settlementWindowId: ${params.settlementWindowId} found`
    )
    logger.error(error)
    throw error
  }

  const settlementWindowContent = await SettlementWindowContentModel.getBySettlementWindowId(settlementWindow.settlementWindowId)

  if (!settlementWindowContent) {
    const error = ErrorHandler.Factory.createFSPIOPError(
      ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
      `No records for settlementWidowContentId : ${params.settlementWindowId} found`
    )
    logger.error(error)
    throw error
  }

  settlementWindow.content = settlementWindowContent
  return settlementWindow
}

/**
 * Get settlement windows by filter parameters
 */
export async function getByParams(params: GetByParamsParams, enums: SettlementWindowEnums): Promise<SettlementWindow[]> {
  // 4 filters - at least one should be used
  if (!hasFilters(params.query) || Object.keys(params.query).length >= 6) {
    const error = ErrorHandler.Factory.createFSPIOPError(
      ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
      'Use at least one parameter: participantId, state, fromDateTime, toDateTime, currency'
    )
    logger.error(error)
    throw error
  }

  const settlementWindows = await SettlementWindowModel.getByParams(params, enums)

  if (!settlementWindows || settlementWindows.length === 0) {
    const error = ErrorHandler.Factory.createFSPIOPError(
      ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
      `settlementWindow by filters: ${JSON.stringify(params.query).replace(/"/g, '')} not found`
    )
    logger.error(error)
    throw error
  }

  // Attach content to each window
  for (const settlementWindow of settlementWindows) {
    const settlementWindowContent = await SettlementWindowContentModel.getBySettlementWindowId(settlementWindow.settlementWindowId)
    if (!settlementWindowContent) {
      const error = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
        `No records for settlementWidowContentId : ${settlementWindow.settlementWindowId} found`
      )
      logger.error(error)
      throw error
    }
    settlementWindow.content = settlementWindowContent
  }

  return settlementWindows
}

/**
 * Close a settlement window
 *
 * This is typically not called directly - use processAndClose() instead.
 * This method requires the window to already be in PROCESSING state.
 *
 * In the legacy architecture, this was only called by the Kafka consumer,
 * never directly from an API endpoint.
 */
export async function close(settlementWindowId: number, reason: string): Promise<SettlementWindow> {
  try {
    await SettlementWindowModel.close(settlementWindowId, reason)
    return SettlementWindowModel.getById({ settlementWindowId })
  } catch (err) {
    logger.error('Error in close:', err)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

/**
 * Process and close a settlement window synchronously
 *
 * This combines the old process() + close() workflow into a single operation:
 * 1. Validates the window is OPEN and has transfers
 * 2. Creates a new OPEN window
 * 3. Closes the old window (aggregates all transfers, creates settlement content)
 *
 * No Kafka messaging - everything happens synchronously.
 *
 * This replaces the legacy pattern of:
 * - API calls process() → publishes Kafka message → returns new window
 * - Kafka consumer calls close() → aggregates transfers → window becomes CLOSED
 */
export async function processAndClose(params: ProcessAndCloseParams, enums: SettlementWindowEnums): Promise<ProcessAndCloseResult> {
  const { settlementWindowId, reason } = params

  try {
    // Step 1: Process the window (validates, creates new window, sets old to PROCESSING)
    // This returns the ID of the NEW window
    const newSettlementWindowId = await SettlementWindowModel.process({ settlementWindowId, reason }, enums)

    // Step 2: Close the old window (aggregates transfers, sets to CLOSED)
    // Note: The facade.close() expects the window to be in PROCESSING state,
    // which it now is after the process() call above
    await SettlementWindowModel.close(settlementWindowId, reason)

    // Step 3: Fetch and return both windows with their content
    const closedWindow = await getById({ settlementWindowId }, enums)
    const newWindow = await getById({ settlementWindowId: newSettlementWindowId }, enums)

    return {
      closedWindow,
      newWindow
    }
  } catch (err) {
    logger.error('Error in processAndClose:', err)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

export default {
  getById,
  getByParams,
  close,
  processAndClose
}
