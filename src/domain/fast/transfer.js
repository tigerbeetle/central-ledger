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

 * TigerBeetle
 - Lewis Daly <lewis@tigerbeetle.com>
 --------------
 ******/

'use strict'



const ErrorHandler = require('@mojaloop/central-services-error-handling')
const Metrics = require('@mojaloop/central-services-metrics')

const { Enum, Util } = require('@mojaloop/central-services-shared')
const TransferEventAction = Enum.Events.Event.Action


const ledger = require('./ledger')
const assert = require('assert')



  /*
   payload:
    {
      transferId: 'd4f5cd10-9775-489c-923e-3ae307ed0779',
      payeeFsp: 'dfsp_b',
      payerFsp: 'dfsp_a',
      amount: { amount: '1', currency: 'USD' },
      ilpPacket: 'DIICtgAAAAAAD0JAMjAyNDEyMDUxNjA4MDM5MDcYjF3nFyiGSaedeiWlO_87HCnJof_86Krj0lO8KjynIApnLm1vamFsb29wggJvZXlKeGRXOTBaVWxrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVnaUxDSjBjbUZ1YzJGamRHbHZia2xrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVvaUxDSjBjbUZ1YzJGamRHbHZibFI1Y0dVaU9uc2ljMk5sYm1GeWFXOGlPaUpVVWtGT1UwWkZVaUlzSW1sdWFYUnBZWFJ2Y2lJNklsQkJXVVZTSWl3aWFXNXBkR2xoZEc5eVZIbHdaU0k2SWtKVlUwbE9SVk5USW4wc0luQmhlV1ZsSWpwN0luQmhjblI1U1dSSmJtWnZJanA3SW5CaGNuUjVTV1JVZVhCbElqb2lUVk5KVTBST0lpd2ljR0Z5ZEhsSlpHVnVkR2xtYVdWeUlqb2lNamMzTVRNNE1ETTVNVElpTENKbWMzQkpaQ0k2SW5CaGVXVmxabk53SW4xOUxDSndZWGxsY2lJNmV5SndZWEowZVVsa1NXNW1ieUk2ZXlKd1lYSjBlVWxrVkhsd1pTSTZJazFUU1ZORVRpSXNJbkJoY25SNVNXUmxiblJwWm1sbGNpSTZJalEwTVRJek5EVTJOemc1SWl3aVpuTndTV1FpT2lKMFpYTjBhVzVuZEc5dmJHdHBkR1JtYzNBaWZYMHNJbVY0Y0dseVlYUnBiMjRpT2lJeU1ESTBMVEV5TFRBMVZERTJPakE0T2pBekxqa3dOMW9pTENKaGJXOTFiblFpT25zaVlXMXZkVzUwSWpvaU1UQXdJaXdpWTNWeWNtVnVZM2tpT2lKWVdGZ2lmWDA',
      condition: 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA',
      expiration: '2025-04-03T19:23:01.961Z'
    }
  */

const handlePayeeResponse = async (transferId, payload, action, fspiopError) => {
  const timerEnd = Metrics.getHistogram(
    'domain_transfer',
    'prepare - Metrics for transfer domain',
    ['success', 'funcName']
  ).startTimer()

  // TODO: handle unhappy path transfers

  switch (action) {
    case TransferEventAction.COMMIT:
    case TransferEventAction.BULK_COMMIT:
    case TransferEventAction.RESERVE:
      break
    case TransferEventAction.REJECT:
    case TransferEventAction.BULK_ABORT:
    case TransferEventAction.ABORT_VALIDATION:
    case TransferEventAction.ABORT:
    default:
      throw new Error(`handlePayeeResponse - not implemented for actions: ${action}`)
  }

  try {
    const transferBatch = await ledger.buildPostedTransfers(payload)
    await Promise.all(transferBatch.map(transfer => ledger.enqueueTransfer(transfer)))

    // TODO: async save transfer metadata to metadata Database

    timerEnd({ success: true, funcName: 'handlePayeeResponse' })
  } catch (err) {
    timerEnd({ success: false, funcName: 'handlePayeeResponse' })
    throw err
  }
}

/**
 * @function LogTransferError
 *
 * @async
 * @description This will insert a record into the transferError table for the latest transfer stage change id.
 *
 * TransferStateChangeModel.getByTransferId called to get the latest transfer state change id
 * TransferError.insert called to insert the record into the transferError table
 *
 * @param {string} transferId - the transfer id
 * @param {integer} errorCode - the error code
 * @param {string} errorDescription - the description error
 *
 * @returns {integer} - Returns the id of the transferError record if successful, or throws an error if failed
 */

const logTransferError = async (transferId, errorCode, errorDescription) => {
  try {
    const transferStateChange = await TransferStateChangeModel.getByTransferId(transferId)
    return TransferError.insert(transferId, transferStateChange.transferStateChangeId, errorCode, errorDescription)
  } catch (err) {
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

const TransferService = {
  handlePayeeResponse,
  logTransferError,
}

module.exports = TransferService
