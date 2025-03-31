const PositionFacade = require('../../models/position/facade')
const ledger = require('./ledger')
const assert = require('assert')

const changeParticipantPosition = (participantCurrencyId, isReversal, amount, transferStateChange) => {
  console.log("LD shim changeParticipantPosition")
 
  const result = PositionFacade.changeParticipantPositionTransaction(participantCurrencyId, isReversal, amount, transferStateChange)
  
  return result
}


/**
  transferList = [
  {
    value: {
      from: 'dfsp_a',
      to: 'dfsp_c',
      id: '745ac705-a283-455f-86b1-445cc6f67cb5',
      content: [Object],
      type: 'application/json',
      metadata: [Object]
    },
    size: 3423,
    key: <Buffer 33>,
    topic: 'topic-transfer-position',
    offset: 63,
    partition: 3,
    timestamp: 1743162252645
  }
]

 result = {
  "preparedMessagesList": [
    {
      "transferState": {
        "transferStateChangeId": null,
        "transferId": "745ac705-a283-455f-86b1-445cc6f67cb5",
        "transferStateId": "RESERVED",
        "reason": null,
        "createdDate": "2025-03-28T10:44:13.000Z"
      },
      "transfer": {
        "transferId": "745ac705-a283-455f-86b1-445cc6f67cb5",
        "payeeFsp": "dfsp_c",
        "payerFsp": "dfsp_a",
        "amount": {
          "amount": "1",
          "currency": "USD"
        },
        "ilpPacket": "DIICtgAAAAAAD0JAMjAyNDEyMDUxNjA4MDM5MDcYjF3nFyiGSaedeiWlO_87HCnJof_86Krj0lO8KjynIApnLm1vamFsb29wggJvZXlKeGRXOTBaVWxrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVnaUxDSjBjbUZ1YzJGamRHbHZia2xrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVvaUxDSjBjbUZ1YzJGamRHbHZibFI1Y0dVaU9uc2ljMk5sYm1GeWFXOGlPaUpVVWtGT1UwWkZVaUlzSW1sdWFYUnBZWFJ2Y2lJNklsQkJXVVZTSWl3aWFXNXBkR2xoZEc5eVZIbHdaU0k2SWtKVlUwbE9SVk5USW4wc0luQmhlV1ZsSWpwN0luQmhjblI1U1dSSmJtWnZJanA3SW5CaGNuUjVTV1JVZVhCbElqb2lUVk5KVTBST0lpd2ljR0Z5ZEhsSlpHVnVkR2xtYVdWeUlqb2lNamMzTVRNNE1ETTVNVElpTENKbWMzQkpaQ0k2SW5CaGVXVmxabk53SW4xOUxDSndZWGxsY2lJNmV5SndZWEowZVVsa1NXNW1ieUk2ZXlKd1lYSjBlVWxrVkhsd1pTSTZJazFUU1ZORVRpSXNJbkJoY25SNVNXUmxiblJwWm1sbGNpSTZJalEwTVRJek5EVTJOemc1SWl3aVpuTndTV1FpT2lKMFpYTjBhVzVuZEc5dmJHdHBkR1JtYzNBaWZYMHNJbVY0Y0dseVlYUnBiMjRpT2lJeU1ESTBMVEV5TFRBMVZERTJPakE0T2pBekxqa3dOMW9pTENKaGJXOTFiblFpT25zaVlXMXZkVzUwSWpvaU1UQXdJaXdpWTNWeWNtVnVZM2tpT2lKWVdGZ2lmWDA",
        "condition": "GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA",
        "expiration": "2025-03-28T11:44:40.516Z"
      },
      "rawMessage": ... skipped for brevity
      "transferAmount": {
        "mlNumber": "1"
      }
    }
  ],
  "limitAlarms": []
}
 */
const calculatePreparePositionsBatch = async (transferList) => {
  console.log("LD shim calculatePreparePositionsBatch")
  assert(Array.isArray(transferList), 'expected transferList to be an array')
  assert(transferList.length === 1, 'calculatePreparePositionsBatch currently only handles 1 tx at a time')

  // TODO: implement this with TigerBeetle

  const transfers = await ledger.buildPendingTransferBatch(transferList)
  await Promise.all(transfers.map(transfer => ledger.enqueueTransfer(transfer)))

  // const result = await PositionFacade.prepareChangeParticipantPositionTransaction(transferList)
  // console.log('calculatePreparePositionsBatch result is', JSON.stringify(result))
  
  // return result
}

module.exports = {
  changeParticipantPosition,
  calculatePreparePositionsBatch
}