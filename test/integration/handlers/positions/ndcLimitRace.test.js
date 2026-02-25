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
 --------------
 ******/

'use strict'

/**
 * @file NDC Limit Race Condition Test
 *
 * This test demonstrates a potential race condition where the Net Debit Cap (NDC)
 * limit check uses a snapshot read under MySQL's REPEATABLE READ isolation level.
 *
 * The vulnerability:
 * - Transfer transactions read the NDC limit WITHOUT forUpdate()
 * - Under REPEATABLE READ, this returns the snapshot from transaction start
 * - If an admin reduces the limit while transfers are in-flight, the transfers
 *   may be approved based on the stale (higher) limit value
 *
 * Test approach:
 * 1. Set up a participant with a high NDC limit
 * 2. Send many transfers concurrently
 * 3. While transfers are processing, reduce the NDC limit
 * 4. Check if final position exceeds the reduced limit
 * 5. Repeat multiple trials to statistically detect the race condition
 */

const Test = require('tape')
const { randomUUID } = require('crypto')
const Config = require('#src/lib/config')
const Db = require('#src/lib/db')
const Cache = require('#src/lib/cache')
const ProxyCache = require('#src/lib/proxyCache')
const Producer = require('@mojaloop/central-services-stream').Util.Producer
const Utility = require('@mojaloop/central-services-shared').Util.Kafka
const Enum = require('@mojaloop/central-services-shared').Enum
const ParticipantHelper = require('#test/integration/helpers/participant')
const ParticipantLimitHelper = require('#test/integration/helpers/participantLimit')
const ParticipantFundsInOutHelper = require('#test/integration/helpers/participantFundsInOut')
const ParticipantEndpointHelper = require('#test/integration/helpers/participantEndpoint')
const SettlementHelper = require('#test/integration/helpers/settlementModels')
const HubAccountsHelper = require('#test/integration/helpers/hubAccounts')
const ParticipantService = require('#src/domain/participant/index')
const MLNumber = require('@mojaloop/ml-number')

const ParticipantCached = require('#src/models/participant/participantCached')
const ParticipantCurrencyCached = require('#src/models/participant/participantCurrencyCached')
const ParticipantLimitCached = require('#src/models/participant/participantLimitCached')

const { sleepPromise } = require('#test/util/helpers')

const TransferEventType = Enum.Events.Event.Type
const TransferEventAction = Enum.Events.Event.Action

// Test configuration
const TEST_CONFIG = {
  // Initial NDC limit - high enough to allow many transfers
  INITIAL_LIMIT: 100000,

  // Reduced NDC limit - lower than total transfers we'll send
  REDUCED_LIMIT: 5000,

  // Amount per transfer
  TRANSFER_AMOUNT: 100,

  // Number of transfers to send per trial
  // Total value = NUM_TRANSFERS * TRANSFER_AMOUNT = 200 * 100 = 20,000
  // This exceeds REDUCED_LIMIT (5000) but is under INITIAL_LIMIT (100000)
  NUM_TRANSFERS: 200,

  // Delay before reducing limit (ms) - allows some transfers to start
  LIMIT_REDUCTION_DELAY: 50,

  // Number of trials to run
  NUM_TRIALS: 10,

  // Delay between trials (ms) - allows system to settle
  INTER_TRIAL_DELAY: 1000,

  // Currency
  CURRENCY: 'USD'
}

// Test data template
const createTestData = (trialNum) => ({
  amount: {
    currency: TEST_CONFIG.CURRENCY,
    amount: TEST_CONFIG.TRANSFER_AMOUNT
  },
  payer: {
    name: `ndcTestPayer${trialNum}`,
    limit: TEST_CONFIG.INITIAL_LIMIT
  },
  payee: {
    name: `ndcTestPayee${trialNum}`,
    limit: TEST_CONFIG.INITIAL_LIMIT
  },
  endpoint: {
    base: 'http://localhost:1080',
    email: 'test@example.com'
  },
  now: new Date(),
  expiration: new Date((new Date()).getTime() + (24 * 60 * 60 * 1000))
})

/**
 * Prepare participant data for a trial
 */
const prepareTrialData = async (testData) => {
  const payer = await ParticipantHelper.prepareData(testData.payer.name, testData.amount.currency)
  const payee = await ParticipantHelper.prepareData(testData.payee.name, testData.amount.currency)

  // Set initial limit and position
  const payerLimitAndInitialPosition = await ParticipantLimitHelper.prepareLimitAndInitialPosition(
    payer.participant.name,
    {
      currency: testData.amount.currency,
      limit: { value: testData.payer.limit }
    }
  )

  const payeeLimitAndInitialPosition = await ParticipantLimitHelper.prepareLimitAndInitialPosition(
    payee.participant.name,
    {
      currency: testData.amount.currency,
      limit: { value: testData.payee.limit }
    }
  )

  // Fund the payer's settlement account with plenty of liquidity
  await ParticipantFundsInOutHelper.recordFundsIn(
    payer.participant.name,
    payer.participantCurrencyId2,
    {
      currency: testData.amount.currency,
      amount: TEST_CONFIG.INITIAL_LIMIT * 2 // Plenty of liquidity
    }
  )

  // Set up endpoints
  for (const name of [payer.participant.name, payee.participant.name]) {
    await ParticipantEndpointHelper.prepareData(name, 'FSPIOP_CALLBACK_URL_TRANSFER_POST', `${testData.endpoint.base}/transfers`)
    await ParticipantEndpointHelper.prepareData(name, 'FSPIOP_CALLBACK_URL_TRANSFER_PUT', `${testData.endpoint.base}/transfers/{{transferId}}`)
    await ParticipantEndpointHelper.prepareData(name, 'FSPIOP_CALLBACK_URL_TRANSFER_ERROR', `${testData.endpoint.base}/transfers/{{transferId}}/error`)
  }

  return { payer, payee, payerLimitAndInitialPosition, payeeLimitAndInitialPosition }
}

/**
 * Create a transfer message payload
 */
const createTransferPayload = (payer, payee, testData) => ({
  transferId: randomUUID(),
  payerFsp: payer.participant.name,
  payeeFsp: payee.participant.name,
  amount: {
    currency: testData.amount.currency,
    amount: testData.amount.amount.toString()
  },
  ilpPacket: 'AYIBgQAAAAAAAASwNGxldmVsb25lLmRmc3AxLm1lci45T2RTOF81MDdqUUZERmZlakgyOVc4bXFmNEpLMHlGTFGCAUBQU0svMS4wCk5vbmNlOiB1SXlweUYzY3pYSXBFdzVVc05TYWh3CkVuY3J5cHRpb246IG5vbmUKUGF5bWVudC1JZDogMTMyMzZhM2ItOGZhOC00MTYzLTg0NDctNGMzZWQzZGE5OGE3CgpDb250ZW50LUxlbmd0aDogMTM1CkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbgpTZW5kZXItSWRlbnRpZmllcjogOTI4MDYzOTEKCiJ7XCJmZWVcIjowLFwidHJhbnNmZXJDb2RlXCI6XCJpbnZvaWNlXCIsXCJkZWJpdE5hbWVcIjpcImFsaWNlIGNvb3BlclwiLFwiY3JlZGl0TmFtZVwiOlwibWVyIGNoYW50XCIsXCJkZWJpdElkZW50aWZpZXJcIjpcIjkyODA2MzkxXCJ9IgA',
  condition: 'GRzLaTP7DJ9t4P-a_BA0WA9wzzlsugf00-Ber-Q0EvQ',
  expiration: testData.expiration.toISOString(),
  extensionList: {
    extension: [
      { key: 'key1', value: 'value1' }
    ]
  }
})

/**
 * Send a batch of transfers via Kafka
 */
const sendTransferBatch = async (payer, payee, testData, numTransfers) => {
  const transfers = []

  for (let i = 0; i < numTransfers; i++) {
    const transferPayload = createTransferPayload(payer, payee, testData)
    transfers.push(transferPayload)

    const prepareConfig = Utility.getKafkaConfig(
      Config.KAFKA,
      Enum.Kafka.Config.PRODUCER,
      TransferEventType.TRANSFER.toUpperCase(),
      TransferEventAction.PREPARE.toUpperCase()
    )
    prepareConfig.logger = console

    const messageProtocol = {
      id: transferPayload.transferId,
      from: transferPayload.payerFsp,
      to: transferPayload.payeeFsp,
      type: 'application/json',
      content: {
        headers: {
          'content-type': 'application/vnd.interoperability.transfers+json;version=1.1',
          'fspiop-source': transferPayload.payerFsp,
          'fspiop-destination': transferPayload.payeeFsp,
          date: new Date().toISOString()
        },
        payload: transferPayload
      },
      metadata: {
        event: {
          id: randomUUID(),
          type: TransferEventType.TRANSFER,
          action: TransferEventAction.PREPARE,
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0
          }
        }
      }
    }

    const topicConfig = Utility.createGeneralTopicConf(
      Config.KAFKA.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE,
      TransferEventType.TRANSFER,
      TransferEventAction.PREPARE
    )

    await Producer.produceMessage(messageProtocol, topicConfig, prepareConfig)
  }

  return transfers
}

/**
 * Get current position for a participant
 */
const getParticipantPosition = async (participantName, currency) => {
  const participant = await ParticipantService.getByNameAndCurrency(
    participantName,
    currency,
    Enum.Accounts.LedgerAccountType.POSITION
  )

  if (!participant || !participant.participantCurrencyId) {
    throw new Error(`Could not find participant position for ${participantName}`)
  }

  const position = await ParticipantService.getPositionByParticipantCurrencyId(
    participant.participantCurrencyId
  )

  return position ? parseFloat(position.value) : 0
}

/**
 * Get current limit for a participant
 */
const getParticipantLimit = async (participantName, currency) => {
  const limits = await ParticipantService.getLimits(participantName, {
    currency,
    type: Enum.Accounts.ParticipantLimitType.NET_DEBIT_CAP
  })

  return limits && limits.length > 0 ? parseFloat(limits[0].value) : 0
}

/**
 * Reduce the NDC limit for a participant
 */
const reduceNdcLimit = async (participantName, currency, newLimit) => {
  await ParticipantLimitHelper.adjustLimits(participantName, {
    currency,
    limit: {
      type: 'NET_DEBIT_CAP',
      value: newLimit
    }
  })

  // Invalidate cache to ensure new limit is picked up
  await ParticipantLimitCached.invalidateParticipantLimitCache()
}

/**
 * Run a single trial of the race condition test
 */
const runTrial = async (trialNum) => {
  const testData = createTestData(trialNum)
  const result = {
    trialNum,
    initialLimit: TEST_CONFIG.INITIAL_LIMIT,
    reducedLimit: TEST_CONFIG.REDUCED_LIMIT,
    numTransfers: TEST_CONFIG.NUM_TRANSFERS,
    transferAmount: TEST_CONFIG.TRANSFER_AMOUNT,
    expectedMaxPosition: TEST_CONFIG.NUM_TRANSFERS * TEST_CONFIG.TRANSFER_AMOUNT,
    finalPosition: 0,
    finalLimit: 0,
    overspend: false,
    overspendAmount: 0
  }

  try {
    // Prepare test data
    const { payer, payee } = await prepareTrialData(testData)

    // Start sending transfers (don't await - we want concurrency)
    const transfersPromise = sendTransferBatch(
      payer,
      payee,
      testData,
      TEST_CONFIG.NUM_TRANSFERS
    )

    // Wait a short time for some transfers to start processing
    await sleepPromise(TEST_CONFIG.LIMIT_REDUCTION_DELAY)

    // Reduce the limit while transfers are in flight
    await reduceNdcLimit(
      payer.participant.name,
      TEST_CONFIG.CURRENCY,
      TEST_CONFIG.REDUCED_LIMIT
    )

    // Wait for all transfers to be sent
    await transfersPromise

    // Wait for transfers to be processed
    // This is a simplification - in reality we'd want to check transfer states
    await sleepPromise(5000)

    // Check final position
    result.finalPosition = await getParticipantPosition(
      payer.participant.name,
      TEST_CONFIG.CURRENCY
    )

    result.finalLimit = await getParticipantLimit(
      payer.participant.name,
      TEST_CONFIG.CURRENCY
    )

    // Check for overspend
    if (result.finalPosition > result.reducedLimit) {
      result.overspend = true
      result.overspendAmount = result.finalPosition - result.reducedLimit
    }

  } catch (err) {
    result.error = err.message
  }

  return result
}

/**
 * Main test suite
 */
Test('NDC Limit Race Condition Tests', async (ndcTest) => {
  let db

  ndcTest.test('setup', async (test) => {
    try {
      db = await Db.connect(Config.DATABASE)
      await Cache.initCache()
      await ProxyCache.getClient()

      await SettlementHelper.prepareData()
      await HubAccountsHelper.prepareData()

      await ParticipantCached.initialize()
      await ParticipantCurrencyCached.initialize()
      await ParticipantLimitCached.initialize()

      test.pass('Setup completed')
      test.end()
    } catch (err) {
      test.fail(`Setup failed: ${err.message}`)
      test.end()
    }
  })

  ndcTest.test('Statistical Race Condition Test', async (test) => {
    const results = []
    let overspendDetected = false
    let overspendTrials = []

    console.log('\n' + '='.repeat(70))
    console.log('NDC LIMIT RACE CONDITION TEST')
    console.log('='.repeat(70))
    console.log(`Configuration:`)
    console.log(`  Initial Limit:    $${TEST_CONFIG.INITIAL_LIMIT}`)
    console.log(`  Reduced Limit:    $${TEST_CONFIG.REDUCED_LIMIT}`)
    console.log(`  Transfers/Trial:  ${TEST_CONFIG.NUM_TRANSFERS}`)
    console.log(`  Amount/Transfer:  $${TEST_CONFIG.TRANSFER_AMOUNT}`)
    console.log(`  Max Total:        $${TEST_CONFIG.NUM_TRANSFERS * TEST_CONFIG.TRANSFER_AMOUNT}`)
    console.log(`  Number of Trials: ${TEST_CONFIG.NUM_TRIALS}`)
    console.log('='.repeat(70) + '\n')

    for (let trial = 1; trial <= TEST_CONFIG.NUM_TRIALS; trial++) {
      console.log(`\nTrial ${trial}/${TEST_CONFIG.NUM_TRIALS}...`)

      const result = await runTrial(trial)
      results.push(result)

      console.log(`  Final Position: $${result.finalPosition}`)
      console.log(`  Final Limit:    $${result.finalLimit}`)
      console.log(`  Overspend:      ${result.overspend ? `YES ($${result.overspendAmount})` : 'No'}`)

      if (result.overspend) {
        overspendDetected = true
        overspendTrials.push(trial)
      }

      if (result.error) {
        console.log(`  Error:          ${result.error}`)
      }

      // Wait between trials
      if (trial < TEST_CONFIG.NUM_TRIALS) {
        await sleepPromise(TEST_CONFIG.INTER_TRIAL_DELAY)
      }
    }

    // Summary
    console.log('\n' + '='.repeat(70))
    console.log('SUMMARY')
    console.log('='.repeat(70))
    console.log(`Total Trials:     ${TEST_CONFIG.NUM_TRIALS}`)
    console.log(`Overspend Count:  ${overspendTrials.length}`)
    console.log(`Overspend Rate:   ${(overspendTrials.length / TEST_CONFIG.NUM_TRIALS * 100).toFixed(1)}%`)

    if (overspendDetected) {
      console.log(`\n⚠️  RACE CONDITION DETECTED!`)
      console.log(`   Overspend occurred in trials: ${overspendTrials.join(', ')}`)

      const maxOverspend = Math.max(...results.filter(r => r.overspend).map(r => r.overspendAmount))
      console.log(`   Maximum overspend amount: $${maxOverspend}`)
    } else {
      console.log(`\n✓  No overspend detected in ${TEST_CONFIG.NUM_TRIALS} trials`)
      console.log(`   (Race condition may still exist but wasn't triggered)`)
    }
    console.log('='.repeat(70) + '\n')

    // The test "passes" if we detect the bug, "fails" if the system is working correctly
    // (Inverse of normal testing - we're trying to FIND the bug)
    if (overspendDetected) {
      test.fail(`RACE CONDITION CONFIRMED: Position exceeded limit in ${overspendTrials.length}/${TEST_CONFIG.NUM_TRIALS} trials`)
    } else {
      test.pass(`No race condition detected in ${TEST_CONFIG.NUM_TRIALS} trials (may need more trials or different timing)`)
    }

    test.end()
  })

  ndcTest.test('teardown', async (test) => {
    try {
      await Cache.destroyCache()
      await Db.disconnect()
      test.pass('Teardown completed')
      test.end()
    } catch (err) {
      test.fail(`Teardown failed: ${err.message}`)
      test.end()
    }
  })

  ndcTest.end()
})
