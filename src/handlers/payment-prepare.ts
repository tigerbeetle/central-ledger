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

import assert from 'node:assert';
import { ApplicationConfig } from '../lib/config';
import { logger } from '../shared/logger';
import CentralServicesShared, { Enum, TransferStateEnum, Util } from '@mojaloop/central-services-shared';
import { CreateRemittanceEntity, KafkaParams, ProxyCache } from './transfer-types';
import RefactorHelper from '../shared/refactor-helper';
const { Kafka, Comparators } = Util
const { decodePayload } = Util.StreamingProtocol
// const Participant = require('../domain/participant')
import Participant from '../domain/participant'
const { Consumer, Producer } = require('@mojaloop/central-services-stream').Util
const { Type, Action } = Enum.Events.Event

const ErrorHandler = require('@mojaloop/central-services-error-handling')
const { FSPIOPError } = ErrorHandler

import BatchPositionModel from '../models/position/batch'
import BatchPositionModelCached from '../models/position/batchCached'
import SettlementModelCached from '../models/settlement/settlementModelCached'
import PositionPrepareDomain from '../domain/position/prepare'
import ParticipantFacade from '../models/participant/facade'

interface Dependencies {
  config: ApplicationConfig,
  proxyCache: ProxyCache,
  createRemittanceEntity: CreateRemittanceEntity,
  /**
   * @deprecated - TODO: remove. We moved this business logic into this class. 
   */
  sendPositionPrepareMessage: (options: {
    isFx: boolean,
    action: string,
    params: any,
    determiningTransferCheckResult: any,
    proxyObligation: any
  }) => Promise<true>
  // positionHandler: null | ((error: null, messages: Array<any>) => Promise<any>)
  positionHandler: null | ((message: any) => Promise<any>)
  definePositionParticipant: (options: {
    isFx: boolean,
    payload: CreatePaymentDto,
    determiningTransferCheckResult: any,
    proxyObligation: any
  }) => Promise<{ messageKey: string, cyrilResult: any }>
}

export interface CreatePaymentDto {
  amount: {
    amount: string,
    currency: string
  },
  condition: string,
  expiration: string,
  ilpPacket: string,
  payeeFsp: string,
  payerFsp: string,
  transferId: string,
}

export interface FusedPrepareHandlerInput {
  message: any;
  payload: CreatePaymentDto;
  headers: any;
  transferId: string;
  action: any;
  metric: string;
  functionality: CentralServicesShared.EventTypeEnum.TRANSFER;
  actionEnum: string;
}

interface ProxyObligation {
  isFx: false,
  payloadClone: CreatePaymentDto,
  isInitiatingFspProxy: boolean,
  isCounterPartyFspProxy: boolean,
  initiatingFspProxyOrParticipantId: {
    inScheme: boolean,
    proxyId: string | null,
    name: string
  } | null,
  counterPartyFspProxyOrParticipantId: {
    inScheme: boolean,
    proxyId: string | null,
    name: string
  } | null
}

export enum PaymentPrepareResultType {
  /**
   * Prepare step completed validation
   */
  PASS = 'PASS',

  /**
   * Duplicate transfer found in a finalized state
   */
  DUPLICATE_FINAL = 'DUPLICATE_FINAL',

  /**
   * Duplicate transfer found that is still being processed
   */
  DUPLICATE_NON_FINAL = 'DUPLICATE_NON_FINAL',

  /**
   * An existing transfer exists with this id but different parameters
   */
  MODIFIED = 'MODIFIED',

  /**
   * Transfer failed validation
   */
  FAIL_VALIDATION = 'FAIL_VALIDATION',

  /**
   * Transfer failed as payee didn't have sufficent liquidity
   */
  FAIL_LIQUIDITY = 'FAIL_LIQUIDITY',

  /**
   * Catch-all Transfer failed for another reason
   */
  FAIL_OTHER = 'FAIL_OTHER',
}

export type PaymentPrepareResult = {
  type: PaymentPrepareResultType.PASS
} | {
  type: PaymentPrepareResultType.DUPLICATE_FINAL
  finalizedTransfer: {
    completedTimestamp: string
    transferState: 'COMMITTED' | 'ABORTED'
    fulfilment?: string
  }
} | {
  type: PaymentPrepareResultType.DUPLICATE_NON_FINAL
} | {
  type: PaymentPrepareResultType.MODIFIED
} | {
  type: PaymentPrepareResultType.FAIL_VALIDATION
  failureReasons: Array<string>
} | {
  type: PaymentPrepareResultType.FAIL_LIQUIDITY
  error: typeof FSPIOPError
} | {
  type: PaymentPrepareResultType.FAIL_OTHER
  error: typeof FSPIOPError
}

export enum PositionPrepareResultType {
  /**
   * Position was prepared correctly.
   */
  PASS = 'PASS',

  /**
   * PositionPrepare failed as the payer didn't have enough liquidity.
   */
  FAIL_LIQUIDITY = 'FAIL_LIQUIDITY',

  /**
   * PositionPrepare failed validation.
   */
  FAIL_VALIDATION = 'FAIL_VALIDATION',

  /**
   * Catch-all Transfer failed for another reason
   */
  FAIL_OTHER = 'FAIL_OTHER',
}

export type PositionPrepareResult = {
  type: PositionPrepareResultType.PASS,
  notifications: Array<Notification>
} | {
  type: PositionPrepareResultType.FAIL_LIQUIDITY,
  notifications: Array<Notification>
} | {
  type: PositionPrepareResultType.FAIL_VALIDATION,
  failureReasons: Array<string>
  notifications: Array<Notification>
} | {
  type: PositionPrepareResultType.FAIL_OTHER,
  notifications: Array<Notification>
}

/**
 * A message to be emitted by the messaging layer; results of the PaymentPrepare handling.
 */
export type Notification = {

}

export class PaymentPrepareHandler {
  constructor(private deps: Dependencies) { }

  async handle(error: any, messages: any): Promise<Array<PromiseSettledResult<PaymentPrepareResult>>> {
    if (error) {
      throw error
    }

    assert(Array.isArray(messages))
    if (messages.length === 0) {
      logger.debug('PaymentPrepareHandler.handle() - received empty batch, nothing to process');
      return []
    }
    if (this.deps.config.HANDLERS_TRANSFER_POSITION_FUSE === 'FUSE') {
      assert(
        this.deps.positionHandler,
        'PaymentPrepareHandler.deps.positionHandler not defined, positions are in `FUSE` mode.')
    }

    logger.debug(`PaymentPrepareHandler.handle() - processing batch of ${messages.length} messages`)
    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));

    const results = await Promise.allSettled(inputs.map(async ({ input }) => this.handleOne(input)))
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.type !== PaymentPrepareResultType.PASS) {
        logger.warn(`handleOne() returned non-success: \n\t${JSON.stringify(result.value)}`)
      }
      if (result.status === 'rejected') {
        logger.error(`handleOne() failed with error: \n\t${result.reason}`)
        if (result.reason.stack) logger.error(`stack\n\t${result.reason.stack}`)
      }
    })

    return results
  }

  async handleOne(input: FusedPrepareHandlerInput): Promise<PaymentPrepareResult> {
    // Check Duplication
    const remittance = this.deps.createRemittanceEntity()
    const { hasDuplicateId, hasDuplicateHash } = await Comparators.duplicateCheckComparator(
      // TODO: not 100%.
      input.payload.transferId,
      input.payload,
      remittance.getDuplicate,
      remittance.saveDuplicateHash
    )

    if (hasDuplicateId && !hasDuplicateHash) {
      // Id was reused for a different request.
      return {
        type: PaymentPrepareResultType.MODIFIED
      }
      // Original also covers case for BULK_PREPARE, but we don't handle that here.
    }

    // If we found the payment, we can assume it was a duplicate!
    const payment = await remittance.getByIdLight(input.payload.transferId)
    if (payment && payment.transferStateEnumeration) {
      switch (payment.transferStateEnumeration) {
        case TransferStateEnum.ABORTED: {
          return {
            type: PaymentPrepareResultType.DUPLICATE_FINAL,
            finalizedTransfer: {
              completedTimestamp: payment.completedTimestamp,
              transferState: payment.transferStateEnumeration,
            }
          }
        }
        case TransferStateEnum.COMMITTED:
        case TransferStateEnum.RESERVED: {
          return {
            type: PaymentPrepareResultType.DUPLICATE_FINAL,
            finalizedTransfer: {
              completedTimestamp: payment.completedTimestamp,
              transferState: payment.transferStateEnumeration,
              fulfilment: payment.fulfilment
            }
          }
        }
      }
    }

    if (hasDuplicateId) {
      return {
        type: PaymentPrepareResultType.DUPLICATE_NON_FINAL,
      }
    }

    let proxyObligation
    try {
      proxyObligation = await this.calculateProxyObligation(input.payload)
    } catch (err: any) {
      return {
        type: PaymentPrepareResultType.FAIL_VALIDATION,
        failureReasons: err.message
      }
    }

    const determiningTransferCheckResult = await remittance.checkIfDeterminingTransferExists(
      proxyObligation.payloadClone,
      proxyObligation
    )

    let validationResult
    if (determiningTransferCheckResult.determiningTransferExistsInWatchList) {
      validationResult = await this.validatePayloadLinkedPayment(proxyObligation.payloadClone)
    } else {
      validationResult = await this.validatePayloadUnlinkedPayment(proxyObligation.payloadClone)
    }

    // In case the payee/payer are not 'in scheme', the proxyObligation payload clone has rewritten
    // the payer/payee to be the proxy payee/payer, so we check _this_ payload.
    // We might want to rewrite this validation, to be aware of native vs non-native payment.
    assert(validationResult)
    if (validationResult.result === 'FAIL') {
      assert(validationResult.reasons.length > 0)
      // Save the request even if it failed validation.
      // This call fails when the participants don't exist.
      await remittance.savePreparedRequest(
        input.payload,
        validationResult.reasons.toString(),
        false,
        // The types in cyril.js are incorrect.
        determiningTransferCheckResult as any,
        proxyObligation,
      )

      return {
        type: PaymentPrepareResultType.FAIL_VALIDATION,
        failureReasons: validationResult.reasons
      }
    }
    assert(validationResult.result === 'PASS')
    assert(validationResult.reasons.length === 0)

    // Save the payment as successfully prepared.
    await remittance.savePreparedRequest(
      input.payload,
      null,
      true,
      // The types in cyril.js are incorrect.
      determiningTransferCheckResult as any,
      proxyObligation,
    )

    // Reserve the funds from the payer.
    switch (this.deps.config.HANDLERS_TRANSFER_POSITION_FUSE) {
      // Legacy logic: fire and forget.
      case 'UNFUSE': {
        await this.sendMessagePosition2(input, determiningTransferCheckResult, proxyObligation)
        return {
          type: PaymentPrepareResultType.PASS
        }
      }

      // New logic - process the prepare synchronously.
      case "FUSE": {
        const result = await this.positionPrepare2(
          input, determiningTransferCheckResult, proxyObligation
        )

        // TODO: emit the notifications.
        // TODO: commit Kafka offsets.

        let type: PaymentPrepareResultType
        switch (result.type) {
          case PositionPrepareResultType.PASS:
            return {
              type: PaymentPrepareResultType.PASS
            }
          case PositionPrepareResultType.FAIL_LIQUIDITY:
            return {
              type: PaymentPrepareResultType.FAIL_LIQUIDITY,
              error: new Error('TODO: some liquidity error')
            }
          case PositionPrepareResultType.FAIL_VALIDATION:
            return {
              type: PaymentPrepareResultType.FAIL_VALIDATION,
              failureReasons: result.failureReasons
            }
          case PositionPrepareResultType.FAIL_OTHER:
            return {
              type: PaymentPrepareResultType.FAIL_OTHER,
              error: new Error('TODO: some other error.')
            }
        }
      }
    }
  }



  /**
   * Validate the payment for a simple Payment with no 'determiningTransfers'.
   * 
   * The structure of the input has been extacted and parsed, now we validate the 
   * message itself.
   */
  private async validatePayloadUnlinkedPayment(payload: CreatePaymentDto): Promise<{
    reasons: Array<string>,
    result: 'PASS' | 'FAIL'
  }> {
    const reasons: Array<string> = []
    const [leftStr, rightStr = ''] = payload.amount.amount.split('.')
    assert(leftStr !== undefined)
    assert(rightStr !== undefined)
    if (rightStr.length > this.deps.config.AMOUNT.SCALE) {
      reasons.push(
        `Amount ${payload.amount.amount} exceeds allowed scale of ${this.deps.config.AMOUNT.SCALE}`
      )
    }
    const precision = leftStr.length + rightStr.length
    if (precision > this.deps.config.AMOUNT.PRECISION) {
      reasons.push(
        `Amount ${precision} exceeds allowed precision of ${this.deps.config.AMOUNT.PRECISION}`
      )
    }

    // TODO: I think there should be a check for determiningTransferCheckResult
    // watch list? But that feels like it doesn't belong here.

    const participantPayer = await Participant.getByName(payload.payerFsp)
    if (!participantPayer) {
      reasons.push(`Participant ${payload.payerFsp} not found`)
    }
    if (participantPayer && !participantPayer.isActive) {
      reasons.push(`Participant ${payload.payerFsp} is inactive`)
    }
    const participantPayee = await Participant.getByName(payload.payeeFsp)
    if (!participantPayee) {
      reasons.push(`Participant ${payload.payerFsp} not found`)
    }
    if (participantPayee && !participantPayee.isActive) {
      reasons.push(`Participant ${payload.payerFsp} is inactive`)
    }

    if (!this.deps.config.ENABLE_ON_US_TRANSFERS) {
      if (payload.payerFsp === payload.payeeFsp) {
        reasons.push(
          'Payer FSP and Payee FSP should be different, unless on-us tranfers are allowed by the Scheme'
        )
      }
    }
    if (!payload.condition) {
      reasons.push('Condition is required for a conditional transfer')
    } else {
      const buffer = Buffer.from(payload.condition, 'base64')
      if (buffer.length !== 32) {
        logger.info(`validateInput() condition validation failed.`)
        reasons.push('Condition validation failed')
      }
    }

    if (!payload.expiration) {
      reasons.push('Expiration is required for conditional transfer')
    } else {
      if (Date.parse(payload.expiration) < Date.parse(new Date().toISOString())) {
        reasons.push(`Expiration date ${new Date(payload.expiration).toISOString()} is already in the past`)
      }
    }

    return {
      reasons,
      result: reasons.length === 0 ? 'PASS' : 'FAIL'
    }
  }

  /**
   * Validate the payment for a simple Payment with 'determiningTransfers'.
   * 
   * The structure of the input has been extacted and parsed, now we validate the 
   * message itself.
   */
  private async validatePayloadLinkedPayment(payload: CreatePaymentDto): Promise<{
    reasons: Array<string>,
    result: 'PASS' | 'FAIL'
  }> {
    const reasons: Array<string> = []
    const [leftStr, rightStr = ''] = payload.amount.amount.split('.')
    assert(leftStr)
    assert(rightStr)
    if (rightStr.length > this.deps.config.AMOUNT.SCALE) {
      reasons.push(
        `Amount ${payload.amount.amount} exceeds allowed scale of ${this.deps.config.AMOUNT.SCALE}`
      )
    }
    const precision = leftStr.length + rightStr.length
    if (precision > this.deps.config.AMOUNT.PRECISION) {
      reasons.push(
        `Amount ${precision} exceeds allowed precision of ${this.deps.config.AMOUNT.PRECISION}`
      )
    }

    // TODO: I think there should be a check for determiningTransferCheckResult
    // watch list? But that feels like it doesn't belong here.
    const participantPayer = await Participant.getByName(payload.payerFsp)
    if (!participantPayer) {
      reasons.push(`Participant ${payload.payerFsp} not found`)
    }
    if (participantPayer && !participantPayer.isActive) {
      reasons.push(`Participant ${payload.payerFsp} is inactive`)
    }
    const participantPayee = await Participant.getByName(payload.payeeFsp)
    if (!participantPayee) {
      reasons.push(`Participant ${payload.payerFsp} not found`)
    }
    if (participantPayee && !participantPayee.isActive) {
      reasons.push(`Participant ${payload.payerFsp} is inactive`)
    }

    if (!payload.condition) {
      reasons.push('Condition is required for a conditional transfer')
    } else {
      const buffer = Buffer.from(payload.condition, 'base64')
      if (buffer.length !== 32) {
        logger.info(`validateInput() condition validation failed.`)
        reasons.push('Condition validation failed')
      }
    }

    if (!payload.expiration) {
      reasons.push('Expiration is required for conditional transfer')
    } else {
      if (Date.parse(payload.expiration) < Date.parse(new Date().toISOString())) {
        reasons.push(`Expiration date ${new Date(payload.expiration).toISOString()} is already in the past`)
      }
    }

    return {
      reasons,
      result: reasons.length === 0 ? 'PASS' : 'FAIL'
    }
  }

  private extractMessageData(message: any): FusedPrepareHandlerInput {
    assert(message)
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.headers)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.action)
    const payloadEncoded = message.value.content.payload
    const payload = decodePayload(payloadEncoded, {}) as unknown as CreatePaymentDto
    const headers = message.value.content.headers

    const transferId = payload.transferId

    const action = message.value.metadata.event.action
    assert.equal(action, 'prepare')

    // TODO: how does this work with the proxy rewrite?
    assert(headers['fspiop-source'] === payload.payerFsp, 'FSPIOP-Source header should match Payer')
    return {
      message,
      payload,
      headers,
      transferId,
      action,
      metric: `handler_transfers_${action.toLowerCase()}`,
      functionality: Enum.Events.Event.Type.TRANSFER,
      actionEnum: this.getActionEnum(action)
    };
  }

  private getActionEnum(action: string): string {
    const actionUpper = action.toUpperCase() as keyof typeof Enum.Events.Event.Action
    return Enum.Events.Event.Action[actionUpper] || actionUpper;
  }

  private sendMessagePosition2 = async (
    input: FusedPrepareHandlerInput,
    determiningTransferCheckResult: any,
    proxyObligation: any
  ): Promise<void> => {
    // Shortcut.
    const config = this.deps.config

    // Forward the payment to the position handler.
    const params: KafkaParams<CreatePaymentDto> = {
      message: input.message,
      kafkaTopic: input.message.topic,
      decodedPayload: input.payload,
      span: null,
      consumer: Consumer,
      producer: Producer
    }

    const { messageKey, cyrilResult } = await this.deps.definePositionParticipant({
      payload: proxyObligation.payloadClone,
      isFx: false,
      determiningTransferCheckResult,
      proxyObligation
    })

    params.message.value.content.context = {
      ...params.message.value.content.context,
      cyrilResult
    }


    await Kafka.proceed(config.KAFKA_CONFIG, params, {
      consumerCommit: true,
      eventDetail: {
        functionality: Type.POSITION,
        action: Action.PREPARE
      },
      messageKey,
      topicNameOverride: config.KAFKA_CONFIG.EVENT_TYPE_ACTION_TOPIC_MAP?.POSITION?.PREPARE,
      hubName: config.HUB_NAME
    })
    return
  }

  private async positionPrepare2(
    input: FusedPrepareHandlerInput,
    determiningTransferCheckResult: any,
    proxyObligation: any
  ): Promise<PositionPrepareResult> {




    

    return {
      type: PositionPrepareResultType.PASS,
      notifications: []
    }
  }

  // TODO: this should return some notification messages to be passed to the 
  // notification handler, and pass on errors!
  private async positionPrepare(input: FusedPrepareHandlerInput,
    determiningTransferCheckResult: any,
    proxyObligation: any): Promise<void> {
    // I think we should just hard code this. In the future we won't be able to support payments
    // without changing the position.
    const changePositions = true

    const trx = await BatchPositionModel.startDbTransaction()
    const transferId = input.transferId

    // get the currencyId from the currency string
    // const currencyId = await 
    const { messageKey, cyrilResult } = await this.deps.definePositionParticipant({
      payload: proxyObligation.payloadClone,
      isFx: false,
      determiningTransferCheckResult,
      proxyObligation
    })
    const accountID = messageKey
    assert(typeof accountID === 'string', 'Expected typeof `accountID` to be `string`.')
    assert(accountID !== '0', `Expected accountId to !== '0'.`)

    const accountPayer = await Participant.getAccountByNameAndCurrency(
      input.payload.payerFsp,
      input.payload.amount.currency,
      Enum.Accounts.LedgerAccountType.POSITION
    )
    const allSettlementModels = await SettlementModelCached.getAll()
    const allParticipantCurrencyIds = await BatchPositionModelCached
      .getParticipantCurrencyByParticipantIds(trx, [accountPayer.participantId])

    assert(allParticipantCurrencyIds, 'Expected allParticipantCurrencyIds to be defined.')
    assert(allParticipantCurrencyIds.length === 2, 'Expected 2 allParticipantCurrencyIds.')

    // Get the position (obtain a lock).
    // const { settlementCurrencyIds, accountIdMap } = await _constructRequiredMaps(participantCurrencyIds, allSettlementModels, trx)
    const positions = await BatchPositionModel.getPositionsByAccountIdsForUpdate(trx, [
      ...allParticipantCurrencyIds.map(currency => currency.participantCurrencyId)
    ])
    assert(positions)
    const settlementAccount = allParticipantCurrencyIds
      .find(currency => currency.ledgerAccountTypeId === Enum.Accounts.LedgerAccountType.SETTLEMENT)
    assert(settlementAccount, 'No settlementAccount found.')
    const settlementPositionAccount = positions[settlementAccount.participantCurrencyId]
    // get the participant limit
    const participantLimit = await ParticipantFacade.getParticipantLimitByParticipantCurrencyLimit(
      accountPayer.participantId,
      input.payload.amount.currency,
      Enum.Accounts.LedgerAccountType.POSITION,
      Enum.Accounts.ParticipantLimitType.NET_DEBIT_CAP
    )
    let accumulatedTransferStates: Record<string, string> = {}
    accumulatedTransferStates[input.transferId] = 'RECEIVED_PREPARE'
    // transform into something so we can call prepare.processPositionPrepareBin
    // take the prepare result and do something with it.
    const prepareActionResult = await PositionPrepareDomain.processPositionPrepareBin(
      [{
        decodedPayload: input.payload,
        message: {
          value: {
            content: {
              context: {
                cyrilResult
              }
            }
          }
        }
      }],
      {
        accumulatedPositionValue: 0,
        accumulatedPositionReservedValue: 0,
        accumulatedTransferStates,
        // How do we get this?
        settlementParticipantPosition: settlementPositionAccount.value,
        participantLimit,
        changePositions
      }
    )
    // Update accumulated position values by calling a facade function
    // LD: I don't understand this, since positions[accountID].participantPositionId,
    // would only apply to one `positions[accountID].participantPositionId`.

    // await BatchPositionModel.updateParticipantPosition(
    //   trx,
    //   positions[accountID].participantPositionId,
    //   accumulatedPositionValue,
    //   accumulatedPositionReservedValue
    // )
    await BatchPositionModel.bulkInsertTransferStateChanges(trx, prepareActionResult.accumulatedTransferStates)
    const fetchedTransferStateChanges = await BatchPositionModel.
      getLatestTransferStateChangesByTransferIdList(
        trx,
        prepareActionResult.accumulatedTransferStateChanges.map(item => item.transferId)
      )


    if (changePositions) {
      // Mutate accumulated positionChanges with transferStateChangeIds and fxTransferStateChangeIds
      for (const positionChange of prepareActionResult.accumulatedPositionChanges) {
        positionChange.transferStateChangeId = fetchedTransferStateChanges[positionChange.transferId].transferStateChangeId
        // TODO: need to rewrite these somehow.
        // positionChange.participantPositionId = position.participantPositionId
        // positionChange.participantCurrencyId = accountID
      }

      // Bulk insert accumulated positionChanges by calling a facade function.
      await BatchPositionModel.bulkInsertParticipantPositionChanges(
        trx, prepareActionResult.accumulatedPositionChanges,
      )
    }
  }

  /**
   * @description Figure out if the participants in the Payment message are native to the scheme
   * or are proxies.
   */
  private async calculateProxyObligation(payload: CreatePaymentDto): Promise<ProxyObligation> {
    // If the proxy isn't enabled, just return the default.
    if (!this.deps.config.PROXY_CACHE_CONFIG.enabled) {
      return {
        isFx: false,
        payloadClone: { ...payload },  // just a copy of the original payload
        isInitiatingFspProxy: false,
        isCounterPartyFspProxy: false,
        initiatingFspProxyOrParticipantId: {
          inScheme: true, proxyId: null, name: payload.payerFsp
        },
        counterPartyFspProxyOrParticipantId: {
          inScheme: true, proxyId: null, name: payload.payeeFsp
        }
      }
    }

    // We need to double check the following validation logic incase of payee side currency conversion
    const payerResult = await this.deps.proxyCache.getFSPProxy(payload.payerFsp)
    const payeeResult = await this.deps.proxyCache.getFSPProxy(payload.payeeFsp, {
      validateCurrencyAccounts: true,
      accounts: [
        {
          currency: payload.amount.currency,
          accountType: Enum.Accounts.LedgerAccountType.POSITION
        }
      ]
    })
    assert(payerResult)
    assert(payeeResult)

    // Validate the not found case.
    if (payerResult.inScheme === false && payerResult.proxyId === null) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
        `Payer proxy or participant not found: payer: ${payload.payerFsp}.`
      )
      throw fspiopError
    }
    if (payeeResult.inScheme === false && payeeResult.proxyId === null) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
        `Payee proxy or participant not found: payee: ${payload.payeeFsp}.`
      )
      throw fspiopError
    }

    const isInitiatingFspProxy = !payerResult.inScheme && payerResult.proxyId !== null
    const isCounterPartyFspProxy = !payeeResult.inScheme && payeeResult.proxyId !== null

    return {
      isFx: false,
      payloadClone: {
        ...payload,
        // Reroute the proxies.
        payerFsp: isInitiatingFspProxy ? payerResult.proxyId! : payload.payerFsp,
        payeeFsp: isCounterPartyFspProxy ? payeeResult.proxyId! : payload.payeeFsp
      },
      isInitiatingFspProxy,
      isCounterPartyFspProxy,
      initiatingFspProxyOrParticipantId: payerResult,
      counterPartyFspProxyOrParticipantId: payeeResult
    }
  }
}


/**
 * TODO: when we're ready to add back the non-happy path notifications:
 */
// private async sendMessageNotification(
//     input: FusedPrepareHandlerInput,
//     opts: {
//       action: string
//       fspiopError?: {
//         errorInformation: {
//           errorCode: string
//           errorDescription: string
//         }
//       }
//       payload?: any  // Override payload (e.g. for duplicate with fulfil info)
//     }
//   ): Promise<void> {
//     const config = this.deps.config
//     const params = {
//       message: input.message,
//       kafkaTopic: input.message.topic,
//       decodedPayload: input.payload,
//       span: null,
//       consumer: Consumer,
//       producer: Producer
//     }

//     if (opts.payload) {
//       params.message.value.content.payload = opts.payload
//       params.message.value.content.uriParams = { id: input.transferId }
//     }

//     await Kafka.proceed(config.KAFKA_CONFIG, params, {
//       consumerCommit: true,
//       fspiopError: opts.fspiopError,
//       eventDetail: {
//         functionality: Type.NOTIFICATION,
//         action: opts.action
//       },
//       fromSwitch: true,
//       hubName: config.HUB_NAME
//     })
//   }

//   Usage:

//   // Modified request
//   await this.sendMessageNotification(input, {
//     action: Action.PREPARE,
//     fspiopError: ErrorHandler.Factory.createFSPIOPError(
//       ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST
//     ).toApiErrorObject(config.ERROR_HANDLING)
//   })

//   // Duplicate finalized
//   await this.sendMessageNotification(input, {
//     action: Action.PREPARE_DUPLICATE,
//     payload: {
//       completedTimestamp: payment.completedTimestamp,
//       transferState: payment.transferStateEnumeration,
//       fulfilment: payment.fulfilment
//     }
//   })

//   // Validation error
//   await this.sendMessageNotification(input, {
//     action: Action.PREPARE,
//     fspiopError: ErrorHandler.Factory.createFSPIOPError(
//       ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
//       reasons.toString()
//     ).toApiErrorObject(config.ERROR_HANDLING)
//   })

//   // ID not found
//   await this.sendMessageNotification(input, {
//     action: Action.PREPARE,
//     fspiopError: ErrorHandler.Factory.createFSPIOPError(
//       ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
//       errorMessage
//     ).toApiErrorObject(config.ERROR_HANDLING)
//   })