import assert from "node:assert";
import { ApplicationConfig } from "../lib/config";
import { logger } from '../shared/logger';
import CentralServicesShared, { Enum, TransferStateEnum, Util } from '@mojaloop/central-services-shared';
const { Kafka, Comparators } = Util
import { createRemittanceEntityPayment } from "./transfers/createRemittanceEntity";
const { decodePayload } = Util.StreamingProtocol
const Validator = require('./transfers/validator')
const Participant = require('../domain/participant')

const { Consumer, Producer } = require('@mojaloop/central-services-stream').Util
const { Type, Action } = Enum.Events.Event


const ErrorHandler = require('@mojaloop/central-services-error-handling')

const { FSPIOPErrorCodes } = ErrorHandler.Enums
const { createFSPIOPError, reformatFSPIOPError } = ErrorHandler.Factory
const { FSPIOPError } = ErrorHandler


interface Dependencies {
  config: ApplicationConfig
}

interface KafkaParams {
  message: any
  kafkaTopic: string
  decodedPayload: CreateTransferDto
  span: any
  consumer: any
  producer: any
}

export interface CreateTransferDto {
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
  payload: CreateTransferDto;
  headers: any;
  transferId: string;
  action: any;
  metric: string;
  functionality: CentralServicesShared.EventTypeEnum.TRANSFER;
  actionEnum: string;
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

    logger.debug(`PaymentPrepareHandler.handle() - processing batch of ${messages.length} messages`)

    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));

    const results = await Promise.allSettled(inputs.map(async ({ input }) => this.handleOne(input)))
    return results
  }

  async handleOne(input: FusedPrepareHandlerInput): Promise<PaymentPrepareResult> {
    // Check Duplication
    const remittance = createRemittanceEntityPayment()
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

    if (hasDuplicateId) {
      return {
        type: PaymentPrepareResultType.DUPLICATE_NON_FINAL,
      }
    }

    // TODO: we need to reimplement the proxyObligation stuff.
    // It's too confusing for now, I think it will be easier to rip it out and 
    // put it back.

    const proxyObligation = {
      isFx: false,
      payloadClone: { ...input.payload },  // just a copy of the original payload
      isInitiatingFspProxy: false,
      isCounterPartyFspProxy: false,
      initiatingFspProxyOrParticipantId: null,
      counterPartyFspProxyOrParticipantId: null
    }

    const determiningTransferCheckResult = {
      determiningTransferExistsInWatchList: false,
      watchListRecords: [],
      participantCurrencyValidationList: [
        { participantName: input.payload.payerFsp, currencyId: input.payload.amount.currency },
        { participantName: input.payload.payeeFsp, currencyId: input.payload.amount.currency }
      ]
    }

    const validationResult = await this.validateInput(input)
    if (validationResult.result === 'FAIL') {
      assert(validationResult.reasons.length > 0)
      // Save the request even if it failed validation.
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

    // Forward the payment to the position handler.
    const params: KafkaParams = {
      message: input.message,
      kafkaTopic: input.message.topic,
      decodedPayload: input.payload,
      span: null,
      consumer: Consumer,
      producer: Producer
    };
    await this.sendPositionMessage(input.payload, params)

    return {
      type: PaymentPrepareResultType.PASS
    }
  }

  /**
   * The structure of the input has been extacted and parsed, now we validate the 
   * message itself.
   */
  private async validateInput(input: FusedPrepareHandlerInput): Promise<{
    reasons: Array<string>,
    result: 'PASS' | 'FAIL'
  }> {
    // Shortcut.
    const { headers, payload } = input
    const reasons: Array<string> = []
    if (headers['fspiop-source'] === payload.payerFsp) {
      reasons.push('FSPIOP-Source header should match Payer')
    }
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

    if (!this.deps.config.ENABLE_ON_US_TRANSFERS) {
      if (payload.payerFsp !== payload.payeeFsp) {
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

  private extractMessageData(message: any): FusedPrepareHandlerInput {
    // TODO: Validate the messages
    // Assert that isFx = false, isForwarded = false
    assert(message)
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.headers)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.action)
    const payloadEncoded = message.value.content.payload
    const payload = decodePayload(payloadEncoded, {}) as unknown as CreateTransferDto
    const headers = message.value.content.headers

    const transferId = payload.transferId

    const action = message.value.metadata.event.action
    // Note: we currently only support prepare messages
    assert.equal(action, 'prepare')

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

  /**
   * TODO: this will eventually be removed when we migrate to the fused handlers.
   */
  private sendPositionMessage = async (
    payload: CreateTransferDto,
    params: KafkaParams,
  ): Promise<void> => {
    // Shortcut.
    const config = this.deps.config
    const participantName = payload.payerFsp;
    const currencyId = payload.amount.currency;

    // Get payer's position account ID for message routing
    const account = await Participant.getAccountByNameAndCurrency(
      participantName,
      currencyId,
      Enum.Accounts.LedgerAccountType.POSITION
    );
    const messageKey = account.participantCurrencyId.toString();

    params.message.value.content.context = {
      ...params.message.value.content.context,
      cyrilResult: {
        participantName,
        currencyId,
        amount: payload.amount.amount
      }
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
  }
}
