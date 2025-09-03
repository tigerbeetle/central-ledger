import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import { Enum } from '@mojaloop/central-services-shared';
import assert from "assert";
import { MessageContext, PositionKafkaMessage, PreparedMessage, PreparePositionsBatchResult } from "src/handlers-v2/PositionHandler";
import { DefinePositionParticipantResult, DuplicationCheckResult, Location, TransferCheckResult, ValidationResult } from "src/handlers-v2/PrepareHandler";
import { CommitTransferDto, CreateTransferDto } from "src/handlers-v2/types";
import { ProxyObligation } from "src/handlers/transfers/prepare";
import { ApplicationConfig } from "src/shared/config";
import { logger } from '../../shared/logger';
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { FusedFulfilHandlerInput } from 'src/handlers-v2/FusedFulfilHandler';


export enum PrepareResultType {
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

export enum PrepareDuplicateResult {
  /**
   * Transfer id is unique
   */
  UNIQUE = 'UNIQUE',

  /**
   * Transfer Id is the same, body is different
   */
  MODIFIED = 'MODIFIED',

  /**
   * Transfer Id is the same, body is the same
   */
  DUPLICATED = 'DUPLICATED'
}

export enum FulfilDuplicateResult {
  /**
   * Message is unique
   */
  UNIQUE = 'UNIQUE',

  /**
   * Transfer Id is the same, body is different
   */
  MODIFIED = 'MODIFIED',

  /**
   * Transfer Id is the same, body is the same
   */
  DUPLICATED = 'DUPLICATED'
}

export enum FulfilResultType {
  /**
   * Fulfil step completed validation
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
   * Transfer failed validation
   */
  FAIL_VALIDATION = 'FAIL_VALIDATION',

  // /**
  //  * Transfer failed as payee didn't have sufficent liquidity
  //  */
  // FAIL_LIQUIDITY = 'FAIL_LIQUIDITY',

  /**
   * Catch-all Transfer failed for another reason
   */
  FAIL_OTHER = 'FAIL_OTHER',
}

export type PrepareResult = PrepareResultPass
  | PrepareResultDuplicateFinal
  | PrepareResultDuplicateNonFinal
  | PrepareResultFailValidation
  | PrepareResultFailLiquidity
  | PrepareResultFailOther

export interface PrepareResultPass {
  type: PrepareResultType.PASS
}

export interface PrepareResultDuplicateFinal {
  type: PrepareResultType.DUPLICATE_FINAL,
  // TODO(LD): add types to this!
  finalisedTransfer: any
}

export interface PrepareResultDuplicateNonFinal {
  type: PrepareResultType.DUPLICATE_NON_FINAL,
}

export interface PrepareResultFailValidation {
  type: PrepareResultType.FAIL_VALIDATION,
  failureReasons: Array<string>
}

export interface PrepareResultFailLiquidity {
  type: PrepareResultType.FAIL_LIQUIDITY,
  fspiopError: any,
}
export interface PrepareResultFailOther {
  type: PrepareResultType.FAIL_OTHER,
  fspiopError: any,
}

export type FulfilResult = FulfilResultPass
  | FulfilResultDuplicateFinal
  | FulfilResultFailValidation
  | FulfilResultFailOther

export interface FulfilResultPass {
  type: FulfilResultType.PASS
}

// TODO(LD): I don't know if we need to distinguish between final and non final
// for the duplicated fulfils
export interface FulfilResultDuplicateFinal {
  type: FulfilResultType.DUPLICATE_FINAL,
}

export interface FulfilResultFailValidation {
  type: FulfilResultType.FAIL_VALIDATION,
  fspiopError: any,
}

export interface FulfilResultFailOther {
  type: FulfilResultType.FAIL_OTHER,
  fspiopError: any,
}



export interface LegacyCompatibleLedgerDependencies {
  config: ApplicationConfig

  // Business logic dependencies - injected from existing modules
  // TODO: type all of these and simplify!
  validator: {
    validatePrepare: (payload: CreateTransferDto, headers: any, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation) => Promise<ValidationResult>;
    validateParticipantByName: (participantName: string) => Promise<boolean>;
    validatePositionAccountByNameAndCurrency: (participantName: string, currency: string) => Promise<boolean>;
    reasons: string[];
    [key: string]: any;
  };
  transferService: {
    handlePayeeResponse: any,
    getById: (transferId: string) => Promise<any>
    getTransferInfoToChangePosition: (thing1: any, thing2: any, thing3: any) => Promise<any>
    getTransferFulfilmentDuplicateCheck: any,
    saveTransferFulfilmentDuplicateCheck: any,
  };

  proxyCache: any;
  comparators: any;
  createRemittanceEntity: any;
  transferObjectTransform: any;
  checkDuplication: (args: {
    payload: CreateTransferDto,
    isFx: boolean,
    ID: string,
    location: Location
  }) => Promise<DuplicationCheckResult>;
  savePreparedRequest: (args: {
    validationPassed: boolean,
    reasons: string[],
    payload: CreateTransferDto,
    isFx: boolean,
    functionality: any,
    params: any,
    location: Location,
    determiningTransferCheckResult: TransferCheckResult,
    proxyObligation: ProxyObligation
  }) => Promise<void>;
  definePositionParticipant: (args: {
    payload: CreateTransferDto,
    isFx: boolean,
    determiningTransferCheckResult: TransferCheckResult,
    proxyObligation: ProxyObligation
  }) => Promise<DefinePositionParticipantResult>;
  getByIDAndCurrency: (thing1: any, thing2: any, thing3: any) => Promise<any>
  calculatePreparePositionsBatch: (transferList: PositionKafkaMessage[]) => Promise<PreparePositionsBatchResult>;
  changeParticipantPosition: (participantCurrencyId: string, isReversal: boolean, amount: string, transferStateChange: any) => Promise<any>;
  getAccountByNameAndCurrency: (participantName: string, currency: string) => Promise<{ currencyIsActive: boolean }>
}

export default class LegacyCompatibleLedger {
  constructor(private deps: LegacyCompatibleLedgerDependencies) {

  }

  public async prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult> {
    const { payload, transferId, headers } = input;
    logger.debug(`prepare() - transferId: ${transferId}`)

    const duplicateResult = await this.checkPrepareDuplicate(payload, transferId)
    switch (duplicateResult) {
      case PrepareDuplicateResult.DUPLICATED: {
        const transfer = await this.deps.transferService.getById(transferId)
        assert(transfer.transferStateEnumeration)
        const finalizedStates = [
          Enum.Transfers.TransferState.COMMITTED,
          Enum.Transfers.TransferState.ABORTED,
          Enum.Transfers.TransferState.RESERVED
        ];

        if (finalizedStates.includes(transfer.transferStateEnumeration)) {
          const payload = this.deps.transferObjectTransform.toFulfil(transfer, false)
          return {
            type: PrepareResultType.DUPLICATE_FINAL,
            finalisedTransfer: payload,
          }
        }

        return {
          type: PrepareResultType.DUPLICATE_NON_FINAL
        }
      }
      case PrepareDuplicateResult.MODIFIED: {
        return {
          type: PrepareResultType.FAIL_OTHER,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST),
        }
      }
      case PrepareDuplicateResult.UNIQUE:
      default: { }
    }

    // Validate participants and their currency accounts
    const participantValidation = await this.validateParticipants(payload)
    if (!participantValidation.validationPassed) {
      return {
        type: PrepareResultType.FAIL_VALIDATION,
        failureReasons: participantValidation.reasons
      };
    }

    // Save the transfer, even if it's invalid
    const transferValidationResult = await this.validateTransfer(payload, headers)
    await this.saveTransfer(payload, transferValidationResult)

    if (!transferValidationResult.validationPassed) {
      return {
        type: PrepareResultType.FAIL_VALIDATION,
        failureReasons: transferValidationResult.reasons,
      }
    }

    // TODO(LD): this is really ugly, but the original method needs a lot of kafka context,
    // so for compatibility we are going to keep it this way for now.
    //
    // Ideally we would refactor the positions to not require all of this Kafka context
    const messageContext = LegacyCompatibleLedger.extractMessageContext(input);
    const { preparedMessagesList } = await this.calculatePreparePositions(payload, messageContext)
    assert(Array.isArray(preparedMessagesList))
    assert(preparedMessagesList.length === 1)

    // Process the prepared messages results
    const prepareMessage: PreparedMessage = preparedMessagesList[0];
    const { transferState, fspiopError } = prepareMessage;

    if (transferState.transferStateId !== Enum.Transfers.TransferState.RESERVED) {
      logger.info(`prepare() - Position prepare failed - insufficient liquidity for transfer: ${transferId}`);

      return {
        type: PrepareResultType.FAIL_LIQUIDITY,
        fspiopError
      }
    }

    logger.debug(`prepare() - Position prepare successful - funds reserved for transfer: ${transferId}`);
    return {
      type: PrepareResultType.PASS
    }
  }

  public async fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    const { payload, transferId, headers } = input;
    logger.debug(`fulfil() - transferId: ${transferId}`)

    if (input.action === Enum.Events.Event.Action.ABORT) {
      throw new Error(`not implemented`)
    }

    try {
      // TODO(LD): we changed the order of processing here to include the condition
      // which might change some of the error messages
      await this.validateFulfilMessage(input)
    } catch (err) {
      return {
        type: FulfilResultType.FAIL_VALIDATION,
        fspiopError: err
      }
    }

    const duplicateResult = await this.checkFulfilDuplicate(payload, transferId)
    switch (duplicateResult) {
      case FulfilDuplicateResult.DUPLICATED: {
        return {
          type: FulfilResultType.DUPLICATE_FINAL
        }
      }
      case FulfilDuplicateResult.MODIFIED: {
        return {
          type: FulfilResultType.FAIL_OTHER,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST,
            'Transfer fulfil has been modified'
          ),
        }
      }
      case FulfilDuplicateResult.UNIQUE:
      default: { }
    }

    // save the fulfil response
    await this.deps.transferService.handlePayeeResponse(transferId, payload, input.action);

    // Update the positions
    logger.info(`Processing position commit for transfer: ${transferId}`);
    try {
      // Get transfer info to change position for PAYEE
      const transferInfo = await this.deps.transferService.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info
      const participantCurrency = await this.deps.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // Validate transfer state - must be RECEIVED_FULFIL
      if (transferInfo.transferStateId !== Enum.Transfers.TransferInternalState.RECEIVED_FULFIL) {
        const expectedState = Enum.Transfers.TransferInternalState.RECEIVED_FULFIL;
        const fspiopError = ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Invalid State: ${transferInfo.transferStateId} - expected: ${expectedState}`
        );

        logger.error(`Position commit validation failed - invalid state for transfer: ${transferId}`, {
          currentState: transferInfo.transferStateId,
          expectedState
        });

        return {
          type: FulfilResultType.FAIL_OTHER,
          fspiopError
        }
      }

      logger.info(`Position commit validation passed for transfer: ${transferId}`);

      // Change participant position (not a reversal for commit)
      const isReversal = false;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferState.COMMITTED
      };

      await this.deps.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      logger.info(`Position commit processed successfully for transfer: ${transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });

      return {
        type: FulfilResultType.PASS
      }

    } catch (error) {
      logger.error(`Position commit failed for transfer: ${transferId}`, { error: error.message });
      return {
        type: FulfilResultType.FAIL_OTHER,
        fspiopError: error
      }
    }
  }

  private async validateFulfilMessage(input: FusedFulfilHandlerInput): Promise<void> {
    const { transferId, payload, message: { value: { from } }, headers } = input;

    // make sure the sender exists
    if (!await this.deps.validator.validateParticipantByName(from)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND, 'Participant not found');
    }

    // Get transfer details
    const transfer = await this.deps.transferService.getById(transferId);
    if (!transfer) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
    }

    if (!await this.deps.validator.validateParticipantTransferId(from, transferId)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant not associated with transfer');
    }

    if (headers[Enum.Http.Headers.FSPIOP.SOURCE].toLowerCase() !== transfer.payeeFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Source header does not match transfer payee');
    }

    if (headers[Enum.Http.Headers.FSPIOP.DESTINATION].toLowerCase() !== transfer.payerFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Destination header does not match transfer payer');
    }

    assert(payload.fulfilment, 'payload.fulfilment not found')
    if (!this.deps.validator.validateFulfilCondition(payload.fulfilment, transfer.condition)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Invalid fulfilment');
    }

  }


  /**
   * Shim Methods to improve usability before refactoring
   */
  private async checkPrepareDuplicate(payload: CreateTransferDto, transferId: string): Promise<PrepareDuplicateResult> {
    const checkDuplicateResult = await this.deps.checkDuplication({
      payload,
      isFx: false,
      ID: transferId,
      location: { module: 'PrepareHandler', method: 'checkDuplication', path: '' }
    });

    if (checkDuplicateResult.hasDuplicateHash && checkDuplicateResult.hasDuplicateId) {
      return PrepareDuplicateResult.DUPLICATED
    }

    if (checkDuplicateResult.hasDuplicateId) {
      return PrepareDuplicateResult.MODIFIED
    }

    // transfers should be unique
    assert(checkDuplicateResult.hasDuplicateHash === false)
    return PrepareDuplicateResult.UNIQUE
  }

  private async checkFulfilDuplicate(payload: CommitTransferDto, transferId: string): Promise<FulfilDuplicateResult> {
    const checkDuplicateResult = await this.deps.comparators.duplicateCheckComparator(
      transferId,
      payload,
      this.deps.transferService.getTransferFulfilmentDuplicateCheck,
      this.deps.transferService.saveTransferFulfilmentDuplicateCheck
    )

    if (checkDuplicateResult.hasDuplicateHash && checkDuplicateResult.hasDuplicateId) {
      return FulfilDuplicateResult.DUPLICATED
    }

    if (checkDuplicateResult.hasDuplicateId && !checkDuplicateResult.hasDuplicateHash) {
      return FulfilDuplicateResult.MODIFIED
    }

    return FulfilDuplicateResult.UNIQUE
  }

  private async validateParticipants(payload: CreateTransferDto): Promise<ValidationResult> {
    assert(payload)
    assert(payload.payerFsp)
    assert(payload.payeeFsp)
    assert(payload.amount)
    assert(payload.amount.currency)

    // shortcuts
    const payerId = payload.payerFsp
    const payeeId = payload.payeeFsp
    const currency = payload.amount.currency

    // First check if participants exist and are active
    const payerValid = await this.deps.validator.validateParticipantByName(payerId);
    const payeeValid = await this.deps.validator.validateParticipantByName(payeeId);

    if (!payerValid || !payeeValid) {
      return {
        validationPassed: false,
        reasons: ['payer or payee invalid']
      };
    }

    let validationPassed = true
    let reasons: Array<string> = []

    const payerAccountValid = await this.deps.validator.validatePositionAccountByNameAndCurrency(payerId, currency)
    const payeeAccountValid = await this.deps.validator.validatePositionAccountByNameAndCurrency(payeeId, currency)

    if (!payerAccountValid || !payeeAccountValid) {
      return {
        validationPassed: false,
        // TODO(LD): nasty globals here
        reasons: [this.deps.validator.reasons[0]]
      }
    }

    // TODO: reinstate this - this was failing because of import issues with the database
    // const payerAccount = await this.deps.getAccountByNameAndCurrency(payerId, currency)
    // if (!payerAccount) {
    //   validationPassed = false
    //   reasons.push(`Participant ${payerId} ${currency} account not found`)
    // }
    // if (payerAccount && payerAccount.currencyIsActive === false) {
    //   validationPassed = false
    //   reasons.push(`Participant ${payerId} ${currency} account is inactive`)
    // }

    // const payeeAccount = await this.deps.getAccountByNameAndCurrency(payeeId, currency)
    // if (!payeeAccount) {
    //   validationPassed = false
    //   reasons.push(`Participant ${payeeId} ${currency} account not found`)
    // }
    // if (payeeAccount && payerAccount.currencyIsActive === false) {
    //   validationPassed = false
    //   reasons.push(`Participant ${payeeId} ${currency} account is inactive`)
    // }

    // if (!validationPassed) {
    //   return {
    //     validationPassed,
    //     reasons
    //   };
    // }

    return {
      validationPassed: true,
      reasons: []
    }
  }

  private async validateTransfer(payload: CreateTransferDto, headers: any): Promise<ValidationResult> {
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    return await this.deps.validator.validatePrepare(
      payload,
      headers,
      isFx,
      determiningTransferCheckResult,
      proxyObligation
    );
  }

  private async saveTransfer(payload: CreateTransferDto, validation: ValidationResult): Promise<void> {
    // hardcoded for our use case
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    return await this.deps.savePreparedRequest({
      validationPassed: validation.validationPassed,
      reasons: validation.reasons,
      payload,
      isFx,
      functionality: Enum.Events.Event.Type.TRANSFER,
      params: { message: null }, // Not used in current implementation
      location: { module: 'PrepareHandler', method: 'saveTransfer', path: '' },
      determiningTransferCheckResult,
      proxyObligation
    });
  }

  private async calculatePreparePositions(
    payload: CreateTransferDto,
    messageContext: MessageContext
  ): Promise<PreparePositionsBatchResult> {
    // this.deps.calculatePreparePositionsBatch expects a whole kafka message
    // so transform the payload to one:
    const message = LegacyCompatibleLedger.createMinimalPositionKafkaMessage(payload, messageContext)
    return this.deps.calculatePreparePositionsBatch([message])
  }

  // Helper methods to create minimal objects for validation compatibility
  private createMinimalProxyObligation(payload: CreateTransferDto): ProxyObligation {
    return {
      isFx: false,
      payloadClone: { ...payload },
      isInitiatingFspProxy: false,
      isCounterPartyFspProxy: false,
      initiatingFspProxyOrParticipantId: null,
      counterPartyFspProxyOrParticipantId: null
    };
  }

  private createMinimalTransferCheckResult(): TransferCheckResult {
    return {
      watchListRecords: [],
      participantCurrencyValidationList: []
    };
  }

  /**
   * Extracts MessageContext from a PrepareMessageInput for position processing
   * @param input - The prepare message input containing the original Kafka message
   * @returns MessageContext with fields needed for position processing
   */
  static extractMessageContext(input: FusedPrepareHandlerInput): MessageContext {
    const message = input.message;

    return {
      from: message.value.from,
      to: message.value.to,
      headers: input.headers,
      action: input.action,
      eventId: message.value.metadata?.event?.id,
      eventType: message.value.metadata?.event?.type,
      messageId: message.value.id,
      messageType: message.value.type,
      trace: message.value.metadata?.trace
    };
  }

  /**
   * Creates a minimal Kafka message for position processing from a transfer DTO and context
   * @param payload - The transfer data (contains transferId, amount, payerFsp, payeeFsp, etc.)
   * @param messageContext - Additional context needed for the Kafka message structure
   * @returns A properly formatted PositionKafkaMessage for calculatePreparePositionsBatch
   */
  static createMinimalPositionKafkaMessage(
    payload: CreateTransferDto,
    messageContext: MessageContext
  ): PositionKafkaMessage {
    const now = new Date().toISOString();

    return {
      topic: 'position-prepare',
      key: payload.transferId,
      value: {
        id: messageContext.messageId || payload.transferId,
        from: messageContext.from,
        to: messageContext.to,
        type: messageContext.messageType || 'application/json',
        content: {
          headers: messageContext.headers,
          payload: payload,
          uriParams: { id: payload.transferId },
          context: {}
        },
        metadata: {
          event: {
            id: messageContext.eventId || payload.transferId,
            type: messageContext.eventType || 'position',
            action: messageContext.action,
            createdAt: now,
            state: {
              status: 'success',
              code: 0,
              description: 'action successful'
            }
          },
          trace: messageContext.trace
        }
      }
    };
  }
}