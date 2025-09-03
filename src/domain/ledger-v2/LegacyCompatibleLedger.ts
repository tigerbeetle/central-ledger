import { DefinePositionParticipantResult, DuplicationCheckResult, Location, PositionData, PrepareMessageInput, TransferCheckResult, ValidationResult } from "src/handlers-v2/PrepareHandler";
import { CreateTransferDto } from "src/handlers-v2/types";
import { ProxyObligation } from "src/handlers/transfers/prepare";
import CentralServicesShared, { Enum, Util } from '@mojaloop/central-services-shared';


export enum PrepareResultType {
  PASS = 'PASS',
  FAIL_DUPLICATE = 'FAIL_DUPLICATE',
  FAIL_VALIDATION = 'FAIL_VALIDATION',
  FAIL_TRANSIENT = 'FAIL_TRANSIENT',
}

type PrepareResult = PrepareResultPass
  | PrepareResultFailDuplicate
  | PrepareResultFailValidation

interface PrepareResultPass {
  type: PrepareResultType.PASS
}

interface PrepareResultFailDuplicate {
  type: PrepareResultType.FAIL_DUPLICATE,
  hasDuplicateHash: boolean
}

interface PrepareResultFailValidation {
  type: PrepareResultType.FAIL_VALIDATION,
  failureReasons: Array<string>
}

export interface LegacyCompatibleLedgerDependencies {
  config: any;

  // Business logic dependencies - injected from existing modules
  validator: {
    validatePrepare: (payload: CreateTransferDto, headers: any, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation) => Promise<ValidationResult>;
    [key: string]: any;
  };
  transferService: any;
  proxyCache: any;
  comparators: any;
  createRemittanceEntity: any;
  transferObjectTransform: any;

  // Business logic functions from prepare.js
  checkDuplication: (args: { payload: CreateTransferDto, isFx: boolean, ID: string, location: Location }) => Promise<DuplicationCheckResult>;
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
}

export default class LegacyCompatibleLedger {
  constructor(private deps: LegacyCompatibleLedgerDependencies) {

  }

  public async prepare(input: PrepareMessageInput): Promise<PrepareResult> {
    const { payload, transferId, headers } = input;

    const duplicateCheckResult = await this.checkForDuplicate(payload, transferId)
    if (duplicateCheckResult.hasDuplicateId) {
      return {
        type: PrepareResultType.FAIL_DUPLICATE,
        hasDuplicateHash: duplicateCheckResult.hasDuplicateHash
      }
    }

    // always save the transfer, even if it's invalid
    const validationResult = await this.validateTransfer(payload, headers)
    await this.saveTransfer(payload, validationResult)

    if (!validationResult.validationPassed) {
      return {
        type: PrepareResultType.FAIL_VALIDATION,
        failureReasons: validationResult.reasons,
      }
    }

    const positionData = await this.calculatePositionData(payload);





    // check the liquidity

    throw new Error(`not implemented`)
  }

  public async fulfil(): Promise<unknown> {
    throw new Error(`not implemented`)
  }


  /**
   * Shim Methods to improve usability before refactoring
   */
  private async checkForDuplicate(payload: CreateTransferDto, transferId: string): Promise<DuplicationCheckResult> {
    return await this.deps.checkDuplication({
      payload,
      isFx: false,
      ID: transferId,
      location: { module: 'PrepareHandler', method: 'checkDuplication', path: '' }
    });
  }

  private async validateTransfer(payload: CreateTransferDto, headers: any): Promise<ValidationResult> {
    // hardcoded for our use case
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    // Delegate to existing validator

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

  private async calculatePositionData(payload: CreateTransferDto): Promise<PositionData> {
    // hardcoded for our use case
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    const result = await this.deps.definePositionParticipant({
      payload: proxyObligation.payloadClone,
      isFx,
      determiningTransferCheckResult,
      proxyObligation
    });

    return {
      participantCurrencyId: result.messageKey,
      amount: payload.amount.amount,
      currency: payload.amount.currency,
      cyrilResult: result.cyrilResult,
      messageKey: result.messageKey
    };
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
}