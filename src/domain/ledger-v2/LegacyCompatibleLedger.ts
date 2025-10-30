import * as ErrorHandler from '@mojaloop/central-services-error-handling';
const { Enum, Util: { Time } } = require('@mojaloop/central-services-shared');
import assert from "assert";
import { FusedFulfilHandlerInput } from '../../handlers-v2/FusedFulfilHandler';
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { MessageContext, PositionKafkaMessage, PreparedMessage, PreparePositionsBatchResult } from "src/handlers-v2/PositionHandler";
import { DuplicationCheckResult, Location, TransferCheckResult, ValidationResult } from "src/handlers-v2/PrepareHandler";
import { CommitTransferDto, CreateTransferDto } from "src/handlers-v2/types";
import { ProxyObligation } from "src/handlers/transfers/prepare";
import { ApplicationConfig } from "../../shared/config";
import { logger } from '../../shared/logger';
import {
  CreateDFSPCommand,
  CreateDFSPResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCollateralCommand,
  DepositCollateralResponse,
  DFSPAccountResponse,
  FulfilDuplicateResult,
  FulfilResult,
  FulfilResultType,
  GetDFSPAccountsQuery,
  GetNetDebitCapQuery,
  LegacyLedgerAccount,
  NetDebitCapResponse,
  ParticipantServiceAccount,
  ParticipantWithCurrency,
  PayeeResponsePayload,
  PrepareDuplicateResult,
  PrepareResult,
  PrepareResultType,
  TransferParticipantInfo,
  TransferReadModel,
  TransferStateChange,
  TransformredTransfer
} from './types';
import { Ledger } from './Ledger';
import { safeStringToNumber } from '../../shared/config/util';

export interface LegacyCompatibleLedgerDependencies {
  config: ApplicationConfig

  /**
   * Legacy functions used for Onboarding the Hub, DFSP, Setting up the switch and Settlement
   * models.
   */
  lifecycle: {
    participantsHandler: {
      create: (request: { payload: { name: string, currency: string } }, callback: any) => Promise<void>
      createHubAccount: (request: { params: { name: string }, payload: { type: string, currency: string } }, callback: any) => Promise<void>
    }
    participantService: {
      getByName: (name: string) => Promise<{ currencyList: any[], participantId: number }>
      getById: (id: number) => Promise<{ currencyList: any[], participantId: number }>
      getAccounts: (name: string, query: { currency: string }) => Promise<Array<ParticipantServiceAccount>>
      getLimits: (name: string, query: {currency: string, type: string }) => Promise<Array<unknown>>
      create: (payload: { name: string, isProxy?: boolean }) => Promise<number>
      createParticipantCurrency: (participantId: number, currency: string, ledgerAccountTypeId: number, isActive?: boolean) => Promise<number>
      getParticipantCurrencyById: (participantCurrencyId: number) => Promise<any>
      validateHubAccounts: (currency: string) => Promise<void>
    },
    settlementModelDomain: {
      createSettlementModel: (model: { name: string, settlementGranularity: string, settlementInterchange: string, settlementDelay: string, currency: string, requireLiquidityCheck: boolean, ledgerAccountType: string, settlementAccountType: string, autoPositionReset: boolean }) => Promise<void>
      getAll: () => Promise<Array<{ currencyId: string | null, ledgerAccountTypeId: number, settlementAccountTypeId: number }>>
    },
    participantFacade: {
      getByNameAndCurrency: (name: string, currency: string, accountType: any) => Promise<{ participantCurrencyId: number }>
      addLimitAndInitialPosition: (positionParticipantCurrencyId: number, settlementParticipantCurrencyId: number, payload: any, processLimitsOnly: boolean) => Promise<void>
    }
    transferService: {
      saveTransferDuplicateCheck: (transferId: string, payload: any) => Promise<void>
      recordFundsIn: (payload: any, transactionTimestamp: string, enums: any) => Promise<void>
    }
    enums: any
  },

  /**
   * Legacy functions used for clearing payments.
   */
  clearing: {
    validatePrepare: (
      payload: CreateTransferDto,
      headers: any,
      isFx: boolean,
      determiningTransferCheckResult: TransferCheckResult,
      proxyObligation: ProxyObligation
    ) => Promise<ValidationResult>;
    validateParticipantByName: (participantName: string) => Promise<boolean>;
    validatePositionAccountByNameAndCurrency: (
      participantName: string,
      currency: string
    ) => Promise<boolean>;
    validateParticipantTransferId: (participantName: string, transferId: string) => Promise<boolean>;
    validateFulfilCondition: (fulfilment: string, condition: string) => boolean;
    validationReasons: string[];
    handlePayeeResponse: (transferId: string, payload: PayeeResponsePayload, action: any) => Promise<TransformredTransfer>;
    getTransferById: (transferId: string) => Promise<TransferReadModel | null>;
    getTransferInfoToChangePosition: (transferId: string, roleType: any, entryType: any) => Promise<TransferParticipantInfo | null>;
    getTransferFulfilmentDuplicateCheck: any;
    saveTransferFulfilmentDuplicateCheck: any;
    transformTransferToFulfil: (transfer: any, isFx: boolean) => any;
    duplicateCheckComparator: (transferId: string, payload: any, getCheck: any, saveCheck: any) => Promise<any>;
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
    getByIDAndCurrency: (
      participantId: number,
      currencyId: string,
      ledgerAccountTypeId: number,
      isCurrencyActive?: boolean
    ) => Promise<ParticipantWithCurrency | null>;
    calculatePreparePositionsBatch: (
      transferList: PositionKafkaMessage[]
    ) => Promise<PreparePositionsBatchResult>;
    changeParticipantPosition: (
      participantCurrencyId: number,
      isReversal: boolean,
      amount: string,
      transferStateChange: TransferStateChange
    ) => Promise<void>;
    getAccountByNameAndCurrency: (participantName: string, currency: string) => Promise<{ currencyIsActive: boolean }>;
  }
}


/**
 * @class LegacyCompatibleLedger
 * @description Collects the business logic from all ledger-related activites into a common 
 *   interface which can be abstracted out and reimplemented with TigerBeetle
 */
export default class LegacyCompatibleLedger implements Ledger {
  constructor(private deps: LegacyCompatibleLedgerDependencies) {

  }

  /**
   * Onboarding/Lifecycle Management
   */

  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    assert(cmd.currency)
    assert(cmd.settlementModel)
    assert(cmd.settlementModel.name)
    assert(cmd.settlementModel.settlementGranularity)
    assert(cmd.settlementModel.settlementInterchange)
    assert(cmd.settlementModel.settlementDelay)
    assert.equal(cmd.settlementModel.currency, cmd.currency)
    assert(cmd.settlementModel.requireLiquidityCheck === true, 'createHubAccount - currently only allows settlements with liquidity checks enabled')
    assert(cmd.settlementModel.ledgerAccountType)
    assert(cmd.settlementModel.settlementAccountType)
    assert(cmd.settlementModel.autoPositionReset === true || cmd.settlementModel.autoPositionReset === false)

    // dummy to suit the `h` object the handlers expect
    const mockCallback = {
      response: (body: any) => {
        return {
          code: (code: number) => { }
        }
      }
    }

    try {
      const requestMultilateralSettlement = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_MULTILATERAL_SETTLEMENT',
          currency: cmd.currency
        }
      }
      const requestHubReconcilation = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_RECONCILIATION',
          currency: cmd.currency,
        }
      }
      try {
        await this.deps.lifecycle.participantsHandler.createHubAccount(requestMultilateralSettlement, mockCallback)
        await this.deps.lifecycle.participantsHandler.createHubAccount(requestHubReconcilation, mockCallback)
      } catch (err) {
        // catch this early, since we can't know if the settlementModel has also already been created
        if ((err as ErrorHandler.FSPIOPError).message === 'Hub account has already been registered.') {
          logger.warn('createHubAccount', { error: err })
        } else {
          throw err
        }
      }

      await this.deps.lifecycle.settlementModelDomain.createSettlementModel(cmd.settlementModel)
      return {
        type: 'SUCCESS'
      }
    } catch (err) {
      if (err.message === 'Settlement Model already exists') {
        return {
          type: 'ALREADY_EXISTS'
        }
      }

      return {
        type: 'FAILED',
        error: err
      }
    }
  }

  /**
   * @description Create the dfsp accounts. Returns a duplicate response if the dfsp is already
   *   created.
   */
  public async createDfsp(cmd: CreateDFSPCommand): Promise<CreateDFSPResponse> {
    assert(cmd.dfspId)
    assert(cmd.currencies)
    assert(cmd.currencies.length > 0)
    assert(cmd.currencies.length < 16, 'Cannot register more than 16 currencies for a DFSP')
    assert(cmd.initialLimits)
    assert.equal(cmd.currencies.length, cmd.initialLimits.length, 'Expected currencies and intiial limits to have the same length')

    try {
      const participant = await this.deps.lifecycle.participantService.getByName(cmd.dfspId);
      if (participant) {
        return {
          type: 'ALREADY_EXISTS'
        }
      }


      // Create the account

      // Mock callback to suit the handler expectations
      const mockCallback = {
        response: (body: any) => {
          return {
            code: (code: number) => { }
          }
        }
      };
      // TODO: this is so hacky
      const positionAccountRequests = cmd.currencies.map(currency => {
        return {
          payload: {
            name: cmd.dfspId,
            currency
          },
          server: {
            app: {
              ledger: this
            }
          }
        }
      })
      // Create participant and currency accounts directly (bypassing handler to avoid circular dependency)
      for (const currency of cmd.currencies) {
        await this.createParticipantWithCurrency(cmd.dfspId, currency);
      }

      // Set the initial limits
      for (let i = 0; i < cmd.currencies.length; i++) {
        const currency = cmd.currencies[i];
        const initialLimit = cmd.initialLimits[i]
        assert(currency)
        assert(initialLimit)
        assert(initialLimit >= 0)

        // Get participant accounts to get the participantCurrencyIds needed by the facade
        const positionAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
          cmd.dfspId,
          currency,
          Enum.Accounts.LedgerAccountType.POSITION
        );
        assert(positionAccount)
        const settlementAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
          cmd.dfspId,
          currency,
          Enum.Accounts.LedgerAccountType.SETTLEMENT
        );
        assert(settlementAccount)

        const limitPayload = {
          limit: {
            type: 'NET_DEBIT_CAP',
            value: initialLimit,
            thresholdAlarmPercentage: 10
          },
          initialPosition: 0
        };

        // Call facade directly to bypass Kafka messaging
        await this.deps.lifecycle.participantFacade.addLimitAndInitialPosition(
          positionAccount.participantCurrencyId,
          settlementAccount.participantCurrencyId,
          limitPayload,
          true
        );
      }

      return {
        type: 'SUCCESS'
      }


    } catch (err) {
      return {
        type: 'FAILED',
        error: err
      }
    }
  }

  public async disableDfsp(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async enableDfsp(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse> {
    assert(cmd.amount)
    assert(cmd.amount > 0, 'depositCollateral amount must be greater than 0')
    assert(cmd.dfspId)
    assert(cmd.currency)
    assert(cmd.dfspId)

    try {
      const settlementAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        cmd.dfspId,
        cmd.currency,
        Enum.Accounts.LedgerAccountType.SETTLEMENT
      );
      assert(settlementAccount);
      const now = new Date()
      const payload = {
        transferId: cmd.transferId,
        participantCurrencyId: settlementAccount.participantCurrencyId,
        action: 'recordFundsIn',
        reason: 'Initial funding for testing',
        externalReference: `funding-${cmd.dfspId}`,
        amount: {
          amount: cmd.amount.toString(),
          currency: cmd.currency
        }
      };

      // Create duplicate check entry first (required by foreign key constraint)
      await this.deps.lifecycle.transferService.saveTransferDuplicateCheck(cmd.transferId, payload);

      // Call the transfer service directly
      const enums = await require('../../lib/enumCached').getEnums('all')
      await this.deps.lifecycle.transferService.recordFundsIn(
        payload,
        Time.getUTCString(now),
        enums
      );

      return {
        type: 'SUCCESS'
      }

    } catch (err) {
      // TODO: catch the duplicate error

      return {
        type: 'FAILED',
        error: err
      }
    }
  }

  public async withdrawCollateral(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async getAccounts(query: GetDFSPAccountsQuery): Promise<DFSPAccountResponse> {
    const legacyQuery = { currency: query.currency }
    try {
      const accounts = await this.deps.lifecycle.participantService.getAccounts(query.dfspId, legacyQuery)
      const formattedAccounts: Array<LegacyLedgerAccount> = []
      accounts.forEach(account => {
        // TODO: parse out the silly strings into proper numbers

        // Map from the internal legacy participantService representation to 
        // a compatible Ledger Interface
        const formattedAccount: LegacyLedgerAccount = {
          id: BigInt(account.id),
          ledgerAccountType: account.ledgerAccountType,
          currency: account.currency,
          isActive: Boolean(account.isActive),
          // TODO(LD): map the numbers!
          value: safeStringToNumber(account.value),
          reservedValue: safeStringToNumber(account.reservedValue),
          changedDate: new Date(account.changedDate)
        }
        formattedAccounts.push(formattedAccount)
      })

      assert(formattedAccounts.length === accounts.length)

      return {
        type: 'SUCCESS',
        accounts: formattedAccounts,
      }

    } catch (err) {
      return {
        type: 'FAILED',
        error: err
      }
    }
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<NetDebitCapResponse> {
    const legacyQuery = { currency: query.currency, type: 'NET_DEBIT_CAP'}
    try {
      const result = await this.deps.lifecycle.participantService.getLimits(query.dfspId, legacyQuery)
      if (result.length === 0) {
        return {
          type: 'FAILED',
          error: new Error(`getNetDebitCap() - no limits found for dfspId: ${query.dfspId}, currency: ${query.currency}, type: 'NET_DEBIT_CAP`)
        }
      }

      assert(result.length === 1)
      const legacyLimit = result[0] as {value: string, thresholdAlarmPercentage: string}
      assert(legacyLimit.value)
      assert(legacyLimit.thresholdAlarmPercentage)
      return {
        type: 'SUCCESS',
        limit: {
          type: 'NET_DEBIT_CAP',
          value: safeStringToNumber(legacyLimit.value),
          alarmPercentage: safeStringToNumber(legacyLimit.thresholdAlarmPercentage)
        }
      }
    } catch (err) {
      return {
        type: 'FAILED',
        error: err
      }
    }


     // const result = await ParticipantService.getLimits(request.params.name, request.query)
    // const limits = []
    // if (Array.isArray(result) && result.length > 0) {
    //   result.forEach(item => {
    //     limits.push({
    //       currency: (item.currencyId || request.query.currency),
    //       limit: {
    //         type: item.name,
    //         value: new MLNumber(item.value).toNumber(),
    //         alarmPercentage: item.thresholdAlarmPercentage !== undefined ? new MLNumber(item.thresholdAlarmPercentage).toNumber() : undefined
    //       }
    //     })
    //   })
    // }
    // return limits
  }

  /**
   * Clearing Methods
   */

  public async prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult> {
    const { payload, transferId, headers } = input;
    logger.debug(`prepare() - transferId: ${transferId}`)

    const duplicateResult = await this.checkPrepareDuplicate(payload, transferId)
    switch (duplicateResult) {
      case PrepareDuplicateResult.DUPLICATED: {
        const transfer = await this.deps.clearing.getTransferById(transferId)
        assert(transfer.transferStateEnumeration)
        const finalizedStates = [
          Enum.Transfers.TransferState.COMMITTED,
          Enum.Transfers.TransferState.ABORTED,
          Enum.Transfers.TransferState.RESERVED
        ].map(e => e.toString())

        if (finalizedStates.includes(transfer.transferStateEnumeration)) {
          const payload = this.deps.clearing.transformTransferToFulfil(transfer, false)
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
    await this.deps.clearing.handlePayeeResponse(transferId, payload, input.action);

    // Update the positions
    logger.info(`Processing position commit for transfer: ${transferId}`);
    try {
      // Get transfer info to change position for PAYEE
      const transferInfo = await this.deps.clearing.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info
      const participantCurrency = await this.deps.clearing.getByIDAndCurrency(
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

      await this.deps.clearing.changeParticipantPosition(
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

  public async getTransfer(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  /**
   * Settlement Methods
   */

  public async closeSettlementWindow(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async settleClosedWindows(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }



  private async validateFulfilMessage(input: FusedFulfilHandlerInput): Promise<void> {
    const { transferId, payload, message: { value: { from } }, headers } = input;

    // make sure the sender exists
    if (!await this.deps.clearing.validateParticipantByName(from)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND, 'Participant not found');
    }

    // Get transfer details
    const transfer = await this.deps.clearing.getTransferById(transferId);
    if (!transfer) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
    }

    if (!await this.deps.clearing.validateParticipantTransferId(from, transferId)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant not associated with transfer');
    }

    if (headers[Enum.Http.Headers.FSPIOP.SOURCE].toLowerCase() !== transfer.payeeFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Source header does not match transfer payee');
    }

    if (headers[Enum.Http.Headers.FSPIOP.DESTINATION].toLowerCase() !== transfer.payerFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Destination header does not match transfer payer');
    }

    assert(payload.fulfilment, 'payload.fulfilment not found')
    if (!this.deps.clearing.validateFulfilCondition(payload.fulfilment, transfer.condition)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Invalid fulfilment');
    }
  }

  /**
   * Shim Methods to improve usability before refactoring
   */
  private async checkPrepareDuplicate(payload: CreateTransferDto, transferId: string): Promise<PrepareDuplicateResult> {
    const checkDuplicateResult = await this.deps.clearing.checkDuplication({
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
    const checkDuplicateResult = await this.deps.clearing.duplicateCheckComparator(
      transferId,
      payload,
      this.deps.clearing.getTransferFulfilmentDuplicateCheck,
      this.deps.clearing.saveTransferFulfilmentDuplicateCheck
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
    const payerValid = await this.deps.clearing.validateParticipantByName(payerId);
    const payeeValid = await this.deps.clearing.validateParticipantByName(payeeId);

    if (!payerValid || !payeeValid) {
      return {
        validationPassed: false,
        reasons: ['payer or payee invalid']
      };
    }

    const payerAccountValid = await this.deps.clearing.validatePositionAccountByNameAndCurrency(payerId, currency)
    const payeeAccountValid = await this.deps.clearing.validatePositionAccountByNameAndCurrency(payeeId, currency)

    if (!payerAccountValid || !payeeAccountValid) {
      return {
        validationPassed: false,
        // TODO(LD): nasty globals here
        reasons: [this.deps.clearing.validationReasons[0]]
      }
    }

    return {
      validationPassed: true,
      reasons: []
    }
  }

  private async validateTransfer(payload: CreateTransferDto, headers: any): Promise<ValidationResult> {
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    return await this.deps.clearing.validatePrepare(
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

    return await this.deps.clearing.savePreparedRequest({
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
    return this.deps.clearing.calculatePreparePositionsBatch([message])
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

  /**
   * Create participant and currency accounts directly (bypassing handler to avoid circular dependency)
   * This extracts the core logic from the participants handler create method
   */
  private async createParticipantWithCurrency(dfspId: string, currency: string): Promise<void> {
    const Enums = require('../../lib/enumCached')
    const Util = require('@mojaloop/central-services-shared').Util

    // Validate hub accounts first
    await this.deps.lifecycle.participantService.validateHubAccounts(currency)

    let participant = await this.deps.lifecycle.participantService.getByName(dfspId)
    if (participant) {
      const currencyExists = participant.currencyList.find((curr: any) => {
        return curr.currencyId === currency
      })
      if (currencyExists) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR,
          'Participant currency has already been registered'
        )
      }
    } else {
      const participantId = await this.deps.lifecycle.participantService.create({ name: dfspId })
      participant = await this.deps.lifecycle.participantService.getById(participantId)
    }

    const ledgerAccountTypes = await Enums.getEnums('ledgerAccountType')
    const allSettlementModels = await this.deps.lifecycle.settlementModelDomain.getAll()
    let settlementModels = allSettlementModels.filter(model => model.currencyId === currency)
    if (settlementModels.length === 0) {
      settlementModels = allSettlementModels.filter(model => model.currencyId === null) // Default settlement model
      if (settlementModels.length === 0) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.GENERIC_SETTLEMENT_ERROR,
          'Unable to find a matching or default, Settlement Model'
        )
      }
    }

    for (const settlementModel of settlementModels) {
      const [participantCurrencyId1, participantCurrencyId2] = await Promise.all([
        this.deps.lifecycle.participantService.createParticipantCurrency(participant.participantId, currency, settlementModel.ledgerAccountTypeId, false),
        this.deps.lifecycle.participantService.createParticipantCurrency(participant.participantId, currency, settlementModel.settlementAccountTypeId, false)
      ])

      if (Array.isArray(participant.currencyList)) {
        participant.currencyList = participant.currencyList.concat([
          await this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyId1),
          await this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyId2)
        ])
      } else {
        participant.currencyList = await Promise.all([
          this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyId1),
          this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyId2)
        ])
      }
    }
  }
}