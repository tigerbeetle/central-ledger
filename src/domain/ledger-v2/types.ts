import { FSPIOPError } from '@mojaloop/central-services-error-handling';


// Type definitions for participant facade functions
export interface ParticipantWithCurrency {
  // From participant table
  participantId: number;
  name: string;
  description: string | null;
  isActive: boolean;
  createdDate: string;
  createdBy: string;
  
  // From participantCurrency table (added when currency is found)
  participantCurrencyId: number;
  currencyId: string;
  currencyIsActive: boolean;
}

export interface TransferStateChange {
  transferId: string;
  transferStateId: string | number;
  reason?: string;
  createdDate?: string;
}

// Transfer service function types
export interface Extension {
  key: string;
  value: string;
  isError?: boolean;
  isFulfilment?: boolean;
}

export interface ExtensionList {
  extension: Extension[];
}

export interface Amount {
  amount: string;
  currency: string;
}

export interface FulfilmentPayload {
  fulfilment?: string;
  completedTimestamp?: string;
  extensionList?: ExtensionList;
}

export interface ErrorPayload {
  errorInformation: {
    errorCode: string;
    errorDescription: string;
    extensionList?: ExtensionList;
  };
}

export type PayeeResponsePayload = FulfilmentPayload | ErrorPayload;

export interface TransformredTransfer {
  transferId: string;
  transferState: string;
  completedTimestamp: string;
  fulfilment?: string;
  extensionList?: Extension[];
}

export interface TransferReadModel {
  transferId: string;
  amount: string;
  currency: string;
  payerParticipantCurrencyId?: number;
  payerAmount: string;
  payerParticipantId: number;
  payerFsp: string;
  payerIsProxy: boolean;
  payeeParticipantCurrencyId?: number;
  payeeAmount: string;
  payeeParticipantId: number;
  payeeFsp: string;
  payeeIsProxy: boolean;
  transferStateChangeId: number;
  transferState: string;
  reason?: string;
  completedTimestamp: string;
  transferStateEnumeration: string;
  transferStateDescription: string;
  ilpPacket: string;
  condition: string;
  fulfilment?: string;
  errorCode?: string;
  errorDescription?: string;
  externalPayerName?: string;
  externalPayeeName?: string;
  extensionList?: Extension[];
  isTransferReadModel: true;
}

export interface TransferParticipantInfo {
  transferId: string;
  participantId: number;
  participantCurrencyId?: number;
  transferParticipantRoleTypeId: number;
  ledgerEntryTypeId: number;
  amount: string;
  externalParticipantId?: number;
  currencyId: string;
  transferStateId: string;
  reason?: string;
}


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
  fspiopError: FSPIOPError,
}
export interface PrepareResultFailOther {
  type: PrepareResultType.FAIL_OTHER,
  fspiopError: FSPIOPError,
}

export type FulfilResult = FulfilResultPass
  | FulfilResultDuplicateFinal
  | FulfilResultFailValidation
  | FulfilResultFailOther

export interface FulfilResultPass {
  type: FulfilResultType.PASS
}

export interface FulfilResultDuplicateFinal {
  type: FulfilResultType.DUPLICATE_FINAL,
}

export interface FulfilResultFailValidation {
  type: FulfilResultType.FAIL_VALIDATION,
  fspiopError: FSPIOPError,
}

export interface FulfilResultFailOther {
  type: FulfilResultType.FAIL_OTHER,
  fspiopError: FSPIOPError,
}


export interface CreateHubAccountCommand {
  currency: string,
  settlementModel: SettlementModel
}

export interface CreateHubAccountResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateHubAccountResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateHubAccountResponseFailed {
  type: 'FAILED',
  error: Error
}

export type CreateHubAccountResponse = CreateHubAccountResponseSuccess
  | CreateHubAccountResponseAlreadyExists
  | CreateHubAccountResponseFailed


export interface CreateDFSPCommand {
  dfspId: string,
  currencies: Array<string>
  initialLimits: Array<number>
}

export interface CreateDFSPResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateDFSPResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateDFSPResponseFailed {
  type: 'FAILED',
  error: Error
}

export type CreateDFSPResponse = CreateDFSPResponseSuccess
  | CreateDFSPResponseAlreadyExists
  | CreateDFSPResponseFailed


export interface DepositCollateralCommand {
  // TODO: should this be named idempotenceId? Or depositId?
  transferId: string,
  dfspId: string,

  // TODO: make this a Mojaloop number to make things easier?
  currency: string,
  amount: number
}

export interface DepositCollateralResponseSuccess {
  type: 'SUCCESS'
}

export interface DepositCollateralResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface DepositCollateralResponseFailed {
  type: 'FAILED',
  error: Error
}

export type DepositCollateralResponse = DepositCollateralResponseSuccess
  | DepositCollateralResponseAlreadyExists
  | DepositCollateralResponseFailed

export interface SetLimitsCommand {

}



export interface SettlementModel {
  name: string,
  settlementGranularity: string,
  settlementInterchange: string,
  settlementDelay: string,
  currency: string,
  requireLiquidityCheck: boolean,
  ledgerAccountType: string,
  settlementAccountType: string,
  autoPositionReset: boolean
}