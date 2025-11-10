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


export enum FulfilResultType {
  /**
   * Fulfil step completed validation. Transfer was either fulfilled succesfully or aborted 
   * sucessfully
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
  | PrepareResultFailModified
  | PrepareResultFailValidation
  | PrepareResultFailLiquidity
  | PrepareResultFailOther

export interface PrepareResultPass {
  type: PrepareResultType.PASS
}

export interface PrepareResultDuplicateFinal {
  type: PrepareResultType.DUPLICATE_FINAL,
  finalizedTransfer: {
    completedTimestamp: string,
    transferState: 'COMMITTED' | 'ABORTED',
    fulfilment?: string,
  }
}

export interface PrepareResultDuplicateNonFinal {
  type: PrepareResultType.DUPLICATE_NON_FINAL,
}

export interface PrepareResultFailModified {
  type: PrepareResultType.MODIFIED,
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

export type SweepResult = SweepResultSuccess
  | SweepResultFailure

export interface TimedOutTransfer {
  id: string,
  payeeId: string,
  payerId: string,
}

export interface SweepResultSuccess {
  type: 'SUCCESS'
  transfers: Array<TimedOutTransfer>
}

export interface SweepResultFailure {
  type: 'FAILURE'
  error: Error
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

export interface CreateHubAccountResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type CreateHubAccountResponse = CreateHubAccountResponseSuccess
  | CreateHubAccountResponseAlreadyExists
  | CreateHubAccountResponseFailure


export interface CreateDFSPCommand {
  dfspId: string,
  // TODO: limit this to just one currency for now!
  currencies: Array<string>
  // TODO: limit this to just one limit for now!
  startingDeposits: Array<number>
}

export interface CreateDFSPResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateDFSPResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateDFSPResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type CreateDFSPResponse = CreateDFSPResponseSuccess
  | CreateDFSPResponseAlreadyExists
  | CreateDFSPResponseFailure


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

export interface DepositCollateralResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type DepositCollateralResponse = DepositCollateralResponseSuccess
  | DepositCollateralResponseAlreadyExists
  | DepositCollateralResponseFailure

export interface SetLimitsCommand {

}

export interface GetDFSPAccountsQuery {
  dfspId: string,
  currency: string
}

export interface DFSPAccountResponseSuccess {
  type: 'SUCCESS',
  accounts: Array<LegacyLedgerAccount>
}

export interface DFSPAccountResponseFailure {
  type: 'FAILURE',
  fspiopError: FSPIOPError
}

export type DFSPAccountResponse = DFSPAccountResponseSuccess
  | DFSPAccountResponseFailure

export interface GetNetDebitCapQuery {
  dfspId: string,
  currency: string
}

export interface NetDebitCapResponseSuccess {
  type: 'SUCCESS',
  limit: LegacyLimit
}

export interface NetDebitCapResponseFailure {
  type: 'FAILURE',
  fspiopError: FSPIOPError
}

export type NetDebitCapResponse = NetDebitCapResponseSuccess
  | NetDebitCapResponseFailure

export interface LookupTransferQuery {
  /**
   * The mojaloop logical transfer id
   */
  transferId: string;
}

export enum LookupTransferResultType {
  /**
   * Found transfer, it's in a non final state.
   */
  FOUND_NON_FINAL = 'FOUND_NON_FINAL',

  /**
   * Found transfer, it's in a final state.
   */
  FOUND_FINAL = 'FOUND_FINAL',

  /**
   * Could not find the Transfer.
   */
  NOT_FOUND = 'NOT_FOUND',

  /**
   * Lookup failed
   */
  FAILED = 'FAILED',
}

export interface LookupTransferQueryResponseFoundNonFinal {
  type: LookupTransferResultType.FOUND_NON_FINAL
}

export interface LookupTransferQueryResponseFoundFinal {
  type: LookupTransferResultType.FOUND_FINAL,
  finalizedTransfer: {
    completedTimestamp: string,
    transferState: 'ABORTED' | 'COMMITTED',
    fulfilment?: string,
  }
}

export interface LookupTransferQueryResponseNotFound {
  type: LookupTransferResultType.NOT_FOUND,
}

export interface LookupTransferQueryResponseFailed {
  type: LookupTransferResultType.FAILED
  fspiopError: FSPIOPError
}

export type LookupTransferQueryResponse = LookupTransferQueryResponseFoundNonFinal
 | LookupTransferQueryResponseFoundFinal
 | LookupTransferQueryResponseNotFound
 | LookupTransferQueryResponseFailed

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

export interface ParticipantServiceAccount {
  id: number,
  ledgerAccountType: string,
  currency: string,
  isActive: number,
  value: string,
  reservedValue: string,
  changedDate: string
}


/**
 * Represents a ledger account 
 * 
 * For the TigerBeetle implementation, we throw away information to make it backwards compatible.
 * 
 * Once everything is migrated to the TigerBeetle Ledger, we can update this interface to be 
 * double-entry compatible.
 */
export interface LegacyLedgerAccount {
  id: bigint,
  ledgerAccountType: string,
  currency: string,
  isActive: boolean,
  value: number,
  reservedValue: number,
  changedDate: Date
}

/**
 * Legacy Representation of the net debit cap limit, backwards compatible with the admin api
 */
export interface LegacyLimit {
  type: 'NET_DEBIT_CAP',
  value: number,
  alarmPercentage: number
}