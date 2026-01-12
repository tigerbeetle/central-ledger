import { FSPIOPError } from '@mojaloop/central-services-error-handling';
import { AccountCode } from './TigerBeetleLedger';




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

export interface ParticipantServiceParticipant {
  participantId: number,
  name: string,
  isActive: number,
  createdDate: string,
  createdBy: string,
  isProxy: number,
  currencyList: Array<ParticipantServiceCurrency>
}

export interface ParticipantServiceCurrency {
  participantCurrencyId: number,
  participantId: number,
  currencyId: string,
  ledgerAccountTypeId: number,
  isActive: number,
  createdDate: string,
  createdBy: string,
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

export interface TransformedTransfer {
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
  error: FSPIOPError,
}
export interface PrepareResultFailOther {
  type: PrepareResultType.FAIL_OTHER,
  error: FSPIOPError,
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
  error: FSPIOPError,
}

export interface FulfilResultFailOther {
  type: FulfilResultType.FAIL_OTHER,
  error: FSPIOPError,
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
  error: FSPIOPError,
}

export interface FulfilResultFailOther {
  type: FulfilResultType.FAIL_OTHER,
  error: FSPIOPError,
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


export interface CreateDfspCommand {
  dfspId: string,
  currencies: Array<string>
}

export interface CreateDfspResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateDfspResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateDfspResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type CreateDfspResponse = CreateDfspResponseSuccess
  | CreateDfspResponseAlreadyExists
  | CreateDfspResponseFailure


export interface DepositCommand {
  transferId: string,
  dfspId: string,
  currency: string,
  amount: number,
  reason: string
}

export interface DepositResponseSuccess {
  type: 'SUCCESS'
}

export interface DepositResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface DepositResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type DepositResponse = DepositResponseSuccess
  | DepositResponseAlreadyExists
  | DepositResponseFailure

export interface WithdrawPrepareCommand {
  transferId: string,
  dfspId: string,
  currency: string,
  amount: number,
  reason: string
}

export interface WithdrawPrepareResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawPrepareResponseInsufficientFunds {
  type: 'INSUFFICIENT_FUNDS',
  // TODO(LD): can we remove this? It's rather hard to get to in TigerBeetle
  availableBalance: number,
  requestedAmount: number
}

export interface WithdrawPrepareResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type WithdrawPrepareResponse = WithdrawPrepareResponseSuccess
  | WithdrawPrepareResponseInsufficientFunds
  | WithdrawPrepareResponseFailure

export interface WithdrawCommitCommand {
  transferId: string
}

export interface WithdrawCommitResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawCommitResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type WithdrawCommitResponse = WithdrawCommitResponseSuccess
  | WithdrawCommitResponseFailure


export interface WithdrawAbortCommand {
  transferId: string
}

export interface WithdrawAbortResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawAbortResponseFailure {
  type: 'FAILURE',
  error: Error
}

export type WithdrawAbortResponse = WithdrawAbortResponseSuccess
  | WithdrawAbortResponseFailure

export interface EnableDfspAccountCommand {
  dfspId: string,
  accountId: number
}

export interface DisableDfspAccountCommand {
  dfspId: string,
  accountId: number
}

export type SetNetDebitCapCommand = SetNetDebitCapAmountCommand | SetNetDebitCapUnlimitedCommand

export interface SetNetDebitCapAmountCommand {
  netDebitCapType: 'AMOUNT',
  dfspId: string,
  currency: string,
  amount: number 
}

export interface SetNetDebitCapUnlimitedCommand {
  netDebitCapType: 'UNLIMITED',
  dfspId: string,
  currency: string,
}

/**
 * Generic interface for Ledger Commands
 */

export type CommandResultSuccess<T> = {
  type: 'SUCCESS'
  result: T
}

export type CommandResultFailure = {
  type: 'FAILURE',
  error: Error
}

export type CommandResult<T> = CommandResultSuccess<T> | CommandResultFailure
/**
 * Empty interface for queries that have no params
 */
export interface AnyQuery {

}

/**
 * Legacy Internal Represenation of Dfsp
 */
export interface LegacyLedgerDfsp {
  name: string,
  // TODO(LD): rename to simply active
  isActive: boolean
  created: Date,
  accounts: Array<LegacyLedgerAccount>
}

/**
 * TigerBeetle Accounting model compatible represenation of the Dfsp
 */
export interface LedgerDfsp {
  name: string,
  status: 'ENABLED' | 'DISABLED',
  created: Date,
  accounts: Array<LedgerAccount>
}

export interface GetAllDfspsResponse {
  dfsps: Array<LegacyLedgerDfsp>
}

export interface GetDfspAccountsQuery {
  dfspId: string,
  currency: string
}

export interface GetAllDfspAccountsQuery {
  dfspId: string
}

export interface DfspAccountResponseSuccess {
  type: 'SUCCESS',
  accounts: Array<LegacyLedgerAccount>
}

export interface DfspAccountResponseFailure {
  type: 'FAILURE',
  error: FSPIOPError
}

export type DfspAccountResponse = DfspAccountResponseSuccess
  | DfspAccountResponseFailure

export interface GetHubAccountsQuery {
  // TODO(LD): should we specify currency here?
  // currency: string
}

export interface HubAccountResponseSuccess {
  type: 'SUCCESS',
  accounts: Array<LegacyLedgerAccount>
}

export interface HubAccountResponseFailure {
  type: 'FAILURE',
  error: FSPIOPError
}

export type HubAccountResponse = HubAccountResponseSuccess
  | HubAccountResponseFailure

export interface GetNetDebitCapQuery {
  dfspId: string,
  currency: string
}

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

export enum DeactivateDfspResponseType {
  /**
   * Closed the account successfully
   */
  SUCCESS = 'SUCCESS',

  /**
   * Account is already closed
   */
  ALREADY_CLOSED = 'ALREADY_CLOSED',

  /**
   * Retryable error - control account not created
   */
  CREATE_ACCOUNT = 'CREATE_ACCOUNT',

  /**
   * Fatal Error
   */
  FAILED = 'FAILED'
}

export interface DeactivateDfspResponseSuccess {
  type: DeactivateDfspResponseType.SUCCESS | DeactivateDfspResponseType.ALREADY_CLOSED
}

export interface DeactivateDfspResponseRetryable {
  type: DeactivateDfspResponseType.CREATE_ACCOUNT
}

export interface DeactivateDfspResponseFailure {
  type: DeactivateDfspResponseType.FAILED,
  error: Error
}

export type DeactivateDfspResponse = DeactivateDfspResponseSuccess |
  DeactivateDfspResponseRetryable |
  DeactivateDfspResponseFailure

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
  error: FSPIOPError
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


/**
 * Represents a ledger account 
 * 
 * For the TigerBeetle implementation, we throw away information to make it backwards compatible.
 * 
 * Once everything is migrated to the TigerBeetle Ledger, we can deprecate this in favour of 
 * LedgerAccount
 */
export interface LegacyLedgerAccount {
  // TODO(LD): should this be a number?
  id: bigint,
  ledgerAccountType: string,
  currency: string,
  isActive: boolean,
  // TODO(LD): this should be a bigint, shouldn't it?
  value: number,
  reservedValue: number,
  // TODO(LD): When do we actually use this? What does it mean?
  // E.g. in the TigerBeetle world, can it be just the creation date since 
  // accounts cannot be modified? Or should it include the reopen date
  // if we closed and reopened an account?
  changedDate: Date
}

/**
 * LedgerAccount with TigerBeetle Accounting Model
 * 
 * Outside of the boundaries of the Ledger, we map from a BigInt represenation -> currency base
 */
export interface LedgerAccount {
  id: bigint,
  code: AccountCode,
  currency: string,
  status: 'ENABLED' | 'DISABLED',

  /**
   * sum(credits_pending)/assetScale
   */
  realCreditsPending: number,

  /**
   * sum(debits_pending)/assetScale
   */
  realDebitsPending: number,

  /**
   * sum(credits_posted)/assetScale
   */
  realCreditsPosted: number,

  /**
   * sum(debits_posted)/assetScale
   */
  realDebitsPosted: number,
}

/**
 * Legacy Representation of the net debit cap limit, backwards compatible with the admin api
 */
export interface LegacyLimit {
  type: 'NET_DEBIT_CAP',
  value: number,
  alarmPercentage: number
}