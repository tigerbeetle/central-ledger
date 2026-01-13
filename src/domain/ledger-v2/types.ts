import { FSPIOPError } from '@mojaloop/central-services-error-handling'


// ============================================================================
// Common/Generic Types
// ============================================================================

/**
 * Generic interface for Ledger Commands
 */
export type CommandResultSuccess<T> = {
  type: 'SUCCESS'
  result: T
}

export type CommandResultFailure = {
  type: 'FAILURE'
  error: Error
}

export type CommandResult<T> = CommandResultSuccess<T> | CommandResultFailure

/**
 * Empty interface for queries that have no params
 */
export interface AnyQuery {}

// ============================================================================
// Domain Models
// ============================================================================

export interface SettlementModel {
  name: string
  settlementGranularity: string
  settlementInterchange: string
  settlementDelay: string
  currency: string
  requireLiquidityCheck: boolean
  ledgerAccountType: string
  settlementAccountType: string
  autoPositionReset: boolean
}

/**
 * Legacy Internal Represenation of Dfsp
 */
export interface LegacyLedgerDfsp {
  name: string
  // TODO(LD): rename to simply active
  isActive: boolean
  created: Date
  accounts: Array<LegacyLedgerAccount>
}

/**
 * TigerBeetle Accounting model compatible represenation of the Dfsp
 */
export interface LedgerDfsp {
  name: string
  status: 'ENABLED' | 'DISABLED'
  created: Date
  accounts: Array<LedgerAccount>
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
  id: bigint
  ledgerAccountType: string
  currency: string
  isActive: boolean
  value: number
  reservedValue: number
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
  id: bigint
  code: AccountCode
  currency: string
  status: 'ENABLED' | 'DISABLED'

  /**
   * sum(credits_pending)/assetScale
   */
  realCreditsPending: number

  /**
   * sum(debits_pending)/assetScale
   */
  realDebitsPending: number

  /**
   * sum(credits_posted)/assetScale
   */
  realCreditsPosted: number

  /**
   * sum(debits_posted)/assetScale
   */
  realDebitsPosted: number
}

export enum AccountCode {
  Settlement_Balance = 10100,
  Deposit = 10200,
  Unrestricted = 20100,
  Unrestricted_Lock = 20101,
  Restricted = 20200,
  Reserved = 20300,
  Committed_Outgoing = 20400,
  Dfsp = 60100,
  Net_Debit_Cap = 60200,
  Net_Debit_Cap_Control = 60201,
  Dev_Null = 60300,

  // TODO(LD): remove me! 
  TIMEOUT = 9000,
}

export enum TransferCode {
  Deposit = 10001,
  Withdraw = 20001,
  Clearing_Reserve = 30001,
  Clearing_Active_Check = 30002,
  Clearing_Fulfil = 30003,
  Clearing_Credit = 30004,
  Clearing_Reverse = 30005,
  Settlement_Deposit_Reduce = 40001,
  Settlement_Deposit_Increase = 40002,
  Net_Debit_Cap_Lock = 50001,
  Net_Debit_Cap_Sweep_To_Restricted = 50002,
  Net_Debit_Cap_Set_Limited = 50004,
  Net_Debit_Cap_Set_Unlimited = 50005,
  Net_Debit_Cap_Sweep_To_Unrestricted = 50006,
  Close_Account = 50007,
}

export const TransferCodeDescription = {
  [TransferCode.Deposit]: 'Deposit funds into Unrestricted',
  [TransferCode.Withdraw]: 'Withdraw funds',
  [TransferCode.Clearing_Reserve]: 'Reserve funds for Payee Participant.',
  [TransferCode.Clearing_Active_Check]: 'Ensure both Participants are active.',
  [TransferCode.Clearing_Fulfil]: 'Fulfil payment.',
  [TransferCode.Clearing_Credit]: 'Make credit available for transfers',
  [TransferCode.Clearing_Reverse]: 'Reverse reservation.',
  [TransferCode.Settlement_Deposit_Reduce]: 'Reduce Deposit amount by sum of debits.',
  [TransferCode.Settlement_Deposit_Increase]: 'Increase Deposit amount by sum of credits.',
  [TransferCode.Net_Debit_Cap_Lock]: 'Temporarily lock up to the net debit cap amount.',
  [TransferCode.Net_Debit_Cap_Sweep_To_Restricted]: 'Sweep whatever remains in Unrestricted to Restricted.',
  [TransferCode.Net_Debit_Cap_Set_Limited]: 'Set the new Net Debit Cap to a finite number.',
  [TransferCode.Net_Debit_Cap_Set_Unlimited]: 'Set the new Net Debit Cap to unlimited.',
  [TransferCode.Net_Debit_Cap_Sweep_To_Unrestricted]: 'Sweep total balance from Restricted to Unrestricted',
  [TransferCode.Close_Account]: 'Close account.',
}


/**
 * Legacy Representation of the net debit cap limit, backwards compatible with the admin api
 */
export interface LegacyLimit {
  type: 'NET_DEBIT_CAP'
  value: number
  alarmPercentage: number
}

export interface TimedOutTransfer {
  id: string
  payeeId: string
  payerId: string
}

// ============================================================================
// Lifecycle Commands (CreateHub/CreateDfsp/Deposit/Withdraw)
// ============================================================================

export interface CreateHubAccountCommand {
  currency: string
  settlementModel: SettlementModel
}

export type CreateHubAccountResponse = CreateHubAccountResponseSuccess
  | CreateHubAccountResponseAlreadyExists
  | CreateHubAccountResponseFailure

export interface CreateHubAccountResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateHubAccountResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateHubAccountResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface CreateDfspCommand {
  dfspId: string
  currencies: Array<string>
}

export type CreateDfspResponse = CreateDfspResponseSuccess
  | CreateDfspResponseAlreadyExists
  | CreateDfspResponseFailure

export interface CreateDfspResponseSuccess {
  type: 'SUCCESS'
}

export interface CreateDfspResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface CreateDfspResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface DepositCommand {
  transferId: string
  dfspId: string
  currency: string
  amount: number
  reason: string
}

export type DepositResponse = DepositResponseSuccess
  | DepositResponseAlreadyExists
  | DepositResponseFailure

export interface DepositResponseSuccess {
  type: 'SUCCESS'
}

export interface DepositResponseAlreadyExists {
  type: 'ALREADY_EXISTS'
}

export interface DepositResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface WithdrawPrepareCommand {
  transferId: string
  dfspId: string
  currency: string
  amount: number
  reason: string
}

export type WithdrawPrepareResponse = WithdrawPrepareResponseSuccess
  | WithdrawPrepareResponseInsufficientFunds
  | WithdrawPrepareResponseFailure

export interface WithdrawPrepareResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawPrepareResponseInsufficientFunds {
  type: 'INSUFFICIENT_FUNDS'
}

export interface WithdrawPrepareResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface WithdrawCommitCommand {
  transferId: string
}

export type WithdrawCommitResponse = WithdrawCommitResponseSuccess
  | WithdrawCommitResponseFailure

export interface WithdrawCommitResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawCommitResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface WithdrawAbortCommand {
  transferId: string
}

export type WithdrawAbortResponse = WithdrawAbortResponseSuccess
  | WithdrawAbortResponseFailure

export interface WithdrawAbortResponseSuccess {
  type: 'SUCCESS'
}

export interface WithdrawAbortResponseFailure {
  type: 'FAILURE'
  error: Error
}

export interface EnableDfspAccountCommand {
  dfspId: string
  accountId: number
}

export interface DisableDfspAccountCommand {
  dfspId: string
  accountId: number
}

export type SetNetDebitCapCommand = SetNetDebitCapAmountCommand | SetNetDebitCapUnlimitedCommand

export interface SetNetDebitCapAmountCommand {
  netDebitCapType: 'AMOUNT'
  dfspId: string
  currency: string
  amount: number
}

export interface SetNetDebitCapUnlimitedCommand {
  netDebitCapType: 'UNLIMITED'
  dfspId: string
  currency: string
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

export type DeactivateDfspResponse = DeactivateDfspResponseSuccess
  | DeactivateDfspResponseRetryable
  | DeactivateDfspResponseFailure

export interface DeactivateDfspResponseSuccess {
  type: DeactivateDfspResponseType.SUCCESS | DeactivateDfspResponseType.ALREADY_CLOSED
}

export interface DeactivateDfspResponseRetryable {
  type: DeactivateDfspResponseType.CREATE_ACCOUNT
}

export interface DeactivateDfspResponseFailure {
  type: DeactivateDfspResponseType.FAILED
  error: Error
}

export interface GetAllDfspsResponse {
  dfsps: Array<LegacyLedgerDfsp>
}

export interface GetDfspAccountsQuery {
  dfspId: string
  currency: string
}

export interface GetAllDfspAccountsQuery {
  dfspId: string
}

export type DfspAccountResponse = DfspAccountResponseSuccess | DfspAccountResponseFailure

export interface DfspAccountResponseSuccess {
  type: 'SUCCESS'
  accounts: Array<LegacyLedgerAccount>
}

export interface DfspAccountResponseFailure {
  type: 'FAILURE'
  error: FSPIOPError
}

export interface GetHubAccountsQuery {
  // TODO(LD): should we specify currency here?
  // currency: string
}

export type HubAccountResponse = HubAccountResponseSuccess | HubAccountResponseFailure

export interface HubAccountResponseSuccess {
  type: 'SUCCESS'
  accounts: Array<LegacyLedgerAccount>
}

export interface HubAccountResponseFailure {
  type: 'FAILURE'
  error: FSPIOPError
}

export interface GetNetDebitCapQuery {
  dfspId: string
  currency: string
}

export interface LookupTransferQuery {
  /**
   * The mojaloop logical transfer id
   */
  transferId: string
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

export type LookupTransferQueryResponse = LookupTransferQueryResponseFoundNonFinal
  | LookupTransferQueryResponseFoundFinal
  | LookupTransferQueryResponseNotFound
  | LookupTransferQueryResponseFailed

export interface LookupTransferQueryResponseFoundNonFinal {
  type: LookupTransferResultType.FOUND_NON_FINAL
}

export interface LookupTransferQueryResponseFoundFinal {
  type: LookupTransferResultType.FOUND_FINAL
  finalizedTransfer: {
    completedTimestamp: string
    transferState: 'ABORTED' | 'COMMITTED'
    fulfilment?: string
  }
}

export interface LookupTransferQueryResponseNotFound {
  type: LookupTransferResultType.NOT_FOUND
}

export interface LookupTransferQueryResponseFailed {
  type: LookupTransferResultType.FAILED
  error: FSPIOPError
}


// ============================================================================
// Clearing
// ============================================================================

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
  type: PrepareResultType.DUPLICATE_FINAL
  finalizedTransfer: {
    completedTimestamp: string
    transferState: 'COMMITTED' | 'ABORTED'
    fulfilment?: string
  }
}

export interface PrepareResultDuplicateNonFinal {
  type: PrepareResultType.DUPLICATE_NON_FINAL
}

export interface PrepareResultFailModified {
  type: PrepareResultType.MODIFIED
}

export interface PrepareResultFailValidation {
  type: PrepareResultType.FAIL_VALIDATION
  failureReasons: Array<string>
}

export interface PrepareResultFailLiquidity {
  type: PrepareResultType.FAIL_LIQUIDITY
  error: FSPIOPError
}

export interface PrepareResultFailOther {
  type: PrepareResultType.FAIL_OTHER
  error: FSPIOPError
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

export type FulfilResult = FulfilResultPass
  | FulfilResultDuplicateFinal
  | FulfilResultFailValidation
  | FulfilResultFailOther

export interface FulfilResultPass {
  type: FulfilResultType.PASS
}

export interface FulfilResultDuplicateFinal {
  type: FulfilResultType.DUPLICATE_FINAL
}

export interface FulfilResultFailValidation {
  type: FulfilResultType.FAIL_VALIDATION
  error: FSPIOPError
}

export interface FulfilResultFailOther {
  type: FulfilResultType.FAIL_OTHER
  error: FSPIOPError
}

export type SweepResult = SweepResultSuccess | SweepResultFailure

export interface SweepResultSuccess {
  type: 'SUCCESS'
  transfers: Array<TimedOutTransfer>
}

export interface SweepResultFailure {
  type: 'FAILURE'
  error: Error
}