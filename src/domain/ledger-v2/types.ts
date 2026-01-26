import { FSPIOPError } from '@mojaloop/central-services-error-handling'
import { Transfer } from 'tigerbeetle-node'


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
  // TODO: rename - we should save Settlement* for accounts that get used in settlement
  Settlement_Balance = 10100,
  Deposit = 10200,
  Unrestricted = 20100,
  Clearing_Credit = 20101,
  Restricted = 20200,
  Reserved = 20300,
  Committed_Outgoing = 20400,
  Dfsp = 60100,
  Net_Debit_Cap = 60200,
  Net_Debit_Cap_Control = 60201,
  Dev_Null = 60300,
  Clearing_Setup = 60400,
  Clearing_Limit = 60500,
  Unrestricted_Lock = 60600,
  Settlement_Outgoing = 60701,
  Settlement_Incoming = 60702,

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

export type CreateHubAccountResponse =  {
  type: 'SUCCESS'
} | {
  type: 'ALREADY_EXISTS'
} | {
  type: 'FAILURE'
  error: Error
}

export interface CreateDfspCommand {
  dfspId: string
  currencies: Array<string>
}

export type CreateDfspResponse =  {
  type: 'SUCCESS'
} | {
  type: 'ALREADY_EXISTS'
} |{
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

export type DepositResponse = {
  type: 'SUCCESS'
} | {
  type: 'ALREADY_EXISTS'
} |{
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

export type WithdrawPrepareResponse = {
  type: 'SUCCESS'
} | {
  type: 'INSUFFICIENT_FUNDS'
} | {
  type: 'FAILURE'
  error: Error
}

export interface WithdrawCommitCommand {
  transferId: string
}

export type WithdrawCommitResponse = {
  type: 'SUCCESS'
} | {
  type: 'FAILURE'
  error: Error
}

export interface WithdrawAbortCommand {
  transferId: string
}

export type WithdrawAbortResponse =  {
  type: 'SUCCESS'
} | {
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

export type SetNetDebitCapCommand = {
  netDebitCapType: 'LIMITED'
  dfspId: string
  currency: string
  amount: number
} | {
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

export type DeactivateDfspResponse = {
  type: DeactivateDfspResponseType.SUCCESS | DeactivateDfspResponseType.ALREADY_CLOSED
} | {
  type: DeactivateDfspResponseType.CREATE_ACCOUNT
} | {
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

export type DfspAccountResponse = {
  type: 'SUCCESS'
  accounts: Array<LegacyLedgerAccount>
} | {
  type: 'FAILURE'
  error: FSPIOPError
}

export interface GetHubAccountsQuery { }

export type HubAccountResponse = {
  type: 'SUCCESS'
  accounts: Array<LegacyLedgerAccount>
} | {
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

export type LookupTransferQueryResponse = {
  type: LookupTransferResultType.FOUND_NON_FINAL,
  // Transfer amount from Clearing Credit -> Reserved
  amountClearingCredit: bigint
  // Transfer amount from Unrestricted -> Reserved
  amountUnrestricted: bigint
} | {
  type: LookupTransferResultType.FOUND_FINAL
  finalizedTransfer: {
    completedTimestamp: string
    transferState: 'ABORTED' | 'COMMITTED'
    fulfilment?: string
  }
} | {
  type: LookupTransferResultType.NOT_FOUND
} | {
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

export type PrepareResult =  {
  type: PrepareResultType.PASS
} |  {
  type: PrepareResultType.DUPLICATE_FINAL
  finalizedTransfer: {
    completedTimestamp: string
    transferState: 'COMMITTED' | 'ABORTED'
    fulfilment?: string
  }
} | {
  type: PrepareResultType.DUPLICATE_NON_FINAL
} | {
  type: PrepareResultType.MODIFIED
} | {
  type: PrepareResultType.FAIL_VALIDATION
  failureReasons: Array<string>
} | {
  type: PrepareResultType.FAIL_LIQUIDITY
  error: FSPIOPError
} | {
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

export type FulfilResult = {
  type: FulfilResultType.PASS
} | {
  type: FulfilResultType.DUPLICATE_FINAL
} | {
  type: FulfilResultType.FAIL_VALIDATION
  error: FSPIOPError
} | {
  type: FulfilResultType.FAIL_OTHER
  error: FSPIOPError
}

export type SweepResult = {
  type: 'SUCCESS'
  transfers: Array<TimedOutTransfer>
} | {
  type: 'FAILURE'
  error: Error
}

// ============================================================================
// Settlement 
// ============================================================================

export type SettlementCloseWindowCommand = {
  id: number,
  reason: string,
}

export type SettlementPrepareCommand = {
  windowIds: Array<number>,
  model: string,
  reason: string,
}

export type SettlementAbortCommand = {

}

/**
 * In the new ledger interface, we either commit a prepared settlement or
 * abort it. All participants are settled at the same time.
 * 
 * We may want to revist this decision later on to provide better interop with
 * the Settlement API, but maintaining the 
 *  PS_TRANSFERS_RECORDED -> PS_TRANSFERS_RESERVED -> PS_TRANSFERS_COMMITTED
 * 
 * For each Dfsp
 */
export type SettlementCommitCommand = {

}




export type SettlementPrepareCommandV2 = {

  /**
   * Unique id (64 bit bigint) to represent the settlement
   */
  settlementId: bigint,

  /**
   * Currency to be settled
   */
  currency: string,

  /**
   * The selector used to select Payments to be settled
   */
  selector: SettlementSelector
}

export type SettlementSelector = {
  type: 'LEDGER_TIMERANGE',

  /**
   * The minimum Ledger creation timestamp to include in the Settlement
   * inclusive range.
   */
  timestampMin: number,

  /**
   * The maximum Ledger creation timestamp to include in the Settlement
   * inclusive range.
   */
  timestampMax: number
} | {
  type: 'TRANSFER_ID',
  transferIds: Array<string>
}

// could also be batchId, time range from Dfsp's perspective?

export type SettlementReport = {
  // TODO(LD): This should be Logical Transfers
  payments: Array<Payment>
  currency: string
  participants: Array<string>
  netMoneyMovements: Record<string, NetMoneyMovement>
}

export type SettlementPrepareResult = {
  type: 'SUCCESS',
  report: SettlementReport
} | {
  // failure during the setup
  type: 'SETUP_FAILURE'
  error: Error
} | {
  type: 'UNKNOWN_FAILURE',
  error: Error
}


// Internal representation of a payment
type Payment = {
  status: 'CREATED' | 'ABORTED' | 'FULFILLED' | 'SETTLED'
  amount: number
  currency: string,
  payer: string,
  payee: string,
  // could do even better and have this as a dict based on the status
  transfers: Array<Transfer>
}

type NetMoneyMovement = {
  participant: string,
  currency: string,
  owingGross: number
  owedGross: number
  net: {
    direction: 'OWING',
    amount: number
  } | {
    direction: 'OWED',
    amount: number
  }
}