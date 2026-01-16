import { QueryResult } from "src/shared/results"

export interface DfspAccountIds {
  deposit: bigint,
  unrestricted: bigint,
  unrestrictedLock: bigint,
  restricted: bigint,
  reserved: bigint,
  commitedOutgoing: bigint,
  netDebitCap: bigint,
  netDebitCapControl: bigint,
}

/**
 * The specification which defines the TigerBeetle Accounts for a dfspId + currency
 */
export interface SpecAccount extends DfspAccountIds {
  readonly type: 'SpecAccount'
  dfspId: string,
  currency: string,
}

/**
 * Defines the Net Debit Cap setting for a dfspId + currency
 */
export type SpecNetDebitCap = {
  type: 'UNLIMITED',
  dfspId: string,
  currency: string,
} | {
  type: 'LIMITED',
  amount: number,
  dfspId: string,
  currency: string,
}


// TODO(LD): refactor this to a simple result type
export interface SpecAccountNone {
  type: 'SpecAccountNone'
}

/**
 * The specification which defines the TigerBeetle Transfer Metadata for a Mojaloop Transfer
 */
export interface SpecTransfer {
  type: 'SpecTransfer'
  id: string,
  // Ideally we wouldn't need to include this here and store it in the SpecStore because it confuses
  // which is the system of record (TigerBeetle) vs the system of reference (MySQL).
  // We _need_ to store amount in the system of reference because we need it in Ledger.fulfil() and
  // can't afford the performance hit to lookup the prepared payment in TigerBeetle
  amount: string,
  currency: string,
  payerId: string,
  payeeId: string,
  condition: string,
  ilpPacket: string,
  fulfilment?: string
}

export interface SpecTransferNone {
  type: 'SpecTransferNone'
  id: string
}

/**
 * The specification which defines the TigerBeetle Transfer Metadata for an Admin Funding operation
 * (deposit/withdrawal)
 */
export interface SpecFunding {
  type: 'SpecFunding'
  transferId: string,
  dfspId: string,
  currency: string,
  action: FundingAction,
  reason: string,
}

export interface SpecFundingNone {
  type: 'SpecFundingNone'
  transferId: string
}

export type FundingAction = 'DEPOSIT' | 'WITHDRAWAL'

export type SaveFundingSpecCommand = Omit<SpecFunding, 'type'>

export interface SaveSpecFundingResultSuccess {
  type: 'SUCCESS'
}

export interface SaveSpecFundingResultExists {
  type: 'EXISTS'
}

export interface SaveSpecFundingResultFailure {
  type: 'FAILURE'
}

export type SaveSpecFundingResult = SaveSpecFundingResultSuccess | SaveSpecFundingResultExists | SaveSpecFundingResultFailure

export type SaveSpecNetDebitCapResult = {
  type: 'SUCCESS'
} | {
  type: 'FAILURE',
  error: Error
}

export type GetSpecNetDebitCapResult = {
  type: 'SUCCESS',
  result: SpecNetDebitCap
} | {
  type: 'FAILURE',
  query: {dfspId: string, currency: string},
  error: Error
}

/**
 * The specification which describes the master account for the DFSP
 */
export interface SpecDfsp {
  readonly type: 'SpecDfsp'
  dfspId: string,
  accountId: bigint
}

export interface SpecDfspNone {
  readonly type: 'SpecDfspNone'
}

export type SaveTransferSpecCommand = Omit<SpecTransfer, 'type' | 'fulfilment'>

export type AttachTransferSpecFulfilment = {
  id: string,
  fulfilment: string
}

export interface SaveSpecTransferResultSuccess {
  type: 'SUCCESS'
}

export interface SaveSpecTransferResultFailure {
  type: 'FAILURE'
}

export type SaveSpecTransferResult = SaveSpecTransferResultSuccess | SaveSpecTransferResultFailure

export interface SpecStore {

  /**
   * Associate a dfspId with the master account in TigerBeetle
   */
  associateDfsp(dfspId: string, accountId: bigint): Promise<void>

  /**
   * Get all SpecDfsps
   */
  queryDfspsAll(): Promise<Array<SpecDfsp>>

  /**
   * Get SpecDfsp for a single Dfsp
   */
  queryDfsp(dfspId: string): Promise<SpecDfsp | SpecDfspNone>

  /**
   * Get all SpecAccounts for all Dfsps + Currencies
   */
  queryAccountsAll(): Promise<Array<SpecAccount>>

  /**
   * Get the SpecAccounts for a single Dfsp
   */
  queryAccounts(dfspId: string): Promise<Array<SpecAccount>>

  /**
   * Gets the SpecAccount for a DFSP + Currency
   */
  getAccountSpec(dfspId: string, currency: string): Promise<SpecAccount | SpecAccountNone>

  /**
   * Stores the account association between the DFSP + Currency + TigerBeetle Account IDs
   */
  associateAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>

  /**
   * Marks the previous account association between DFSP + Currency and TigerBeetle AccountIds as invalid
   */
  tombstoneAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>

  /**
   * Looks up the transfer spec for a given set of Mojaloop Ids. Always returns the transfers 
   * in the order they are given.
   */
  lookupTransferSpec(ids: Array<string>): Promise<Array<SpecTransfer | SpecTransferNone>>

  /**
   * Saves the transfer spec to the spec store
   */
  saveTransferSpec(spec: Array<SaveTransferSpecCommand>): Promise<Array<SaveSpecTransferResult>>

  /**
   * Attaches the fulfilment to the already created spec
   */
  attachTransferSpecFulfilment(attachments: Array<AttachTransferSpecFulfilment>): Promise<Array<SaveSpecTransferResult>>

  /**
   * Looks up the funding spec for a given set of transfer IDs. Always returns the fundings
   * in the order they are given.
   */
  lookupFundingSpec(transferIds: Array<string>): Promise<Array<SpecFunding | SpecFundingNone>>

  /**
   * Saves the funding spec to the spec store
   */
  saveFundingSpec(spec: Array<SaveFundingSpecCommand>): Promise<Array<SaveSpecFundingResult>>

  /**
   * Saves the net debit caps
   */
  saveSpecNetDebitCaps(netDebitCaps: Array<SpecNetDebitCap>): Promise<Array<SaveSpecNetDebitCapResult>>

  /**
   * Gets the net debit caps
   */
  getSpecNetDebitCaps(dfspCurrencies: Array<{dfspId: string, currency: string}>): Promise<Array<GetSpecNetDebitCapResult>>
}