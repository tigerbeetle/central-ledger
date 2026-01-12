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
 * The specification which defines the TigerBeetle Accounts for the dfspId + currency
 */
export interface SpecAccount extends DfspAccountIds {
  readonly type: 'SpecAccount'
  dfspId: string,
  currency: string,
}

export interface SpecAccountNone {
  type: 'SpecAccountNone'
}

/**
 * The specification which defines the TigerBeetle Transfer Metadata for a Mojaloop Transfer
 */
export interface SpecTransfer {
  type: 'SpecTransfer'
  id: string,
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

export interface SaveSpecFundingResultFailure {
  type: 'FAILURE'
}

export type SaveSpecFundingResult = SaveSpecFundingResultSuccess | SaveSpecFundingResultFailure

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

export type SaveTransferSpecCommand = Omit<SpecTransfer, 'type'>

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

  // /**
  //  * Update existing transfers, attaching the fulfilment
  //  */
  // updateTransferSpecFulfilment(transfersToUpdate: Array<{id: string, fulfilment: string}>): Promise<Array<SaveTransferSpecResult>>


  /**
   * Looks up the funding spec for a given set of transfer IDs. Always returns the fundings
   * in the order they are given.
   */
  lookupFundingSpec(transferIds: Array<string>): Promise<Array<SpecFunding | SpecFundingNone>>

  /**
   * Saves the funding spec to the spec store
   */
  saveFundingSpec(spec: Array<SaveFundingSpecCommand>): Promise<Array<SaveSpecFundingResult>>
}