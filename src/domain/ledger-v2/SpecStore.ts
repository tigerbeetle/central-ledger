export interface DfspAccountIds {
  collateral: bigint,
  liquidity: bigint,
  clearing: bigint,
  settlementMultilateral: bigint,
}

export interface SpecAccount extends DfspAccountIds {
  readonly type: 'SpecAccount'
  dfspId: string,
  currency: string,
}

export interface SpecAccountNone {
  type: 'SpecAccountNone'
}

export interface SpecTransfer {
  type: 'SpecTransfer'
  id: string,
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
   * Get all all Dfsp Account Spec for all Dfsps + Currencies
   */
  queryAccountsAll(): Promise<Array<SpecAccount>>

  /**
   * Get all all Dfsp Account Spec for a single Dfsp
   */
  queryAccountsDfsp(dfspId: string): Promise<Array<SpecAccount>>

  /**
   * Gets the account spec for a DFSP + Currency
   */
  getDfspAccountSpec(dfspId: string, currency: string): Promise<SpecAccount | SpecAccountNone>

  /**
   * Stores the account association between the DFSP + Currency + TigerBeetle Account IDs
   */
  associateDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>

  /**
   * Marks the previous account association between DFSP + Currency and TigerBeetle AccountIds as invalid
   */
  tombstoneDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>


  // Ok now we also need to store the following spec:
  // 1. transfer conditions (for validating the fulfil stage)
  // 2. Some subset of the payloads, so we can implement the GET -> PUT for duplicate requests
  // 3. What else? Need to scour

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
}