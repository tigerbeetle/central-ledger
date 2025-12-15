export interface DfspAccountIds {
  collateral: bigint,
  liquidity: bigint,
  clearing: bigint,
  settlementMultilateral: bigint,
}

export interface DfspAccountSpec extends DfspAccountIds {
  readonly type: 'DfspAccountSpec'
  dfspId: string,
  currency: string,
}

export interface DfspAccountSpecNone {
  type: 'DfspAccountSpecNone'
}

export interface TransferSpec {
  type: 'TransferSpec'
  id: string,
  payerId: string,
  payeeId: string,
  condition: string,
  ilpPacket: string,
  fulfilment?: string
}

export interface TransferSpecNone {
  type: 'TransferSpecNone'
  id: string
}

export type SaveTransferSpecCommand = Omit<TransferSpec, 'type'>

export interface SaveTransferSpecResultSuccess {
  type: 'SUCCESS'
}

export interface SaveTransferSpecResultFailure {
  type: 'FAILURE'
}

export type SaveTransferSpecResult = SaveTransferSpecResultSuccess | SaveTransferSpecResultFailure

export interface SpecStore {

  /**
   * Get all all Dfsp Account Spec for all Dfsps + Currencies
   */
  queryAccountsAll():  Promise<Array<DfspAccountSpec>>

  /**
   * Get all all Dfsp Account Spec for a single Dfsp
   */
  queryAccountsDfsp(dfspId: string): Promise<Array<DfspAccountSpec>>

  /**
   * Gets the account spec for a DFSP + Currency
   */
  getDfspAccountSpec(dfspId: string, currency: string): Promise<DfspAccountSpec | DfspAccountSpecNone>

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
  lookupTransferSpec(ids: Array<string>): Promise<Array<TransferSpec | TransferSpecNone>>

  /**
   * Saves the transfer spec to the spec store
   */
  saveTransferSpec(spec: Array<SaveTransferSpecCommand>): Promise<Array<SaveTransferSpecResult>>

  // /**
  //  * Update existing transfers, attaching the fulfilment
  //  */
  // updateTransferSpecFulfilment(transfersToUpdate: Array<{id: string, fulfilment: string}>): Promise<Array<SaveTransferSpecResult>>
}