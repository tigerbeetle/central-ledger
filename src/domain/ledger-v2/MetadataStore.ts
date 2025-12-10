export interface DfspAccountIds {
  collateral: bigint,
  liquidity: bigint,
  clearing: bigint,
  settlementMultilateral: bigint,
}

export interface DfspAccountMetadata extends DfspAccountIds {
  readonly type: 'DfspAccountMetadata'
  dfspId: string,
  currency: string,
}

export interface DfspAccountMetadataNone {
  type: 'DfspAccountMetadataNone'
}

export interface TransferMetadata {
  type: 'TransferMetadata'
  id: string,
  payerId: string,
  payeeId: string,
  condition: string,
  ilpPacket: string,
  fulfilment?: string
}

export interface TransferMetadataNone {
  type: 'TransferMetadataNone'
  id: string
}

export type SaveTransferMetadataCommand = Omit<TransferMetadata, 'type'>

export interface SaveTransferMetadataResultSuccess {
  type: 'SUCCESS'
}

export interface SaveTransferMetadataResultFailure {
  type: 'FAILURE'
}

export type SaveTransferMetadataResult = SaveTransferMetadataResultSuccess | SaveTransferMetadataResultFailure

export interface MetadataStore {

  getAllDfspAccountMetadata(): Promise<Array<DfspAccountMetadata>>

  /**
   * Gets the account metadata for a DFSP + Currency
   */
  getDfspAccountMetadata(dfspId: string, currency: string): Promise<DfspAccountMetadata | DfspAccountMetadataNone>

  /**
   * Stores the account association between the DFSP + Currency + TigerBeetle Account IDs
   */
  associateDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>

  /**
   * Marks the previous account association between DFSP + Currency and TigerBeetle AccountIds as invalid
   */
  tombstoneDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void>


  // Ok now we also need to store the following metadata:
  // 1. transfer conditions (for validating the fulfil stage)
  // 2. Some subset of the payloads, so we can implement the GET -> PUT for duplicate requests
  // 3. What else? Need to scour

  /**
   * Looks up the transfer metadata for a given set of Mojaloop Ids. Always returns the transfers 
   * in the order they are given.
   */
  lookupTransferMetadata(ids: Array<string>): Promise<Array<TransferMetadata | TransferMetadataNone>>

  /**
   * Saves the transfer metadata to the metadata store
   */
  saveTransferMetadata(metadata: Array<SaveTransferMetadataCommand>): Promise<Array<SaveTransferMetadataResult>>

  // /**
  //  * Update existing transfers, attaching the fulfilment
  //  */
  // updateTransferMetadataFulfilment(transfersToUpdate: Array<{id: string, fulfilment: string}>): Promise<Array<SaveTransferMetadataResult>>
}