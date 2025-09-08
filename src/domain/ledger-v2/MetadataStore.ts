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

export interface MetadataStore {

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
}