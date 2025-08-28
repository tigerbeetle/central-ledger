export interface CreateTransferDto {
  amount: {
    amount: string,
    currency: string
  },
  condition: string,
  expiration: string,
  ilpPacket: string,
  payeeFsp: string,
  payerFsp: string,
  transferId: string,
}

export interface CommitTransferDto {
  transferState: 'COMMITTED' | 'RESERVED',
  fulfilment: string,
  completedTimestamp: string,
}

export interface AbortTransferDto {
  errorInformation: {
    errorCode: string,
    errorDescription: string,
    extensionList?: {
      extension: Array<{
        key: string,
        value: string
      }>
    }
  }
}

export interface TimeoutTransferDto {
  transferState: 'EXPIRED',
  completedTimestamp: string,
}