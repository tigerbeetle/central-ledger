// Common types for handlers

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
  transferState: 'COMMITTED' | 'ABORTED',
  fulfilment?: string,
  completedTimestamp: string,
}

// Additional types can be added here as needed
export interface AbortTransferDto {
  transferState: 'ABORTED',
  completedTimestamp: string,
  errorInformation?: {
    errorCode: string,
    errorDescription: string,
  },
}

export interface TimeoutTransferDto {
  transferState: 'EXPIRED',
  completedTimestamp: string,
}