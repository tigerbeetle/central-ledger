export interface PositionMessage {
  transferId: string;
  participantCurrencyId: string;
  amount: string;
  currency: string;
  action: 'PREPARE' | 'COMMIT' | 'ABORT' | 'FX_PREPARE' | 'FX_COMMIT' | 'FX_ABORT' | 'BULK_PREPARE';
  cyrilResult?: any;
  messageKey?: string;
  from: string;
  to: string;
  headers: Record<string, any>;
  payload: string; // base64 encoded payload
  metadata: any;
}

export type NotificationMessage = NotificationProceedMessage | NotificationErrorMessage

export interface NotificationProceedMessage {
  transferId: string;
  action: string;
  to: string;
  from: string;
  payload?: any;
  headers: Record<string, any>;
  metadata: any;
}

export interface NotificationErrorMessage {
  transferId: string;
  fspiopError: any;
  action: string;
  to: string;
  from: string;
  payload?: any;
  headers: Record<string, any>;
  metadata: any;
}

export interface IPositionProducer {
  sendPrepare(message: PositionMessage): Promise<void>;
  sendCommit(message: PositionMessage): Promise<void>;
  sendAbort(message: PositionMessage): Promise<void>;
  sendFxPrepare(message: PositionMessage): Promise<void>;
  sendBulkPrepare(message: PositionMessage): Promise<void>;
}

export interface INotificationProducer {
  sendError(message: NotificationMessage): Promise<void>;
  sendSuccess(message: NotificationMessage): Promise<void>;
  sendForwarded(message: NotificationMessage): Promise<void>;
  sendDuplicate(message: NotificationMessage): Promise<void>;
}

export interface IMessageCommitter {
  commit(message: any): Promise<void>;
}

export interface KafkaMessage {
  topic: string;
  key?: string;
  partition?: number;
  value: {
    id: string;
    from: string;
    to: string;
    type: string;
    content: {
      headers: Record<string, any>;
      payload: any;
      uriParams?: { id: string };
      context?: any;
    };
    metadata: {
      event: {
        id: string;
        type: string;
        action: string;
        createdAt: string;
        state: {
          status: string;
          code: number;
          description?: string;
        };
      };
    };
  };
}

export interface ProcessResult {
  type: 'success' | 'duplicate' | 'error';
  data?: any;
  positionData?: {
    participantCurrencyId: string;
    amount: string;
    currency: string;
    cyrilResult: any;
    messageKey: string;
  };
  transferId?: string;
  error?: any;
}