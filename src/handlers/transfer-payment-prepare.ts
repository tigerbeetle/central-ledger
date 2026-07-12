import assert from "node:assert";
import { ApplicationConfig } from "../lib/config";
import { logger } from '../shared/logger';
import CentralServicesShared, { Enum, Util } from '@mojaloop/central-services-shared';
const { decodePayload } = Util.StreamingProtocol


interface Dependencies {
  config: ApplicationConfig
}


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


export interface FusedPrepareHandlerInput {
  message: any;
  payload: CreateTransferDto;
  headers: any;
  transferId: string;
  action: any;
  metric: string;
  functionality: CentralServicesShared.EventTypeEnum.TRANSFER;
  actionEnum: string;
}

export class TransferPaymentPrepareHandler {
  constructor(private deps: Dependencies) { }

  async handle(error: any, messages: any): Promise<void> {
    assert(Array.isArray(messages))
     if (messages.length === 0) {
      logger.debug('TransferPaymentPrepareHandler.handle() - received empty batch, nothing to process');
      return;
    }

    logger.debug(`TransferPaymentPrepareHandler.handle() - processing batch of ${messages.length} messages`)

    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));

    // now call the old methods
    await Promise

  }

  async handleOne(input: FusedPrepareHandlerInput): Promise<void> {

  }

  private extractMessageData(message: any): FusedPrepareHandlerInput {
    // TODO: Validate the messages
    // Assert that isFx = false, isForwarded = false

    assert(message)
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.headers)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.action)
    const payloadEncoded = message.value.content.payload
    const payload = decodePayload(payloadEncoded, {}) as unknown as CreateTransferDto
    const headers = message.value.content.headers

    const transferId = payload.transferId

    const action = message.value.metadata.event.action
    // Note: we currently only support prepare messages
    assert.equal(action, 'prepare')

    return {
      message,
      payload,
      headers,
      transferId,
      action,
      metric: `handler_transfers_${action.toLowerCase()}`,
      functionality: Enum.Events.Event.Type.TRANSFER,
      actionEnum: this.getActionEnum(action)
    };
  }

  private getActionEnum(action: string): string {
    const actionUpper = action.toUpperCase() as keyof typeof Enum.Events.Event.Action
    return Enum.Events.Event.Action[actionUpper] || actionUpper;
  }
}
