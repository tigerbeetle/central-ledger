import { Kafka } from '@mojaloop/central-services-stream';
import { INotificationProducer, NotificationErrorMessage, NotificationMessage } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { Enum } from '@mojaloop/central-services-shared';

const KafkaUtil = require('@mojaloop/central-services-shared').Util.Kafka


export class NotificationProducer implements INotificationProducer {
  private notificationTopic: string;

  constructor(
    private producer: Kafka.Producer,
    private config: any
  ) {
    this.notificationTopic = KafkaUtil.transformGeneralTopicName(
      this.config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE,
      Enum.Events.Event.Type.NOTIFICATION,
      Enum.Events.Event.Action.EVENT
    );
  }

  async sendError(message: NotificationErrorMessage): Promise<void> {
    const kafkaMessage = this.buildErrorMessage(message);

    await this.producer.sendMessage(
      kafkaMessage,
      {
        opaqueKey: '12345',
        topicName: this.notificationTopic,
        key: message.transferId,
      }
    )
  }

  async sendSuccess(message: NotificationMessage): Promise<void> {
    const kafkaMessage = this.buildSuccessMessage(message);

    await this.producer.sendMessage(
      kafkaMessage,
      {
        opaqueKey: '12345',
        topicName: this.notificationTopic,
        key: message.transferId,
      }
    );
  }

  async sendForwarded(message: NotificationMessage): Promise<void> {
    throw new Error('not implemented')
    // const kafkaMessage = this.buildForwardedMessage(message);
    // const topic = this.getNotificationTopic();

    // await this.producer.sendMessage({
    //   topic,
    //   key: message.transferId,
    //   value: kafkaMessage
    // });
  }

  async sendDuplicate(message: NotificationMessage): Promise<void> {
    const kafkaMessage = this.buildDuplicateMessage(message);

    await this.producer.sendMessage(
      kafkaMessage,
      {
        opaqueKey: '12345',
        topicName: this.notificationTopic,
        key: message.transferId,
      }
    );
  }

  private buildErrorMessage(message: NotificationErrorMessage): Kafka.MessageProtocol {
    // Clone headers and update FSPIOP headers like the original system
    const updatedHeaders = { ...message.headers };
    updatedHeaders[Enum.Http.Headers.FSPIOP.SOURCE] = message.from;
    updatedHeaders[Enum.Http.Headers.FSPIOP.DESTINATION] = message.to;

    // Preserve the original metadata but update the event portion like the original system
    const updatedMetadata = { ...message.metadata };
    const originalEventId = updatedMetadata?.event?.id;

    updatedMetadata.event = {
      id: message.transferId,
      type: 'notification',
      action: message.action,
      createdAt: (new Date).toISOString(),
      state: {
        status: 'error',
        code: message.fspiopError.errorInformation.errorCode,
        description: message.fspiopError.errorInformation.errorDescription,
      },
      ...(originalEventId && { responseTo: originalEventId })
    };

    return {
      content: {
        uriParams: {
          id: message.transferId
        },
        headers: updatedHeaders,
        payload: message.fspiopError,
        context: {}
      },
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      metadata: updatedMetadata
    }
  }

  private buildSuccessMessage(message: NotificationMessage): Kafka.MessageProtocol {
    // Handle both NotificationProceedMessage and NotificationErrorMessage
    const headers = 'headers' in message ? message.headers : {};
    const metadata = 'metadata' in message ? message.metadata : {};

    // Clone headers and update FSPIOP headers like the original system
    const updatedHeaders = { ...headers };
    updatedHeaders[Enum.Http.Headers.FSPIOP.SOURCE] = message.from;
    updatedHeaders[Enum.Http.Headers.FSPIOP.DESTINATION] = message.to;

    // Preserve the original metadata but update the event portion like the original system
    const updatedMetadata = { ...metadata };
    const originalEventId = updatedMetadata?.event?.id;

    updatedMetadata.event = {
      id: message.transferId,
      type: 'notification',
      action: message.action,
      createdAt: (new Date).toISOString(),
      state: {
        status: 'success',
        code: 0,
        description: 'success'
      },
      ...(originalEventId && { responseTo: originalEventId })
    };

    return {
      content: {
        uriParams: {
          id: message.transferId
        },
        headers: updatedHeaders,
        payload: message.payload || {},
        context: {}
      },
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      metadata: updatedMetadata
    }
  }

  private buildForwardedMessage(message: NotificationMessage): any {
    return {
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      content: {
        headers: {
          [Enum.Http.Headers.FSPIOP.SOURCE]: message.from,
          [Enum.Http.Headers.FSPIOP.DESTINATION]: message.to
        },
        payload: message.payload || {},
        uriParams: { id: message.transferId }
      },
      metadata: {
        event: {
          id: uuidv4(),
          type: 'notification',
          action: 'forwarded',
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0
          }
        }
      }
    };
  }

  private buildDuplicateMessage(message: NotificationMessage): Kafka.MessageProtocol {
    // Clone headers and update FSPIOP headers like the original system
    const updatedHeaders = { ...message.headers };
    updatedHeaders[Enum.Http.Headers.FSPIOP.SOURCE] = message.from;
    updatedHeaders[Enum.Http.Headers.FSPIOP.DESTINATION] = message.to;

    // Preserve the original metadata but update the event portion like the original system
    const updatedMetadata = { ...message.metadata };
    const originalEventId = updatedMetadata?.event?.id;

    // Convert action to duplicate format (e.g., "PREPARE_DUPLICATE" -> "prepare-duplicate")
    const duplicateAction = message.action.toLowerCase().replace('_', '-');

    updatedMetadata.event = {
      id: uuidv4(),
      type: 'notification', 
      action: duplicateAction,
      createdAt: new Date().toISOString(),
      state: {
        status: 'success',
        code: 0,
        description: 'action successful'
      },
      ...(originalEventId && { responseTo: originalEventId })
    };

    return {
      content: {
        uriParams: {
          id: message.transferId
        },
        headers: updatedHeaders,
        payload: message.payload,
        context: {}
      },
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      metadata: updatedMetadata
    };
  }
}