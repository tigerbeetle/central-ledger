import { Kafka } from '@mojaloop/central-services-stream';
import { INotificationProducer, NotificationErrorMessage, NotificationMessage } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { Enum } from '@mojaloop/central-services-shared';

const KafkaUtil = require('@mojaloop/central-services-shared').Util.Kafka


export class NotificationProducer implements INotificationProducer {
  constructor(
    private producer: Kafka.Producer,
    private config: any
  ) { }

  async sendError(message: NotificationErrorMessage): Promise<void> {
    const kafkaMessage = this.buildErrorMessage(message);
    const topic = this.getNotificationTopic();

    await this.producer.sendMessage(
      kafkaMessage,
      {
        opaqueKey: '12345',
        topicName: topic,
        key: message.transferId,
      })

    // await this.producer.sendMessage(message, {
    //   topic,
    //   key: message.transferId,
    //   value: kafkaMessage
    // },);
  }

  async sendSuccess(message: NotificationMessage): Promise<void> {
    throw new Error('not implemented')
    // const kafkaMessage = this.buildSuccessMessage(message);
    // const topic = this.getNotificationTopic();

    // await this.producer.sendMessage({
    //   topic,
    //   key: message.transferId,
    //   value: kafkaMessage
    // });
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
    throw new Error('not implemented')
    // const kafkaMessage = this.buildDuplicateMessage(message);
    // const topic = this.getNotificationTopic();

    // await this.producer.sendMessage({
    //   topic,
    //   key: message.transferId,
    //   value: kafkaMessage
    // });
  }

  private buildErrorMessage(message: NotificationErrorMessage): Kafka.MessageProtocol {
    // TODO(LD): check what this is meant to be based on real examples
    return {
      content: {
        uriParams: {
          id: message.transferId
        },
        headers: {
          // TODO: we need to get headers somehow
        },
        payload: message.fspiopError,
        context: {}
      },
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      metadata: {
        correlationId: message.transferId,
        event: {
          type: 'notification',
          action: message.action,
          createdAt: (new Date).toISOString(),
          state: {
            status: 'error',
            code: message.fspiopError.errorInformation.errorCode,
            description: message.fspiopError.errorInformation.errorDescription,
          }
        }
      },
    }
  }

  private buildSuccessMessage(message: NotificationMessage): any {
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
          action: message.action.toLowerCase().replace('_', '-'),
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0
          }
        }
      }
    };
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

  private buildDuplicateMessage(message: NotificationMessage): any {
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
          action: message.action.toLowerCase().replace('_', '-') + '-duplicate',
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0
          }
        }
      }
    };
  }

  private getNotificationTopic(): string {
    // Default notification topic generation
    const Kafka = require('@mojaloop/central-services-shared').Util.Kafka;
    return Kafka.transformGeneralTopicName(
      this.config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE,
      Enum.Events.Event.Type.NOTIFICATION,
      Enum.Events.Event.Action.EVENT
    );
  }
}