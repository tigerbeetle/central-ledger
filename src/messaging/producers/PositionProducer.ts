import { Kafka } from '@mojaloop/central-services-stream';
import { IPositionProducer, PositionMessage, KafkaMessage } from '../types';
import { v4 as uuidv4 } from 'uuid';
import { Enum } from '@mojaloop/central-services-shared';

export class PositionProducer implements IPositionProducer {
  constructor(
    private producer: Kafka.Producer,
    private config: any
  ) { }

  async sendPrepare(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'PREPARE');
    const topic = this.getTopicName('PREPARE');
    throw new Error('not implemented')


    // await this.producer.sendMessage({
    //   topic,
    //   key: message.messageKey || message.participantCurrencyId,
    //   value: kafkaMessage
    // });
  }

  async sendCommit(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'COMMIT');
    const topic = this.getTopicName('COMMIT');

    throw new Error('not implemented')


    // await this.producer.sendMessage({
    //   topic,
    //   key: message.messageKey || message.participantCurrencyId,
    //   value: kafkaMessage
    // });
  }

  async sendAbort(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'ABORT');
    const topic = this.getTopicName('ABORT');

    throw new Error('not implemented')


    // await this.producer.sendMessage({
    //   topic,
    //   key: message.messageKey || message.participantCurrencyId,
    //   value: kafkaMessage
    // });
  }

  async sendFxPrepare(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'FX_PREPARE');
    const topic = this.getTopicName('FX_PREPARE');
    throw new Error('not implemented')


    // await this.producer.sendMessage({
    //   topic,
    //   key: message.messageKey || message.participantCurrencyId,
    //   value: kafkaMessage
    // });
  }

  async sendBulkPrepare(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'BULK_PREPARE');
    const topic = this.getTopicName('BULK_PREPARE');
    throw new Error('not implemented')


    // await this.producer.sendMessage({
    //   topic,
    //   key: message.messageKey || message.participantCurrencyId,
    //   value: kafkaMessage
    // });
  }

  private buildKafkaMessage(message: PositionMessage, action: string): any {
    return {
      id: message.transferId,
      from: this.config.HUB_NAME,
      to: null, // Will be set by position handler
      type: 'application/json',
      content: {
        headers: {},
        payload: {
          transferId: message.transferId,
          participantCurrencyId: message.participantCurrencyId,
          amount: message.amount,
          currency: message.currency
        },
        uriParams: { id: message.transferId },
        context: message.cyrilResult ? { cyrilResult: message.cyrilResult } : undefined
      },
      metadata: {
        event: {
          id: uuidv4(),
          type: 'position',
          action: action.toLowerCase().replace('_', '-'),
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0
          }
        }
      }
    };
  }

  private getTopicName(action: string): string {
    // Get topic from config mapping, fall back to default
    const topicMap = this.config.KAFKA_CONFIG?.EVENT_TYPE_ACTION_TOPIC_MAP?.POSITION;
    const overrideTopic = topicMap?.[action];

    if (overrideTopic) {
      return overrideTopic;
    }

    // Default topic generation
    const Kafka = require('@mojaloop/central-services-shared').Util.Kafka;
    return Kafka.transformGeneralTopicName(
      this.config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE,
      Enum.Events.Event.Type.POSITION,
      action.toLowerCase().replace('_', '-')
    );
  }
}