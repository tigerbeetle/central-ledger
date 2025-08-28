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

    await this.producer.sendMessage(
      kafkaMessage,
      {
        topicName: topic,
        key: message.messageKey || message.participantCurrencyId,
        opaqueKey: message.transferId
      }
    );
  }

  async sendCommit(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'COMMIT');
    const topic = this.getTopicName('COMMIT');

    await this.producer.sendMessage(
      kafkaMessage,
      {
        topicName: topic,
        key: message.messageKey || message.participantCurrencyId,
        opaqueKey: message.transferId
      }
    );
  }

  async sendReserve(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'RESERVE');
    const topic = this.getTopicName('RESERVE');

    await this.producer.sendMessage(
      kafkaMessage,
      {
        topicName: topic,
        key: message.messageKey || message.participantCurrencyId,
        opaqueKey: message.transferId
      }
    );
  }

  async sendAbort(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessageAbort(message);
    const topic = this.getTopicName('ABORT');

    await this.producer.sendMessage(
      kafkaMessage,
      {
        topicName: topic,
        key: message.messageKey || message.participantCurrencyId,
        opaqueKey: message.transferId
      }
    );
  }

  async sendFxPrepare(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'FX_PREPARE');
    const topic = this.getTopicName('FX_PREPARE');
    throw new Error('not implemented')
  }

  async sendBulkPrepare(message: PositionMessage): Promise<void> {
    const kafkaMessage = this.buildKafkaMessage(message, 'BULK_PREPARE');
    const topic = this.getTopicName('BULK_PREPARE');
    throw new Error('not implemented')
  }

  private buildKafkaMessage(message: PositionMessage, action: string): any {
    return {
      id: message.transferId,
      from: message.from,
      to: message.to,
      type: 'application/json',
      content: {
        uriParams: { id: message.transferId },
        headers: message.headers,
        payload: message.payload,
        context: message.cyrilResult ? { cyrilResult: message.cyrilResult } : undefined
      },
      metadata: {
        correlationId: message.transferId,
        event: {
          type: 'position',
          action: action.toLowerCase().replace('_', '-'),
          createdAt: new Date().toISOString(),
          state: {
            status: 'success',
            code: 0,
            description: 'action successful'
          },
          id: message.transferId
        },
        trace: message.metadata?.trace
      }
    };
  }

  private buildKafkaMessageAbort(message: PositionMessage): any {
    return {
      from: "Hub",
      to: message.to,
      id: message.transferId,
      content: {
        uriParams: {
          id: message.transferId
        },
        headers: {
          accept: "application/vnd.interoperability.transfers+json;version=1.0",
          "FSPIOP-Destination": message.to,
          "Content-Type": "application/vnd.interoperability.transfers+json;version=1.0",
          date: new Date().toUTCString(),
          "FSPIOP-Source": "Hub"
        },
        payload: message.payload,
        context: message.cyrilResult ? { cyrilResult: message.cyrilResult } : undefined
      },
      type: "application/vnd.interoperability.transfers+json;version=1.0",
      metadata: {
        correlationId: message.transferId,
        event: {
          type: "position",
          action: "timeout-reserved",
          createdAt: new Date().toISOString(),
          state: {
            status: "error",
            code: "3303",
            description: "Transfer expired"
          },
          id: uuidv4()
        },
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