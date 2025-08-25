import { IMessageCommitter } from './types';
import { Kafka } from '@mojaloop/central-services-stream';

export class MessageCommitter implements IMessageCommitter {
  constructor(private consumer: Kafka.Consumer) {}

  async commit(message: any): Promise<void> {
    await this.consumer.commitMessageSync(message);
  }
}