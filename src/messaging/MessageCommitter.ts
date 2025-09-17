import { IMessageCommitter } from './types';
import { Kafka } from '@mojaloop/central-services-stream';

export class MessageCommitter implements IMessageCommitter {
  constructor(private consumer: Kafka.Consumer) {}

  async commit(message: any): Promise<void> {
    await this.consumer.commitMessageSync(message);
  }

  async commitBatch(messages: any[]): Promise<void> {
    if (messages.length === 0) return;

    // Group messages by topic-partition and find highest offset per partition
    const offsetMap = new Map<string, { topic: string; partition: number; offset: string }>();

    messages.forEach(message => {
      const key = `${message.topic}-${message.partition}`;
      const current = offsetMap.get(key);

      if (!current || parseInt(message.offset) > parseInt(current.offset)) {
        offsetMap.set(key, {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset
        });
      }
    });

    // Commit highest offset per partition
    const topicPartitions = Array.from(offsetMap.values()).map(tp => ({
      topic: tp.topic,
      partition: tp.partition,
      offset: (parseInt(tp.offset) + 1).toString() // Kafka commits next offset to process
    }));

    (this.consumer as any).commitSync(topicPartitions);
  }
}