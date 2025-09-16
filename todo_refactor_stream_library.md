# Refactor: Replace central-services-stream with KafkaJS

## Overview
Replace `@mojaloop/central-services-stream` (node-rdkafka wrapper) with `kafkajs` to enable better compression support (Snappy) and modernize the Kafka client stack.

## Current Architecture Analysis

### Dependencies to Replace
- `@mojaloop/central-services-stream` v11.8.0
- Uses `node-rdkafka` v3.4.1 (native C library)
- Hardcoded `compression.codec: 'none'` in all configs

### Current Usage Patterns

#### Consumers
- **Location**: `src/handlers-v2/FusedPrepareHandler.ts`, `src/handlers-v2/FusedFulfilHandler.ts`
- **Pattern**: Receive `messages` array from consumer callback
- **Message Format**:
  ```typescript
  {
    value: {
      content: { headers, payload },
      metadata: { event: { action, type } },
      from, to
    }
  }
  ```
- **Commit**: Via `MessageCommitter.commit(message)`

#### Producers
- **Location**: `src/messaging/producers/NotificationProducer.ts`, `src/messaging/producers/PositionProducer.ts`
- **Pattern**: `producer.sendMessage(kafkaMessage, { topicName, key })`
- **Abstraction**: Clean interfaces `INotificationProducer`, `IPositionProducer`

## Implementation Plan

### Phase 1: Dependency Setup
1. **Add Dependencies**
   ```bash
   npm install kafkajs snappy
   npm uninstall @mojaloop/central-services-stream
   ```

2. **Update package.json**
   - Remove `@mojaloop/central-services-stream` from dependencies
   - Add `kafkajs` and `snappy` dependencies

### Phase 2: Configuration Migration

1. **Create KafkaJS Config Format**
   ```typescript
   // src/shared/kafkajs-config.ts
   export interface KafkaJSConfig {
     brokers: string[]
     clientId: string
     compression: 'gzip' | 'snappy' | 'lz4' | 'zstd' | 'uncompressed'
     producer: {
       maxInFlightRequests: number
       idempotent: boolean
       transactionTimeout: number
       batch: {
         size: number
         lingerMs: number
       }
     }
     consumer: {
       groupId: string
       sessionTimeout: number
       rebalanceTimeout: number
       heartbeatInterval: number
       maxBytesPerPartition: number
       minBytes: number
       maxBytes: number
       maxWaitTimeInMs: number
     }
   }
   ```

2. **Update config/default.json**
   - Replace KAFKA section with KafkaJS-compatible config
   - Enable Snappy compression
   - Add batching optimizations

### Phase 3: Core Infrastructure

1. **Create KafkaJS Client Wrapper**
   ```typescript
   // src/messaging/kafka-client.ts
   import { Kafka } from 'kafkajs'

   export class KafkaClient {
     private kafka: Kafka

     constructor(config: KafkaJSConfig) {
       this.kafka = new Kafka({
         clientId: config.clientId,
         brokers: config.brokers,
         // Add compression, retry logic, etc.
       })
     }

     createProducer() { /* ... */ }
     createConsumer() { /* ... */ }
   }
   ```

2. **Implement Message Committer**
   ```typescript
   // src/messaging/KafkaJSMessageCommitter.ts
   import { IMessageCommitter } from './types'
   import { Consumer } from 'kafkajs'

   export class KafkaJSMessageCommitter implements IMessageCommitter {
     constructor(
       private consumer: Consumer,
       private messageOffsetMap: Map<string, { topic: string, partition: number, offset: string }>
     ) {}

     async commit(message: any): Promise<void> {
       const offsetInfo = this.messageOffsetMap.get(message.id)
       if (offsetInfo) {
         await this.consumer.commitOffsets([{
           topic: offsetInfo.topic,
           partition: offsetInfo.partition,
           offset: (parseInt(offsetInfo.offset) + 1).toString()
         }])
       }
     }
   }
   ```

### Phase 4: Producer Implementation

1. **Replace NotificationProducer**
   ```typescript
   // src/messaging/producers/KafkaJSNotificationProducer.ts
   import { INotificationProducer, NotificationMessage } from '../types'
   import { Producer } from 'kafkajs'

   export class KafkaJSNotificationProducer implements INotificationProducer {
     constructor(
       private producer: Producer,
       private notificationTopic: string
     ) {}

     async sendSuccess(message: NotificationMessage): Promise<void> {
       const kafkaMessage = this.buildSuccessMessage(message)

       await this.producer.send({
         topic: this.notificationTopic,
         messages: [{
           key: message.transferId,
           value: JSON.stringify(kafkaMessage)
         }]
       })
     }

     async sendError(message: NotificationErrorMessage): Promise<void> {
       const kafkaMessage = this.buildErrorMessage(message)

       await this.producer.send({
         topic: this.notificationTopic,
         messages: [{
           key: message.transferId,
           value: JSON.stringify(kafkaMessage)
         }]
       })
     }

     // Copy message building logic from current NotificationProducer
     private buildSuccessMessage(message: NotificationMessage) { /* ... */ }
     private buildErrorMessage(message: NotificationErrorMessage) { /* ... */ }
   }
   ```

2. **Replace PositionProducer** (similar pattern)
   ```typescript
   // src/messaging/producers/KafkaJSPositionProducer.ts
   export class KafkaJSPositionProducer implements IPositionProducer {
     // Implement all position-related send methods
   }
   ```

### Phase 5: Consumer Implementation

1. **Create Consumer Handler Wrapper**
   ```typescript
   // src/messaging/consumers/KafkaJSConsumerHandler.ts
   export class KafkaJSConsumerHandler {
     constructor(
       private consumer: Consumer,
       private handler: (error: any, messages: any[]) => Promise<void>
     ) {}

     async start() {
       await this.consumer.run({
         eachBatch: async ({ batch, commitOffsetsIfNecessary }) => {
           // Transform KafkaJS messages to current format
           const messages = batch.messages.map(msg => this.transformMessage(msg, batch))

           try {
             // Store offset info for manual commits
             this.storeOffsetInfo(batch.messages)

             await this.handler(null, messages)

             // Commit is handled individually by MessageCommitter
           } catch (error) {
             await this.handler(error, [])
           }
         }
       })
     }

     private transformMessage(msg: KafkaMessage, batch: Batch): any {
       return {
         topic: batch.topic,
         partition: batch.partition,
         offset: msg.offset,
         key: msg.key?.toString(),
         value: JSON.parse(msg.value?.toString() || '{}'),
         // Add other required fields
       }
     }
   }
   ```

### Phase 6: Handler Updates

1. **Update Handler Registration**
   ```typescript
   // src/handlers-v2/register.ts
   export async function registerFusedHandlers(config: ApplicationConfig) {
     // Replace central-services-stream setup with KafkaJS
     const kafkaClient = new KafkaClient(config.KAFKA_CONFIG)

     const producer = kafkaClient.createProducer()
     const consumer = kafkaClient.createConsumer()

     const notificationProducer = new KafkaJSNotificationProducer(producer, 'topic-notification-event')
     const positionProducer = new KafkaJSPositionProducer(producer, 'topic-transfer-position')
     const committer = new KafkaJSMessageCommitter(consumer, messageOffsetMap)

     const fusedPrepareHandler = new FusedPrepareHandler({
       positionProducer,
       notificationProducer,
       committer,
       config,
       ledger
     })

     const consumerHandler = new KafkaJSConsumerHandler(consumer, fusedPrepareHandler.handle.bind(fusedPrepareHandler))
     await consumer.subscribe({ topics: ['topic-transfer-prepare'] })
     await consumerHandler.start()
   }
   ```

### Phase 7: Testing Strategy

1. **Unit Tests**
   - Mock KafkaJS producer/consumer
   - Test message transformation logic
   - Verify interface compliance

2. **Integration Tests**
   - Test with real Kafka cluster
   - Verify message format compatibility
   - Test compression with Snappy
   - Performance comparison with central-services-stream

3. **Migration Tests**
   - Process messages created by old system
   - Ensure backward compatibility
   - Test consumer group migration

### Phase 8: Configuration Migration

1. **Update Environment Variables**
   ```bash
   # Old format
   KAFKA_HOST=localhost:9092

   # New format
   KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094
   KAFKA_COMPRESSION=snappy
   KAFKA_BATCH_SIZE=16384
   KAFKA_LINGER_MS=5
   ```

2. **Update Docker Configs**
   - Update all config files in `config/` directory
   - Update docker-compose configurations

### Phase 9: Deployment Strategy

1. **Feature Flag Approach**
   - Add environment variable to switch between libraries
   - Deploy with central-services-stream as default
   - Gradually enable KafkaJS in test environments

2. **Blue-Green Deployment**
   - Deploy new version alongside old
   - Test thoroughly before switching traffic
   - Rollback plan ready

## Benefits After Migration

1. **Compression Support**: Native Snappy/LZ4/ZSTD support
2. **Better Batching**: Fine-tuned batch size and linger time controls
3. **Modern Library**: Active development, better TypeScript support
4. **Performance**: Potentially better throughput with optimized batching
5. **Reduced Dependencies**: No native C library compilation

## Risks and Mitigation

1. **Message Format Changes**: Ensure compatibility with existing consumers
2. **Performance Regression**: Thorough benchmarking before deployment
3. **Consumer Group Migration**: Plan offset migration strategy
4. **Error Handling**: Ensure error scenarios work identically

## Implementation Time Estimate

- **Phase 1-2**: 4 hours (dependencies, config)
- **Phase 3-4**: 8 hours (infrastructure, producers)
- **Phase 5-6**: 6 hours (consumers, handlers)
- **Phase 7**: 12 hours (testing)
- **Phase 8-9**: 4 hours (deployment)
- **Total**: ~34 hours

## Files to Modify/Create

### New Files
- `src/messaging/kafka-client.ts`
- `src/messaging/KafkaJSMessageCommitter.ts`
- `src/messaging/producers/KafkaJSNotificationProducer.ts`
- `src/messaging/producers/KafkaJSPositionProducer.ts`
- `src/messaging/consumers/KafkaJSConsumerHandler.ts`
- `src/shared/kafkajs-config.ts`

### Modified Files
- `package.json` (dependencies)
- `config/default.json` (Kafka config)
- `src/handlers-v2/register.ts` (handler setup)
- `src/shared/setup-new.ts` (initialization)
- All test files related to Kafka

### Removed Dependencies
- References to `@mojaloop/central-services-stream`
- Node-rdkafka specific configurations