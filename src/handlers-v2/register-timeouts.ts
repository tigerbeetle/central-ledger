// import { TimeoutHandler, TimeoutHandlerDependencies } from './TimeoutHandler';
// import { TimeoutScheduler, TimeoutSchedulerConfig } from '../messaging/jobs/TimeoutScheduler';
// import { NotificationProducer } from '../messaging/producers/NotificationProducer';
// import { PositionProducer } from '../messaging/producers/PositionProducer';
// import { INotificationProducer, IPositionProducer } from '../messaging/types';
// import { Kafka } from '@mojaloop/central-services-stream';
// import { logger } from '../shared/logger';
// const { createLock } = require('../../lib/distLock');
// const { TIMEOUT_HANDLER_DIST_LOCK_KEY } = require('../../shared/constants');

// const log = logger.child({ context: 'TimeoutHandlerRegister' });

// export function createTimeoutHandler(
//   config: any,
//   notificationProducer: INotificationProducer,
//   positionProducer: IPositionProducer
// ): TimeoutHandler {
//   // Get TimeoutService - this would normally be injected
//   const TimeoutService = require('../../domain/timeout');
  
//   // Initialize distributed lock if enabled
//   let distLock;
//   const distLockEnabled = config.HANDLERS_TIMEOUT_DIST_LOCK_ENABLED === true;
//   if (distLockEnabled) {
//     distLock = createLock(config.HANDLERS_TIMEOUT.DIST_LOCK, log);
//   }

//   const deps: TimeoutHandlerDependencies = {
//     notificationProducer,
//     positionProducer,
//     config,
//     timeoutService: TimeoutService,
//     distLock
//   };

//   return new TimeoutHandler(deps);
// }

// export async function registerTimeoutHandler_new(
//   config: any,
//   notificationProducer: Kafka.Producer,
//   positionProducer: Kafka.Producer
// ): Promise<TimeoutScheduler> {
//   try {
//     if (config.HANDLERS_TIMEOUT_DISABLED) {
//       log.info('Timeout handler is disabled');
//       throw new Error('Timeout handler is disabled');
//     }

//     // Create wrapped producers
//     const wrappedNotificationProducer = new NotificationProducer(notificationProducer, config);
//     const wrappedPositionProducer = new PositionProducer(positionProducer, config);

//     // Create timeout handler
//     const timeoutHandler = createTimeoutHandler(config, wrappedNotificationProducer, wrappedPositionProducer);

//     // Create scheduler config
//     const schedulerConfig: TimeoutSchedulerConfig = {
//       cronTime: config.HANDLERS_TIMEOUT_TIMEXP,
//       timeZone: config.HANDLERS_TIMEOUT_TIMEZONE,
//       onTick: async () => {
//         try {
//           const result = await timeoutHandler.processTimeouts();
//           log.debug('Timeout processing completed', result);
//           return result;
//         } catch (err) {
//           log.error('Error in timeout processing:', err);
//           throw err;
//         }
//       }
//     };

//     // Create and start scheduler
//     const timeoutScheduler = TimeoutScheduler.createWithSignalHandlers(schedulerConfig);
//     await timeoutScheduler.start();

//     log.info('registerTimeoutHandler_new completed successfully');
//     return timeoutScheduler;
//   } catch (err) {
//     log.error('Error in registerTimeoutHandler_new:', err);
//     throw err;
//   }
// }