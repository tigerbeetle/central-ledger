import { Enum, Util } from '@mojaloop/central-services-shared'
import { Kafka } from '@mojaloop/central-services-stream'
import { ApplicationConfig } from 'src/shared/config'
import { MessageCommitter } from '../../messaging/MessageCommitter'
import { logger } from '../../shared/logger'
import { AdminHandler, AdminHandlerDependencies } from './AdminHandler'

const rethrow = Util.rethrow

export const createAdminHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
) => {
  // Import existing business logic modules
  const TransferService = require('../../domain/transfer/index')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const Db = require('../../lib/db')

  const dependencies: AdminHandlerDependencies = {
    committer: new MessageCommitter(consumer),
    config,
    transferService: TransferService,
    comparators: Comparators,
    db: Db
  }

  const handler = new AdminHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerAdminHandler_new = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
): Promise<void> => {
  try {
    logger.debug(`registerAdminHandler_new registering`)

    const handleMessage = createAdminHandler(config, consumer)
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerAdminHandler_new' })
  }
}