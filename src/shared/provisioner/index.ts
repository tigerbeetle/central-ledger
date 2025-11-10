import assert from "node:assert"
import { Ledger } from "src/domain/ledger-v2/Ledger"
import LegacyCompatibleLedger from "src/domain/ledger-v2/LegacyCompatibleLedger"
import { CreateHubAccountCommand, CreateHubAccountResponse } from "src/domain/ledger-v2/types"

const logger = require('../logger').logger

export interface ProvisioningConfig {
  currencies: Array<string>,
  hubAlertEmailAddress?: string,
  settlementModels: Array<unknown>
  oracles: Array<unknown>
}

export interface ProvisionerDependencies {
  ledger: Ledger,
}


/**
 * Provision the ledger based on the config on startup
 * 
 * Steps:
 * 1. Check if the ledger has been provisioned previously, if not, then provision the switch 
 *    based on the config and exit
 * 2. If the ledger has been provisioned, figure out the delta between what's already been
 *    provisioned, and the current settings
 * 3. If the delta is irreconcilable (e.g. change of a settlement model from one type to another), then
 *    error and crash
 * 4. If the delta is reconcilable (e.g. adding a new currency), then apply the change and continue
 * 5. If there is no delta, then continue
 */
export default class Provisioner {
  private config: ProvisioningConfig
  private deps: ProvisionerDependencies

  constructor(config: ProvisioningConfig, deps: ProvisionerDependencies) {
    this.config = config
    this.deps = deps
  }

  public async run(): Promise<void> {
    const commands: Array<CreateHubAccountCommand> = this.config.currencies.map(currency => {
      return {
        currency,
        settlementModel: {
          name: `DEFERRED_MULTILATERAL_NET_${currency}`,
          settlementGranularity: "NET",
          settlementInterchange: "MULTILATERAL",
          settlementDelay: "DEFERRED",
          currency,
          requireLiquidityCheck: true,
          ledgerAccountType: "POSITION",
          settlementAccountType: "SETTLEMENT",
          autoPositionReset: true
        }
      }
    })

    // This _was_ previously in a Promise.all, but race conditions at the database meant we were
    // getting nondeterministic results, so now we run each command one at a time
    const results: Array<CreateHubAccountResponse> = []
    for await (const command of commands) {
      results.push(await this.deps.ledger.createHubAccount(command))
    }
    // const results = await Promise.all(commands.map(async command => await this.deps.ledger.createHubAccount(command)))

    const errorMessages = []
    results.forEach((result, idx) => {
      const command = commands[idx]
      if (result.type === 'FAILURE') {
        logger.error(`Provisioner.run() failed with error`, { error: result.error })
        errorMessages.push(result.error.message)
      }

      if (result.type === 'ALREADY_EXISTS') {
        logger.warn(`Provisioner.run() - Hub account already created for: ${command.currency}, ${command.settlementModel.name}`)
        return
      }

      logger.warn(`Provisioner.run() - Hub account created for: ${command.currency}, ${command.settlementModel.name}`)
    })

    if (errorMessages.length > 0) {
      throw new Error(`Provisioner.run() failed with ${errorMessages.length} underlying errors: ${errorMessages.join('\;')}`)
    }
  }
}

