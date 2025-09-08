import assert from "node:assert"
import { Ledger } from "src/domain/ledger-v2/Ledger"
import LegacyCompatibleLedger from "src/domain/ledger-v2/LegacyCompatibleLedger"
import { CreateHubAccountCommand } from "src/domain/ledger-v2/types"

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
    const createHubAccountCommand: CreateHubAccountCommand = {
      currency: 'USD',
      settlementModel: {
        name: `DEFERRED_MULTILATERAL_NET_USD`,
        settlementGranularity: "NET",
        settlementInterchange: "MULTILATERAL",
        settlementDelay: "DEFERRED",
        currency: 'USD',
        requireLiquidityCheck: true,
        ledgerAccountType: "POSITION",
        settlementAccountType: "SETTLEMENT",
        autoPositionReset: true
      }
    }
    const result = await this.deps.ledger.createHubAccount(createHubAccountCommand)
    if (result.type === 'FAILED') {
      logger.error(`Provisioner.run() failed with error`, {error: result.error})
      throw result.error
    }

    if (result.type === 'ALREADY_EXISTS') {
      logger.warn(`Hub account already created for: ${createHubAccountCommand.currency}, ${createHubAccountCommand.settlementModel.name}`)
    }
  }
}

