
export interface ProvisioningConfig {
  currencies: Array<string>,
  hubAlertEmailAddress: string | undefined,
  settlementModels: Array<unknown>
  oracles: Array<unknown>
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


  // TODO(LD): in the future we should use the new Ledger interface, but for now, and to 
  // make sure we don't get blocked, let's talk directly to the database

  constructor(config: ProvisioningConfig) {
    this.config = config
  }

  public async run(): Promise<void> {
    if (await this.isFreshLedger()) {
      await this.provisionFromScratch()
      return
    }

    const delta = await this.findProvisioningDelta()
    if (!delta.reconcilable) {
      throw new Error(`Provisioner.run() - encountered fatal error. Please check provisioning
        config and rerun.`)
    }

    // TODO: apply delta
  }

  private async isFreshLedger(): Promise<boolean> {

    // TODO: look this up
    return true
  }

  private async findProvisioningDelta(): Promise<{reconcilable: boolean}> {
    // lookup the difference between what's in the Ledger and what's in the config
    // irreconcilable:
    //   - removing a currency
    //   - changing the settlement model of an existing currency
    //
    // reconcileable:
    //   - adding a new currency
    // 
    return {reconcilable: false}
  }

  private async provisionFromScratch(): Promise<void> {
    // Create hub accounts
    this.config.currencies.forEach(currency => {
      // TODO: call api/participants/handler.createHubAccount
      const requestA = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_MULTILATERAL_SETTLEMENT',
          currency,
        }
      }
      const requestB = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_RECONCILIATION',
          currency,
        }
      }
    })
  }

}

