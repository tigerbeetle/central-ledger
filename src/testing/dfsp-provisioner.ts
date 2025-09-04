import assert from "assert";
import { randomUUID } from 'crypto';
import LegacyCompatibleLedger from "../domain/ledger-v2/LegacyCompatibleLedger";
import { logger } from '../shared/logger';


export interface DFSPProvisionerConfig {
  /**
   * The id of the DFSP
   */
  dfspId: string

  /**
   * Which currencies to create accounts for
   */
  currencies: Array<string>

  /**
   * The account opening limits, one per currency
   */
  initialLimits: Array<number>

}

export interface DFSPProvisionerDependencies {
  ledger: LegacyCompatibleLedger
}

/**
 * Testing utility to configure a dfsp based on a config file.
 */
export default class DFSPProvisioner {

  constructor(private deps: DFSPProvisionerDependencies) {

  }

  public async run(config: DFSPProvisionerConfig): Promise<void> {
    assert(config.currencies.length > 0, 'DFSP should have at least 1 currency')
    assert.equal(config.currencies.length, config.initialLimits.length)

    const childLogger = logger.child({ dfspId: config.dfspId });

    try {
      const result = await this.deps.ledger.createDfsp(config)
      if (result.type === 'FAILED') {
        throw result.error
      }

      if (result.type === 'ALREADY_EXISTS') {
        childLogger.info('DFSP already created')
        return
      }

      for (let i = 0; i < config.currencies.length; i++) {
        const currency = config.currencies[i];
        const initialLimit = config.initialLimits[i]
        assert(currency)
        assert(initialLimit)

        const depositResult = await this.deps.ledger.depositCollateral({
          transferId: randomUUID(),
          dfspId: config.dfspId,
          currency,
          amount: initialLimit,
        })
        if (depositResult.type === 'FAILED') {
          throw depositResult.error
        }
        if (depositResult.type === 'ALREADY_EXISTS') {
          return
        }

        childLogger.info(`depositted collateral of: ${currency} ${initialLimit}`)
      }

      childLogger.info('DFSP provisioning completed successfully');
    } catch (error) {
      childLogger.error('DFSP provisioning failed', { error: error.message });
      throw error;
    }
  }
}