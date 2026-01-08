import assert from "assert";
import { randomUUID } from 'crypto';
import { logger } from '../shared/logger';
import { Ledger } from "src/domain/ledger-v2/Ledger";
import { IParticipantService } from "src/api/participants/HandlerV2";


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
   * The account opening deposits, one per currency
   */
  startingDeposits: Array<number>

}

export interface DFSPProvisionerDependencies {
  ledger: Ledger
  participantService: IParticipantService
}

/**
 * Testing utility to configure a dfsp based on a config file.
 */
export default class DFSPProvisioner {

  constructor(private deps: DFSPProvisionerDependencies) { }

  public async run(config: DFSPProvisionerConfig): Promise<void> {
    assert(config.currencies.length > 0, 'DFSP should have at least 1 currency')
    assert.equal(config.currencies.length, config.startingDeposits.length)

    const childLogger = logger.child({ dfspId: config.dfspId });

    try {
      await this.deps.participantService.ensureExists(config.dfspId)
      const result = await this.deps.ledger.createDfsp(config)
      if (result.type === 'FAILURE') {
        throw result.error
      }

      if (result.type === 'ALREADY_EXISTS') {
        childLogger.info('DFSP already created')
        return
      }

      for (let i = 0; i < config.currencies.length; i++) {
        const currency = config.currencies[i];
        const startingDeposit = config.startingDeposits[i]
        assert(currency)
        assert(startingDeposit)

        const depositResult = await this.deps.ledger.deposit({
          transferId: randomUUID(),
          dfspId: config.dfspId,
          currency,
          amount: startingDeposit,
        })
        if (depositResult.type === 'FAILURE') {
          throw depositResult.error
        }
        if (depositResult.type === 'ALREADY_EXISTS') {
          return
        }

        // TODO(LD): we need to set this limit for the LegacyLedger because it has a side effect
        // of enabling the account.
        await this.deps.ledger.setNetDebitCap({
          netDebitCapType: "AMOUNT",
          dfspId: config.dfspId,
          currency,
          amount: startingDeposit
        })

        childLogger.info(`deposited collateral of: ${currency} ${startingDeposit}`)
      }

      childLogger.info('DFSP provisioning completed successfully');
    } catch (error) {
      childLogger.error('DFSP provisioning failed', { error: error.message });
      throw error;
    }
  }
}