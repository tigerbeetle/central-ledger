import assert from "assert";
import LegacyCompatibleLedger from "../domain/ledger-v2/LegacyCompatibleLedger";
import { logger } from '../shared/logger';

const { Enum } = require('@mojaloop/central-services-shared');


export interface DFSPProvisionerConfig {
  /**
   * The id of the DFSP
   */
  id: string

  /**
   * Which currencies to create accounts for
   */
  currencies: Array<string>

  /**
   * The account opening positions per currency
   */
  initialLimits: Array<number>

}

export interface DFSPProvisionerDependencies {
  ledger: LegacyCompatibleLedger

  // TODO(LD): remove the need for these, eventually we should just be calling the ledger itself
  participantsHandler: any
  participantService: any
  participantFacade: any
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

    const childLogger = logger.child({ dfspId: config.id });
    
    try {
      // 1. Check if DFSP already exists. If it does, then exit
      const existingParticipant = await this.checkParticipantExists(config);
      if (existingParticipant) {
        childLogger.info(`DFSP already exists, skipping provisioning`);
        return;
      }

      // 2. Create the participant
      childLogger.info(`Creating DFSP participant`);
      await this.createParticipant(config);

      childLogger.info('Creating participant accounts for currencies', { currencies: config.currencies });
      await this.createParticipantAccounts(config);

      childLogger.info('Setting initial limits for participant');
      await this.setInitialLimits(config);
      
      childLogger.info('DFSP provisioning completed successfully');
    } catch (error) {
      childLogger.error('DFSP provisioning failed', { error: error.message });
      throw error;
    }
  }

  private async checkParticipantExists(config: DFSPProvisionerConfig): Promise<boolean> {
    try {
      const participant = await this.deps.participantService.getByName(config.id);
      assert(participant)
      return true
    } catch (error) {
      // If getByName throws an error, the participant doesn't exist
      return false;
    }
  }

  private async createParticipant(config: DFSPProvisionerConfig): Promise<void> {
    // The participant is created automatically by the handler when creating the first account
    // So we don't need a separate createParticipant step
  }

  private async createParticipantAccounts(config: DFSPProvisionerConfig): Promise<void> {
    // Mock callback to suit the handler expectations
    const mockCallback = {
      response: (body: any) => {
        return {
          code: (code: number) => { }
        }
      }
    };

    for (let i = 0; i < config.currencies.length; i++) {
      const currency = config.currencies[i];
      
      // Create participant with position account (this creates participant if it doesn't exist)
      const positionAccountRequest = {
        payload: {
          name: config.id,
          currency
        }
      };
      
      await this.deps.participantsHandler.create(positionAccountRequest, mockCallback);
    }
  }

  private async setInitialLimits(config: DFSPProvisionerConfig): Promise<void> {
    for (let i = 0; i < config.currencies.length; i++) {
      const currency = config.currencies[i];
      const limitValue = config.initialLimits[i]
      assert(limitValue)
      assert(limitValue >= 0)
      
      // Get participant accounts to get the participantCurrencyIds needed by the facade
      const positionAccount = await this.deps.participantFacade.getByNameAndCurrency(
        config.id, 
        currency, 
        Enum.Accounts.LedgerAccountType.POSITION
      );
      assert(positionAccount)
      const settlementAccount = await this.deps.participantFacade.getByNameAndCurrency(
        config.id, 
        currency, 
        Enum.Accounts.LedgerAccountType.SETTLEMENT
      );
      assert(settlementAccount)
      
      const limitPayload = {
        limit: {
          type: 'NET_DEBIT_CAP',
          value: limitValue,
          thresholdAlarmPercentage: 10
        },
        initialPosition: 0
      };
      
      // Call facade directly to bypass Kafka messaging
      await this.deps.participantFacade.addLimitAndInitialPosition(
        positionAccount.participantCurrencyId,
        settlementAccount.participantCurrencyId,
        limitPayload,
        true
      );
    }
  }
}