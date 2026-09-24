import { Knex } from 'knex';
import assert from 'node:assert';
import LedgerTigerBeetleHelper from './helper';
import { ApplicationConfig } from '../../../lib/config';
import { AccountCode, Enums, QueryResultWithNotFound } from '../shared/types';
import Helper from './helper';

export type CmdHubCurrencyEnable = {
  currency: string,
  accounts: Array<string>
}

export type EnableHubCurrencyResponse = {
  type: 'OK'
} | {
  type: 'EXISTS'
} | {
  type: 'FAILURE',
  error: any
}

/**
 * Mapping from a currency => Set of TigerBeetle Ledger Ids.
 */
export interface CurrencyLedger {
  currency: string,
  /**
   * The TigerBeetle ledger where 'real' funds are tracked. 
   * Used for financial reporting.
   */
  ledgerOperation: number,

  /**
   * A separate 'control' ledger for non-financial operations.
   */
  ledgerControl: number,

  /** 
   * The AccountId for the settlement balance account.
   * This isn't really the best place for this, but we need to put it somewhere!
   */
  settlementBalance: bigint,
}

/**
 * The Hub account's representation of each individual accounts. 
 * This is mainly used to maintain backwards compatibility with LedgerSQL.
 */
export interface CurrencyAccount {
  /**
   * A mocked out id to match LedgerSQL.
   */
  id: number,

  currency: string,
  /**
   * The Legacy account type.
   */
  account: string,

  /**
   * When the CurrencyAccount was created.
   */
  createdDate: Date,

  /**
   * When the CurrencyAccount was last updated.
   */
  changedDate: Date,
}

/**
 * The set of TigerBeetle Account ids.
 */
export interface DfspAccountIds {
  deposit: bigint,
  unrestricted: bigint,
  unrestrictedLock: bigint,
  restricted: bigint,
  reserved: bigint,
  commitedOutgoing: bigint,
  clearingCredit: bigint
  clearingSetup: bigint
  clearingLimit: bigint
}

/**
 * The specification which defines the TigerBeetle Accounts for a dfspId + currency.
 */
export interface SpecAccount extends DfspAccountIds {
  dfspId: string,
  currency: string,
  // TODO: get from a join
  participantId: number,

}



export interface DepsSpecStore {
  config: ApplicationConfig
  enums: Enums,
  db: Knex,
  helper: Helper,
}


/**
 * @class SpecStore
 * @description Metadata-sidecar store for TigerBeetle Account, Transfer and Hub metadata: 'Specs'.
 */
export default class SpecStore {
  private db: Knex;
  private readonly helper: Helper

  private hubAccountId = 0;

  /**
   * Backwards compatibility - keep track of the very first date the first currency was created.
   */
  private firstCreationDate: Date | null = null;

  // TODO: make this a 'Table'.
  // We store the account for backwards compatibility, but LedgerTB doesn't really care about it.
  private currencyAccounts: Array<CurrencyAccount> = [];
  // A mapping of currency => TigerBeetle Ledger Ids
  private currencyLedgers: Array<CurrencyLedger> = [];
  private dfsps: Array<{
    id: string;
    /**
     * The master account id of the dfsp.
     */
    masterAccountId: bigint;
  }> = [];
  private dfspSpecs: Array<SpecAccount> = [];

  constructor(private deps: DepsSpecStore) {
    this.db = deps.db;
    this.helper = deps.helper
    // This mimicks how the participant gets setup in LedgerSQL.
    this.getFirstOrImplyCreationDate();
  }

  /**
   * Strip the MS off of the date, this mimicks what MySQL does internally.
   */
  public static stripMs(date: Date): Date {
    return new Date(new Date().setMilliseconds(0));
  }

  public async getFirstOrImplyCreationDate(): Promise<Date> {
    if (!this.firstCreationDate) {
      this.firstCreationDate = SpecStore.stripMs(new Date());
    }

    return this.firstCreationDate;
  }

  private nextHubAccountId(): number {
    this.hubAccountId += 1;
    return this.hubAccountId;
  }

  public async validateCurrency(currency: string): Promise<void> {
    assert(currency)
    const result = await this.db('currency').where('currencyId', currency).first()
    if (!result) {
      throw new Error(`Currency: ${currency} not defined.`)
    }
  }

  public async enableHubCurrency(cmd: CmdHubCurrencyEnable): Promise<EnableHubCurrencyResponse> {
    try {
      assert(cmd.currency);
      assert(Array.isArray(cmd.accounts));

      // If accounts is empty, we just assume it's these two.
      if (cmd.accounts.length === 0) {
        cmd.accounts.push('HUB_MULTILATERAL_SETTLEMENT', 'HUB_RECONCILIATION');
      }

      // Validate the account types.
      cmd.accounts.forEach(account => {
        const ledgerAccountTypeId = this.deps.enums.ledgerAccountType[account];
        if (!ledgerAccountTypeId) {
          throw new Error('Ledger account type was not found.');
        }

        const permittedHubAccountType = this.deps.config.HUB_ACCOUNTS.find(acc => acc === account);
        if (!permittedHubAccountType) {
          throw new Error(`The requested hub operator account type is not allowed.`);
        }
      });

      // Create the currencyLedger if not exists.
      const currencyLedger = this.currencyLedgers
        .find(currencyLedger => currencyLedger.currency === cmd.currency);
      if (!currencyLedger) {
        const currencyCount = this.currencyLedgers.length;
        const [
          ledgerOperation,
          ledgerControl
        ] = LedgerTigerBeetleHelper.generateLedgerIds(currencyCount);

        this.currencyLedgers.push({
          currency: cmd.currency,
          ledgerOperation,
          ledgerControl,
          settlementBalance: this.helper.idSmall(),
        });

        if (!this.firstCreationDate) {
          this.firstCreationDate = SpecStore.stripMs(new Date());
        }
      }

      for (const account of cmd.accounts) {
        const currencyAccounts = this.currencyAccounts
          .find(currencyAccounts => currencyAccounts.currency === cmd.currency
            && currencyAccounts.account === account);

        // Backwards compatibility. Create one at a time, this means a partial creation could
        // take place.
        this.currencyAccounts.push({
          id: this.nextHubAccountId(),
          currency: cmd.currency,
          account,
          createdDate: SpecStore.stripMs(new Date()),
          changedDate: SpecStore.stripMs(new Date()),
        });
        if (currencyAccounts) {
          return {
            type: 'EXISTS'
          };
        }
      }

      return { type: 'OK' };
    } catch (error) {
      return {
        type: 'FAILURE', error
      };
    }
  }

  public async getCurrencyLedger(currency: string): Promise<CurrencyLedger> {
    const currencyLedger = this.currencyLedgers
      .find(currencyLedger => currencyLedger.currency === currency);
    if (!currencyLedger) {
      throw new Error(`getCurrencyLedger() - no ledger found for currency: ${currency}`);
    }

    return currencyLedger;
  }

  public async getCurrencyLedgers(): Promise<Array<CurrencyLedger>> {
    return this.currencyLedgers;
  }

  public async getAllCurrencyAccounts(): Promise<Array<CurrencyAccount>> {
    return this.currencyAccounts;
  }

  public async getCurrencyAccounts(currency: string): Promise<Array<CurrencyAccount>> {
    return this.currencyAccounts.filter(acc => acc.currency === currency);
  }

  public async getAccountIdSettlementBalance(currency: string): Promise<bigint> {
    const currencyLedger = this.currencyLedgers.find(acc => acc.currency === currency);
    if (!currencyLedger) {
      throw new Error(`getAccountIdSettlementBalance() - no currencyLedger found for ` +
        `currency:${currency}`
      );
    }

    return currencyLedger.settlementBalance;
  }

  public async assertCurrenciesEnabled(currencies: Array<string>): Promise<void> {
    assert(Array.isArray(currencies));
    assert(currencies.length > 0, 'Expected at least one currency.');

    const errors: Array<string> = [];
    currencies.forEach(currency => {
      const found = this.currencyAccounts.find(account => account.currency === currency);
      if (!found) {
        errors.push(`No currencyAccounts found for: ${currency}.`);
      }
    });

    if (errors.length > 0) {
      throw new Error(`assertCurrenciesFailed with errors: [${errors.join(', ')}]`);
    }
  }

  /**
   * Create the TigerBeetle master account id for this DFSP.
   */
  public async getOrCreateDfspMasterAccount(id: string): Promise<bigint> {
    const found = this.dfsps.find(dfsp => dfsp.id === id);
    if (found) {
      return found.masterAccountId;
    }

    const masterAccountId = this.deps.helper.idSmall();
    this.dfsps.push({ id, masterAccountId });

    return masterAccountId;
  }

  public async getDfspMasterAccount(id: string): Promise<bigint> {
    const found = this.dfsps.find(dfsp => dfsp.id === id);
    if (!found) {
      throw new Error(`No dfsp found for id: ${id}`);
    }

    return found.masterAccountId;
  }

  public async getAccountSpec(id: string, currency: string):
    Promise<QueryResultWithNotFound<SpecAccount>> {
    const spec = this.dfspSpecs.find(spec => spec.dfspId === id && spec.currency === currency);
    if (!spec) {
      return {
        type: 'NOT_FOUND',
        error: new Error(`getAccountSpec no spec found for id:${id} + currency: ${currency}.`)
      };
    }

    return {
      type: 'SUCCESS',
      result: spec
    };
  }

  public async getAccountSpecs(id: string): Promise<Array<SpecAccount>> {
    return this.dfspSpecs.filter(spec => spec.dfspId === id);
  }

  /**
   * Look up the account within the spec for the dfspid and account id.
   */
  public async getCurrencyCodeAndSpec(id: string, accountId: bigint):
    Promise<{ currency: string; code: AccountCode; spec: SpecAccount; }> {
    const specs = await this.getAccountSpecs(id);
    if (specs.length === 0) {
      throw new Error(`getCurrencyAndType() no specs found for id: ${id}.`);
    }

    const currencyAndCode = specs.reduce<{ currency: string; code: AccountCode; spec: SpecAccount; } | null>((acc, curr) => {
      if (acc) return acc;
      if (curr.clearingCredit === accountId) {
        return { currency: curr.currency, code: AccountCode.Clearing_Credit, spec: curr };
      }
      if (curr.deposit === accountId) {
        return { currency: curr.currency, code: AccountCode.Deposit, spec: curr };
      }
      if (curr.unrestricted === accountId) {
        return { currency: curr.currency, code: AccountCode.Unrestricted, spec: curr };
      }
      if (curr.unrestrictedLock === accountId) {
        return { currency: curr.currency, code: AccountCode.Unrestricted_Lock, spec: curr };
      }
      if (curr.restricted === accountId) {
        return { currency: curr.currency, code: AccountCode.Restricted, spec: curr };
      }
      if (curr.reserved === accountId) {
        return { currency: curr.currency, code: AccountCode.Reserved, spec: curr };
      }
      if (curr.commitedOutgoing === accountId) {
        return { currency: curr.currency, code: AccountCode.Committed_Outgoing, spec: curr };
      }
      if (curr.clearingSetup === accountId) {
        return { currency: curr.currency, code: AccountCode.Clearing_Setup, spec: curr };
      }
      if (curr.clearingLimit === accountId) {
        return { currency: curr.currency, code: AccountCode.Clearing_Limit, spec: curr };
      }
      return acc;
    }, null);

    if (!currencyAndCode) {
      throw new Error(`getCurrencyAndType() not found for id: ${id}, accountId: ${accountId}.`);
    }

    return currencyAndCode;
  }

  public async newAccountSpec(id: string, currency: string): Promise<SpecAccount> {
    // TODO: do we _need_ this check?
    const existing = await this.getAccountSpec(id, currency);
    if (existing.type === 'SUCCESS') {
      return existing.result;
    }

    const spec: SpecAccount = {
      dfspId: id,
      currency,
      // TODO: how can we get away without this?!
      participantId: 0,
      deposit: this.helper.idSmall(),
      unrestricted: this.helper.idSmall(),
      unrestrictedLock: this.helper.idSmall(),
      restricted: this.helper.idSmall(),
      reserved: this.helper.idSmall(),
      commitedOutgoing: this.helper.idSmall(),
      clearingCredit: this.helper.idSmall(),
      clearingSetup: this.helper.idSmall(),
      clearingLimit: this.helper.idSmall(),
    };
    this.dfspSpecs.push(spec);

    return spec;
  }
}
