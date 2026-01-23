import { CurrencyLedgerConfig } from "src/shared/config"
import Helper from "./TigerBeetleLedgerHelper"

export class CurrencyManager {
  private currencyMap: Record<string, CurrencyLedgerConfig> = {}
  constructor (currencyLedgerConfig: Array<CurrencyLedgerConfig>) {
    this.currencyMap = currencyLedgerConfig.reduce((acc, curr) => {
      acc[curr.currency] = curr
      return acc
    }, {} as Record<string, CurrencyLedgerConfig>)
  }

  public get(curreny: string): CurrencyLedgerConfig {
    const config = this.currencyMap[curreny]
    if (!config) {
      throw new Error(`CurrencyManager.get() - no currency defined: ${curreny}`)
    }

    return config
  }

  public assertCurrenciesEnabled(currencies: Array<string>): void {
    currencies.forEach(currency => this.get(currency))
  }

  public getAssetScale(currency: string): number {
    return this.get(currency).assetScale
  }

  /**
   * @deprecated
   */
  public getClearingLedgerId(currency: string): number {
    return this.get(currency).clearingLedgerId
  }

  public getLedgerOperation(currency: string): number {
    return this.get(currency).ledgerOperation
  }

  public getLedgerControl(currency: string): number {
    return this.get(currency).ledgerControl
  }

  public getAccountIdSettlementBalance(currency: string): bigint {
    return this.get(currency).accountIdSettlementBalance
  }

  /**
   * @deprecated
   */
  public getControlLedgerId(currency: string): number {
    return this.get(currency).controlLedgerId
  }

  /**
   * Get currency from ledger ID (operation or control ledger)
   * Returns undefined for fixed ledger IDs
   * Throws error if ledger ID is not found and not a fixed ledger
   */
  public getCurrencyFromLedger(ledgerId: number): string | undefined {
    // Fixed ledger IDs that don't have a currency
    const fixedLedgerIds = Object.values(Helper.ledgerIds);
    if (fixedLedgerIds.includes(ledgerId)) {
      return undefined;
    }

    for (const [currency, config] of Object.entries(this.currencyMap)) {
      if (config.ledgerOperation === ledgerId || config.ledgerControl === ledgerId) {
        return currency;
      }
    }

    throw new Error(`CurrencyManager.getCurrencyFromLedger() - no currency found for ledger ID: ${ledgerId}`);
  }

}