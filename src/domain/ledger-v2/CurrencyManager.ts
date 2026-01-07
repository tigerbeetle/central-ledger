import { CurrencyLedgerConfig } from "src/shared/config"

export class CurrencyManager {
  private currencyMap: Record<string, CurrencyLedgerConfig> = {}
  constructor (currencyLedgerConfig: Array<CurrencyLedgerConfig>) {
    this.currencyMap = currencyLedgerConfig.reduce((acc, curr) => {
      acc[curr.currency] = curr
      return acc
    }, {} as Record<string, CurrencyLedgerConfig>)
  }

  private get(curreny: string): CurrencyLedgerConfig {
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

}