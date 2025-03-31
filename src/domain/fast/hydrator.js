const AccountType = require("./AccountType")
const assert = require('assert')

const DebitCredit = Object.freeze({
  Credit: 'Credit',
  Debit: 'Debit',
  Zero: 'Zero',
})

/**
 * @class Hydrator
 * @description Responsbile for hydrating from the TigerBeetle representation
 *   of an account to Domain-specific representations. Likely won't be able to stay
 *   as a static class, but let's see
 */
class Hydrator {

  /** 
   * Calculate the net balance of the account
   */
  static _netBalance(debits, credits) {
    const net = Number(debits - credits)

    const debitCredit = ((debits, credits) => {
      if (debits === credits) {
        return DebitCredit.Zero
      }
      if (debits > credits) {
        return DebitCredit.Debit
      }

      return DebitCredit.Credit
    })

    return {
      amount: Math.abs(net),
      debitCredit: debitCredit(debits, credits)
    }
  }

  static accountTypeByValue(accountTypeValue) {
    const key = Object.entries(AccountType).find(([key, val]) => val === accountTypeValue)?.[0];
    assert(key)

    return key
  }


  static hydrateLedgerAccount(accountMetadata, account) {

    // TODO: think about bigints and losing precision here
    return {
      accountType: Hydrator.accountTypeByValue(accountMetadata.accountType),
      fspId: accountMetadata.dfspId,
      transferType: accountMetadata.transferType,
      creditsPosted: Number(account.credits_posted),
      creditsPending: Number(account.credits_pending),
      debitsPosted: Number(account.debits_posted),
      debitsPending: Number(account.debits_pending),
      netBalancePosted: Hydrator._netBalance(
        account.debits_posted, account.credits_posted
      ),
      netBalancePending: Hydrator._netBalance(
        account.debits_pending, account.credits_pending
      )
    }

  }
}

module.exports = Hydrator