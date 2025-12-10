import { failureWithError, QueryResult } from "src/shared/results";
import { Account, Client } from "tigerbeetle-node";

export default class TigerBeetleLedgerHelper {

  public static async safeLookupAccounts(client: Client, accountIds: Array<bigint>): 
  Promise<QueryResult<Array<Account>>> {
    const accounts = await client.lookupAccounts(accountIds)
    if (accounts.length !== accountIds.length) {
      return failureWithError(new Error(`account lookup mismatch. Expected: ${accountIds.length}, \
        but instead found: ${accounts.length}`))
    }
  }

}