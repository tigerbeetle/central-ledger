import { failureWithError, QueryResult } from "../../shared/results"
import { Account, Client } from "tigerbeetle-node";
import crypto from "crypto";

export default class TigerBeetleLedgerHelper {

  public static async safeLookupAccounts(client: Client, accountIds: Array<bigint>):
    Promise<QueryResult<Array<Account>>> {
    if (accountIds.length === 0) {
      return {
        type: 'SUCCESS',
        result: []
      }
    }
    const accounts = await client.lookupAccounts(accountIds)
    if (accounts.length !== accountIds.length) {
      return failureWithError(new Error(`account lookup mismatch. Expected: ${accountIds.length}, \
        but instead found: ${accounts.length}`))
    }

    return {
      type: 'SUCCESS',
      result: accounts
    }
  }

  /**
   * Create a random bigint id within Number.MAX_SAFE_INTEGER (53 bits)
   */
  public static id53(): bigint {
    const bytes = crypto.randomBytes(8);
    const value = bytes.readBigUInt64BE();
    return value & 0x1FFFFFFFFFFFFFn;
  }
}