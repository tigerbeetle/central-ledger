import { failureWithError, QueryResult } from "../../shared/results"
import { Account, amount_max, Client } from "tigerbeetle-node";
import crypto from "crypto";
import assert from "assert";

interface InterledgerValidationPass {
  type: 'PASS'
}

interface InterledgerValidationFail {
  type: 'FAIL',
  reason: string
}

export type InterledgerValidationResult = InterledgerValidationPass
  | InterledgerValidationFail


export default class TigerBeetleLedgerHelper {

  /**
   * Global account ids that persist across all Ledgers
   */
  public static accountIds = {
    // TODO(LD): Find better account ids
    bookmarkDebit: 1000n,
    bookmarkCredit: 1001n,

    /**
     * Counterparty account for Super Ledger
     * Essentially a /dev/null for Accounts
     */
    devNull: 80000000000n
  }

  /**
   * Fixed ledger ids
   */
  public static ledgerIds = {
    globalControl: 9000,
    /**
     * @deprecated
     */
    timeoutHandler: 9001,
  }

  // TODO
  public static transferCodes = {
    unknown: 1,
    timeoutBookmark: 9000,
  }

  /**
   * If the Net Debit Cap account's net credits are beyond this number,
   * we consider the Net Debits to be uncapped.
   */
  public static netDebitCapEventHorizon = BigInt(2 ** 64)

  public static createAccountTemplate = {
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    reserved: 0,
    timestamp: 0n,
  }

  public static createTransferTemplate = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    timeout: 0,
    code: 1,
    timestamp: 0n,
    pending_id: 0n,
  }

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
   * 
   * TigerBeetle Accounts can be 128 bits, but since the Admin API uses javascript/json numbers
   * to maintain backwards compatibility, we generate our own random accountIds under 
   * Number.MAX_SAFE_INTEGER to be safe.
   */
  public static idSmall(): bigint {
    const bytes = crypto.randomBytes(8);
    const value = bytes.readBigUInt64BE();
    return value & 0x1FFFFFFFFFFFFFn;
  }

  public static fromMojaloopId(mojaloopId: string): bigint {
    assert(mojaloopId)
    // TODO: assert that this actually is a uuid

    const hex = mojaloopId.replace(/-/g, '');
    return BigInt(`0x${hex}`);
  }

  public static toMojaloopId(id: bigint): string {
    assert(id !== undefined && id !== null, 'id is required')

    // Convert bigint to hex string (without 0x prefix)
    let hex = id.toString(16);

    // Pad to 32 characters (128 bits = 16 bytes = 32 hex chars)
    hex = hex.padStart(32, '0');

    // Insert dashes to create UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  }


  /**
    * Converts a Mojaloop amount string to a bigint representation based on the currency's scale
    */
  public static fromMojaloopAmount(amountStr: string, currencyScale: number): bigint {
    assert(currencyScale >= 0)
    assert(currencyScale <= 10)
    // Validate input
    if (typeof amountStr !== 'string') {
      throw new Error('Amount must be a string');
    }

    if (!/^-?\d+(\.\d+)?$/.test(amountStr.trim())) {
      throw new Error('Invalid amount format. Expected format: "123.45" or "123"');
    }

    const trimmed = amountStr.trim();
    const [integerPart, decimalPart = ''] = trimmed.split('.');

    const normalizedDecimal = decimalPart.padEnd(currencyScale, '0').slice(0, currencyScale);
    const combinedStr = integerPart + normalizedDecimal;

    return BigInt(combinedStr);
  }


  /**
    * Checks that the fulfilment matches the preimage
    * 
    * From the Mojaloop FSPIOP Specification v1.1:
    * https://docs.mojaloop.io/api/fspiop/v1.1/api-definition.html#interledger-payment-request-2
    * 
    * > The fulfilment is submitted to the Payee FSP ledger to instruct the ledger to commit the 
    * > reservation in favor of the Payee. The ledger will validate that the SHA-256 hash of the
    * > fulfilment matches the condition attached to the transfer. If it does, it commits the 
    * > reservation of the transfer. If not, it rejects the transfer and the Payee FSP rejects the 
    * > payment and cancels the previously-performed reservation.
    * 
    */
  public static validateFulfilmentAndCondition(fulfilment: string, condition: string):
    InterledgerValidationResult {
    try {
      assert(fulfilment)
      assert(condition)
      const preimage = Buffer.from(fulfilment, 'base64url')
      if (preimage.length !== 32) {
        return {
          type: 'FAIL',
          reason: 'Interledger preimages must be exactly 32 bytes'
        }
      }

      const calculatedCondition = crypto.createHash('sha256')
        .update(preimage)
        .digest('base64url')

      if (calculatedCondition !== condition) {
        return {
          type: 'FAIL',
          reason: 'Condition and Fulfulment mismatch'
        }
      }

      return {
        type: 'PASS'
      }
    } catch (err) {
      return {
        type: "FAIL",
        reason: err.message
      }
    }
  }

  /**
   * Convert from a real positive number money amount to a TigerBeetle Ledger representation.
   */
  public static toTigerBeetleAmount(input: number, assetScale: number): bigint {
    assert(input >= 0, `toTigerBeetleAmount expected 0 or positive number`)
    assert(assetScale >= -7)
    assert(assetScale <= 8)
    const valueMultiplier = 10 ** assetScale

    // we have to do this before converting to BigInt because input could be a decimal.
    const tigerBeetleAmount = input * valueMultiplier
    if (tigerBeetleAmount > Number.MAX_SAFE_INTEGER) {
      throw new Error(`toTigerBeetleAmount() - lost precision`)
    }

    return BigInt(tigerBeetleAmount)
  }

  /**
   * Convert from an TigerBeetle Ledger representation of an amount to a real amount.
   */
  public static toRealAmount(input: bigint, assetScale: number): number {
    assert(assetScale >= -7)
    assert(assetScale <= 8)
    const valueDivisor = 10 ** assetScale

    if (input === 0n) {
      return 0
    }

    const realAmount = input / BigInt(valueDivisor)
    if (realAmount > BigInt(Number.MAX_SAFE_INTEGER) ||
      realAmount < BigInt(Number.MIN_SAFE_INTEGER)
    ) {
      throw new Error(`toRealAmount() failed: realAmount is outside of safe range.`)
    }

    return Number(realAmount)
  }
}