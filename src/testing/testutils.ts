import { Enum } from '@mojaloop/central-services-shared';
import assert from 'assert';
import { createServer } from 'net';
import { FusedFulfilHandlerInput } from '../handlers-v2/FusedFulfilHandler';
import { FusedPrepareHandlerInput } from '../handlers-v2/FusedPrepareHandler';
import { CommitTransferDto, CreateTransferDto } from '../handlers-v2/types';
import { AccountCode, TransferCodeDescription } from '../domain/ledger-v2/TigerBeetleLedger';
import { LedgerDfsp } from '../domain/ledger-v2/types';
import { Transfer, TransferFlags } from 'tigerbeetle-node';
import Helper from '../domain/ledger-v2/TigerBeetleLedgerHelper';

const MojaloopLogger = require('@mojaloop/central-services-logger')
const { ilpFactory, ILP_VERSIONS } = require('@mojaloop/sdk-standard-components').Ilp
// Don't use in production!
const ilpService = ilpFactory(ILP_VERSIONS.v1, { secret: 'password', logger: MojaloopLogger })

export interface MojaloopMockQuoteILPResponse {
  quoteId: string,
  transactionId: string,
  transactionType: string,
  payerId: string,
  payeeId: string,
  transferId: string,
  amount: number,
  currency: string,
  expiration: string,
}

export interface QuoteIlpResponse {
  fulfilment: string;
  ilpPacket: string;
  condition: string;
}

export class TestUtils {

  /**
   * @function sleep
   * @param {*} timeMs - how long to sleep for
   */
  static async sleep(timeMs) {
    return new Promise<void>((resolve, reject) => setTimeout(() => resolve(), timeMs))
  }

  static generateMockQuoteILPResponse(transferId: string, expiration: Date): MojaloopMockQuoteILPResponse {

    return {
      quoteId: '00001',
      // TODO: how do we get this determinitically?
      transactionId: '00001',
      transactionType: 'unknown',
      payerId: 'dfsp_a',
      payeeId: 'dfsp_b',
      transferId,
      amount: 100,
      currency: 'USD',
      expiration: expiration.toISOString()
    }
  }

  static buildValidFulfilInput(transferId: string, payload: CommitTransferDto): FusedFulfilHandlerInput {
    const input: FusedFulfilHandlerInput = {
      payload,
      transferId,
      headers: {
        'fspiop-source': 'dfsp_b',
        'fspiop-destination': 'dfsp_a',
        'content-type': 'application/vnd.interoperability.transfers+json;version=1.0'
      },
      message: {
        value: {
          from: 'dfsp_b',
          to: 'dfsp_a',
          id: `msg-${transferId}`,
          type: 'application/json',
          content: {
            headers: {
              'fspiop-source': 'dfsp_b',
              'fspiop-destination': 'dfsp_a',
            },
            payload,
            uriParams: { id: transferId }
          },
          metadata: {
            event: {
              id: `event-${transferId}`,
              type: 'transfer',
              action: 'commit',
              createdAt: new Date().toISOString(),
              state: {
                status: 'success',
                code: 0
              }
            }
          }
        }
      },
      action: Enum.Events.Event.Action.COMMIT,
      eventType: 'fulfil',
      kafkaTopic: 'topic-transfer-fulfil'
    };

    return input
  }

  static buildValidPrepareInput(transferId: string, payload: CreateTransferDto): FusedPrepareHandlerInput {
    assert(payload.transferId === transferId)
    const input: FusedPrepareHandlerInput = {
      payload,
      transferId: payload.transferId,
      headers: {
        'fspiop-source': 'dfsp_a',
        'fspiop-destination': 'dfsp_b',
        'content-type': 'application/vnd.interoperability.transfers+json;version=1.0'
      },
      message: {
        value: {
          from: 'payerfsp',
          to: 'payeefsp',
          id: `msg-${transferId}`,
          type: 'application/json',
          content: {
            headers: {
              'fspiop-source': 'dfsp_a',
              'fspiop-destination': 'dfsp_b'
            },
            payload,
            uriParams: { id: transferId }
          },
          metadata: {
            event: {
              id: `event-${transferId}`,
              type: 'transfer',
              action: 'prepare',
              createdAt: new Date().toISOString(),
              state: {
                status: 'success',
                code: 0
              }
            }
          }
        }
      },
      action: Enum.Events.Event.Action.PREPARE,
      metric: 'transfer_prepare',
      functionality: Enum.Events.Event.Type.TRANSFER,
      actionEnum: 'PREPARE'
    };

    return input
  }

  static generateQuoteILPResponse(params: MojaloopMockQuoteILPResponse): QuoteIlpResponse {
    // Build an imaginary Quote Request/Response to generate the ILP packet, fulfilment and condition
    // not for use in production!
    const quoteRequest = {
      quoteId: params.quoteId,
      transactionId: params.transactionId,
      transactionType: params.transactionType,
      payee: {
        partyIdInfo: {
          partyIdType: 'MSISDN',
          partyIdentifier: '12346',
          fspId: params.payeeId,
        },
      },
      payer: {
        partyIdInfo: {
          partyIdType: 'MSISDN',
          partyIdentifier: '78901',
          fspId: params.payerId,
        },
      },
      expiration: params.expiration
    }
    const quoteResponse = {
      transferAmount: {
        amount: params.amount.toString(),
        currency: params.currency
      },
      expiration: params.expiration,
    }

    const {
      fulfilment,
      ilpPacket,
      condition,
    } = ilpService.getQuoteResponseIlp(quoteRequest, quoteResponse)

    return {
      fulfilment, ilpPacket, condition
    }
  }

  /**
   * Find an available port starting from the given port number
   */
  public static async findAvailablePort(startPort: number): Promise<number> {
    for (let port = startPort; port < startPort + 100; port++) {
      if (await this.isPortAvailable(port)) {
        return port;
      }
    }
    throw new Error(`No available ports found in range ${startPort}-${startPort + 99}`);
  }

  public static async randomAvailablePort(): Promise<number> {
    return new Promise((resolve, reject) => {
      const server = createServer();
      server.listen(0)
      server.on('listening', () => {
        try {
          const address = server.address() as unknown as { port: number }
          assert.equal(typeof address, 'object')
          assert(address.port)

          server.close(() => {
            resolve(address.port);
          });
        } catch (err) {
          reject(err)
        }
      })

      server.on('error', (err) => {
        reject(err)
      });
    });
  }

  /**
   * Check if a port is available
   */
  private static async isPortAvailable(port: number): Promise<boolean> {
    return new Promise((resolve) => {
      const server = createServer();

      server.listen(port, () => {
        server.close(() => {
          resolve(true);
        });
      });

      server.on('error', () => {
        resolve(false);
      });
    });
  }

  /**
   * @function unwrapHapiResponse
   */
  public static async unwrapHapiResponse(asyncFunction: (reply: any) => Promise<unknown>): Promise<{ body: any, code: number }> {
    let body
    let code
    const nestedReply = {
      response: (response) => {
        body = response
        return {
          code: statusCode => {
            code = statusCode
          }
        }
      }
    }
    await asyncFunction(nestedReply)

    return {
      body,
      code
    }
  }

  /**
   * @function unwrapSuccess
   * @description Unwraps a result type with SUCCESS/FAILURE variants, asserting success and returning the result
   */
  public static unwrapSuccess<T>(result: { type: string, result?: T, error?: any }): T {
    assert.strictEqual(result.type, 'SUCCESS', `Expected SUCCESS but got ${result.type}${result.error ? ': ' + result.error.message : ''}`);
    return result.result as T;
  }


  /**
   * Take an easily writeable, shorthand string that represents an array of ledger accounts, and
   * convert it into a snapshot.
   *
   * @param input
   *
   * USD,10200,0,20000,-,-;
   * USD,20100,-,-,0,20000;
   * USD,20101,-,-,0,0;
   * USD,20200,-,-,0,0;
   * USD,20300,-,-,0,0;
   * USD,20400,-,-,0,0;
   * USD,60200,-,-,0,6000;
   *
   */
  public static ledgerDfspsSnapshotString(input: string): string {
    const formatNumber = (value: string): string => {
      if (value === '-') {
        return '';
      }
      const num = parseInt(value, 10);
      return num.toLocaleString('en-US');
    };

    const lines: string[] = [];

    // Headers
    const headers = ['Curr', 'Code', 'Account Name', 'Net Dr (Pend)', 'Net Dr (Post)', 'Net Cr (Pend)', 'Net Cr (Post)'];
    const colWidths = [4, 5, 18, 14, 14, 14, 14];

    const printRow = (values: string[], isHeader: boolean = false) => {
      const row = values.map((val, i) => {
        // Right-align numeric columns (index 3-6), left-align text columns
        if (i >= 3 && !isHeader) {
          return val.padStart(colWidths[i]);
        }
        return val.padEnd(colWidths[i]);
      }).join(' | ');
      return row;
    };

    lines.push(printRow(headers, true));
    lines.push('-'.repeat(colWidths.reduce((a, b) => a + b + 3, 0)));

    // Parse input lines
    const inputLines = input.trim().split('\n').filter(line => line.trim().length > 0);

    for (const line of inputLines) {
      // Remove trailing semicolon and split by comma
      const parts = line.replace(/;$/, '').split(',').map(p => p.trim());

      if (parts.length !== 6) {
        continue; // Skip malformed lines
      }

      const [currency, codeStr, netDrPend, netDrPost, netCrPend, netCrPost] = parts;
      const code = parseInt(codeStr, 10);
      const accountName = AccountCode[code] || 'Unknown';

      lines.push(printRow([
        currency,
        codeStr,
        accountName,
        formatNumber(netDrPend),
        formatNumber(netDrPost),
        formatNumber(netCrPend),
        formatNumber(netCrPost)
      ]));
    }

    return lines.join('\n');
  }

  /**
   * @function formatLedgerDfsps
   * @description Formats LedgerDfsp balance sheets as a string for snapshot testing
   */
  public static formatLedgerDfsps(ledgerDfsps: LedgerDfsp[]): string {
    const formatNumber = (num: number): string => {
      return num.toLocaleString('en-US');
    };

    const isAssetAccount = (code: number): boolean => {
      return Math.floor(code / 10000) === 1;
    };

    const isLiabilityAccount = (code: number): boolean => {
      const firstDigit = Math.floor(code / 10000)
      return firstDigit === 2 || firstDigit === 6
    };

    const allLines: string[] = [];

    for (const ledgerDfsp of ledgerDfsps) {
      // allLines.push(`=== [${ledgerDfsp.name}] Balance Sheet ===`);
      // allLines.push(`Status: ${ledgerDfsp.status}`);
      // allLines.push('');

      // Table headers (debits on left, credits on right per accounting convention)
      const headers = ['Curr', 'Code', 'Account Name', '(Pending)', 'Available', '(Pending)', 'Available'];
      const colWidths = [4, 5, 18, 14, 14, 14, 14];

      const printRow = (values: string[], isHeader: boolean = false) => {
        const row = values.map((val, i) => {
          // Right-align numeric columns (index 3-6), left-align text columns
          if (i >= 3 && !isHeader) {
            return val.padStart(colWidths[i]);
          }
          return val.padEnd(colWidths[i]);
        }).join(' | ');
        return row;
      };

      allLines.push(printRow(headers, true));
      allLines.push('-'.repeat(colWidths.reduce((a, b) => a + b + 3, 0)));

      // Sort accounts by currency then by code for consistent output
      const sortedAccounts = [...ledgerDfsp.accounts].sort((a, b) => {
        if (a.currency !== b.currency) return a.currency.localeCompare(b.currency);
        return a.code - b.code;
      });

      for (const account of sortedAccounts) {
        const accountName = AccountCode[account.code] || 'Unknown';
        const isAsset = isAssetAccount(account.code);
        const isLiability = isLiabilityAccount(account.code);

        allLines.push(printRow([
          account.currency,
          account.code.toString(),
          accountName,
          // Assets: show debits, Liabilities: blank
          isAsset ? formatNumber(account.netCreditsPending) : '',
          isAsset ? formatNumber(account.netDebitsPosted) : '',
          // Assets: blank, Liabilities: show credits
          isLiability ? formatNumber(account.netDebitsPending) : '',
          isLiability ? formatNumber(account.netCreditsPosted) : ''
        ]));
      }
    }

    return allLines.join('\n');
  }

  /**
   * @function printLedgerDfsps
   * @description Prints LedgerDfsp balance sheets to console
   */
  public static printLedgerDfsps(ledgerDfsps: LedgerDfsp[]): void {
    ledgerDfsps.forEach(ledgerDfsp => {
      console.log(`=== [${ledgerDfsp.name}] Balance Sheet ===`);
      console.log(`Status: ${ledgerDfsp.status}\n`);
      console.log(TestUtils.formatLedgerDfsps([ledgerDfsp]));
      console.log('\n')
    })
  }

  /**
   * @function printTransferHistory
   * @description Prints TigerBeetle transfer history in a formatted table
   */
  public static printTransferHistory(transfers: Array<Transfer & {
    debitAccountInfo: { dfspId: string, accountName: string, accountCode: AccountCode },
    creditAccountInfo: { dfspId: string, accountName: string, accountCode: AccountCode },
    currency: string | undefined,
    amountReal: number,
    ledgerName: string
  }>): void {
    console.log('\n=== Transfer History ===\n');

    const formatNumber = (num: number): string => {
      return num.toLocaleString('en-US');
    };

    const formatFlags = (flags: number): string => {
      const flagNames: string[] = [];
      if (flags & TransferFlags.linked) flagNames.push('linked');
      if (flags & TransferFlags.pending) flagNames.push('pending');
      if (flags & TransferFlags.post_pending_transfer) flagNames.push('post_pending');
      if (flags & TransferFlags.void_pending_transfer) flagNames.push('void_pending');
      if (flags & TransferFlags.balancing_debit) flagNames.push('balancing_debit');
      if (flags & TransferFlags.balancing_credit) flagNames.push('balancing_credit');
      return flagNames.join(', ') || 'none';
    };

    const truncate = (str: string, maxLen: number): string => {
      return str.length > maxLen ? str.substring(0, maxLen) : str;
    };

    // Table headers
    const headers = ['Debit Account', 'Credit Account', 'Description', 'Amount', 'Ledger', 'Code', 'Flags'];
    const colWidths = [22, 22, 48, 10, 14, 6, 40];

    const printRow = (values: string[], isHeader: boolean = false) => {
      const row = values.map((val, i) => {
        // Right-align numeric columns (Amount, Code)
        if ((i === 3 || i === 5) && !isHeader) {
          return val.padStart(colWidths[i]);
        }
        return val.padEnd(colWidths[i]);
      }).join(' | ');
      console.log(row);
    };

    printRow(headers, true);
    console.log('-'.repeat(colWidths.reduce((a, b) => a + b + 3, 0)));

    for (const transfer of transfers) {
      const description = truncate(TransferCodeDescription[transfer.code] || 'Unknown transfer code', 48);
      const flags = formatFlags(transfer.flags);
      const debitAccount = `${transfer.debitAccountInfo.dfspId}_${transfer.debitAccountInfo.accountName}`;
      const creditAccount = `${transfer.creditAccountInfo.dfspId}_${transfer.creditAccountInfo.accountName}`;

      printRow([
        truncate(debitAccount, 22),
        truncate(creditAccount, 22),
        description,
        formatNumber(transfer.amountReal),
        transfer.ledgerName,
        transfer.code.toString(),
        flags
      ]);
    }

    console.log('\n');
  }
}

