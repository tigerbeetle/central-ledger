import { Enum } from '@mojaloop/central-services-shared';
import assert from 'assert';
import { createServer } from 'net';
import { FusedFulfilHandlerInput } from 'src/handlers-v2/FusedFulfilHandler';
import { FusedPrepareHandlerInput } from 'src/handlers-v2/FusedPrepareHandler';
import { CommitTransferDto, CreateTransferDto } from 'src/handlers-v2/types';

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
}

