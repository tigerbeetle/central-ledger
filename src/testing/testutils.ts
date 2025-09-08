import { createServer } from 'net';

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
}