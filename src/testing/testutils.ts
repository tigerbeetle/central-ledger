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
}