import { LedgerSql } from "../domain/ledger/ledger-sql"
import { ApplicationConfig } from "../lib/config"
import { ResponseToolkit } from '@hapi/hapi';
import { RequestGetSettlementByParams } from "./types";

const ErrorHandler = require('@mojaloop/central-services-error-handling')
const Settlements = require('../../domain/settlement/index')
const Utility = require('@mojaloop/central-services-shared').Util
const Enum = require('@mojaloop/central-services-shared').Enum
const EventSdk = require('@mojaloop/event-sdk')


const logger = require('../../shared/logger').logger

interface Dependencies {
  config: ApplicationConfig,
  ledger: LedgerSql,
}

export default class HandlerSettlementV2 {
  constructor(private deps: Dependencies) {
    logger.warn(`participants HandlerV2.constructor() - API_MODE_ADMIN=${this.deps.config.API_MODE_ADMIN}`)
  }

  /**
   * summary: Returns Settlement(s) as per parameter(s).
   * description:
   * parameters: currency, participantId, settlementWindowId, accountId, state, fromDateTime, toDateTime
   * produces: application/json
   * responses: 200, 400, 401, 404, 415, default
   */
  public async getSettlementByParams(
    request: RequestGetSettlementByParams,
    h: ResponseToolkit
  ): Promise<any> {
    try {
      const { span, headers } = request
      const spanTags = Utility.EventFramework.getSpanTags(
        Enum.Events.Event.Type.SETTLEMENT,
        Enum.Events.Event.Action.GET,
        undefined,
        headers[Enum.Http.Headers.FSPIOP.SOURCE],
        headers[Enum.Http.Headers.FSPIOP.DESTINATION]
      )
      span.setTags(spanTags)
      await span.audit({
        headers: request.headers,
        params: request.params
      }, EventSdk.AuditEventAction.start)

      const Enums = await request.server.methods.enums('settlementState')
      const settlementResult = await Settlements.getSettlementsByParams({ query: request.query }, Enums)
      return h.response(settlementResult)
    } catch (err: any) {
      request.server.log('error', err)
      return ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  }


}