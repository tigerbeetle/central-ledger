import { LedgerSql } from "../domain/ledger/ledger-sql"
import { ApplicationConfig } from "../lib/config"
import { ResponseToolkit } from '@hapi/hapi';
import { RequestCreateSettlementEvent, RequestGetSettlementsByParams, RequestGetSettlementWindowsByParams } from "./types";
import Settlements from '../domain/settlement/index';
import settlementWindows from '../domain/settlementWindow/index';

import { logger } from "../shared/logger";

const ErrorHandler = require('@mojaloop/central-services-error-handling')
// const Settlements = require('../domain/settlement/index')
const Utility = require('@mojaloop/central-services-shared').Util
const Enum = require('@mojaloop/central-services-shared').Enum
const EventSdk = require('@mojaloop/event-sdk')


interface Dependencies {
  config: ApplicationConfig,
  ledger: LedgerSql,
}

export default class HandlerSettlementV2 {
  constructor(private deps: Dependencies) {
    logger.warn(`HandlerSettlementV2.constructor() - API_MODE_SETTLEMENT=TODO`)
  }

  /**
   * summary: Returns Settlement(s) as per parameter(s).
   * description:
   * parameters: currency, participantId, settlementWindowId, accountId, state, fromDateTime, toDateTime
   * produces: application/json
   * responses: 200, 400, 401, 404, 415, default
   */
  public async getSettlementByParams(
    request: RequestGetSettlementsByParams,
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


  /**
   * summary: Trigger the creation of a settlement event, that does the calculation of the net
   *          settlement position per participant and marks all transfers in the affected windows 
   *          as Pending settlement. Returned dataset is the net settlement report for the
   *          settlement window
   * description:
   * parameters: settlementEventPayload
   * produces: application/json
   * responses: 200, 400, 401, 404, 415, default
   */
  public async createSettlementEvent(
    request: RequestCreateSettlementEvent,
    h: ResponseToolkit
  ): Promise<any> {
    try {
      const { span, payload, headers } = request
      const spanTags = Utility.EventFramework.getSpanTags(
        Enum.Events.Event.Type.SETTLEMENT,
        Enum.Events.Event.Action.POST,
        payload.settlementWindows.map(id => id.id).join(''),
        headers[Enum.Http.Headers.FSPIOP.SOURCE],
        headers[Enum.Http.Headers.FSPIOP.DESTINATION]
      )
      span.setTags(spanTags)
      await span.audit(request.payload, EventSdk.AuditEventAction.start)

      const Enums = {
        ledgerEntryType: await request.server.methods.enums('ledgerEntryType'),
        settlementDelay: await request.server.methods.enums('settlementDelay'),
        settlementGranularity: await request.server.methods.enums('settlementGranularity'),
        settlementInterchange: await request.server.methods.enums('settlementInterchange'),
        settlementState: await request.server.methods.enums('settlementState'),
        settlementWindowState: await request.server.methods.enums('settlementWindowState'),
        transferParticipantRoleType: await request.server.methods.enums('transferParticipantRoleType'),
        transferState: await request.server.methods.enums('transferState')
      }
      const settlementResult = await Settlements.settlementEventTrigger(request.payload, Enums)
      return settlementResult
    } catch (err: any) {
      request.server.log('error', err)
      return ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  }

  /**
   * summary: Returns a Settlement Window(s) as per parameter(s).
   * description:
   * parameters: participantId, state, fromDateTime, toDateTime
   * produces: application/json
   * responses: 200, 400, 401, 404, 415, default
   */
  public async getSettlementWindowsByParams(
    request: RequestGetSettlementWindowsByParams,
    h: ResponseToolkit
  ): Promise<any> {
    try {
      const { span, headers } = request
      const spanTags = Utility.EventFramework.getSpanTags(
        Enum.Events.Event.Type.SETTLEMENT_WINDOW,
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

      const Enums = await request.server.methods.enums('settlementWindowState')
      const settlementWindowResult = await settlementWindows.getByParams({ query: request.query }, Enums)
      return settlementWindowResult
    } catch (err: any) {
      request.server.log('error', err)
      return ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  }
}