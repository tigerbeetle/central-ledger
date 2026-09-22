import { Enum, Util } from '@mojaloop/central-services-shared';
const { TransferState } = Enum.Transfers
import assert from "node:assert"
import Transaction from '../../domain/transactions'
import {
  CommitPaymentDtoAborted,
  FulfilHandlerInput,
  PaymentFulfilResult,
  PaymentFulfilResultType
} from '../../handlers/payment-fulfil';
import {
  CreatePaymentDto,
  PaymentPrepareResult,
  PaymentPrepareResultType,
  PrepareHandlerInput
} from "../../handlers/payment-prepare";
import { PositionHandlerV2, PositionResultType } from "../../handlers/position-v2";
import {
  CreateRemittanceEntityPayment,
  ProxyCache,
  TransferDeterminingCheckResult,
  TransferProxyObligation
} from "../../handlers/transfer-types";
import { ApplicationConfig } from "../../lib/config";
import { Effect, MessageBus } from "../../messaging/message-bus";
import ParticipantFacade from '../../models/participant/facade';
import { getTransferErrorDuplicateCheck } from '../../models/transfer/transferErrorDuplicateCheck';
import { logger } from "../../shared/logger";
import TransferService, {
  getTransferFulfilmentDuplicateCheck,
  saveTransferErrorDuplicateCheck,
  saveTransferFulfilmentDuplicateCheck,
  getTransferDuplicateCheck,
  saveTransferDuplicateCheck
} from "../transfer";
import {
  AnyQuery,
  CloseSettlementWindowResult,
  CommandResult,
  CreateDfspCommand,
  CreateDfspResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCommand,
  DepositResponse,
  DfspAccountResponse,
  GetAllDfspAccountsQuery,
  GetAllDfspsResponse,
  GetDfspAccountsQuery,
  GetHubAccountsQuery,
  GetNetDebitCapQuery,
  GetNetDebitCapsQuery,
  GetSettlementQuery,
  GetSettlementQueryResponse,
  GetSettlementsQuery,
  GetSettlementsQueryResponse,
  GetSettlementWindowQuery,
  GetSettlementWindowsQuery,
  GetSettlementWindowsQueryResponse,
  HubAccountResponse,
  Ledger,
  LegacyLedgerAccount,
  LegacyLedgerDfsp,
  LegacyLimit,
  LegacyLimitItem,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  LookupTransferResultType,
  QueryResult,
  QueryResultWithNotFound,
  SetNetDebitCapCommand,
  Settlement,
  SettlementAbortCommand,
  SettlementCloseWindowCommand,
  SettlementCommitCommand,
  SettlementPrepareCommand,
  SettlementUpdateCommand,
  SettlementUpdateResult,
  SettlementWindow,
  SettlementWindowState,
  SweepResult,
  WithdrawAbortCommand,
  WithdrawAbortResponse,
  WithdrawCommitCommand,
  WithdrawCommitResponse,
  WithdrawPrepareCommand,
  WithdrawPrepareResponse
} from "./types";
import { Client } from 'tigerbeetle-node';


const ErrorHandler = require('@mojaloop/central-services-error-handling')
const { FSPIOPError } = ErrorHandler
const { Comparators, resourceVersions } = Util
const { Type, Action } = Enum.Events.Event

interface Dependencies {
  config: ApplicationConfig
  client: Client
}

export class LedgerTigerBeetle implements Ledger {
  private readonly config: ApplicationConfig
  private readonly client: Client

  constructor(private deps: Dependencies) {
    this.config = deps.config
    this.client = deps.client
  }

  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    throw new Error('Method not implemented.');
  }

  public async disableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async enableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async enableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async disableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async deposit(cmd: DepositCommand): Promise<DepositResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawAbort(cmd: WithdrawAbortCommand): Promise<WithdrawAbortResponse> {
    throw new Error('Method not implemented.');
  }

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async getHubAccounts(query: AnyQuery): Promise<HubAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async getDfsp(query: { dfspId: string; }): Promise<QueryResultWithNotFound<LegacyLedgerDfsp>> {
    throw new Error('Method not implemented.');
  }

  public async getAllDfsps(query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>> {
    throw new Error('Method not implemented.');
  }

  public async getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResultWithNotFound<LegacyLimit>> {
    throw new Error('Method not implemented.');
  }

  public async getNetDebitCaps(query: GetNetDebitCapsQuery): Promise<QueryResultWithNotFound<Array<LegacyLimitItem>>> {
    throw new Error('Method not implemented.');
  }

  public async prepare(inputs: Array<PrepareHandlerInput>): Promise<Array<PaymentPrepareResult>> {
    throw new Error('Method not implemented.');
  }
  public async fulfil(inputs: Array<FulfilHandlerInput>): Promise<Array<PaymentFulfilResult>> {
    throw new Error('Method not implemented.');
  }
  public async sweepTimedOut(now: Date): Promise<SweepResult> {
    throw new Error('Method not implemented.');
  }
  public async lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    throw new Error('Method not implemented.');
  }
  public async closeSettlementWindow(cmd: SettlementCloseWindowCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }
  public async settlementPrepare(cmd: SettlementPrepareCommand): Promise<CommandResult<{ id: number; }>> {
    throw new Error('Method not implemented.');
  }
  public async settlementAbort(cmd: SettlementAbortCommand): Promise<CommandResult<SettlementUpdateResult>> {
    throw new Error('Method not implemented.');
  }
  public async settlementCommit(cmd: SettlementCommitCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }
  public async settlementUpdate(cmd: SettlementUpdateCommand): Promise<CommandResult<SettlementUpdateResult>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlementWindows(query: GetSettlementWindowsQuery): Promise<QueryResult<GetSettlementWindowsQueryResponse>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlementWindow(query: GetSettlementWindowQuery): Promise<QueryResultWithNotFound<SettlementWindow>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlement(query: GetSettlementQuery): Promise<QueryResultWithNotFound<Settlement>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlements(query: GetSettlementsQuery): Promise<GetSettlementsQueryResponse> {
    throw new Error('Method not implemented.');
  }
}