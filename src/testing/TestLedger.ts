import { Ledger } from "src/domain/ledger-v2/Ledger";
import { CreateHubAccountCommand, CreateHubAccountResponse, GetNetDebitCapQuery, LegacyLimit, PrepareResult, FulfilResult, SweepResult, LookupTransferQuery, LookupTransferQueryResponse, AnyQuery, CommandResult, DepositCommand, DepositResponse, DfspAccountResponse, GetAllDfspsResponse, GetDfspAccountsQuery, GetHubAccountsQuery, HubAccountResponse, LedgerDfsp, WithdrawCommitCommand, WithdrawCommitResponse, WithdrawPrepareCommand, WithdrawPrepareResponse, CreateDfspCommand, CreateDfspResponse, SetNetDebitCapCommand, GetAllDfspAccountsQuery, LegacyLedgerDfsp, WithdrawAbortCommand, WithdrawAbortResponse, SettlementCloseWindowCommand, SettlementPrepareCommand, SettlementAbortCommand, SettlementCommitCommand, GetSettlementQuery, GetSettlementQueryResponse, GetSettlementsQuery, GetSettlementsQueryResponse, SettlementUpdateCommand, GetSettlementWindowsQuery, GetSettlementWindowsQueryResponse } from "src/domain/ledger-v2/types";
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { QueryResult } from "src/shared/results";


/**
 * @class TestLedger
 * @description Use TestLedger as a superclass for test-specific ledger mocks
 */
export default class TestLedger implements Ledger {
  getSettlementWindows(query: GetSettlementWindowsQuery): Promise<QueryResult<GetSettlementWindowsQueryResponse>> {
    throw new Error("Method not implemented.");
  }
  settlementUpdate(cmd: SettlementUpdateCommand): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  getSettlement(query: GetSettlementQuery): Promise<GetSettlementQueryResponse> {
    throw new Error("Method not implemented.");
  }
  getSettlements(query: GetSettlementsQuery): Promise<GetSettlementsQueryResponse> {
    throw new Error("Method not implemented.");
  }
  createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    throw new Error("Method not implemented.");
  }
  getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>> {
    throw new Error("Method not implemented.");
  }
  setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error("Method not implemented.");
  }
  prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult> {
    throw new Error("Method not implemented.");
  }
  fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    throw new Error("Method not implemented.");
  }
  sweepTimedOut(): Promise<SweepResult> {
    throw new Error("Method not implemented.");
  }
  lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    throw new Error("Method not implemented.");
  }
  closeSettlementWindow(cmd: SettlementCloseWindowCommand): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  settlementPrepare(cmd: SettlementPrepareCommand): Promise<CommandResult<{id: number}>> {
    throw new Error("Method not implemented.");
  }
  settlementAbort(cmd: SettlementAbortCommand): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  settlementCommit(cmd: SettlementCommitCommand): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  settleClosedWindows(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  disableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  enableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  enableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  disableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    throw new Error("Method not implemented.");
  }
  deposit(cmd: DepositCommand): Promise<DepositResponse> {
    throw new Error("Method not implemented.");
  }
  withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    throw new Error("Method not implemented.");
  }
  withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    throw new Error("Method not implemented.");
  }
  withdrawAbort(cmd: WithdrawAbortCommand): Promise<WithdrawAbortResponse> {
    throw new Error("Method not implemented.");
  }
  getHubAccounts(query: AnyQuery): Promise<HubAccountResponse> {
    throw new Error("Method not implemented.");
  }
  getDfsp(query: { dfspId: string; }): Promise<QueryResult<LegacyLedgerDfsp>> {
    throw new Error("Method not implemented.");
  }
  getAllDfsps(query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>> {
    throw new Error("Method not implemented.");
  }
  getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error("Method not implemented.");
  }
  createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    throw new Error("Method not implemented.");
  }
  
  
  
}