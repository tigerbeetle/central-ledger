import { Ledger } from "src/domain/ledger-v2/Ledger";
import { CreateHubAccountCommand, CreateHubAccountResponse, CreateDFSPCommand, CreateDFSPResponse, DepositCollateralCommand, DepositCollateralResponse, GetDFSPAccountsQuery, DFSPAccountResponse, GetNetDebitCapQuery, NetDebitCapResponse, PrepareResult, FulfilResult, SweepResult, LookupTransferQuery, LookupTransferQueryResponse } from "src/domain/ledger-v2/types";
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";


/**
 * @class TestLedger
 * @description Use TestLedger as a superclass for test-specific ledger mocks
 */
export default class TestLedger implements Ledger {
  createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    throw new Error("Method not implemented.");
  }
  createDfsp(cmd: CreateDFSPCommand): Promise<CreateDFSPResponse> {
    throw new Error("Method not implemented.");
  }
  disableDfsp(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  enableDfsp(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse> {
    throw new Error("Method not implemented.");
  }
  withdrawCollateral(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  getDFSPAccounts(query: GetDFSPAccountsQuery): Promise<DFSPAccountResponse> {
    throw new Error("Method not implemented.");
  }
  getNetDebitCap(query: GetNetDebitCapQuery): Promise<NetDebitCapResponse> {
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
  closeSettlementWindow(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  settleClosedWindows(cmd: unknown): Promise<unknown> {
    throw new Error("Method not implemented.");
  }
  
}