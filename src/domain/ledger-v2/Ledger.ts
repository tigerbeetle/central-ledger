import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler"
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler"
import {
  CreateDFSPCommand,
  CreateDFSPResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCollateralCommand,
  DepositCollateralResponse,
  DFSPAccountResponse,
  FulfilResult,
  GetDFSPAccountsQuery,
  GetNetDebitCapQuery,
  NetDebitCapResponse,
  PrepareResult,
} from './types'

/**
  * Common interface for all ledger implementations
  */
export interface Ledger {
  /**
   * Onboarding/Lifecycle Management
   */
  createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse>;
  createDfsp(cmd: CreateDFSPCommand): Promise<CreateDFSPResponse>;
  disableDfsp(cmd: unknown): Promise<unknown>;
  enableDfsp(cmd: unknown): Promise<unknown>;
  depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse>;
  withdrawCollateral(cmd: unknown): Promise<unknown>;
  getAccounts(query: GetDFSPAccountsQuery): Promise<DFSPAccountResponse>
  getNetDebitCap(query: GetNetDebitCapQuery): Promise<NetDebitCapResponse>
  // setLimits(cmd: SetLimitsCommand): Promise<SetLimitsResponse>;
  // getLimits(cmd: GetLimitsCommand): Promise<GetLimitsResponse>;

  /**
   * Clearing Methods
   */
  prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult>;
  fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult>;
  /**
   * Settlement Methods
   */
  closeSettlementWindow(cmd: unknown): Promise<unknown>;
  settleClosedWindows(cmd: unknown): Promise<unknown>;
}