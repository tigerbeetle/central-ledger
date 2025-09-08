import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler"
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler"
import {
  CreateDFSPCommand,
  CreateDFSPResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCollateralCommand,
  DepositCollateralResponse,
  FulfilResult,
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
  disableDfsp(thing: unknown): Promise<unknown>;
  enableDfsp(thing: unknown): Promise<unknown>;
  depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse>;
  withdrawCollateral(thing: unknown): Promise<unknown>;
  /**
   * Clearing Methods
   */
  prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult>;
  fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult>;
  /**
   * Settlement Methods
   */
  closeSettlementWindow(thing: unknown): Promise<unknown>;
  settleClosedWindows(thing: unknown): Promise<unknown>;
}