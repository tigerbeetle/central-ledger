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
  GetHubAccountsQuery,
  GetNetDebitCapQuery,
  HubAccountResponse,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  NetDebitCapResponse,
  PrepareResult,
  SweepResult,
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
  getHubAccounts(query: GetHubAccountsQuery): Promise<HubAccountResponse>
  getDFSPAccounts(query: GetDFSPAccountsQuery): Promise<DFSPAccountResponse>
  getNetDebitCap(query: GetNetDebitCapQuery): Promise<NetDebitCapResponse>
  // setLimits(cmd: SetLimitsCommand): Promise<SetLimitsResponse>;
  // getLimits(cmd: GetLimitsCommand): Promise<GetLimitsResponse>;

  /**
   * Clearing Methods
   */

  /**
   * @method prepare
   * @description Prepares a payment for clearing, reserving the payment amount from the Payer's
   *   account to prevent double spending.
   */
  prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult>;

  /**
   * @method fulfil
   * @description Clears a previously prepared payment.
   */
  fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult>;

  /**
   * @method sweepTimedOut
   * @description Looks through the ledger timed out transfers. Once a transfer has been swept,
   *  it will not be returned again with sweepForTimedOutTransfers()
   */
  sweepTimedOut(): Promise<SweepResult>;

  /**
   * @method lookupTransfer
   * @description Looks up a previously created Mojaloop Transfer.
   * 
   * TODO(LD): We need to also include the transfer metadata, such as payer and payee ids
   *   in the response here, so that we can check if the ultimate caller of lookupTransfer
   *   is allowed to execute this request.
   */
  lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse>

  /**
   * Settlement Methods
   */
  closeSettlementWindow(cmd: unknown): Promise<unknown>;
  settleClosedWindows(cmd: unknown): Promise<unknown>;
}