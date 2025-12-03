import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler"
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler"
import {
  AnyQuery,
  CommandResult,
  CreateDfspCommand,
  CreateDfspResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCollateralCommand,
  DepositCollateralResponse,
  DfspAccountResponse,
  FulfilResult,
  GetAllDfspsResponse,
  GetDfspAccountsQuery,
  GetHubAccountsQuery,
  GetNetDebitCapQuery,
  HubAccountResponse,
  LedgerDfsp,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  NetDebitCapResponse,
  PrepareResult,
  QueryResult,
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
  createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse>;
  disableDfsp(cmd: {dfspId: string}): Promise<CommandResult<void>>;
  enableDfsp(cmd: {dfspId: string}): Promise<CommandResult<void>>;
  depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse>;
  withdrawCollateral(cmd: unknown): Promise<unknown>;
  getHubAccounts(query: GetHubAccountsQuery): Promise<HubAccountResponse>

  getDfsp(query: {dfspId: string}): Promise<QueryResult<LedgerDfsp>>
  getAllDfsps(query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>>
  getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse>
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