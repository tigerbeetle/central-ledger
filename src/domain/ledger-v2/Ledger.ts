import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler"
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler"
import {
  AnyQuery,
  CommandResult,
  CreateDfspCommand,
  CreateDfspResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCommand,
  DepositResponse,
  DfspAccountResponse,
  FulfilResult,
  GetAllDfspAccountsQuery,
  GetAllDfspsResponse,
  GetDfspAccountsQuery,
  GetNetDebitCapQuery,
  HubAccountResponse,
  LegacyLedgerDfsp,
  LegacyLimit,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  PrepareResult,
  SetNetDebitCapCommand,
  SweepResult,
  WithdrawCommitCommand,
  WithdrawCommitResponse,
  WithdrawPrepareCommand,
  WithdrawPrepareResponse,
} from './types'
import { QueryResult } from "src/shared/results";

/**
  * Common interface for all ledger implementations
  */
export interface Ledger {
  /**
   * Onboarding/Lifecycle Management Commands
   */
  createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse>;
  createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse>;
  disableDfsp(cmd: {dfspId: string}): Promise<CommandResult<void>>;
  enableDfsp(cmd: {dfspId: string}): Promise<CommandResult<void>>;
  enableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>>;
  disableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>>;
  deposit(cmd: DepositCommand): Promise<DepositResponse>;
  withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse>;
  withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse>;
  setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>>;

  /**
   * Onboarding/Lifecycle Management Queries
   */
  getHubAccounts(query: AnyQuery): Promise<HubAccountResponse>
  getDfsp(query: {dfspId: string}): Promise<QueryResult<LegacyLedgerDfsp>>
  getAllDfsps(query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>>
  getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse>
  getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse>
  getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>>
  

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