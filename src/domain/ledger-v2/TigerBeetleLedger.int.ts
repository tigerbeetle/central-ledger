import assert from 'node:assert';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { CommitTransferDto, CreateTransferDto } from '../../handlers-v2/types';
import { checkSnapshotLedgerDfsp, unwrapSnapshot } from '../../testing/snapshot';
import { IntegrationHarness } from '../../testing/harness/harness';
import { TestUtils } from '../../testing/testutils';
import TigerBeetleLedger from "./TigerBeetleLedger";
import { AccountCode, FulfilResultType, PrepareResultType } from './types';

const participantService = require('../participant')

describe('TigerBeetleLedger', () => {
  let harness: IntegrationHarness;
  let ledger: TigerBeetleLedger;

  before(async () => {
    harness = await IntegrationHarness.create({
      hubCurrencies: ['USD'],
      provisionDfsps: [
        { dfspId: 'dfsp_a', currencies: ['USD'], startingDeposits: [100000] },
        { dfspId: 'dfsp_b', currencies: ['USD'], startingDeposits: [100000] }
      ]
    })

    ledger = harness.getResources().ledger as TigerBeetleLedger;
  })

  after(async () => {
    await harness.teardown()
  })

  describe('lifecycle', () => {
    const setupDfsp = async (dfspId: string, depositAmount: number, currency: string = 'USD') => {
      await participantService.ensureExists(dfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))
    }

    it('creates a dfsp, deposits funds, sets the limit and adjusts the limit', async () => {
      const dfspId = 'dfsp_c';
      const currency = 'USD';
      const depositAmount = 10000;
      const adjustedLimit = 6000;

      // Arrange: Create participant, DFSP, and deposit funds
      await setupDfsp(dfspId, depositAmount)

      // Assert
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,0,0,10000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,0,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,0,0;`
      ))

      // Act: Adjust the net debit cap to lower than deposit amount
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: adjustedLimit
      }))

      // Assert
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,4000,0,10000,6000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,4000,4000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))

      // Act: Now adjust NDC to be greater than deposit amount
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'UNLIMITED',
        dfspId,
        currency,
      }))

      // Assert: Query DFSP after limit adjustment
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,4000,0,14000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,4000,0,4000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))

      // Act: Now deposit more funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        dfspId,
        currency,
        transferId: randomUUID(),
        amount: 10000,
        reason: 'Additional deposit'
      }))

      // Assert: Query DFSP after limit adjustment
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,20000,0,0,20000;
        USD,20100,0,4000,0,24000,20000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,4000,0,4000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))
    })

    it('applies the net debit cap on the entire deposit amount', async () => {
      // Set net debit cap to 10k, deposit 11k
      // Then deposit another 2k, unrestricted should be 10k, restricted should be 3k
      const dfspId = 'dfsp_d';
      const currency = 'USD';

      // Arrange: Create participant and DFSP
      await participantService.ensureExists(dfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: 10000
      }))

      // Act: Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: 11000,
        reason: 'Test deposit'
      }))

      // Assert
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,11000,0,0,11000;
        USD,20100,0,1000,0,11000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,1000,1000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,10000,10000;`
      ))

      // Act: Deposit another 2,000
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: 2000,
        reason: 'Additional deposit'
      }))

      // Assert
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,13000,0,0,13000;
        USD,20100,0,4000,0,14000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,1000,0,4000,3000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,10000,10000;`
      ))
    })

    it('deposit is idempotent', async () => {
      // Set net debit cap to 10k, deposit 11k
      // Then deposit another 2k, unrestricted should be 10k, restricted should be 3k
      const dfspId = 'dfsp_e';
      const currency = 'USD';
      const transferId = '123456'

      // Arrange: Create participant and DFSP
      await participantService.ensureExists(dfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      // Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId,
        dfspId,
        currency,
        amount: 11000,
        reason: 'First deposit'
      }))


      // Act
      const depositResponseB = await ledger.deposit({
        transferId,
        dfspId,
        currency,
        amount: 11000,
        reason: 'First deposit'
      })

      assert(depositResponseB.type === 'ALREADY_EXISTS')
    })

    it('prepares the withdrawal', async () => {
      // Arrange
      const dfspId = 'dfsp_f'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = '230482309234234'

      await setupDfsp(dfspId, depositAmount)
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,5000,0,10000,5000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,5000,5000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawPrepareResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawPrepareResult.type === 'SUCCESS', 'expected success result')
    })

    it('withdraws funds in 2 phases', async () => {
      // Arrange
      const dfspId = 'dfsp_g'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = '2345872398928374'

      await setupDfsp(dfspId, depositAmount)
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      TestUtils.unwrapSuccess(await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawCommitResult = await ledger.withdrawCommit({
        transferId: withdrawalTransferId,
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,6000,4000;
        USD,20100,0,11000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawCommitResult.type === 'SUCCESS', 'expected success result')
    })

    it('withdraw fails if there are not enough funds available', async () => {
      // Arrange
      const dfspId = 'dfsp_h'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawAmount = 3000
      const withdrawalTransferId = '23984723984723'

      await setupDfsp(dfspId, depositAmount)

      // Act
      const result = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })

      // Assert
      assert(result.type === 'INSUFFICIENT_FUNDS')
    })

    it('fails in the prepare phase if the id has been reused', async () => {
      // Arrange
      const dfspId = 'dfsp_i'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawAmount = 3000
      const withdrawalTransferId = '12348239898723498'

      await setupDfsp(dfspId, depositAmount)
      await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'First withdrawal'
      })

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: 100,
        reason: 'Duplicate withdrawal'
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(
        duplicateWithdrawalResult.error.message,
        'Withdrawal failed - transferId has already been used.'
      )
    })

    it('handles a withdrawCommit() where the id is not found', async () => {
      // Arrange
      const dfspId = 'dfsp_j'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawalTransferId = randomUUID()

      await setupDfsp(dfspId, depositAmount)

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawCommit({
        transferId: withdrawalTransferId,
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(duplicateWithdrawalResult.error.message, `transferId: ${withdrawalTransferId} not found`)
    })

    it('aborts a withdrawal', async () => {
      // Arrange
      const dfspId = 'dfsp_k'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = randomUUID()

      await setupDfsp(dfspId, depositAmount)
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      TestUtils.unwrapSuccess(await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawCommitResult = await ledger.withdrawAbort({
        transferId: withdrawalTransferId,
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,5000,0,15000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawCommitResult.type === 'SUCCESS', 'expected success result')
    })

    it('handles a withdrawal abort where the id is not found', async () => {
      // Arrange
      const dfspId = 'dfsp_l'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawalTransferId = randomUUID()

      await setupDfsp(dfspId, depositAmount)

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawAbort({
        transferId: withdrawalTransferId,
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(duplicateWithdrawalResult.error.message, `transferId: ${withdrawalTransferId} not found`)
    })

    it('fails to disable the Deposit account', async () => {
      // Arrange
      const dfspId = 'dfsp_m'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const depositAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert(depositAccount, 'deposit account not found')

      // Act
      const disableAccountResult = await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(depositAccount.id)
      })

      // Assert
      assert(disableAccountResult.type === 'FAILURE')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const updatedDepositAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert.strictEqual(updatedDepositAccount.status, 'ENABLED')
    })

    it('fails to disable an account for a dfsp that does not exist', async () => {
      // Arrange
      // Act
      const disableAccountResult = await ledger.disableDfspAccount({
        dfspId: 'not_a_dfsp',
        accountId: Number(1245234234)
      })

      // Assert
      assert(disableAccountResult.type === 'FAILURE')
      assert.strictEqual(disableAccountResult.error.message, 'disableDfspAccount() - dfsp: not_a_dfsp not found.')
    })

    it('fails to disable a valid account that is not a Deposit or Unrestricted account', async () => {
      // Arrange
      const dfspId = 'dfsp_n'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const restrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Restricted)
      assert(restrictedAccount, 'deposit account not found')

      // Act
      const disableAccountResult = await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(restrictedAccount.id)
      })

      // Assert
      assert(disableAccountResult.type === 'FAILURE')
      assert.strictEqual(
        disableAccountResult.error.message,
        'disableDfspAccount() - account id not found, or is not Deposit or Unrestricted.'
      )
    })

    it('has no effect if the account is already closed', async () => {
      // Arrange
      const dfspId = 'dfsp_o'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestricted = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestricted, 'deposit account not found')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestricted.id)
      }))

      // Act
      const disableAccountResult = await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestricted.id)
      })

      // Assert
      assert(disableAccountResult.type === 'SUCCESS')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const updatedUnrestricted = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert.strictEqual(updatedUnrestricted.status, 'DISABLED')
    })

    it('enabling an already enabled account is a noop', async () => {
      // Arrange
      const dfspId = 'dfsp_p'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestrictedAccount, 'unrestricted account not found')

      // Act
      const enableDfspAccountResult = await ledger.enableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      })

      // Assert
      assert(enableDfspAccountResult.type === 'SUCCESS')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const updatedUnrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert.strictEqual(updatedUnrestrictedAccount.status, 'ENABLED')
    })

    it('enables a disabled account', async () => {
      // Arrange
      const dfspId = 'dfsp_q'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestrictedAccount, 'unrestricted account not found')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      }))

      // Act
      const enableDfspAccountResult = await ledger.enableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      })

      // Assert
      assert(enableDfspAccountResult.type === 'SUCCESS')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const updatedUnrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert.strictEqual(updatedUnrestrictedAccount.status, 'ENABLED')
    })

    it('enables an account a second time after disabling is a noop', async () => {
      // Arrange
      const dfspId = 'dfsp_r'
      const currency = 'USD'

      await setupDfsp(dfspId, 2500)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestrictedAccount, 'unrestricted account not found')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      }))
      TestUtils.unwrapSuccess(await ledger.enableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      }))

      // Act
      const enableDfspAccountResult = await ledger.enableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      })

      // Assert
      assert(enableDfspAccountResult.type === 'SUCCESS')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const updatedUnrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert.strictEqual(updatedUnrestrictedAccount.status, 'ENABLED')
    })

    it('fails to withdraw if the unrestricted account is disabled', async () => {
      // Arrange
      const dfspId = 'dfsp_s'
      const currency = 'USD'
      const depositAmount = 10000
      const withdrawAmount = 5000
      const withdrawalTransferId = randomUUID()

      await setupDfsp(dfspId, depositAmount)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestricted = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestricted, 'unrestricted account not found')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestricted.id)
      }))

      // Act
      const withdrawPrepareResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })

      // Assert
      assert(withdrawPrepareResult.type === 'FAILURE')
      assert.strictEqual(
        withdrawPrepareResult.error.message,
        'Withdrawal failed as one or more accounts is closed.'
      )
    })

    it('fails to withdraw if the unrestricted account is disabled', async () => {
      // Arrange
      const dfspId = 'dfsp_t'
      const currency = 'USD'
      const depositAmount = 10000
      const withdrawAmount = 5000
      const withdrawalTransferId = randomUUID()

      await setupDfsp(dfspId, depositAmount)
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      const unrestrictedAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert(unrestrictedAccount, 'unrestricted account not found')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(unrestrictedAccount.id)
      }))

      // Act
      const withdrawPrepareResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })

      // Assert
      assert(withdrawPrepareResult.type === 'FAILURE')
      assert.strictEqual(
        withdrawPrepareResult.error.message,
        'Withdrawal failed as one or more accounts is closed.'
      )
    })

  })

  // TODO(LD): come back to these next week!
  describe.skip('timeout handling', () => {
    it('prepares a transfer, waits for timeout, and sweeps', async () => {
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Arrange
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        // 1 second expiry
        expiration: new Date(Date.now() + 1050).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(input)
      assert(prepareResult.type === PrepareResultType.PASS)

      // Act
      await TestUtils.sleep(1500) // wait for TigerBeetle to timeout the transfer
      const sweepResult = await ledger.sweepTimedOut()

      // Assert
      assert(sweepResult.type === 'SUCCESS')
      const ids = sweepResult.transfers.map(t => t.id)
      assert(ids.includes(transferId))
    })

    it('once a transfer is swept, it cannot be swept again', async () => {
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Arrange
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        // 1 second expiry
        expiration: new Date(Date.now() + 1050).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(input)
      assert(prepareResult.type === PrepareResultType.PASS)

      // Act
      await TestUtils.sleep(1500) // wait for TigerBeetle to timeout the transfer
      const sweepResultA = await ledger.sweepTimedOut()
      const sweepResultB = await ledger.sweepTimedOut()

      // Assert
      assert(sweepResultA.type === 'SUCCESS')
      const ids = sweepResultA.transfers.map(t => t.id)
      assert(ids.includes(transferId))
      assert(sweepResultB.type === 'SUCCESS')
      assert(sweepResultB.transfers.length === 0)
    })
  })

  describe('clearing happy path', () => {
    const transferId = randomUUID()
    const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
    const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

    it('01 prepare transfer', async () => {
      // Arrange
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Act
      const result = await ledger.prepare(input)

      const tbLedger = ledger as TigerBeetleLedger
      const accounts = TestUtils.unwrapSuccess(
        await tbLedger.getDfspV2({dfspId: 'dfsp_a'})
      )
      TestUtils.printLedgerDfsps([accounts])

      // Assert
      assert.ok(result)
      assert.equal(result.type, PrepareResultType.PASS)
    })

    it('02 fulfill transfer', async () => {
      // Arrange
      const payload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      };
      const input = TestUtils.buildValidFulfilInput(transferId, payload)

      // Act
      const result = await ledger.fulfil(input)

      const tbLedger = ledger as TigerBeetleLedger
      const accountsA = TestUtils.unwrapSuccess(
        await tbLedger.getDfspV2({dfspId: 'dfsp_a'})
      )      
      const accountsB = TestUtils.unwrapSuccess(
        await tbLedger.getDfspV2({dfspId: 'dfsp_b'})
      )
      TestUtils.printLedgerDfsps([accountsA, accountsB])

      // Assert
      assert.ok(result)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        console.log('failed with error\n:', result.error)
      }
      assert.equal(result.type, FulfilResultType.PASS)
    })
  })

  describe('clearing unhappy path - prepare validation', () => {
    it('should fail when payer DFSP does not exist', async () => {
        // Arrange
        const transferId = randomUUID()
        const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
        const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
        const payload: CreateTransferDto = {
          transferId,
          payerFsp: 'non_existent_payer_dfsp',
          payeeFsp: 'dfsp_b',
          amount: { amount: '100', currency: 'USD' },
          ilpPacket,
          condition,
          expiration: new Date(Date.now() + 60000).toISOString()
        }
        const input = TestUtils.buildValidPrepareInput(transferId, payload)

        // Act
        const result = await ledger.prepare(input)

        // Assert
        assert.equal(result.type, PrepareResultType.FAIL_OTHER)
        if (result.type === PrepareResultType.FAIL_OTHER) {
          assert.ok(result.error)
          assert.match(result.error.message, /payer fsp.*not found/i)
        }
      })

      it('should fail when payee DFSP does not exist', async () => {
        // Arrange
        const transferId = randomUUID()
        const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
        const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
        const payload: CreateTransferDto = {
          transferId,
          payerFsp: 'dfsp_a',
          payeeFsp: 'non_existent_payee_dfsp',
          amount: { amount: '100', currency: 'USD' },
          ilpPacket,
          condition,
          expiration: new Date(Date.now() + 60000).toISOString()
        }
        const input = TestUtils.buildValidPrepareInput(transferId, payload)

        // Act
        const result = await ledger.prepare(input)

        // Assert
        assert.equal(result.type, PrepareResultType.FAIL_OTHER)
        if (result.type === PrepareResultType.FAIL_OTHER) {
          assert.ok(result.error)
          assert.match(result.error.message, /payee fsp.*not found/i)
        }
      })

      it('should fail when expiration format is invalid', async () => {
        // Arrange
        const transferId = randomUUID()
        const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
        const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
        const payload: CreateTransferDto = {
          transferId,
          payerFsp: 'dfsp_a',
          payeeFsp: 'dfsp_b',
          amount: { amount: '100', currency: 'USD' },
          ilpPacket,
          condition,
          expiration: 'invalid-date-format'
        }
        const input = TestUtils.buildValidPrepareInput(transferId, payload)

        // Act
        const result = await ledger.prepare(input)

        // Assert
        assert.equal(result.type, PrepareResultType.FAIL_OTHER)
        if (result.type === PrepareResultType.FAIL_OTHER) {
          assert.ok(result.error)
          assert.match(result.error.message, /invalid.*expiration/i)
        }
      })

      it('should fail when expiration is already in the past', async () => {
        // Arrange
        const transferId = randomUUID()
        const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() - 60000))
        const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
        const payload: CreateTransferDto = {
          transferId,
          payerFsp: 'dfsp_a',
          payeeFsp: 'dfsp_b',
          amount: { amount: '100', currency: 'USD' },
          ilpPacket,
          condition,
          expiration: new Date(Date.now() - 60000).toISOString()
        }
        const input = TestUtils.buildValidPrepareInput(transferId, payload)

        // Act
        const result = await ledger.prepare(input)

        // Assert
        assert.equal(result.type, PrepareResultType.FAIL_OTHER)
        if (result.type === PrepareResultType.FAIL_OTHER) {
          assert.ok(result.error)
          assert.match(result.error.message, /expiration.*past/i)
        }
      })
  })

  describe('clearing unhappy path - prepare liquidity', () => {
    it('should fail with insufficient liquidity when payer exceeds available balance', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Create a new DFSP with limited funds (100 USD)
      const limitedDfspId = 'dfsp_limited_' + randomUUID().substring(0, 8)
      await participantService.ensureExists(limitedDfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId: limitedDfspId,
        currencies: ['USD']
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId: limitedDfspId,
        currency: 'USD',
        amount: 100,
        reason: 'Initial deposit for insufficient liquidity test'
      }))

      // Attempt to transfer 200 USD (more than available 100)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: limitedDfspId,
        payeeFsp: 'dfsp_b',
        amount: { amount: '200', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Act
      const result = await ledger.prepare(input)

      // Assert
      assert.equal(result.type, PrepareResultType.FAIL_LIQUIDITY)
    })

    it('should fail when payer account is closed', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Create a new DFSP, deposit funds, then close it
      const closedPayerDfspId = 'dfsp_closed_payer_' + randomUUID().substring(0, 8)
      await participantService.ensureExists(closedPayerDfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId: closedPayerDfspId,
        currencies: ['USD']
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId: closedPayerDfspId,
        currency: 'USD',
        amount: 1000,
        reason: 'Initial deposit before closing account'
      }))

      // Close the payer's unrestricted account
      const dfspAccounts = TestUtils.unwrapSuccess(await ledger.getDfspV2({dfspId: closedPayerDfspId}))
      const unrestricted = dfspAccounts.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert.ok(unrestricted, 'Unrestricted account should exist')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId: closedPayerDfspId,
        accountId: Number(unrestricted.id)
      }))

      // Attempt to prepare transfer with closed payer
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: closedPayerDfspId,
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Act
      const result = await ledger.prepare(input)

      // Assert
      assert.equal(result.type, PrepareResultType.FAIL_OTHER)
      if (result.type === PrepareResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payer.*not active/i)
      }
    })

    it('should fail when payee account is closed', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Create a new DFSP, deposit funds, then close it
      const closedPayeeDfspId = 'dfsp_closed_payee_' + randomUUID().substring(0, 8)
      await participantService.ensureExists(closedPayeeDfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId: closedPayeeDfspId,
        currencies: ['USD']
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId: closedPayeeDfspId,
        currency: 'USD',
        amount: 1000,
        reason: 'Initial deposit before closing account'
      }))

      // Close the payee's unrestricted account
      const dfspAccounts = TestUtils.unwrapSuccess(await ledger.getDfspV2({dfspId: closedPayeeDfspId}))
      const unrestricted = dfspAccounts.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert.ok(unrestricted, 'Unrestricted account should exist')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId: closedPayeeDfspId,
        accountId: Number(unrestricted.id)
      }))

      // Attempt to prepare transfer with closed payee
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: closedPayeeDfspId,
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Act
      const result = await ledger.prepare(input)

      // Assert
      assert.equal(result.type, PrepareResultType.FAIL_OTHER)
      if (result.type === PrepareResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payee.*not active/i)
      }
    })
  })

  describe('clearing unhappy path - prepare duplicates', () => {
    it('should handle duplicate prepare (idempotent - exact same)', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare once successfully
      const firstResult = await ledger.prepare(input)
      assert.equal(firstResult.type, PrepareResultType.PASS)

      // Act
      const result = await ledger.prepare(input)

      // Assert
      assert.equal(result.type, PrepareResultType.PASS)
    })

    it('should detect duplicate with modified parameters', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Prepare with amount 100
      const payload1: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input1 = TestUtils.buildValidPrepareInput(transferId, payload1)
      const firstResult = await ledger.prepare(input1)
      assert.equal(firstResult.type, PrepareResultType.PASS)

      // Act - Prepare again with same transferId but different amount
      const payload2: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '200', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const input2 = TestUtils.buildValidPrepareInput(transferId, payload2)
      const result = await ledger.prepare(input2)

      // Assert
      assert.equal(result.type, PrepareResultType.MODIFIED)
    })

    it.only('should detect duplicate after fulfil (final state)', async () => {
      // Arrange - Complete full happy path
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare and fulfil
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const fulfilResult = await ledger.fulfil(fulfilInput)
      assert.equal(fulfilResult.type, FulfilResultType.PASS)

      // Act - Attempt prepare again after fulfil
      const result = await ledger.prepare(prepareInput)

      // Assert
      assert.equal(result.type, PrepareResultType.DUPLICATE_FINAL)
    })
  })

  describe('clearing unhappy path - abort errors', () => {
    it('should fail to abort non-existent transfer', async () => {
      // Arrange
      const transferId = randomUUID()
      const input = TestUtils.buildValidAbortInput(transferId)

      // Act
      const result = await ledger.fulfil(input)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payment.*not.*found/i)
      }
    })

    it('should detect already aborted transfer (idempotent check)', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare and abort once
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      const abortInput = TestUtils.buildValidAbortInput(transferId)
      const firstAbortResult = await ledger.fulfil(abortInput)
      assert.equal(firstAbortResult.type, FulfilResultType.PASS)

      // Act - Abort again
      const result = await ledger.fulfil(abortInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /already aborted/i)
      }
    })

    it('should fail to abort already fulfilled transfer', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare and fulfil
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const fulfilResult = await ledger.fulfil(fulfilInput)
      assert.equal(fulfilResult.type, FulfilResultType.PASS)

      // Act - Attempt abort after fulfil
      const abortInput = TestUtils.buildValidAbortInput(transferId)
      const result = await ledger.fulfil(abortInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /already fulfilled/i)
      }
    })
  })

  describe('clearing unhappy path - fulfil validation', () => {
    it('should fail and auto-abort with wrong fulfilment', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare transfer
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      // Generate wrong fulfilment (not matching the condition)
      const wrongFulfilment = 'A'.repeat(48)

      // Act
      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment: wrongFulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const result = await ledger.fulfil(fulfilInput)

      // Assert - Should fail validation
      assert.equal(result.type, FulfilResultType.FAIL_VALIDATION)
      if (result.type === FulfilResultType.FAIL_VALIDATION) {
        assert.ok(result.error)
      }

      // TODO: instead of aborting again to check if it's aborted, why not 
      // check the balance sheet!?

      // Verify transfer was auto-aborted
      // Try to abort again - should get "already aborted" error
      const retryInput = TestUtils.buildValidAbortInput(transferId)
      const retryResult = await ledger.fulfil(retryInput)
      assert.equal(retryResult.type, FulfilResultType.FAIL_OTHER)
      if (retryResult.type === FulfilResultType.FAIL_OTHER) {
        assert.match(retryResult.error.message, /already aborted/i)
      }
    })

    it('should fail to fulfil non-existent transfer', async () => {
      // Arrange
      const transferId = randomUUID()
      const fulfilment = 'A'.repeat(48)
      const payload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const input = TestUtils.buildValidFulfilInput(transferId, payload)

      // Act
      const result = await ledger.fulfil(input)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payment.*not.*found/i)
      }
    })

    it.todo('should handle fulfil with missing transfer spec')
  })

  describe('clearing unhappy path - fulfil state errors', () => {
    it('should detect already fulfilled transfer (idempotent check)', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare and fulfil once
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const firstFulfilResult = await ledger.fulfil(fulfilInput)
      assert.equal(firstFulfilResult.type, FulfilResultType.PASS)

      // Act - Fulfil again
      const result = await ledger.fulfil(fulfilInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /already fulfilled/i)
      }
    })

    it('should fail to fulfil already aborted transfer', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)

      // Prepare and abort
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      const abortInput = TestUtils.buildValidAbortInput(transferId)
      const abortResult = await ledger.fulfil(abortInput)
      assert.equal(abortResult.type, FulfilResultType.PASS)

      // Act - Attempt fulfil after abort
      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const result = await ledger.fulfil(fulfilInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /already aborted/i)
      }
    })

    it('should fail to fulfil with closed payer account', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Create a new DFSP with funds
      const closingPayerDfspId = 'dfsp_closing_payer_' + randomUUID().substring(0, 8)
      await participantService.ensureExists(closingPayerDfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId: closingPayerDfspId,
        currencies: ['USD']
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId: closingPayerDfspId,
        currency: 'USD',
        amount: 1000,
        reason: 'Initial deposit'
      }))

      // Prepare transfer (creates pending transfer on payer's reserved account)
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: closingPayerDfspId,
        payeeFsp: 'dfsp_b',
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      // Close payer's unrestricted account
      const dfspAccounts = TestUtils.unwrapSuccess(await ledger.getDfspV2({dfspId: closingPayerDfspId}))
      const unrestricted = dfspAccounts.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert.ok(unrestricted, 'Unrestricted account should exist')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId: closingPayerDfspId,
        accountId: Number(unrestricted.id)
      }))

      // Act - Attempt fulfil with closed payer account
      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const result = await ledger.fulfil(fulfilInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payer.*account.*closed/i)
      }
    })

    it('should fail to fulfil with closed payee account', async () => {
      // Arrange
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Create a new DFSP for payee with funds
      const closingPayeeDfspId = 'dfsp_closing_payee_' + randomUUID().substring(0, 8)
      await participantService.ensureExists(closingPayeeDfspId)
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId: closingPayeeDfspId,
        currencies: ['USD']
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId: closingPayeeDfspId,
        currency: 'USD',
        amount: 1000,
        reason: 'Initial deposit'
      }))

      // Prepare transfer
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: closingPayeeDfspId,
        amount: { amount: '100', currency: 'USD' },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      }
      const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(prepareInput)
      assert.equal(prepareResult.type, PrepareResultType.PASS)

      // Close payee's unrestricted account
      const dfspAccounts = TestUtils.unwrapSuccess(await ledger.getDfspV2({dfspId: closingPayeeDfspId}))
      const unrestricted = dfspAccounts.accounts.find(acc => acc.code === AccountCode.Unrestricted)
      assert.ok(unrestricted, 'Unrestricted account should exist')
      TestUtils.unwrapSuccess(await ledger.disableDfspAccount({
        dfspId: closingPayeeDfspId,
        accountId: Number(unrestricted.id)
      }))

      // Act - Attempt fulfil with closed payee account
      const fulfilPayload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      }
      const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload)
      const result = await ledger.fulfil(fulfilInput)

      // Assert
      assert.equal(result.type, FulfilResultType.FAIL_OTHER)
      if (result.type === FulfilResultType.FAIL_OTHER) {
        assert.ok(result.error)
        assert.match(result.error.message, /payee.*account.*closed/i)
      }
    })
  })
})