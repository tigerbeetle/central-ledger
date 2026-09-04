import { describe, it } from "node:test"
import assert from "node:assert"
import Harness from '../../testing/harness'
import PRNG from "../../testing/prng"
import { logger } from "../../shared/logger"
import * as ApiHelpers from '../../testing/api-helpers'
import Coverage from "../../testing/coverage"
import { randomAvailablePort } from "../../testing/util"
import { Server } from "@hapi/hapi"

const harness = Harness.getInstance()
let Handler: any
let server: Server

describe('api/participants/handler', () => {

  it('handler fuzz', async () => {
    try {
      await harness.up()
      await harness.setupGlobals()

      const routes = require('./routes')
      const port = await randomAvailablePort()
      server = new Server({
        port
      })
      server.route(routes)
      await server.start()

      const coverage = new Coverage([
        'src/api/participants/handler.js'
      ])
      coverage.start()

      const seed = 1
      const prng = new PRNG(seed)
      const fuzzer = new HandlerApiFuzzer(harness, Handler, server, prng, seed)
      await fuzzer.run()

      console.log(`trace is:\n\n${fuzzer.traceOutput}`)

      await server.stop()
      coverage.stopAndReport()
    } catch (err: any) {
      logger.error(err.message)
      logger.error(err.stack)
      throw err
    } finally {
      await harness.teardownGlobals()
      await harness.down()
    }
  })
})

type ActionName =
  | 'getAll'
  | 'getByName'
  | 'create'
  | 'update'
  | 'addEndpoint'
  | 'getEndpoint'
  | 'addLimitAndInitialPosition'
  | 'getLimits'
  | 'getLimitsForAllParticipants'
  | 'adjustLimits'
  | 'createHubAccount'
  | 'getPositions'
  | 'getAccounts'
  | 'updateAccount'
  | 'recordFundsCreate'
  | 'recordFundsUpdate'

type Mutation =
  | 'deleteKey'
  | 'addKey'
  | 'nullifyValue'
  | 'changeType'
  | 'mutate'

class HandlerApiFuzzer {
  private step = 1
  private readonly stepsMax = 11000
  private responses: Array<{
    action: ActionName,
    input: any,
    body: any,
    code: any
  }> = []

  private dfspNames: Array<string> = []
  private dfspAccountsSettlement: Record<string, Array<number>> = {}
  private transferIds: Array<string> = []
  private registeredCurrencies: Array<string> = []

  private weights: Record<ActionName, number> = {
    getAll: 1,
    getByName: 1,
    create: 1,
    update: 1,
    addEndpoint: 2,
    getEndpoint: 2,
    addLimitAndInitialPosition: 2,
    getLimits: 2,
    getLimitsForAllParticipants: 2,
    adjustLimits: 2,
    createHubAccount: 10,
    getPositions: 2,
    getAccounts: 2,
    updateAccount: 2,
    recordFundsCreate: 2,
    recordFundsUpdate: 2
  }

  constructor(
    private harness: Harness,
    private handler: any,
    private server: Server,
    private prng: PRNG,
    private seed: number,
  ) { }

  public async run() {
    try {
      while (this.step <= this.stepsMax) {
        await this.doStep()

        this.step += 1
      }
    } catch (err: any) {
      logger.error(`HandlerApiFuzzer.run() died on step: ${this.step}.\nError: ${err.message}\nStack: ${err.stack}`)
      logger.error(`HandlerApiFuzzer.run() rerun with SEED=${this.seed}`)
      throw err
    }
  }

  get traceOutput(): string {
    const width = 150
    return this.responses
      .map(response => {
        return [
          `${response.action.padEnd(12)}:`,
          `\t${JSON.stringify(response.input).padEnd(width).slice(0, width)}`,
          `\t${JSON.stringify(response.body).padEnd(width).slice(0, width)}`
        ].join('\n')
      })
      .join('\n')
  }

  private async doStep() {
    return this.randomAction()()
  }

  private actions: Record<ActionName, () => Promise<void>> = {
    getAll: () => this.getAll(),
    getByName: () => this.getByName(),
    create: () => this.create(),
    update: () => this.update(),
    addEndpoint: () => this.addEndpoint(),
    getEndpoint: () => this.getEndpoint(),
    addLimitAndInitialPosition: () => this.addLimitAndInitialPosition(),
    getLimits: () => this.getLimits(),
    getLimitsForAllParticipants: () => this.getLimitsForAllParticipants(),
    adjustLimits: () => this.adjustLimits(),
    createHubAccount: () => this.createHubAccount(),
    getPositions: () => this.getPositions(),
    getAccounts: () => this.getAccounts(),
    updateAccount: () => this.updateAccount(),
    recordFundsCreate: () => this.recordFundsCreate(),
    recordFundsUpdate: () => this.recordFundsUpdate()
  }

  private randomAction(): () => Promise<void> {
    const table = PRNG.generateWeightedChoiceTable<ActionName>(this.weights)
    const action = this.prng.randomElementFrom(table)
    return this.actions[action]
  }

  private async req(action: ActionName, method: string, path: string, payload?: any) {
    const res = await this.server.inject({
      method,
      url: path,
      payload,
      headers: { 'Content-Type': 'application/json' }
    })

    this.responses.push({
      action,
      input: payload,
      code: res.statusCode,
      body: res.result
    })

    if (action === 'getAccounts' && res.statusCode === 200) {
      assert(res)
      const name = path.match(/^\/participants\/(.*)\/accounts/)
      assert(name !== null && name[1], `Could not match dfsp name from path: '${path}'.`)
      const accounts = res.result as Array<{ id: number, ledgerAccountType: string }>
      // Store the dfsp=>[account] mapping to the list of ids.
      this.dfspAccountsSettlement[name[1]] = [
        ...accounts
          .filter(account => account.ledgerAccountType === 'SETTLEMENT')
          .map(account => account.id),
      ]
    }
  }

  // API Methods under test.
  public async getAll() {
    const query = this.prng.randomElementFrom([
      this.mutateString(`?isProxy=${this.prng.headsOrTails()}`), ''
    ])
    await this.req('getAll', 'GET', '/participants' + query, {})
  }

  public async getByName() {
    const name = this.randomDfspName()
    await this.req('getByName', 'GET', `/participants/${name}`, {})
  }

  public async create() {
    if (this.prng.headsOrTails() && this.registeredCurrencies.length > 0) {
      const name = `dfsp_${this.prng.randomString(this.prng.intInRange(1, 5))}`
      this.dfspNames.push(name)
      const currency = this.prng.randomElementFrom(this.registeredCurrencies)
      try {
        await ApiHelpers.buildDfsp()
          .deps(this.harness)
          .name(name)
          .currency(currency)
          .proxy(this.prng.headsOrTails())
          .build()
          .create()
      } catch (err) {
        logger.error(`create() buildDfsp died on currency: ${currency}`)
        throw err
      }

      this.weights.createHubAccount = 1
      this.weights.create = 1
    }

    const payload = this.mutateObject({
      name: this.randomDfspName(),
      currency: this.randomCurrency(),
      isProxy: this.prng.headsOrTails()
    })
    await this.req('create', 'POST', `/participants`, payload)
  }

  public async update() {
    const name = this.randomDfspName()
    const payload = this.mutateObject({
      isActive: this.prng.headsOrTails()
    })
    await this.req('update', 'PUT', `/participants/${name}`, payload)
  }

  public async addEndpoint() {
    const name = this.randomDfspName()
    const payload = this.mutateObject({
      type: this.randomEndpointType(),
      value: `http://` + this.prng.randomString()
    })
    await this.req('addEndpoint', 'POST', `/participants/${name}/endpoints`, payload)
  }

  public async getEndpoint() {
    const name = this.randomDfspName()
    const query = this.prng.randomElementFrom([
      this.mutateString(`?type=${this.randomEndpointType()}`),
      ''
    ])

    await this.req('getEndpoint', 'GET', `/participants/${name}/endpoints` + query, {})
  }

  public async addLimitAndInitialPosition() {
    const name = this.randomDfspName()
    const payload = this.mutateObject({
      currency: this.randomCurrency(),
      limit: {
        type: this.prng.randomElementFrom([
          'NET_DEBIT_CAP', // Valid.
          this.prng.randomString()
        ]),
        value: this.prng.intInRange(0, 1000000)
      },
      type: this.randomEndpointType(),
      initialPosition: this.prng.intInRange(0, 1000000),
    })
    await this.req(
      'addLimitAndInitialPosition',
      'POST',
      `/participants/${name}/initialPositionAndLimits`,
      payload
    )
  }

  public async getLimits() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/limits`
    const query = this.mutateString(`?currency=${this.randomCurrency()}&type=NET_DEBIT_CAP`)
    await this.req('getLimits', 'GET', url + query, {})
  }

  public async getLimitsForAllParticipants() {
    const url = `/participants/limits`
    const query = this.mutateString(`?currency=${this.randomCurrency()}&type=NET_DEBIT_CAP`)
    await this.req('getLimitsForAllParticipants', 'GET', url + query, {})
  }

  public async adjustLimits() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/limits`
    const payload = this.mutateObject({
      currency: this.randomCurrency(),
      limit: {
        type: this.prng.randomElementFrom([
          'NET_DEBIT_CAP', // Valid.
          this.prng.randomString()
        ]),
        value: this.prng.intInRange(0, 1000000),
        alarmPercentage: this.prng.intInRange(-1, 101),
      },
    })
    await this.req('adjustLimits', 'PUT', url, payload)
  }

  public async createHubAccount() {
    // The odds of this all getting created properly are quite low, so let's flip a coin and just
    // set up the hub if true.
    if (this.prng.headsOrTails() && this.registeredCurrencies.length < 2) {
      const currency = this.prng.randomElementFrom(['USD', 'EUR', 'GBP'])
      await ApiHelpers.buildHub()
        .deps(this.harness)
        .currency(currency)
        .build()
        .create()
      this.registeredCurrencies.push(currency)

      this.weights.createHubAccount = 1
      this.weights.create = 10
    }

    const name = this.prng.randomElementFrom([
      'Hub',
      this.randomDfspName(),
      this.mutateString('Hub')
    ])
    const url = `/participants/${name}/accounts`
    const payload = this.mutateObject({
      currency: this.prng.randomElementFrom(['USD', 'EUR', 'GBP']),
      type: this.prng.randomElementFrom([
        'POSITION',
        'SETTLEMENT',
        'HUB_RECONCILIATION',
        'HUB_MULTILATERAL_SETTLEMENT',
        'HUB_FEE',
        'POSITION_REMITTANCE',
        'SETTLEMENT_REMITTANCE',
        this.prng.randomString(),
      ])
    })
    await this.req('createHubAccount', 'POST', url, payload)
  }

  public async getPositions() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/positions`
    const query = this.mutateString(`?currency=${this.randomCurrency()}`)
    await this.req('getPositions', 'GET', url + query, {})
  }

  public async getAccounts() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/accounts`
    const query = this.prng.randomElementFrom([
      '', `?currency${this.randomCurrency}`
    ])
    await this.req('getAccounts', 'GET', url + query, {})
  }

  public async updateAccount() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccount(name)
    const url = `/participants/${name}/accounts/${account}`
    const payload = {
      isActive: this.prng.randomElementFrom([
        this.prng.headsOrTails(), 
        // this.prng.randomValue
      ])
    }
    await this.req('updateAccount', 'PUT', url, payload)
  }

  public async recordFundsCreate() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccount(name)
    const url = `/participants/${name}/accounts/${account}`
    const payload = this.mutateObject({
      transferId: this.randomTransferId(),
      externalReference: this.prng.randomString(),
      action: this.prng.randomElementFrom([
        'recordFundsIn',
        'recordFundsOutPrepareReserve',
        this.prng.randomString(),
      ]),
      reason: this.prng.randomString(),
      amount: {
        amount: this.prng.intInRange(-1, 10000) / 100,
        currency: this.randomCurrency()
      }
    })
    await this.req('recordFundsCreate', 'POST', url, payload)
  }

  public async recordFundsUpdate() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccount(name)
    const transferId = this.randomTransferId()
    const url = `/participants/${name}/accounts/${account}/${transferId}`
    const payload = this.mutateObject({
      action: this.prng.randomElementFrom([
        'recordFundsOutCommit',
        'recordFundsOutAbort',
        this.prng.randomString(),
      ]),
      reason: this.prng.randomString(),
    })
    await this.req('recordFundsUpdate', 'PUT', url, payload)
  }

  private randomDfspName(): string {
    if (this.dfspNames.length > 0 && this.prng.headsOrTails()) {
      // Reuse
      return this.prng.randomElementFrom(this.dfspNames)
    }

    const name = `dfsp_${this.prng.randomString(this.prng.intInRange(1, 5))}`
    return name
  }

  private randomDfspAccount(dfsp: string): number {
    if (this.dfspAccountsSettlement[dfsp] && 
      this.dfspAccountsSettlement[dfsp].length > 0 && 
      this.prng.headsOrTails()
    ) {
      return this.prng.randomElementFrom(this.dfspAccountsSettlement[dfsp])
    }

    return this.prng.intInRange(-1, 10)
  }

  private randomTransferId(): string {
    if (this.transferIds.length > 0 && this.prng.headsOrTails()) {
      // Reuse
      return this.prng.randomElementFrom(this.transferIds)
    }

    const id = this.mutateString(this.prng.uuidv4())
    this.transferIds.push(id)

    return id
  }

  private randomCurrency(): string {
    const currency = this.prng.randomElementFrom(['USD', 'BGP', 'EUR', 'GBP'])
    if (this.prng.headsOrTails()) {
      return currency
    }
    return this.mutateString(currency)
  }

  private mutateString(input: string): string {
    if (this.prng.headsOrTails()) {
      // Safe.
      return input
    }

    if (this.prng.headsOrTails() && input.length > 0) {
      // Make it shorter.
      return input.substring(0, this.prng.intInRange(0, input.length))
    }

    // Make it longer.
    return input + this.prng.randomString(this.prng.intInRange(1, 5))
  }

  private mutateNumber(input: number): number {
    const mutation = this.prng.randomElementFrom([
      'negate',
      'zero',
      'overflow',
      'fraction',
      'increment',
    ])

    switch (mutation) {
      case 'negate': return input * -1
      case 'zero': return 0
      case 'overflow': return Number.MAX_SAFE_INTEGER
      case 'fraction': return input + 0.1
      case 'increment': return input + this.prng.intInRange(-10, 10)
      default: return input
    }
  }

  private mutateObject(input: any, iterations: number = 3): any {
    if (iterations === 0 || this.prng.headsOrTails()) {
      return input
    }

    const clone = structuredClone(input)
    const keys = Object.keys(clone)

    if (keys.length === 0) {
      clone[this.prng.randomString(5)] = this.prng.randomValue()
      return clone
    }

    const table = PRNG.generateWeightedChoiceTable<Mutation>({
      'deleteKey': 1,
      'addKey': 1,
      'nullifyValue': 1,
      'changeType': 2,
      'mutate': 7,
    })
    const mutation = this.prng.randomElementFrom(table)
    const key = this.prng.randomElementFrom(keys)
    switch (mutation) {
      case "deleteKey":
        delete clone[key]
        break
      case "addKey":
        clone[this.prng.randomString(5)] = this.prng.randomValue()
        break
      case "nullifyValue":
        clone[key] = this.prng.randomElementFrom([null, undefined, ''])
        break
      case "changeType":
        clone[key] = this.randomValueDifferentType(clone[key])
        break
      case "mutate":
        if (typeof clone[key] === 'string') {
          clone[key] = this.mutateString(clone[key])
        }
        if (typeof clone[key] === 'number') {
          clone[key] = this.mutateNumber(clone[key])
        }
        if (typeof clone[key] === 'object' && clone[key] !== null) {
          clone[key] = this.mutateObject(clone[key])
        }
        break;
    }

    return this.mutateObject(clone, iterations - 1)
  }

  private randomValueDifferentType(current: any): any {
    const type = typeof current
    const options = [0, '', null, true, [], {}].filter(v => typeof v !== type)
    return this.prng.randomElementFrom(options)
  }

  private randomEndpointType(): string {
    const endpointTypesValid = [
      'FSPIOP_CALLBACK_URL_TRANSFER_POST',
      'FSPIOP_CALLBACK_URL_TRANSFER_PUT',
      'FSPIOP_CALLBACK_URL_TRANSFER_ERROR',
      'FSPIOP_CALLBACK_URL_FX_QUOTES',
      'FSPIOP_CALLBACK_URL_FX_TRANSFER_POST',
      'FSPIOP_CALLBACK_URL_FX_TRANSFER_PUT',
      'FSPIOP_CALLBACK_URL_FX_TRANSFER_ERROR',
      'FSPIOP_CALLBACK_URL_BULK_TRANSFER_POST',
      'FSPIOP_CALLBACK_URL_BULK_TRANSFER_PUT',
      'FSPIOP_CALLBACK_URL_BULK_TRANSFER_ERROR',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_PUT',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_SUB_ID_PUT',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_PUT_ERROR',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_SUB_ID_PUT_ERROR',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_DELETE',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_SUB_ID_DELETE',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_BATCH_PUT',
      'FSPIOP_CALLBACK_URL_PARTICIPANT_BATCH_PUT_ERROR',
      'FSPIOP_CALLBACK_URL_PARTIES_GET',
      'FSPIOP_CALLBACK_URL_PARTIES_SUB_ID_GET',
      'FSPIOP_CALLBACK_URL_PARTIES_PUT',
      'FSPIOP_CALLBACK_URL_PARTIES_SUB_ID_PUT',
      'FSPIOP_CALLBACK_URL_PARTIES_PUT_ERROR',
      'FSPIOP_CALLBACK_URL_PARTIES_SUB_ID_PUT_ERROR',
      'FSPIOP_CALLBACK_URL_QUOTES',
      'FSPIOP_CALLBACK_URL_BULK_QUOTES',
      'FSPIOP_CALLBACK_URL_AUTHORIZATIONS',
      'FSPIOP_CALLBACK_URL_TRX_REQ_SERVICE',
      'ALARM_NOTIFICATION_URL',
      'ALARM_NOTIFICATION_TOPIC',
      'NET_DEBIT_CAP_THRESHOLD_BREACH_EMAIL',
      'NET_DEBIT_CAP_ADJUSTMENT_EMAIL',
      'SETTLEMENT_TRANSFER_POSITION_CHANGE_EMAIL',
    ]
    let endpoint = this.prng.randomElementFrom(endpointTypesValid)
    if (this.prng.headsOrTails()) {
      return endpoint
    }

    return this.mutateString(endpoint)
  }
}