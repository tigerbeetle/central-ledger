import { describe, it } from "node:test"
import path from 'path'
import LoggerMock from "../../testing/logger-mock"
import { logger as loggerGlobal } from "../../shared/logger"
// @ts-ignore  Override the globally exported logger.
loggerGlobal = new LoggerMock()
import assert from "node:assert"
import Harness from '../../testing/harness'
import PRNG from "../../testing/prng"

import * as ApiHelpers from '../../testing/api-helpers'
import Coverage from "../../testing/coverage"
import { envOrDefaultNumber, randomAvailablePort } from "../../testing/util"
import { Server } from "@hapi/hapi"
import { loggerFactory } from "@mojaloop/central-services-logger/src/contextLogger"
import fs from "node:fs"
import { Snapshot } from "../../testing/snapshot"
const logger = loggerFactory()

// We need to patch the date globally before starting the harness.
const prng = new PRNG(envOrDefaultNumber('SEED', 123124))
Harness.patchDateGlobal(prng)
const harness = Harness.getInstance()
let Handler: any
let server: Server

describe('api/participants/handler', () => {
  it.only('is fully deterministic', async () => {
    const stepsMax = 100
    const traceA = await run(stepsMax)
    const traceB = await run(stepsMax)

    const filename = path.basename(__filename)
    assert(filename)
    const pathBase = `.fuzz_output/${filename}`
    fs.mkdirSync(pathBase, { recursive: true });
    const pathA = `${pathBase}/traceA.txt`
    const pathB = `${pathBase}/traceB.txt`

    fs.writeFileSync(pathA, traceA)
    fs.writeFileSync(pathB, traceB)

    console.log(`Fuzz trace written to ${pathBase}.`)
    console.log(`Compare the two files with:\n\tgit diff --no-index ${pathA} ${pathB}`)

    assert.deepStrictEqual(traceA, traceB, `Traces donn't match!`)
    // Snapshot.from(traceA).checkStringUnwrap(traceB)
  })

  const run = async (stepsMax: number): Promise<string> => {
    harness.clock.reset()
    harness.prng.reset()

    try {
      const options: FuzzOptions = {
        stepsMax
      }

      await harness.up()
      await harness.setupGlobals()

      const routes = require('./routes')
      const port = await randomAvailablePort()
      server = new Server({
        port
      })
      server.route(routes)
      await server.start()

      harness.prng.reset()
      const fuzzer = new HandlerApiFuzzer(options, harness, server)
      await fuzzer.run()

      await server.stop()
      return fuzzer.traceOutput
    } catch (err: any) {
      logger.error(err.message)
      logger.error(err.stack)
      throw err
    } finally {
      await harness.teardownGlobals()
      await harness.down()
    }
  }


  it('handler fuzz', async () => {
    try {
      const options: FuzzOptions = {
        stepsMax: envOrDefaultNumber('STEPS_MAX', 5000),
      }

      await harness.up()
      await harness.setupGlobals()

      const routes = require('./routes')
      const port = await randomAvailablePort()
      server = new Server({
        port
      })
      server.route(routes)
      await server.start()

      // TODO: would be cool if we can make this work with nyc, since that's what we're using
      // elsewhere.
      // const coverage = new Coverage([
      //   'src/api/participants/handler.js'
      // ])
      // coverage.start()


      const fuzzer = new HandlerApiFuzzer(options, harness, server)
      await fuzzer.run()
      logger.info(`trace:\n${fuzzer.traceOutput}`)

      await server.stop()
      // coverage.stopAndReport()
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

interface FuzzOptions {
  /**
   * How many steps the fuzzer should take.
   */
  stepsMax: number,
}

class HandlerApiFuzzer {
  private step = 1
  private readonly stepsMax: number
  private responses: Array<{
    action: ActionName,
    input: any,
    body: any,
    code: any,
    prngCalls: number
  }> = []

  private dfspNames: Array<string> = []
  private dfspAccountsPosition: Record<string, Array<number>> = {}
  private dfspAccountsSettlement: Record<string, Array<number>> = {}
  private dfspEndpoints: Record<string, Array<string>> = {}
  private transferIds: Array<string> = []
  private registeredCurrencies: Array<string> = []

  private weights: Record<ActionName, number> = {
    getAll: 0,
    getByName: 0,
    create: 1,
    update: 0,
    addEndpoint: 0,
    getEndpoint: 0,
    addLimitAndInitialPosition: 0,
    getLimits: 0,
    getLimitsForAllParticipants: 0,
    adjustLimits: 0,
    createHubAccount: 10,
    getPositions: 0,
    getAccounts: 1,
    updateAccount: 0,
    recordFundsCreate: 0,
    recordFundsUpdate: 0,
  }

  private _dbCalls = 0
  private _dbOriginal: any

  constructor(
    private options: FuzzOptions,
    private harness: Harness,
    private server: Server,
  ) {
    assert(options.stepsMax)

    this.stepsMax = options.stepsMax
    this.injectDbFaults()
  }

  public async run() {
    logger.warn(`HandlerApiFuzzer.run() running:`)
    logger.warn(`\tSEED = ${this.harness.seed}`)
    logger.warn(`\tSTEPS_MAX = ${this.stepsMax} `)

    try {
      while (this.step <= this.stepsMax) {
        await this.doStep()
        this.harness.clock.tick()

        this.step += 1
      }
    } catch (err: any) {
      logger.error(`HandlerApiFuzzer.run() died on step: ${this.step}.`)
      logger.error(`Error: ${err.message}\nStack: ${err.stack}`)
      logger.error(`HandlerApiFuzzer.run() rerun with SEED=${this.harness.seed}`)
      throw err
    } finally {
      this.resetDbFaults()
    }
  }

  get traceOutput(): string {
    return this.responses
      .map(response => {
        return [
          `${response.action.padEnd(12)}:`,
          `\tinput=${JSON.stringify(response.input)}`,
          `\tbody=${JSON.stringify(response.body, null, 2)}`,
          `\tprngCalls=${response.prngCalls}`,
        ].join('\n')
      })
      .join('\n')
  }

  private async doStep() {
    // this.responses.push({
    //   // @ts-ignore
    //   action: 'doStep',
    //   input: {},
    //   code: "",
    //   body: "",
    //   prngCalls: this.harness.prng.callCount
    // })

    return this.randomAction()()
  }

  /**
   * Override the global DB to sometimes fail.
   */
  private injectDbFaults() {
    const Db = require('../../lib/db')
    this._dbOriginal = Db.from.bind(Db)

    Db.from = (tableName: string) => {
      this._dbCalls += 1
      if (this.harness.prng.intExclusive(250) === 0) {
        throw new Error('Injected DB fault.')
      }
      return this._dbOriginal(tableName)
    }
  }

  private resetDbFaults() {
    const Db = require('../../lib/db')
    Db.from = this._dbOriginal
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
    recordFundsUpdate: () => this.recordFundsUpdate(),
  }

  private randomAction(): () => Promise<void> {
    const table = PRNG.generateWeightedChoiceTable<ActionName>(this.weights)
    const action = this.harness.prng.randomElementFrom(table)
    return this.actions[action]
  }

  private async request(action: ActionName, method: string, path: string, payload?: any) {
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
      body: res.result,
      prngCalls: this.harness.prng.callCount
    })

    if (action === 'getAccounts' && res.statusCode === 200) {
      assert(res)
      const name = path.match(/^\/participants\/(.*)\/accounts/)
      assert(name !== null && name[1], `Could not match dfsp name from path: '${path}'.`)
      const accounts = res.result as Array<{ id: number, ledgerAccountType: string }>
      // Store the dfsp=>[account] mapping to the list of ids.
      this.dfspAccountsPosition[name[1]] = [
        ...accounts
          .filter(account => account.ledgerAccountType === 'POSITION')
          .map(account => account.id)
      ]
      this.dfspAccountsSettlement[name[1]] = [
        ...accounts
          .filter(account => account.ledgerAccountType === 'SETTLEMENT')
          .map(account => account.id),
      ]
    }

    if (action === 'addEndpoint' && res.statusCode === 201) {
      const match = path.match(/^\/participants\/(.*)\/endpoints/)
      assert(match !== null)
      assert(match[1])
      const name = match[1]

      if (name && payload?.type) {
        if (!this.dfspEndpoints[name]) {
          this.dfspEndpoints[name] = []
        }
        this.dfspEndpoints[name].push(payload.type)
      }
    }
  }

  // API Methods under test.
  public async getAll() {
    const query = this.harness.prng.randomElementFrom([
      this.mutateString(`?isProxy=${this.harness.prng.headsOrTails()}`), ''
    ])
    await this.request('getAll', 'GET', '/participants' + query, {})
  }

  public async getByName() {
    const name = this.randomDfspName()
    await this.request('getByName', 'GET', `/participants/${name}`, {})
  }

  public async create() {
    if (this.harness.prng.headsOrTails() && this.registeredCurrencies.length > 0) {
      const name = `dfsp_${this.harness.prng.randomString(this.harness.prng.intInRange(1, 5))}`
      this.dfspNames.push(name)
      const currency = this.harness.prng.randomElementFrom(this.registeredCurrencies)
      try {
        await ApiHelpers.buildDfsp()
          .deps(this.harness)
          .name(name)
          .currency(currency)
          .proxy(this.harness.prng.headsOrTails())
          .build()
          .create()
      } catch (err: any) {
        // Ignoring the create errors here.
      }

      this.weights.createHubAccount = 1
      this.weights.create = 1
    }

    const payload = this.mutateObject({
      name: this.randomDfspName(),
      currency: this.randomCurrency(),
      isProxy: this.harness.prng.headsOrTails()
    })
    await this.request('create', 'POST', `/participants`, payload)
  }

  public async update() {
    const name = this.randomDfspName()
    const payload = this.mutateObject({
      isActive: this.harness.prng.headsOrTails()
    })
    await this.request('update', 'PUT', `/participants/${name}`, payload)
  }

  public async addEndpoint() {
    const name = this.randomDfspName()
    const payload = this.mutateObject({
      type: this.randomEndpointType(),
      value: `http://` + this.harness.prng.randomString()
    })
    await this.request('addEndpoint', 'POST', `/participants/${name}/endpoints`, payload)
  }

  public async getEndpoint() {
    const knownDfsps = Object.keys(this.dfspEndpoints).filter(d => this.dfspEndpoints[d].length > 0)

    if (knownDfsps.length > 0 && this.harness.prng.headsOrTails()) {
      const name = this.harness.prng.randomElementFrom(knownDfsps)
      const endpointType = this.harness.prng.randomElementFrom(this.dfspEndpoints[name])
      await this.request(
        'getEndpoint', 'GET', `/participants/${name}/endpoints?type=${endpointType}`, {}
      )
      return
    }

    const name = this.randomDfspName()
    const query = this.harness.prng.randomElementFrom([
      `?type=${this.randomEndpointType()}`,
      ''
    ])
    await this.request('getEndpoint', 'GET', `/participants/${name}/endpoints${query}`, {})
  }

  public async addLimitAndInitialPosition() {
    const name = this.randomDfspName()
    const currency = this.randomCurrency()

    const payload = {
      currency,
      limit: {
        type: this.harness.prng.randomElementWeighted([
          'NET_DEBIT_CAP', // Valid.
          this.harness.prng.randomString()
        ], [5, 1]),
        value: this.harness.prng.intInRange(0, 1000000)
      },
      initialPosition: this.harness.prng.randomElementFrom([
        this.harness.prng.intInRange(0, 1000000),
      ])
    }
    await this.request(
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
    await this.request('getLimits', 'GET', url + query, {})
  }

  public async getLimitsForAllParticipants() {
    const url = `/participants/limits`
    const query = this.mutateString(`?currency=${this.randomCurrency()}&type=NET_DEBIT_CAP`)
    await this.request('getLimitsForAllParticipants', 'GET', url + query, {})
  }

  public async adjustLimits() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/limits`
    const payload = this.mutateObject({
      currency: this.randomCurrency(),
      limit: {
        type: this.harness.prng.randomElementFrom([
          'NET_DEBIT_CAP', // Valid.
          this.harness.prng.randomString()
        ]),
        value: this.harness.prng.intInRange(0, 1000000),
        alarmPercentage: this.harness.prng.intInRange(-1, 101),
      },
    })
    await this.request('adjustLimits', 'PUT', url, payload)
  }

  public async createHubAccount() {
    // The odds of this all getting created properly are quite low, so let's flip a coin and just
    // set up the hub if true.
    if (this.harness.prng.headsOrTails() && this.registeredCurrencies.length < 2) {
      const currency = this.harness.prng.randomElementFrom(['USD', 'EUR', 'GBP'])
      try {
        await ApiHelpers.buildHub()
          .deps(this.harness)
          .currency(currency)
          .build()
          .create()
      } catch (err: any) {
        assert.equal(err.message, 'Injected DB fault.')
      }
      this.registeredCurrencies.push(currency)

      this.weights.createHubAccount = 1
      this.weights.create = 10
    }

    const name = this.harness.prng.randomElementFrom([
      'Hub',
      this.randomDfspName(),
      this.mutateString('Hub')
    ])
    const url = `/participants/${name}/accounts`
    const payload = this.mutateObject({
      currency: this.harness.prng.randomElementFrom(['USD', 'EUR', 'GBP']),
      type: this.harness.prng.randomElementFrom([
        'POSITION',
        'SETTLEMENT',
        'HUB_RECONCILIATION',
        'HUB_MULTILATERAL_SETTLEMENT',
        'HUB_FEE',
        'POSITION_REMITTANCE',
        'SETTLEMENT_REMITTANCE',
        this.harness.prng.randomString(),
      ])
    })
    await this.request('createHubAccount', 'POST', url, payload)
  }

  public async getPositions() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/positions`
    const query = this.mutateString(`?currency=${this.randomCurrency()}`)
    await this.request('getPositions', 'GET', url + query, {})
  }

  public async getAccounts() {
    const name = this.randomDfspName()
    const url = `/participants/${name}/accounts`
    const query = this.harness.prng.randomElementFrom([
      '', `?currency${this.randomCurrency}`
    ])
    await this.request('getAccounts', 'GET', url + query, {})
  }

  public async updateAccount() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccountPosition(name)
    const url = `/participants/${name}/accounts/${account}`
    const payload = {
      isActive: this.harness.prng.headsOrTails(),
    }
    await this.request('updateAccount', 'PUT', url, payload)
  }

  public async recordFundsCreate() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccountSettlement(name)
    const url = `/participants/${name}/accounts/${account}`
    const payload = this.mutateObject({
      transferId: this.randomTransferId(),
      externalReference: this.harness.prng.randomString(),
      action: this.harness.prng.randomElementFrom([
        'recordFundsIn',
        'recordFundsOutPrepareReserve',
        this.harness.prng.randomString(),
      ]),
      reason: this.harness.prng.randomString(),
      amount: {
        amount: this.harness.prng.intInRange(-1, 10000) / 100,
        currency: this.randomCurrency()
      }
    })
    await this.request('recordFundsCreate', 'POST', url, payload)
  }

  public async recordFundsUpdate() {
    const name = this.randomDfspName()
    const account = this.randomDfspAccountSettlement(name)
    const transferId = this.randomTransferId()
    const url = `/participants/${name}/accounts/${account}/${transferId}`
    const payload = this.mutateObject({
      action: this.harness.prng.randomElementFrom([
        'recordFundsOutCommit',
        'recordFundsOutAbort',
        this.harness.prng.randomString(),
      ]),
      reason: this.harness.prng.randomString(),
    })
    await this.request('recordFundsUpdate', 'PUT', url, payload)
  }

  private randomDfspName(): string {
    if (this.dfspNames.length > 0 && this.harness.prng.intExclusive(100) > 20) {
      // Reuse
      return this.harness.prng.randomElementFrom(this.dfspNames)
    }

    const name = `dfsp_${this.harness.prng.randomString(this.harness.prng.intInRange(1, 5))}`
    return name
  }

  private randomDfspAccountPosition(dfsp: string): number {
    if (this.dfspAccountsPosition[dfsp] &&
      this.dfspAccountsPosition[dfsp].length > 0 &&
      this.harness.prng.headsOrTails()
    ) {
      return this.harness.prng.randomElementFrom(this.dfspAccountsPosition[dfsp])
    }

    return this.harness.prng.intInRange(-1, 10)
  }

  private randomDfspAccountSettlement(dfsp: string): number {
    if (this.dfspAccountsSettlement[dfsp] &&
      this.dfspAccountsSettlement[dfsp].length > 0 &&
      this.harness.prng.headsOrTails()
    ) {
      return this.harness.prng.randomElementFrom(this.dfspAccountsSettlement[dfsp])
    }

    return this.harness.prng.intInRange(-1, 10)
  }

  private randomTransferId(): string {
    if (this.transferIds.length > 0 && this.harness.prng.headsOrTails()) {
      // Reuse
      return this.harness.prng.randomElementFrom(this.transferIds)
    }

    const id = this.mutateString(this.harness.prng.uuidv4())
    this.transferIds.push(id)

    return id
  }

  private randomCurrency(): string {
    const currency = this.harness.prng.randomElementFrom(['USD', 'BGP', 'EUR', 'GBP'])
    return this.harness.prng.randomElementWeighted([currency, this.mutateString(currency)], [8, 2])
  }

  private mutateString(input: string): string {
    if (this.harness.prng.headsOrTails()) {
      // Safe.
      return input
    }

    if (this.harness.prng.headsOrTails() && input.length > 0) {
      return input.substring(0, this.harness.prng.intInRange(0, input.length))
    }

    return input + this.harness.prng.randomString(this.harness.prng.intInRange(1, 5))
  }

  private mutateNumber(input: number): number {
    const mutation = this.harness.prng.randomElementFrom([
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
      case 'increment': return input + this.harness.prng.intInRange(-10, 10)
      default: return input
    }
  }

  private mutateObject(input: any, iterations: number = 3): any {
    if (iterations === 0 || this.harness.prng.headsOrTails()) {
      return input
    }

    const clone = structuredClone(input)
    const keys = Object.keys(clone)

    if (keys.length === 0) {
      clone[this.harness.prng.randomString(5)] = this.harness.prng.randomValue()
      return clone
    }

    const table = PRNG.generateWeightedChoiceTable<Mutation>({
      'deleteKey': 1,
      'addKey': 1,
      'nullifyValue': 1,
      'changeType': 2,
      'mutate': 7,
    })
    const mutation = this.harness.prng.randomElementFrom(table)
    const key = this.harness.prng.randomElementFrom(keys)
    switch (mutation) {
      case "deleteKey":
        delete clone[key]
        break
      case "addKey":
        clone[this.harness.prng.randomString(5)] = this.harness.prng.randomValue()
        break
      case "nullifyValue":
        clone[key] = this.harness.prng.randomElementFrom([null, undefined, ''])
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
    return this.harness.prng.randomElementFrom(options)
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
    let endpoint = this.harness.prng.randomElementFrom(endpointTypesValid)
    if (this.harness.prng.headsOrTails()) {
      return endpoint
    }

    return this.mutateString(endpoint)
  }
}