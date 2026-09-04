import { after, before, describe, it } from "node:test"
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
      // const setup = require('../../shared/setup')
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
  | 'update'
  | 'addEndpoint'
  | 'getEndpoint'
  | 'addLimitAndInitialPosition'
  | 'getLimits'
  | 'getLimitsForAllParticipants'
  | 'adjustLimits'
  | 'getPositions'
  | 'getAccounts'
  | 'updateAccount'
  | 'recordFunds'
  | 'createDfsp'
  | 'createHub'

type Mutation =
  | 'deleteKey'
  | 'addKey'
  | 'nullifyValue'
  | 'changeType'
  | 'mutate'

class HandlerApiFuzzer {
  private step = 1
  private readonly stepsMax = 100
  private responses: Array<{
    action: ActionName,
    input: any,
    body: any,
    code: any
  }> = []

  private dfspNames: Array<string> = []

  private weights: Record<ActionName, number> = {
    getAll: 100,
    getByName: 0,
    createDfsp: 0,
    createHub: 10,
    update: 1,
    addEndpoint: 2,
    getEndpoint: 2,
    addLimitAndInitialPosition: 0,
    getLimits: 0,
    getLimitsForAllParticipants: 0,
    adjustLimits: 0,
    getPositions: 0,
    getAccounts: 0,
    updateAccount: 0,
    recordFunds: 0
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
    createDfsp: () => this.createDfsp(),
    createHub: () => this.createHub(),
    getByName: () => this.getByName(),
    update: () => this.update(),
    addEndpoint: () => this.addEndpoint(),
    getEndpoint: () => this.getEndpoint(),
    addLimitAndInitialPosition: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    getLimits: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    getLimitsForAllParticipants: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    adjustLimits: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    getPositions: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    getAccounts: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    updateAccount: function (): Promise<void> {
      throw new Error("Function not implemented.")
    },
    recordFunds: function (): Promise<void> {
      throw new Error("Function not implemented.")
    }
  }

  private randomAction(): () => Promise<void> {
    const table = PRNG.generateWeightedChoiceTable<ActionName>(this.weights)
    const action = this.prng.randomElementFrom(table)
    return this.actions[action]
  }

  // API Methods under test.
  public async getAll() {
    const method = 'GET'
    const path = '/participants'
    const payload = {}
    const res = await this.server.inject({
      method,
      url: path,
      payload,
      headers: { 'Content-Type': 'application/json' }
    })

    this.responses.push({
      action: `getAll`,
      input: payload,
      code: res.statusCode,
      body: res.result
    })


    // const request = this.mutateObject({
    //   query: {
    //     isProxy: this.prng.headsOrTails()
    //   }
    // })
    // try {
    //   const response = await this.handler.getAll(request)
    //   this.responses.push({
    //     action: 'getAll',
    //     input: request,
    //     body: response,
    //     code: 0
    //   })
    // } catch (err: any) {
    //   this.responses.push({
    //     action: 'getAll',
    //     input: request,
    //     body: err.message,
    //     code: 0
    //   })
    // }
  }

  public async getByName() {
    const request = this.mutateObject({
      params: {
        name: this.randomDfspName()
      }
    })
    try {
      const response = await this.handler.getByName(request)
      this.responses.push({
        action: 'getByName',
        input: request,
        body: response,
        code: 0
      })
    } catch (err: any) {
      this.responses.push({
        action: 'getByName',
        input: request,
        body: err.message,
        code: 0
      })
    }
  }

  public async addEndpoint() {
    // TODO: need to wrap the hapi request
    const request = {
      params: {
        name: this.randomDfspName()
      },
      payload: {}
    }
    try {
      const response = await this.handler.addEndpoint(request)
      this.responses.push({
        action: 'addEndpoint',
        input: request,
        body: response,
        code: 0
      })
    } catch (err: any) {
      this.responses.push({
        action: 'addEndpoint',
        input: request,
        body: err.message,
        code: 0
      })
    }
  }

  public async getEndpoint() {
    // TODO: need to wrap the hapi request
    const request = {
      params: {
        name: this.randomDfspName()
      },
      query: {
        type: this.randomEndpointType()
      }
    }
    try {
      const response = await this.handler.getEndpoint(request)
      this.responses.push({
        action: 'getEndpoint',
        input: request,
        body: response,
        code: 0
      })
    } catch (err: any) {
      this.responses.push({
        action: 'getEndpoint',
        input: request,
        body: err.message,
        code: 0
      })
    }
  }

  public async update() {
    // TODO: mutate this.
    const request = {
      params: {
        name: this.randomDfspName()
      },
      payload: {}
    }
    try {
      const response = await this.handler.update(request)
      this.responses.push({
        action: 'update',
        input: request,
        body: response,
        code: 0
      })
    } catch (err: any) {
      this.responses.push({
        action: 'update',
        input: request,
        body: err.message,
        code: 0
      })
    }
  }

  // Helper methods to make things interesting.
  // TODO: we should just replace this with the api itself!
  private async createHub() {
    const currency = this.randomCurrency()
    try {
      await ApiHelpers.buildHub()
        .deps(this.harness)
        .currency(currency)
        .build()
        .create()

      this.responses.push({
        action: 'createHub',
        input: '',
        body: `success currency: ${currency}`,
        code: 1
      })

      // Once we've created the hub, make it less likely to create again.
      this.weights.createHub = 1
      this.weights.createDfsp = 5
    } catch (err) {
      this.responses.push({
        action: 'createHub',
        input: '',
        body: `fail currency: ${currency}`,
        code: 1
      })
    }
  }

  private async createDfsp() {
    const name = this.randomDfspName()
    const currency = this.randomCurrency()
    try {
      await ApiHelpers.buildDfsp()
        .deps(this.harness)
        .currency(currency)
        .name(name)
        .build()
        .create()

      this.responses.push({
        action: 'createDfsp',
        input: '',
        body: `createDfsp() success: dfsp: ${name} currency: ${currency}`,
        code: 1
      })

      this.weights.createDfsp = 2
      this.weights.getAll = 10
      this.weights.getByName = 10
      this.weights.update = 5
      this.weights.addEndpoint = 2
      this.weights.getEndpoint = 5
    } catch (err) {
      this.responses.push({
        action: 'createDfsp',
        input: '',
        body: `createDfsp() fail: dfsp: ${name} currency: ${currency}`,
        code: 1
      })
    }
  }

  private randomDfspName(): string {
    if (this.dfspNames.length > 0 && this.prng.headsOrTails()) {
      // Reuse
      this.prng.randomElementFrom(this.dfspNames)
    }

    const name = `dfsp_${this.prng.randomString(this.prng.intInRange(1, 5))}`
    this.dfspNames.push(name)

    return name
  }

  private randomCurrency(): string {
    const currency = this.prng.randomElementFrom(['USD', 'BGP', 'EUR', 'GBP'])
    return this.mutateString(currency)
  }

  private mutateString(input: string): string {
    if (this.prng.headsOrTails()) {
      // Safe.
      return input
    }

    if (this.prng.headsOrTails()) {
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