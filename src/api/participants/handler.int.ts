import { after, before, describe, it } from "node:test"
import assert from "node:assert"
import Harness from '../../testing/harness'
import { unwrapResponse, createRequest, sleepSeconds } from "../../testing/util"
import PRNG from "../../testing/prng"
import { logger } from "../../shared/logger"
import * as ApiHelpers from '../../testing/api-helpers'

const harness = Harness.getInstance()
let Handler: any

describe('api/participants/handler', () => {
  before(async () => {
    await harness.up()
    await harness.setupGlobals()

    Handler = require('./handler')
  })

  after(async () => {
    await harness.teardownGlobals()
    await harness.down()
  })

  it('handler fuzz', async () => {
    const seed = 1
    const prng = new PRNG(seed)
    const fuzzer = new HandlerApiFuzzer(harness, Handler, prng, seed)
    await fuzzer.run()

    console.log(`trace is:\n\n${fuzzer.traceOutput}`)

  })
})

type ActionName = 'getAll' | 'createDfsp' | 'createHub'

class HandlerApiFuzzer {
  

  private step = 1
  private readonly stepsMax = 100
  private responses: Array<{ 
    action: ActionName,
    body: any, 
    code: any 
  }> = []

  private dfspNames: Array<string> = []

  private weights: Record<ActionName, number> = {
    getAll: 0,
    createDfsp: 0,
    createHub: 10
  }

  constructor(
    private harness: Harness,
    private handler: any,
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
    return this.responses
      .map(response => `${response.action}:\t${response.code}\t${JSON.stringify(response.body)}`)
      .join('\n')
  }

  private async doStep() {
    return this.randomAction()()
  }

  private actions: Record<ActionName, () => Promise<void>> = {
    getAll: () => this.getAll(),
    createDfsp: () => this.createDfsp(),
    createHub: () => this.createHub(),
  }

  private randomAction(): () => Promise<void> {
    const table = PRNG.generateWeightedChoiceTable<ActionName>(this.weights)
    const action = this.prng.randomElementFrom(table)
    return this.actions[action]
  }

  // API Methods under test.
  public async getAll() {
    const {
      responseBody,
      responseCode
    } = await unwrapResponse((reply: any) => Handler.getAll(
      // @ts-ignore
      createRequest({}), reply
    ))

    this.responses.push({ 
      action: 'getAll',
      body: responseBody, 
      code: responseCode 
    })
  }

  // Helper methods to make things interesting.
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
        body: `success currency: ${currency}`,
        code: 1
      })

      // Once we've created the hub, make it less likely to create again.
      this.weights.createHub = 1
      this.weights.createDfsp = 5
    } catch (err) {
      this.responses.push({
        action: 'createHub',
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
        body: `createDfsp() success: dfsp: ${name} currency: ${currency}`,
        code: 1
      })

      this.weights.createDfsp = 2
      this.weights.getAll = 10
    } catch (err) {
      this.responses.push({
        action: 'createDfsp',
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

    const name = `dfsp_${this.prng.randomBytes(10).toString()}`
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
    return input + this.prng.randomBytes(1).toString()
  }
}