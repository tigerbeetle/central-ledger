import assert from "node:assert";
import PRNG from "./prng";
import { futureDate } from "./util";

/**
 * @description A mock clock for injecting into the application.
 *   For now, this clock only ticks forwards, but who knows, maybe it will be
 *   able to tick backwas
 */
export default class Clock {
  private _now

  constructor (private prng: PRNG, start: Date) {
    assert(prng instanceof PRNG)
    assert.ok(start instanceof Date)

    this._now = start
  }

  public tick() {
    const unit = this.prng.randomElementWeighted(
      ['ms', 's', 'm', 'h', 'd'],
      [100, 60, 12, 5, 1],
    )
    const amount = this.prng.intExclusive(3) + 1

    this._now = futureDate(amount, unit as Parameters<typeof futureDate>[1], this._now)
  }

  get now(): Date {
    return this._now
  }
}