import { Knex } from 'knex';
import { CommandResultSuccess, CommandResultFailure, QueryResultSuccess, QueryResultFailure } from './types';
import assert from 'node:assert';


export default class Helper {

  constructor(private knex: Knex) {

  }
 
  public static commandResultSuccess<T>(result: T): CommandResultSuccess<T> {
    return {
      type: 'SUCCESS',
      result,
    } as CommandResultSuccess<T>;
  }

  public static emptyCommandResultSuccess(): CommandResultSuccess<void> {
    return {
      type: 'SUCCESS'
    };
  }

  public static commandResultFailure(error: any): CommandResultFailure {
    return {
      type: 'FAILURE',
      error: error
    };
  }

  public static queryResultSuccess<T>(result: T): QueryResultSuccess<T> {
    return {
      type: 'SUCCESS',
      result,
    } as QueryResultSuccess<T>;
  }

  public static queryResultFailure(error: any): QueryResultFailure {
    return {
      type: 'FAILURE',
      error: error
    };
  }

  public async validateCurrency(currency: string): Promise<void> {
    assert(currency)
    const result = await this.knex('currency').where('currencyId', currency).first()
    if (!result) {
      throw new Error(`Currency: ${currency} not defined.`)
    }
  }
}
