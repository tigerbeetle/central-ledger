import { CommandResultFailure, CommandResultSuccess, QueryResult, QueryResultFailure, QueryResultSuccess } from "./types";

export default class Helper {

  public static commandResultSuccess<T>(result: T): CommandResultSuccess<T> {
    return {
      type: 'SUCCESS',
      result,
    }
  }

  public static emptyCommandResultSuccess(): CommandResultSuccess<void> {
    return {
      type: 'SUCCESS',
      result: undefined
    }
  }

  public static commandResultFailure(error: any): CommandResultFailure {
    return {
      type: 'FAILURE',
      error: error
    }
  }

  public static queryResultSuccess<T>(result: T): QueryResultSuccess<T> {
    return {
      type: 'SUCCESS',
      result,
    }
  }

  public static emptyQueryResultSuccess(): QueryResultSuccess<void> {
    return {
      type: 'SUCCESS',
      result: undefined
    }
  }

  public static queryResultFailure(error: any): QueryResultFailure {
    return {
      type: 'FAILURE',
      error: error
    }
  }
}