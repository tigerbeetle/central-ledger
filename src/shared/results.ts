/**
 * Convenience Types for QueryResult
 */

export type QueryResultSuccess<T> = {
  type: 'SUCCESS',
  result: T
}

export type QueryResultFailure = {
  type: 'FAILURE',
  error: Error
}

export type QueryResult<T> = QueryResultSuccess<T> | QueryResultFailure

export function failureWithError(error: any): QueryResultFailure {
  return {
    type: 'FAILURE',
    error
  }
}