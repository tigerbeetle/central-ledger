export interface Harness {
  start(): Promise<any>;
  teardown(): Promise<void>;
}