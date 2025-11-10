import { describe, it } from "node:test";
import { CreateDFSPCommand, CreateDFSPResponse } from "../../domain/ledger-v2/types";
import TestLedger from "../../testing/TestLedger";
import { TestUtils } from "../../testing/testutils";
import { create } from "./handler";

class ApiTestLedger extends TestLedger {
  createDfsp(cmd: CreateDFSPCommand): Promise<CreateDFSPResponse> {
    throw new Error("Method not implemented.");
  }
}

describe('api/participants/handler', () => {
  const ledger = new ApiTestLedger()

  describe('GET  /participants', () => {
    it('Lists information about all participants', async() => {
      // Arrange

      // Act

      // Assert

    })
  })
  describe('POST /participants')
  describe('GET  /participants/limits')
  describe('GET  /participants/{name}')
  describe('PUT  /participants/{name}')
  describe('GET  /participants/{name}/endpoints')
  describe('POST /participants/{name}/endpoints')
  describe('GET  /participants/{name}/limits')
  describe('PUT  /participants/{name}/limits')
  describe('GET  /participants/{name}/positions')
  describe('GET  /participants/{name}/accounts')
  describe('PUT  /participants/{name}/accounts')
  describe('PUT  /participants/{name}/accounts/{id}')
  describe('POST /participants/{name}/accounts/{id}')
  describe('POST /participants/{name}/accounts/{id}/transfers/{id}')
  describe('POST /participants/{name}/initialPositionAndLimits')


})