// TODO(LD): I think we can remove this file altogether
// these cases are all handled now in the integration test
// although we might want to use these tests to verify that the ledgers return the correct responses

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


  describe('create', () => {
    it('creates a new DFSP', async () => {
      // Arrange
      ledger.createDfsp = async (cmd: CreateDFSPCommand): Promise<CreateDFSPResponse> => {
        return {
          type: 'SUCCESS'
        }
      }

      const request = {
        payload: {
          currency: 'XXX',
          name: 'dfsp_a'
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const {
        code, body
      } = await TestUtils.unwrapHapiResponse(reply => create(request, reply))


      // Assert
    })
    it.todo('does nothing when the DFSP already exists')
    it.todo('returns a http error when the ledger fails to create the DFSP')
  })
})