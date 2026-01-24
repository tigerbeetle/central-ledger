import assert from 'assert';
import { randomUUID } from 'crypto';
import { after, before, describe, it } from 'node:test';
import { Ledger } from '../../../domain/ledger-v2/Ledger';
import { IntegrationHarness } from '../../../testing/harness/harness';
import { checkSnapshotObject, checkSnapshotString, unwrapSnapshot } from '../../../testing/snapshot';
import { TestUtils } from '../../../testing/testutils';

import HealthHandler from './health'

describe('settlement/api/health', () => {
  let harness: IntegrationHarness;
  let ledger: Ledger;

  before(async () => {
    harness = await IntegrationHarness.create({
      hubCurrencies: ['USD', 'KES']
    });

    ledger = harness.getResources().ledger;
  });

  after(async () => {
    await harness.teardown();
  });

  describe('/health', () => {
    it('service is sucesfully connected', async () => {
      // Arrange
      const request = {
        params: { name: 'Hub' },
        payload: {},
        server: {
          app: { ledger }
        }
      }

      // Act
       const {
        code,
        body
        // @ts-ignore
        // TODO(LD): there is some typescript issue with the central services health library
      } = await TestUtils.unwrapHapiResponse(h => HealthHandler.get(request, h))

      // Assert
      assert.strictEqual(code, 200)
      assert(body)
      assert.strictEqual(body.status, 'OK')
    })
  })
})