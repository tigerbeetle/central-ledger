/*****
 License
 --------------
 Copyright © 2020-2025 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * TigerBeetle
 - Lewis Daly <lewis@tigerbeetle.com>
 --------------
 **********/

exports.up = async (knex) => {
  return knex.schema.hasTable('tigerBeetleSpecTransfer').then(function (exists) {
    if (!exists) {
      return knex.schema.createTable('tigerBeetleSpecTransfer', (t) => {
        t.string('id', 36).primary().notNullable()
        t.string('payerId', 256).notNullable()
        t.foreign('payerId').references('name').inTable('participant')
        t.string('payeeId', 256).notNullable()
        t.foreign('payeeId').references('name').inTable('participant')
        t.string('ilpCondition', 256).notNullable()
        t.text('ilpPacket').notNullable()
        t.string('currency', 3).notNullable()
        t.string('fulfilment', 256)
      })
    }
  })
}

exports.down = function (knex) {
  return knex.schema.hasTable('tigerBeetleSpecTransfer').then(function (exists) {
    if (exists) {
      return knex.schema.dropTableIfExists('tigerBeetleSpecTransfer')
    }
  })
}