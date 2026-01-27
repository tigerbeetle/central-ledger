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

 * Mojaloop Foundation
 - Name Surname <name.surname@mojaloop.io>
 --------------
 ******/

'use strict'

exports.up = async (knex) => {
  // Create tigerbeetle_settlement_window table
  await knex.schema.hasTable('tigerbeetle_settlement_window').then(function (exists) {
    if (!exists) {
      return knex.schema.createTable('tigerbeetle_settlement_window', (t) => {
        t.bigIncrements('id').primary().notNullable()
        t.enum('state', ['OPEN', 'CLOSED', 'SETTLED']).notNullable()
        t.dateTime('opened_at').notNullable()
        t.dateTime('closed_at').nullable()
        t.string('reason', 512).nullable()
        t.dateTime('created_at').notNullable().defaultTo(knex.fn.now())

        t.index('state')
      })
    }
  })

  // Create tigerbeetle_settlement table
  await knex.schema.hasTable('tigerbeetle_settlement').then(function (exists) {
    if (!exists) {
      return knex.schema.createTable('tigerbeetle_settlement', (t) => {
        t.bigIncrements('id').primary().notNullable()
        t.enum('state', ['PENDING', 'PROCESSING', 'COMMITTED', 'ABORTED']).notNullable()
        t.string('model', 128).notNullable()
        t.string('reason', 512).nullable()
        t.dateTime('created_at').notNullable().defaultTo(knex.fn.now())

        t.index('state')
      })
    }
  })

  // Create tigerbeetle_settlement_window_mapping table
  await knex.schema.hasTable('tigerbeetle_settlement_window_mapping').then(function (exists) {
    if (!exists) {
      return knex.schema.createTable('tigerbeetle_settlement_window_mapping', (t) => {
        t.bigInteger('settlement_id').unsigned().notNullable()
        t.bigInteger('window_id').unsigned().notNullable()

        t.primary(['settlement_id', 'window_id'])
        t.foreign('settlement_id').references('id').inTable('tigerbeetle_settlement')
        t.foreign('window_id').references('id').inTable('tigerbeetle_settlement_window')
      })
    }
  })

  // Create tigerbeetle_settlement_balance table
  await knex.schema.hasTable('tigerbeetle_settlement_balance').then(function (exists) {
    if (!exists) {
      return knex.schema.createTable('tigerbeetle_settlement_balance', (t) => {
        t.bigIncrements('id').primary().notNullable()
        t.bigInteger('settlement_id').unsigned().notNullable()
        t.string('participant_id', 128).notNullable()
        t.string('currency', 3).notNullable()
        t.decimal('amount', 18, 4).notNullable()
        t.enum('direction', ['INBOUND', 'OUTBOUND']).notNullable()
        t.enum('state', ['PENDING', 'RESERVED', 'COMMITTED', 'ABORTED']).notNullable()
        t.string('external_reference', 256).nullable()
        t.dateTime('created_at').notNullable().defaultTo(knex.fn.now())
        t.dateTime('updated_at').notNullable().defaultTo(knex.fn.now())

        t.unique(['settlement_id', 'participant_id', 'currency'], 'tb_stl_bal_sid_pid_cur_uniq')
        t.foreign('settlement_id').references('id').inTable('tigerbeetle_settlement')
        t.index(['participant_id', 'currency'], 'tb_stl_bal_pid_cur_idx')
        t.index('state', 'tb_stl_bal_state_idx')
      })
    }
  })
}

exports.down = async (knex) => {
  await knex.schema.dropTableIfExists('tigerbeetle_settlement_balance')
  await knex.schema.dropTableIfExists('tigerbeetle_settlement_window_mapping')
  await knex.schema.dropTableIfExists('tigerbeetle_settlement')
  await knex.schema.dropTableIfExists('tigerbeetle_settlement_window')
}
