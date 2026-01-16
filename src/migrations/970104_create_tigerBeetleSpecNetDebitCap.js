exports.up = async function(knex) {
  return knex.schema.createTable('tigerBeetleSpecNetDebitCap', (t) => {
    t.bigIncrements('id').primary().notNullable()
    t.string('dfspId', 256).notNullable()
    t.foreign('dfspId').references('name').inTable('participant')
    t.string('currency', 3).notNullable()
    t.enum('type', ['UNLIMITED', 'LIMITED']).notNullable()
    t.string('amount', 64).nullable()  // Only populated when type='LIMITED'
    t.dateTime('createdDate').defaultTo(knex.fn.now()).notNullable()
    t.dateTime('updatedDate').defaultTo(knex.fn.now()).notNullable()

    // One net debit cap policy per dfsp/currency pair
    t.unique(['dfspId', 'currency'])
    t.index(['dfspId', 'currency'])
    t.index('dfspId')
  })
}

exports.down = async function(knex) {
  return knex.schema.dropTableIfExists('tigerBeetleSpecNetDebitCap')
}
