'use strict'

exports.plugin = {
  name: 'api routes',
  register: function (server) {
    server.route(require('./transactions/routes.js'))
    server.route(require('./settlementModels/routes.js'))
    server.route(require('./root/routes.js'))
    server.route(require('./participants/routes.js'))
    server.route(require('./ledgerAccountTypes/routes.js'))

    // Settlement API uses the swagger plugin to define its routes I think, so we need to
    // adapt it somehow
    // const settlementApi = require('../settlement/api/routes.js')
    // console.log('settlementApi', settlementApi)

    // await server.register(settlementApi)
  }
}
