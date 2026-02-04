'use strict'

exports.plugin = {
  name: 'api routes',
  register: function (server) {
    server.route(require('./transactions/routes.js'))
    // We can eventually remove this, since the GET /settlementModels was used by the separate 
    // settlement service.
    server.route(require('./settlementModels/routes.js'))
    server.route(require('./root/routes.js'))
    server.route(require('./participants/routes.js'))
    server.route(require('./ledgerAccountTypes/routes.js'))
  }
}
