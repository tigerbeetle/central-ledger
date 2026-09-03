'use strict'

const Test = require('tapes')(require('tape'))
const Sinon = require('sinon')
const Config = require('../../../src/lib/config')
const Proxyquire = require('proxyquire')
const MongoUriBuilder = require('mongo-uri-builder')

// TODO: I'm thinking of removing this altogether, it's much more effective to move it to an 
// integration test.

Test('setup', setupTest => {
  let sandbox
  let uuidStub
  let oldHostName
  let oldMongoDbUsername
  let oldMongoDbPassword
  let oldMongoDbHost
  let oldMongoDbPort
  let oldMongoDbDatabase
  let oldProxyCacheEnabled
  let mongoDbUri
  const hostName = 'http://test.com'
  let Setup
  let DbStub
  let ProxyCacheStub
  let CacheStub
  let ObjStoreStub
  // let ObjStoreStubThrows
  let MigratorStub
  let RegisterHandlersStub
  let PluginsStub
  let HapiStub
  let allStubs

  // Proxyquire stubs with optional overrides.
  const getProxyquireStubs = (overrides = {}) => ({
    ...allStubs,
    ...overrides
  })

  const createSetup = (overrides = {}) => {
    return Proxyquire('../../../src/shared/setup', getProxyquireStubs(overrides))
  }
  let UrlParserStub
  let serverStub
  // let KafkaCronStub
  let EnumCachedStub
  let ParticipantCachedStub
  let ParticipantCurrencyCachedStub
  let ParticipantLimitCachedStub
  let externalParticipantCachedStub
  let BatchPositionModelCachedStub
  let SettlementModelCachedStub
  let MessageBusStub
  let DispatchTransferHandlerStub
  let PositionHandlerV2Stub
  let LedgerSqlStub
  let TimeoutHandlerV2Stub
  let HandlerV2Stub
  let routesAdminBuilderStub
  let routesSettlementStub
  let createRemittanceEntityStub
  let definePositionParticipantStub

  setupTest.beforeEach(test => {
    sandbox = Sinon.createSandbox()
    PluginsStub = {
      registerPlugins: sandbox.stub().resolves()
    }

    serverStub = {
      connection: sandbox.stub(),
      register: sandbox.stub(),
      method: sandbox.stub(),
      ext: sandbox.stub(),
      start: sandbox.stub(),
      info: {
        uri: sandbox.stub()
      }
    }

    HapiStub = {
      Server: sandbox.stub().returns(serverStub)
    }

    UrlParserStub = {
      idFromTransferUri: sandbox.stub()
    }

    DbStub = {
      connect: sandbox.stub().resolves(),
      disconnect: sandbox.stub().resolves()
    }

    ProxyCacheStub = {
      connect: sandbox.stub().returns(Promise.resolve()),
      disconnect: sandbox.stub().returns(Promise.resolve()),
      getCache: sandbox.stub().returns(
        {
          connect: sandbox.stub().returns(Promise.resolve(true)),
          disconnect: sandbox.stub().returns(Promise.resolve(true))
        }
      )
    }

    CacheStub = {
      initCache: sandbox.stub().resolves()
    }

    ObjStoreStub = {
      Db: {
        connect: sandbox.stub().resolves(),
        Mongoose: {
          set: sandbox.stub()
        }
      }
    }
    // ObjStoreStubThrows = {
    //   Db: {
    //     connect: sandbox.stub().throws(new Error('MongoDB unavailable'))
    //   }
    // }

    uuidStub = sandbox.stub()

    MigratorStub = {
      migrate: sandbox.stub().resolves()
    }

    RegisterHandlersStub = {
      registerAllHandlers: sandbox.stub().resolves(),
      transfers: {
        registerPrepareHandler: sandbox.stub().resolves(),
        registerGetHandler: sandbox.stub().resolves(),
        registerFulfilHandler: sandbox.stub().resolves()
        // registerRejectHandler: sandbox.stub().resolves()
      },
      positions: {
        registerPositionHandler: sandbox.stub().resolves()
      },
      positionsBatch: {
        registerPositionHandler: sandbox.stub().resolves()
      },
      timeouts: {
        registerAllHandlers: sandbox.stub().resolves(),
        registerTimeoutHandler: sandbox.stub().resolves()
      },
      admin: {
        registerAdminHandlers: sandbox.stub().resolves()
      },
      bulk: {
        registerBulkPrepareHandler: sandbox.stub().resolves(),
        registerBulkFulfilHandler: sandbox.stub().resolves(),
        registerBulkProcessingHandler: sandbox.stub().resolves(),
        registerBulkGetHandler: sandbox.stub().resolves()
      },
      deferredSettlement: {
        registerSettlementWindowHandler: sandbox.stub().resolves()
      },
      grossSettlement: {
        registerTransferSettlementHandler: sandbox.stub().resolves()
      },
      rules: {
        registerRulesHandler: sandbox.stub().resolves()
      }
    }

    // Stubs for cached models
    EnumCachedStub = {
      initialize: sandbox.stub().resolves(),
      getEnums: sandbox.stub().resolves({})
    }
    ParticipantCachedStub = {
      initialize: sandbox.stub().resolves()
    }
    ParticipantCurrencyCachedStub = {
      initialize: sandbox.stub().resolves()
    }
    ParticipantLimitCachedStub = {
      initialize: sandbox.stub().resolves()
    }
    externalParticipantCachedStub = {
      initialize: sandbox.stub()
    }
    BatchPositionModelCachedStub = {
      initialize: sandbox.stub().resolves()
    }
    SettlementModelCachedStub = {
      initialize: sandbox.stub().resolves()
    }

    // Stubs for handlers and services
    MessageBusStub = {
      MessageBus: sandbox.stub().returns({
        init: sandbox.stub().resolves()
      })
    }
    DispatchTransferHandlerStub = {
      DispatchTransferHandler: sandbox.stub().returns({})
    }
    PositionHandlerV2Stub = {
      PositionHandlerV2: sandbox.stub().returns({})
    }
    LedgerSqlStub = {
      LedgerSql: sandbox.stub().returns({})
    }
    TimeoutHandlerV2Stub = {
      TimeoutHandlerV2: sandbox.stub().returns({})
    }
    HandlerV2Stub = {
      default: sandbox.stub().returns({})
    }
    routesAdminBuilderStub = sandbox.stub().returns({
      name: 'routesAdmin',
      register: sandbox.stub()
    })
    routesSettlementStub = {
      name: 'routesSettlement',
      register: sandbox.stub()
    }
    createRemittanceEntityStub = {
      createRemittanceEntityPayment: sandbox.stub(),
      createRemittanceEntityForex: sandbox.stub()
    }
    definePositionParticipantStub = {
      definePositionParticipant: sandbox.stub()
    }

    const ConfigStub = Config
    ConfigStub.HANDLERS_API_DISABLED = false
    ConfigStub.HANDLERS_CRON_DISABLED = false
    ConfigStub.MONGODB_DISABLED = false

    // Build the allStubs object used by getProxyquireStubs().
    allStubs = {
      crypto: {
        randomUUID: uuidStub
      },
      '../handlers/register': RegisterHandlersStub,
      '../lib/db': DbStub,
      '../lib/proxyCache': ProxyCacheStub,
      '../lib/cache': CacheStub,
      '@mojaloop/object-store-lib': ObjStoreStub,
      '../lib/migrator': MigratorStub,
      './plugins': PluginsStub,
      '../lib/urlParser': UrlParserStub,
      '@hapi/hapi': HapiStub,
      '../lib/config': ConfigStub,
      '../lib/enumCached': EnumCachedStub,
      '../models/participant/participantCached': ParticipantCachedStub,
      '../models/participant/participantCurrencyCached': ParticipantCurrencyCachedStub,
      '../models/participant/participantLimitCached': ParticipantLimitCachedStub,
      '../models/participant/externalParticipantCached': externalParticipantCachedStub,
      '../models/position/batchCached': BatchPositionModelCachedStub,
      '../models/settlement/settlementModelCached': SettlementModelCachedStub,
      '../messaging/message-bus': MessageBusStub,
      '../handlers/dispatch-transfer-handler': DispatchTransferHandlerStub,
      '../handlers/position-v2': PositionHandlerV2Stub,
      '../domain/ledger/ledger-sql': LedgerSqlStub,
      '../handlers/timeout-v2': TimeoutHandlerV2Stub,
      '../api/participants/handler-v2': HandlerV2Stub,
      '../api/routes-v2': { default: routesAdminBuilderStub },
      '../settlement/api/routes': routesSettlementStub,
      '../handlers/transfers/createRemittanceEntity': createRemittanceEntityStub,
      '../handlers/transfers/prepare': definePositionParticipantStub
    }

    Setup = createSetup()

    oldHostName = Config.HOSTNAME
    oldMongoDbUsername = Config.MONGODB_USER
    oldMongoDbPassword = Config.MONGODB_PASSWORD
    oldMongoDbHost = Config.MONGODB_HOST
    oldMongoDbPort = Config.MONGODB_PORT
    oldMongoDbDatabase = Config.MONGODB_DATABASE
    oldProxyCacheEnabled = Config.PROXY_CACHE_CONFIG.enabled
    Config.HOSTNAME = hostName
    Config.MONGODB_HOST = 'testhost'
    Config.MONGODB_PORT = '1111'
    Config.MONGODB_USER = 'user'
    Config.MONGODB_PASSWORD = 'pass'
    Config.MONGODB_DATABASE = 'mlos'
    Config.PROXY_CACHE_CONFIG.enabled = true
    mongoDbUri = MongoUriBuilder({
      username: Config.MONGODB_USER,
      password: Config.MONGODB_PASSWORD,
      host: Config.MONGODB_HOST,
      port: Config.MONGODB_PORT,
      database: Config.MONGODB_DATABASE
    })

    test.end()
  })

  setupTest.afterEach(test => {
    sandbox.restore()

    Config.HOSTNAME = oldHostName
    Config.MONGODB_HOST = oldMongoDbHost
    Config.MONGODB_PORT = oldMongoDbPort
    Config.MONGODB_USER = oldMongoDbUsername
    Config.MONGODB_PASSWORD = oldMongoDbPassword
    Config.MONGODB_DATABASE = oldMongoDbDatabase
    Config.PROXY_CACHE_CONFIG.enabled = oldProxyCacheEnabled

    test.end()
  })

  setupTest.test('createServer should', async (createServerTest) => {
    createServerTest.test('throw Boom error on fail', async (test) => {
      const errorToThrow = new Error('Throw Boom error')

      const HapiStubThrowError = {
        Server: sandbox.stub().callsFake((opt) => {
          opt.routes.validate.failAction(sandbox.stub(), sandbox.stub(), errorToThrow)
        })
      }

      Setup = createSetup({ '@hapi/hapi': HapiStubThrowError })

      Setup.createServer(200, []).then(() => {
        test.fail('Should not have successfully created server')
        test.end()
      }).catch(err => {
        test.ok(err instanceof Error)
        test.end()
      })
    })
    createServerTest.end()
  })

  setupTest.test('initialize should', async (initializeTest) => {
    initializeTest.test('connect to Database & ObjStore', async (test) => {
      const service = 'api'

      Setup.initialize({ service }).then(s => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.ok(CacheStub.initCache.called)
        test.notOk(MigratorStub.migrate.called)
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('connect to Database, but NOT too ObjStore', async (test) => {
      const ConfigStub = Config
      ConfigStub.MONGODB_DISABLED = true

      const service = 'api'

      Setup = createSetup({ '../lib/config': ConfigStub })

      Setup.initialize({ service }).then(s => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.notOk(ObjStoreStub.Db.connect.called)
        test.notOk(MigratorStub.migrate.called)
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('connect to db and return hapi server for "api"', async (test) => {
      const service = 'api'

      Setup.initialize({ service }).then(s => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.notOk(MigratorStub.migrate.called)
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('connect to db and return hapi server for "admin"', async (test) => {
      const service = 'admin'

      Setup.initialize({ service }).then(s => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.notOk(MigratorStub.migrate.called)
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('connect to db and return hapi server for "handler"', async (test) => {
      const service = 'handler'

      Setup.initialize({ service }).then(s => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.notOk(MigratorStub.migrate.called)
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('throw when the service is "undefined"', async (test) => {
      const service = 'undefined'

      try {
        await Setup.initialize({ service })
        test.fail('Setup.initialize() should have thrown.')
        test.end()
      } catch (err) {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.notOk(MigratorStub.migrate.called)
        test.end()
      }
    })

    initializeTest.test('run migrations if runMigrations flag enabled', async (test) => {
      const service = 'api'

      Setup.initialize({ service, runMigrations: true }).then(() => {
        test.ok(DbStub.connect.calledWith(Config.DATABASE))
        test.ok(ObjStoreStub.Db.connect.calledWith(mongoDbUri))
        test.ok(MigratorStub.migrate.called)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run Handlers if runHandlers flag enabled and start API', async (test) => {
      const service = 'handler'

      Setup.initialize({ service, runHandlers: true }).then((s) => {
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run Handlers if runHandlers flag enabled and cronjobs are enabled and start API and do register cronJobs', async (test) => {
      Setup = createSetup()

      const service = 'handler'

      Setup.initialize({ service, runHandlers: true }).then((s) => {
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run Handlers if runHandlers flag enabled and cronjobs are disabled and start API but dont register cronJobs', async (test) => {
      const ConfigStub = Config
      ConfigStub.HANDLERS_CRON_DISABLED = true

      Setup = createSetup({ '../lib/config': ConfigStub })

      const service = 'handler'

      Setup.initialize({ service, runHandlers: true }).then((s) => {
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run Handlers if runHandlers flag enabled and dont start API', async (test) => {
      const ConfigStub = Config
      ConfigStub.HANDLERS_CRON_DISABLED = false
      ConfigStub.HANDLERS_API_DISABLED = true

      Setup = createSetup({ '../lib/config': ConfigStub })

      const service = 'handler'

      sandbox.stub(Config, 'HANDLERS_API_DISABLED').returns(true)
      Setup.initialize({ service, runHandlers: true }).then((s) => {
        test.equal(s, undefined)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('do not initialize instrumentation if INSTRUMENTATION_METRICS_DISABLED is true', async (test) => {
      const ConfigStub = Config
      ConfigStub.HANDLERS_CRON_DISABLED = false
      ConfigStub.HANDLERS_API_DISABLED = true
      ConfigStub.INSTRUMENTATION_METRICS_DISABLED = true

      Setup = createSetup({ '../lib/config': ConfigStub })

      const service = 'handler'

      sandbox.stub(Config, 'HANDLERS_API_DISABLED').returns(true)
      sandbox.stub(Config, 'INSTRUMENTATION_METRICS_DISABLED').returns(true)
      Setup.initialize({ service, runHandlers: true }).then((s) => {
        test.equal(s, undefined)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run invalid Handler if runHandlers flag enabled with handlers[] populated', async (test) => {
      const service = 'api'

      const fspList = ['dfsp1', 'dfsp2']

      const prepareHandler = {
        type: 'prepare',
        enabled: true,
        fspList
      }

      const positionHandler = {
        type: 'position',
        enabled: true,
        fspList
      }

      const positionBatchHandler = {
        type: 'positionbatch',
        enabled: true,
        fspList
      }

      const fulfilHandler = {
        type: 'fulfil',
        enabled: true
      }

      const timeoutHandler = {
        type: 'timeout',
        enabled: true
      }

      const adminHandler = {
        type: 'admin',
        enabled: true
      }

      const getHandler = {
        type: 'get',
        enabled: true
      }

      const bulkBrepareHandler = {
        type: 'bulkprepare',
        enabled: true
      }

      const bulkFulfilHandler = {
        type: 'bulkfulfil',
        enabled: true
      }

      const bulkProcessingHandler = {
        type: 'bulkprocessing',
        enabled: true
      }

      const bulkGetHandler = {
        type: 'bulkget',
        enabled: true
      }

      const unknownHandler = {
        type: 'undefined',
        enabled: true
      }

      const modulesList = [
        prepareHandler,
        positionHandler,
        positionBatchHandler,
        fulfilHandler,
        timeoutHandler,
        adminHandler,
        getHandler,
        bulkBrepareHandler,
        bulkFulfilHandler,
        bulkProcessingHandler,
        bulkGetHandler,
        unknownHandler
        // rejectHandler
      ]

      Setup.initialize({ service, runHandlers: true, handlers: modulesList }).then((s) => {
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.test('run specific Handlers if runHandlers flag enabled with handlers[] populated with CronJob disabled', async (test) => {
      const ConfigStub = Config
      ConfigStub.HANDLERS_CRON_DISABLED = true
      ConfigStub.HANDLERS_API_DISABLED = false

      Setup = createSetup({ '../lib/config': ConfigStub })

      const service = 'api'

      Setup.initialize({ service, runHandlers: true, handlers: [] }).then((s) => {
        test.equal(s, serverStub)
        test.end()
      }).catch(err => {
        test.fail(`Should have not received an error: ${err}`)
        test.end()
      })
    })

    initializeTest.end()
  })

  setupTest.end()
})
