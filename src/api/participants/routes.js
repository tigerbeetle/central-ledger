/*****
 License
 --------------
 Copyright © 2020-2024 Mojaloop Foundation
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

 * Georgi Georgiev <georgi.georgiev@modusbox.com>
 --------------
 ******/

'use strict'

const Joi = require('joi')
const currencyList = require('../../seeds/currency.js').currencyList

const tags = ['api', 'participants']
const nameValidator = Joi.string().min(2).max(30).required().description('Name of the participant')
const currencyValidator = Joi.string().valid(...currencyList).description('Currency code')

const ParticipantAPIHandlerV2 = require('./HandlerV2').default
const handler = new ParticipantAPIHandlerV2()

module.exports = [
  {
    method: 'GET',
    path: '/participants',
    handler: (request, h) => handler.getAll(request, h),
    options: {
      tags
    }
  },
  {
    method: 'GET',
    path: '/participants/{name}',
    handler: (request, h) => handler.getByName(request, h),
    options: {
      tags,
      validate: {
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'POST',
    path: '/participants',
    handler: (request, h) => handler.create(request, h),
    options: {
      tags,
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.object({
          name: nameValidator,
          // password: passwordValidator,
          currency: currencyValidator,
          isProxy: Joi.boolean().falsy(0, '0', '').truthy(1, '1').allow(true, false, 0, 1, '0', '1', null)
          // emailAddress: Joi.string().email().required()
        })
      }
    }
  },
  {
    method: 'PUT',
    path: '/participants/{name}',
    handler: (request, h) => handler.update(request, h),
    options: {
      tags,
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.object({
          isActive: Joi.boolean().required().description('Participant isActive boolean')
        }),
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'POST',
    path: '/participants/{name}/endpoints',
    handler: (request, h) => handler.addEndpoint(request, h),
    options: {
      id: 'participants_endpoints_add',
      tags,
      description: 'Add/Update participant endpoints (single or array)',
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.alternatives().try(
          // Single endpoint
          Joi.object({
            type: Joi.string().required().description('Endpoint Type'),
            value: Joi.string().required().description('Endpoint Value')
          }),
          // Array of endpoints
          Joi.array().items(
            Joi.object({
              type: Joi.string().required().description('Endpoint Type'),
              value: Joi.string().required().description('Endpoint Value')
            })
          ).min(1)
        ),
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'GET',
    path: '/participants/{name}/endpoints',
    handler: (request, h) => handler.getEndpoint(request, h),
    options: {
      id: 'participants_endpoints_get',
      tags,
      description: 'View participant endpoints',
      validate: {
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'POST',
    path: '/participants/{name}/initialPositionAndLimits',
    handler: (request, h) => handler.addLimitAndInitialPosition(request, h),
    options: {
      id: 'participants_limits_pos_add',
      tags,
      description: 'Add initial participant limits and position',
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.object({
          currency: currencyValidator,
          limit: Joi.object().keys({
            type: Joi.string().required().description('Limit Type'),
            value: Joi.number().positive().allow(0).required().description('Limit Value')
          }).required().description('Participant Limit'),
          initialPosition: Joi.number().optional().description('Initial Position Value')
        }),
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'GET',
    path: '/participants/{name}/limits',
    handler: (request, h) => handler.getLimits(request, h),
    options: {
      id: 'participants_limits_get',
      tags,
      description: 'View participant limits',
      validate: {
        params: Joi.object({
          name: nameValidator
        }),
        query: Joi.object({
          currency: currencyValidator,
          type: Joi.string().optional().description('Limit Type')
        })
      }
    }
  },
  {
    method: 'GET',
    path: '/participants/limits',
    handler: (request, h) => handler.getLimitsForAllParticipants(request, h),
    options: {
      id: 'participants_limits_get_all',
      tags,
      description: 'View limits for all participants',
      validate: {
        query: Joi.object({
          currency: currencyValidator,
          type: Joi.string().optional().description('Limit Type')
        })
      }
    }
  },
  {
    method: 'PUT',
    path: '/participants/{name}/limits',
    handler: (request, h) => handler.adjustLimits(request, h),
    options: {
      id: 'participants_limits_adjust',
      tags,
      description: 'Adjust participant limits',
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.object({
          currency: currencyValidator,
          limit: Joi.object().keys({
            type: Joi.string().required().description('Limit Type'),
            value: Joi.number().required().description('Limit Value'),
            alarmPercentage: Joi.number().required().description('limit threshold alarm percentage value')
          }).required().description('Participant Limit')
        }),
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'POST',
    path: '/participants/{name}/accounts',
    handler: (request, h) => handler.createHubAccount(request, h),
    options: {
      id: 'hub_accounts_create',
      tags,
      description: 'Create hub accounts',
      payload: {
        allow: ['application/json'],
        failAction: 'error'
      },
      validate: {
        payload: Joi.object({
          currency: currencyValidator,
          type: Joi.string().required().description('Account type') // Needs a validator here
        }),
        params: Joi.object({
          name: nameValidator // nameValidator
        })
      }
    }
  },
  {
    method: 'GET',
    path: '/participants/{name}/positions',
    handler: (request, h) => handler.getPositions(request, h),
    options: {
      id: 'participants_positions_get',
      tags,
      description: 'View participant positions',
      validate: {
        params: Joi.object({
          name: nameValidator
        }),
        query: Joi.object({
          currency: currencyValidator
        })
      }
    }
  },
  {
    method: 'GET',
    path: '/participants/{name}/accounts',
    handler: (request, h) => handler.getAccounts(request, h),
    options: {
      id: 'participants_accounts_get',
      tags,
      description: 'View participant accounts and balances',
      validate: {
        params: Joi.object({
          name: nameValidator
        })
      }
    }
  },
  {
    method: 'PUT',
    path: '/participants/{name}/accounts/{id}',
    handler: (request, h) => handler.updateAccount(request, h),
    options: {
      id: 'participants_accounts_update',
      tags,
      description: 'Update participant accounts',
      validate: {
        payload: Joi.object({
          isActive: Joi.boolean().required().description('Participant currency isActive boolean')
        }),
        params: Joi.object({
          name: nameValidator,
          id: Joi.number().integer().positive()
        })
      }
    }
  },
  {
    method: 'POST',
    path: '/participants/{name}/accounts/{id}',
    handler: (request, h) => handler.recordFunds(request, h),
    options: {
      id: 'post_participants_accounts_funds',
      tags,
      description: 'Record Funds In or Out of participant account',
      validate: {
        payload: Joi.object({
          // Some tests still use uuid, so we need to support both uuid and ulid here for now
          transferId: Joi.string().pattern(/^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-7][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$|^[0-9A-HJKMNP-TV-Z]{26})$/).required(),
          externalReference: Joi.string().required(),
          action: Joi.string().required().valid('recordFundsIn', 'recordFundsOutPrepareReserve').label('action is missing or not supported'),
          reason: Joi.string().required(),
          amount: Joi.object({
            amount: Joi.number().positive().precision(4).required(),
            currency: currencyValidator
          }).required().label('No amount provided'),
          extensionList: Joi.object({
            extension: Joi.array().items({
              key: Joi.string(),
              value: Joi.string()
            })
          })
        }),
        params: Joi.object({
          name: nameValidator,
          id: Joi.number().integer().positive()
        })
      }
    }
  },
  {
    method: 'PUT',
    path: '/participants/{name}/accounts/{id}/transfers/{transferId}',
    handler: (request, h) => handler.recordFunds(request, h),
    options: {
      id: 'put_participants_accounts_funds',
      tags,
      description: 'Record Funds In or Out of participant account',
      validate: {
        payload: Joi.object({
          action: Joi.string().valid('recordFundsOutCommit', 'recordFundsOutAbort').label('action is missing or not supported'),
          reason: Joi.string().required()
        }),
        params: Joi.object({
          name: nameValidator,
          id: Joi.number().integer().positive(),
          // Some tests still use uuid, so we need to support both uuid and ulid here for now
          transferId: Joi.string().pattern(/^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-7][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$|^[0-9A-HJKMNP-TV-Z]{26})$/).required()
        })
      }
    }
  }
]
