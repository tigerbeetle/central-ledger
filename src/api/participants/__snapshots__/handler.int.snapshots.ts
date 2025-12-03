/**
 * I'm playing around with this format of snapshot file, or something similar
 * The idea is that the snapshots dont' end up taking up 90% of the test file
 * But at the same time, are easy to reference and fix
 */

export const returnsHubInformation = [
  {
    name: 'Hub',
    id: 'http://central-ledger/participants/Hub',
    "created:ignore": true,
    isActive: 1,
    links: { self: 'http://central-ledger/participants/Hub' },
    accounts: [
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '1',
        isActive: 1,
        ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '2',
        isActive: 1,
        ledgerAccountType: "HUB_RECONCILIATION"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "KES",
        id: '3',
        isActive: 1,
        ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "KES",
        id: '4',
        isActive: 1,
        ledgerAccountType: "HUB_RECONCILIATION"
      }
    ],
    isProxy: 0
  }
]

export const createsNewDfspThenCallsGetAll = [
  {
    accounts: [
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '5',
        isActive: 0,
        ledgerAccountType: "POSITION"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '6',
        isActive: 0,
        ledgerAccountType: "SETTLEMENT"
      }
    ],
    "created:ignore": true,
    id: "http://central-ledger/participants/dfsp_d",
    isActive: 1,
    isProxy: 0,
    links: {
      self: "http://central-ledger/participants/dfsp_d"
    },
    name: "dfsp_d"
  },
  {
    name: 'Hub',
    id: 'http://central-ledger/participants/Hub',
    "created:ignore": true,
    isActive: 1,
    links: { self: 'http://central-ledger/participants/Hub' },
    accounts: [
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '1',
        isActive: 1,
        ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "USD",
        id: '2',
        isActive: 1,
        ledgerAccountType: "HUB_RECONCILIATION"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "KES",
        id: '3',
        isActive: 1,
        ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
      },
      {
        createdBy: "unknown",
        createdDate: null,
        currency: "KES",
        id: '4',
        isActive: 1,
        ledgerAccountType: "HUB_RECONCILIATION"
      }
    ],
    isProxy: 0
  }
]

export const createsANewDfsp = {
  name: 'dfsp_x',
  id: 'http://central-ledger/participants/dfsp_x',
  created: ':ignore',
  isActive: 1,
  links: { self: 'http://central-ledger/participants/dfsp_x' },
  accounts: [
    {
      id: ':string',
      ledgerAccountType: 'POSITION',
      currency: 'USD',
      isActive: 0,
      "createdDate:ignore": true,
      createdBy: 'unknown'
    },
    {
      id: ':string',
      ledgerAccountType: 'SETTLEMENT',
      currency: 'USD',
      isActive: 0,
      "createdDate:ignore": true,
      createdBy: 'unknown'
    }
  ],
  isProxy: 0
}