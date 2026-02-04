export const prepares_the_settlement = {
  "changedDate": ":ignore",
  "createdDate": ":ignore",
  "id": 1,
  "participants": [
    {
      "accounts": [
        {
          "currency": "BBB",
          "id": 3,
          "netSettlementAmount": {
            "amount": "15",
            "currency": "BBB"
          },
          "owed": "45.0000",
          "owing": "60.0000",
          "reason": "",
          "state": "PENDING"
        }
      ],
      "id": 2
    },
    {
      "accounts": [
        {
          "currency": "BBB",
          "id": 2,
          "netSettlementAmount": {
            "amount": "15",
            "currency": "BBB"
          },
          "owed": "60.0000",
          "owing": "75.0000",
          "reason": "",
          "state": "PENDING"
        }
      ],
      "id": 3
    },
    {
      "accounts": [
        {
          "currency": "BBB",
          "id": 1,
          "netSettlementAmount": {
            "amount": "-30",
            "currency": "BBB"
          },
          "owed": "85.0000",
          "owing": "55.0000",
          "reason": "",
          "state": "PENDING"
        }
      ],
      "id": 4
    }
  ],
  "reason": "settlement prepare test",
  "settlementModel": "DEFERRED_MULTILATERAL_NET_BBB",
  "settlementWindows": [
    {
      "changedDate": ":ignore",
      "content": [
        {
          "changedDate": ":ignore",
          "createdDate": ":ignore",
          "currencyId": "BBB",
          "id": 3,
          "ledgerAccountType": "POSITION",
          "state": "PENDING"
        },
        {
          "changedDate": ":ignore",
          "createdDate": ":ignore",
          "currencyId": "BBB",
          "id": 2,
          "ledgerAccountType": "POSITION",
          "state": "PENDING"
        },
        {
          "changedDate": ":ignore",
          "createdDate": ":ignore",
          "currencyId": "BBB",
          "id": 1,
          "ledgerAccountType": "POSITION",
          "state": "PENDING"
        }
      ],
      "createdDate": ":ignore",
      "id": 1,
      "reason": "test close",
      "state": "CLOSED"
    }
  ],
  "state": "PENDING_SETTLEMENT"
}  