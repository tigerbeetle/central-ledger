# Settlement Abort Issues in Legacy Ledger

## Overview

The current settlement abort implementation in the legacy ledger has a critical flaw: it allows aborting settlements that are partially completed, resulting in inconsistent state where the database claims a participant is ABORTED but their funds have actually been transferred permanently.

## How Abort Currently Works

When calling `PUT /settlements/{id}` with `state: 'ABORTED'`:

### 1. All Participants Get Marked as ABORTED
**File**: `src/settlement/models/settlement/facade.js:1240-1266`

```javascript
// Queries ALL settlementParticipantCurrency records for the settlement
const settlementAccountList = await knex('settlementParticipantCurrency AS spc')
  .where('spc.settlementId', settlementId)

// Marks EVERY participant currency as ABORTED
for (const sal of settlementAccountList) {
  const spcsc = {
    settlementParticipantCurrencyId: sal.key,
    settlementStateId: enums.settlementStates.ABORTED,
    // ...
  }
}
```

**Key issue**: No filtering by participant state - everyone gets marked ABORTED regardless of their actual progress.

### 2. Settlement Transfers Get Selectively Reversed
**File**: `src/settlement/models/settlement/facade.js:419-561`

```javascript
const settlementTransferList = await knex('settlementParticipantCurrency AS spc')
  .join('settlementParticipantCurrencyStateChange AS spcsc', ...)
  .whereNull('tsc2.transferId')  // Only transfers NOT already ABORTED
  .where('spc.settlementId', settlementId)
```

For each settlement transfer:
- Creates `REJECTED` then `ABORTED` state changes
- **IF the transfer was RESERVED** (line 480):
  - Reverses position changes:
    ```javascript
    .update('value', dfspPositionValue - dfspAmount)  // Reverse DFSP position
    .update('value', hubPositionValue - hubAmount)    // Reverse HUB position
    ```
- **IF the transfer was COMMITTED** (line 453: `whereNull('tsc2.transferId')`):
  - **Does nothing** - the transfer is skipped and positions remain changed

### 3. All Windows Get Marked as ABORTED
**File**: `src/settlement/models/settlement/facade.js:1272-1295`

All settlement windows in the settlement are marked as ABORTED.

## The Problem: Partial Completion

Consider a settlement with partial completion:

| Participant | State | Settlement Transfer State | What Happens on Abort |
|------------|-------|---------------------------|----------------------|
| Participant A | `PS_TRANSFERS_COMMITTED` | `COMMITTED` | ❌ Marked as ABORTED in DB, but **positions NOT reversed** (permanent) |
| Participant B | `PS_TRANSFERS_RESERVED` | `RESERVED` | ✅ Marked as ABORTED in DB, positions reversed |
| Participant C | `PENDING_SETTLEMENT` | No transfer yet | ✅ Marked as ABORTED in DB, no position changes needed |

**Result**: The database state is **inconsistent**. Participant A shows as ABORTED, but their money actually moved and stayed moved.

## Real-World Impact

### 1. Reconciliation Nightmare
- External systems (banks, settlement systems) see state as ABORTED
- Actual ledger has funds transferred for some participants
- Manual reconciliation required to identify which participants actually settled

### 2. Double-Payment Risk
- If the settlement is retried after abort:
  - Participant A might pay twice (once in the "aborted" settlement, once in the retry)
  - Participant B correctly pays once (their first attempt was properly reversed)

### 3. Audit Failure
- The database state (`settlementParticipantCurrency.state = ABORTED`) doesn't match reality
- Audit logs show aborted settlement, but funds actually moved
- Regulatory compliance issues

### 4. Position Mismatch
- DFSP position balances don't match what the settlement state claims
- Could lead to liquidity check failures or incorrect NDC enforcement

## What a Proper Implementation Should Do

### Option 1: Validation (Recommended)
**Don't allow abort if ANY participant is past RESERVED state**

```typescript
async function settlementAbort(settlementId) {
  // Check all participant states
  const participants = await getSettlementParticipants(settlementId)

  for (const p of participants) {
    if (p.state === 'PS_TRANSFERS_COMMITTED' || p.state === 'SETTLED') {
      throw new Error(
        `Cannot abort settlement ${settlementId}: ` +
        `Participant ${p.id} is in state ${p.state} (already committed)`
      )
    }
  }

  // Only proceed if everyone can be safely reversed
  // ...
}
```

### Option 2: Two-Phase Abort
1. **Validation phase**: Check if ALL participants can be aborted
2. **Execution phase**: Only proceed if everyone is in a reversible state
3. Perform entire abort in a single database transaction

### Option 3: Partial Abort with Tracking
- Support aborting only the participants that haven't committed yet
- Keep detailed state tracking:
  - Which participants were successfully aborted
  - Which participants had already committed
- Return detailed status to caller
- **Complexity**: Significantly more complex to implement and reason about

## Current Workaround

The legacy implementation assumes you'll only abort settlements that haven't progressed far. This is enforced only through **operational procedures**, not by the code itself.

**Risk**: A race condition or operator error can still trigger a partial abort.

## Recommendation for TigerBeetle Ledger

When implementing settlement in TigerBeetle:

1. ✅ **Enforce abort validation**: Return error if any participant is past RESERVED
2. ✅ **Atomic operations**: All settlement operations in a single transaction
3. ✅ **Clear state transitions**: Define valid state transition rules and enforce them
4. ✅ **Idempotency**: Safe to retry abort operations without side effects
5. ✅ **Audit trail**: Track exactly which participants were in which states during abort

---

## References

- Handler: `src/settlement/api/handlers/settlements/{id}.ts:151`
- Domain: `src/settlement/domain/settlement/index.js:129`
- Model (abort logic): `src/settlement/models/settlement/facade.js:1212`
- Transfer reversal: `src/settlement/models/settlement/facade.js:419`
