const assert = require('assert')

const accountWithIdAndFlags = (id, flags) => {
  return {
    id,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    reserved: 0,
    ledger: 1,
    code: 1,
    flags,
    timestamp: 0n,
  }
}

function fromMojaloopId(mojaloopId) {
  assert(mojaloopId)
  // TODO: assert that this actually is a uuid

  const hex = mojaloopId.replace(/-/g, '');
  return BigInt(`0x${hex}`);
}


module.exports = {
  accountWithIdAndFlags,
  fromMojaloopId
}