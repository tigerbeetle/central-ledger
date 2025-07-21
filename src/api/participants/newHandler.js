"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeHandlers = makeHandlers;
// I'm not sure if this is the pattern we want to go with, but it's an idea
function makeHandlers(config) {
    // 1. get a list of all accounts from metdatadb
    // 2. lookup balances from TigerBeetle
    // 3. filter out internal accounts
    const getAll = async () => {
        throw new Error('Not Implemented');
    };
    const getByName = async () => {
        throw new Error('Not Implemented');
    };
    // 1. dfsp+currency generate ids
    // 2. save to metadata db - if this fails, then this account is already created, fail
    // 3. open accounts in tigerbeetle
    const create = async () => {
        throw new Error('Not Implemented');
    };
    const update = async () => {
        throw new Error('Not Implemented');
    };
    // TODO: pass through to old implementation
    const addEndpoint = async () => {
        throw new Error('Not Implemented');
    };
    // TODO: pass through to old implementation
    const getEndpoint = async () => {
        throw new Error('Not Implemented');
    };
    // I don't think we want to support this endpoint
    const addLimitAndInitialPosition = async () => {
        throw new Error('Not Implemented');
    };
    const getLimits = async () => {
        throw new Error('Not Implemented');
    };
    const getLimitsForAllParticipants = async () => {
        throw new Error('Not Implemented');
    };
    const adjustLimits = async () => {
        throw new Error('Not Implemented');
    };
    const createHubAccount = async () => {
        throw new Error('Not Implemented');
    };
    const getPositions = async () => {
        throw new Error('Not Implemented');
    };
    const getAccounts = async () => {
        throw new Error('Not Implemented');
    };
    const updateAccount = async () => {
        throw new Error('Not Implemented');
    };
    const recordFunds = async () => {
        throw new Error('Not Implemented');
    };
    return {
        addEndpoint,
        create,
        getAll,
        getByName,
        update,
        getEndpoint,
        addLimitAndInitialPosition,
        getLimits,
        getLimitsForAllParticipants,
        adjustLimits,
        createHubAccount,
        getPositions,
        getAccounts,
        updateAccount,
        recordFunds
    };
}
