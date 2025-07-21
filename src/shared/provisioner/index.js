"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Logger = require('../logger').logger;
/**
 * Provision the ledger based on the config on startup
 *
 * Steps:
 * 1. Check if the ledger has been provisioned previously, if not, then provision the switch
 *    based on the config and exit
 * 2. If the ledger has been provisioned, figure out the delta between what's already been
 *    provisioned, and the current settings
 * 3. If the delta is irreconcilable (e.g. change of a settlement model from one type to another), then
 *    error and crash
 * 4. If the delta is reconcilable (e.g. adding a new currency), then apply the change and continue
 * 5. If there is no delta, then continue
 */
class Provisioner {
    constructor(config) {
        this.config = config;
        this.participantsHandler = require('../../api/participants/handler');
        this.participantService = require('../../domain/participant');
        this.settlementModelDomain = require('../../domain/settlement');
    }
    async run() {
        if (await this.isFreshLedger()) {
            await this.provisionFromScratch();
            return;
        }
        const delta = await this.findProvisioningDelta();
        if (!delta.reconcilable) {
            throw new Error(`Provisioner.run() - encountered fatal error. Please check provisioning
        config and rerun.`);
        }
        // TODO: apply delta
    }
    async isFreshLedger() {
        const hubParticipant = await this.participantService.getByName('Hub');
        // TODO(LD): not sure how kosher it is to imply whether or not the hub has been configured based
        // on the currency list
        if (hubParticipant.currencyList.length === 0) {
            return true;
        }
        return false;
    }
    async findProvisioningDelta() {
        // TODO(LD): 
        // lookup the difference between what's in the Ledger and what's in the config
        // irreconcilable:
        //   - removing a currency
        //   - changing the settlement model of an existing currency
        //
        // reconcilable:
        //   - adding a new currency
        // 
        return { reconcilable: true };
    }
    async provisionFromScratch() {
        // TODO(LD): in the future we should use the new Ledger interface, but for now, and to 
        // make sure we don't get blocked, we send a mock http request to the handlers
        Logger.info('Provisioner.provisionFromScratch()');
        // Create hub accounts
        for await (const currency of this.config.currencies) {
            Logger.debug(`Provisioner.provisionFromScratch() - creating accounts for currency: ${currency}`);
            const requestMultilateralSettlement = {
                params: {
                    name: 'Hub'
                },
                payload: {
                    type: 'HUB_MULTILATERAL_SETTLEMENT',
                    currency,
                }
            };
            // dummy to suit the `h` object the handlers expect
            const mockCallback = {
                response: (body) => {
                    return {
                        code: (code) => { }
                    };
                }
            };
            await this.participantsHandler.createHubAccount(requestMultilateralSettlement, mockCallback);
            const requestHubReconcilation = {
                params: {
                    name: 'Hub'
                },
                payload: {
                    type: 'HUB_RECONCILIATION',
                    currency,
                }
            };
            await this.participantsHandler.createHubAccount(requestHubReconcilation, mockCallback);
            // settlement models per currency
            // TODO(LD): add settlement model config to config file
            const model = {
                name: `DEFERRED_MULTILATERAL_NET_${currency}`,
                settlementGranularity: "NET",
                settlementInterchange: "MULTILATERAL",
                settlementDelay: "DEFERRED",
                currency,
                requireLiquidityCheck: true,
                ledgerAccountType: "POSITION",
                settlementAccountType: "SETTLEMENT",
                autoPositionReset: true
            };
            await this.settlementModelDomain.createSettlementModel(model);
        }
    }
}
exports.default = Provisioner;
