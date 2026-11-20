import {WorkerContainer as CapacityContainer} from './capacity.mjs';

export {default, ScopeRegistry, WorkerLedger} from './capacity.mjs';

export class WorkerContainer extends CapacityContainer {
    async holdOpen() {
        await this.container.setInactivityTimeout(120_000);
    }

    // Keep the trial DO eligible for eviction so only the platform timer
    // governs the idle window. Observe the instance from the account API.
    observeExit() {}
}
