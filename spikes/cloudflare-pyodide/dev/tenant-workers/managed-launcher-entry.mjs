import {WorkerEntrypoint} from 'cloudflare:workers';
export {CloudflareLauncher, TenantHost, default} from './launcher-entry.mjs';

// WorkerLoader needs a Fetcher carrying the tenant props. This adapter sends
// only this tenant's object capability to the private routing service.
export class TenantApi extends WorkerEntrypoint {
  fetch(request) {
    const {tenantId, generation} = this.ctx.props ?? {};
    if (typeof tenantId !== 'string' || !tenantId) return new Response('unknown tenant', {status: 401});
    const host = this.ctx.exports.TenantHost.getByName(tenantId);
    return this.env.TENANT_GATEWAY.route(request, host, generation);
  }
}
