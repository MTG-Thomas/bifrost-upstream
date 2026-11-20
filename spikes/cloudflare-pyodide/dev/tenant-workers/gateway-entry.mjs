import {WorkerEntrypoint} from 'cloudflare:workers';
import {routeTenantRequest} from './tenant-gateway-service.mjs';

export default class TenantGateway extends WorkerEntrypoint {
  route(request, host, generation) {
    return routeTenantRequest(request, host, generation, this.env);
  }

  fetch() {
    return new Response(null, {status: 404});
  }
}
