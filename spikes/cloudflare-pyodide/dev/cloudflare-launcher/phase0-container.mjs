import {DurableObject} from 'cloudflare:workers';
import {ScopeRegistry} from './capacity.mjs';

export {ScopeRegistry};

export class WorkerContainer extends DurableObject {
  async fetch(request) {
    const container = this.ctx.container;
    if (!container) return new Response('container-not-bound', {status: 503});
    if (request.method === 'DELETE') {
      await this.alarm();
      return Response.json({running: container.running});
    }
    if (!container.images?.fixture) return new Response('image-not-bound', {status: 503});
    if (this.ctx.storage.kv.get('started')) return new Response(null, {status: 409});
    this.ctx.storage.kv.put('started', true);
    await this.ctx.storage.setAlarm(Date.now() + 120_000);
    container.start({
      image: container.images.fixture,
      instance: 'lite',
      entrypoint: ['/bin/sleep', '120'],
      enableInternet: false,
    });
    this.ctx.waitUntil(container.monitor().catch(() => {}));
    await container.setInactivityTimeout(120_000);
    return new Response(null, {status: 202});
  }

  async alarm() {
    if (this.ctx.container.running) await this.ctx.container.destroy();
  }
}

export default {
  async fetch(request, env) {
    if (request.headers.get('Authorization') !== `Bearer ${env.TRIAL_BEARER}`) {
      return new Response(null, {status: 401});
    }
    if (!['PUT', 'DELETE'].includes(request.method)) return new Response(null, {status: 405});
    const workerId = new URL(request.url).pathname.slice(1);
    if (!['first', 'second'].includes(workerId)) return new Response(null, {status: 404});
    if (request.method === 'PUT') {
      const reservation = await env.REGISTRY.getByName('phase0').fetch('https://scope/slot', {
        method: 'PUT', body: JSON.stringify({workerId, ceiling: 1}),
      });
      if (!reservation.ok) return reservation;
    }
    // One physical object and a one-shot start fence bound even a buggy registry.
    return env.CONTAINER.getByName('phase0').fetch(request);
  },
};
