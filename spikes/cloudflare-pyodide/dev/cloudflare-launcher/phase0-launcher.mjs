import {DurableObject} from 'cloudflare:workers';

export class CloudflareLauncher extends DurableObject {
  async fetch() {
    const response = await this.env.RUNTIME.fetch('https://runtime/');
    const receipt = await response.text();
    this.ctx.storage.kv.put('receipt', receipt);
    return new Response(this.ctx.storage.kv.get('receipt'));
  }
}

export default {fetch: () => new Response(null, {status: 404})};
