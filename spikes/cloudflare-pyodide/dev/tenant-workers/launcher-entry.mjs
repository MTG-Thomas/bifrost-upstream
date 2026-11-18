export {CloudflareLauncher} from '../cloudflare-launcher/launcher-do.mjs';
export {TenantApi, TenantHost} from './tenant-api.mjs';

export default {
  async scheduled(_event, env, ctx) {
    if (env.BIFROST_CLOUDFLARE_LAUNCHER_ENABLED !== 'true') return;
    await ctx.exports.CloudflareLauncher.getByName(env.BIFROST_WORKER_INCARNATION_ID).start();
  },
  fetch() {
    return new Response('Not found', {status: 404});
  },
};
