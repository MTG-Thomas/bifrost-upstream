import {ScopeRegistry} from './capacity.mjs';

export {ScopeRegistry};
export default {
  async fetch(request, env) {
    if (request.headers.get('Authorization') !== `Bearer ${env.TRIAL_BEARER}`) {
      return new Response(null, {status: 401});
    }
    return env.LAUNCHER.getByName('phase0').fetch('https://launcher/');
  },
};
