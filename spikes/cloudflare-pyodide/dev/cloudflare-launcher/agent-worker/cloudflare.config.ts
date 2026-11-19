import {defineConfig} from 'cf/config';

const name = process.env.BIFROST_CLOUDFLARE_AGENT_WORKER;
const endpoint = process.env.BIFROST_CLOUDFLARE_AGENT_ENDPOINT;
if (!name || !endpoint) throw new Error('Configure the private agent Worker name and endpoint');

export default defineConfig({
  worker: {
    name,
    entrypoint: './entry.mjs',
    exports: {AgentProvider: {type: 'worker'}},
    compatibilityDate: '2026-11-19',
    workersDev: false,
    previewUrls: false,
    observability: {enabled: false},
    env: {BIFROST_CLOUDFLARE_AGENT_ENDPOINT: {type: 'text', value: endpoint}},
  },
});
