import {defineConfig} from 'cf/config';

const name = process.env.BIFROST_CLOUDFLARE_SUMMARY_WORKER;
const endpoint = process.env.BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT;
if (!name || !endpoint) throw new Error('Configure the private summary Worker name and endpoint');

export default defineConfig({
  worker: {
    name,
    entrypoint: '../summary-provider.mjs',
    compatibilityDate: '2026-10-30',
    workersDev: false,
    previewUrls: false,
    observability: {enabled: false},
    env: {BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: {type: 'text', value: endpoint}},
  },
});
