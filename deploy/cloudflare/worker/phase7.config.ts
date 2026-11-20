import {bindings, defineConfig, defineContainer, exports} from 'cf/config';

const root = process.env.PHASE7_NAME ?? '';
if (!/^bifrost-hq-[a-f0-9]{9}$/.test(root)) throw new Error('Set a dedicated PHASE7_NAME');
const zone = process.env.PHASE7_ZONE ?? '';
const label = '[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?';
if (zone.length > 253 || zone !== zone.trim() || !new RegExp(`^(?:${label}\\.)+${label}$`).test(zone)) {
  throw new Error('Set PHASE7_ZONE to the dedicated trial DNS zone');
}
const image = process.env.PHASE7_IMAGE ?? '';
if (!new RegExp('^registry\\.cloudflare\\.com/[a-f0-9]{32}/' + root + '@sha256:[a-f0-9]{64}$').test(image)) {
  throw new Error('Set the digest of the dedicated trial image');
}
const name = root + '-containers';
const object = (exportName: string) => bindings.durableObject({worker: name, exportName});
const container = defineContainer({
  name: root + '-container',
  image: {reference: image},
  instanceType: 'basic',
  maxInstances: 1,
  ssh: {enabled: false},
});

export default defineConfig({
  containers: [container],
  worker: {
    name,
    entrypoint: './capacity.mjs',
    compatibilityDate: '2026-11-10',
    workersDev: true,
    previewUrls: false,
    exports: {
      ScopeRegistry: exports.durableObject({storage: 'sqlite'}),
      WorkerLedger: exports.durableObject({storage: 'sqlite'}),
      WorkerContainer: exports.durableObject({storage: 'sqlite', container}),
    },
    env: {
      SCOPE_REGISTRY: object('ScopeRegistry'),
      WORKER_LEDGER: object('WorkerLedger'),
      WORKER_CONTAINER: object('WorkerContainer'),
      SCOPE: {type: 'text', value: 'containers'},
      WORKER_VERSION: {type: 'text', value: root},
      MEMORY_LIMIT_BYTES: {type: 'text', value: String(1024 ** 3)},
      MAX_WORKERS: {type: 'text', value: '1'},
      MAX_LIFETIME_SECONDS: {type: 'text', value: '600'},
      STARTING_GRACE_MS: {type: 'text', value: '30000'},
      WORKER_CONTAINER_ENV: {type: 'text', value: JSON.stringify({
        BIFROST_API_URL: `https://${root}.${zone}`, HOME: '/home/bifrost',
      })},
    },
    observability: {enabled: true, headSamplingRate: 1},
  },
});
