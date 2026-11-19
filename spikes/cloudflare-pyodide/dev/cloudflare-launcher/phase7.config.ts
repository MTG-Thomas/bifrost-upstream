import {bindings, defineConfig, exports} from 'cf/config';

const root = process.env.PHASE7_NAME ?? '';
if (!/^bifrost-hq-[a-f0-9]{9}$/.test(root)) throw new Error('Set a dedicated PHASE7_NAME');
const name = root + '-workers';
const object = (exportName: string) => bindings.durableObject({worker: name, exportName});

export default defineConfig({
  worker: {
    name,
    entrypoint: './capacity.mjs',
    compatibilityDate: '2026-10-30',
    workersDev: true,
    previewUrls: false,
    exports: {
      ScopeRegistry: exports.durableObject({storage: 'sqlite'}),
      WorkerLedger: exports.durableObject({storage: 'sqlite'}),
    },
    env: {
      SCOPE_REGISTRY: object('ScopeRegistry'),
      WORKER_LEDGER: object('WorkerLedger'),
      CLOUDFLARE_LAUNCHER: bindings.durableObject({worker: root + '-launcher', exportName: 'CloudflareLauncher'}),
      SCOPE: {type: 'text', value: 'phase6-local'},
      WORKER_VERSION: {type: 'text', value: root},
      WORKFLOW_SLOTS: {type: 'text', value: '2'},
      MAX_WORKERS: {type: 'text', value: '1'},
      MAX_LIFETIME_SECONDS: {type: 'text', value: '600'},
    },
    observability: {enabled: true, headSamplingRate: 1},
  },
});
