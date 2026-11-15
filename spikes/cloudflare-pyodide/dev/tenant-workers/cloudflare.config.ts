import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {defineConfig, exports} from 'cf/config';

// Cloud acceptance config for the per-tenant persistent-worker prototype.
// Deploy with:
//   cf deploy --secrets-file "$env:TEMP/bifrost-pyodide-tenant-ddf316c1-secrets.json"
// The secret file supplies SPIKE_AUTH. LOCAL_PROBE is intentionally absent, so
// the entry requires X-Spike-Auth inside an unexpired SPIKE_EXPIRES_AT window.
//
// WORKER_NAME selects the deployed worker. The default is the staging tenant
// worker; the Stage 3 canary deploys the same code and runtime under its own
// name, secret and expiry window so stopping it cannot touch staging evidence:
//   WORKER_NAME=bifrost-pyodide-canary-ddf316c1 \
//   STAGING_API_ORIGIN=https://bifrost.midtowntg.com SPIKE_EXPIRES_AT=<epoch-ms> \
//   cf deploy --secrets-file <canary secrets>
// Kill switch: redeploy with SPIKE_EXPIRES_AT=0, or rotate SPIKE_AUTH.
const workerName = process.env.WORKER_NAME ?? 'bifrost-pyodide-tenant-ddf316c1';

// Identity of the packaged Python runtime the engine child loads. Hashed here,
// once per deploy, so the supervisor can key child isolates by engine version
// without hashing 15 MB per request.
const runtimeSha256 = createHash('sha256')
  .update(readFileSync(new URL('../runtime/generated-runtime.mjs', import.meta.url)))
  .digest('hex');

export default defineConfig({
  worker: {
    name: workerName,
    entrypoint: './tenant-entry.mjs',
    compatibilityDate: '2026-10-30',
    workersDev: true,
    previewUrls: false,
    exports: {
      // One host per tenant: owns the warm engine child, serializes the
      // tenant's attempts, and holds each run token in memory under an opaque
      // handle for the attempt only (tenant-api.mjs).
      TenantHost: exports.durableObject({storage: 'sqlite'}),
      // Retired per-attempt token broker (one DO created per attempt cost
      // 0.2-0.5 s on every run); tombstoned so its namespace is deleted.
      RunTokenBroker: {type: 'durable-object', state: 'deleted'},
    },
    env: {
      LOADER: {type: 'worker-loader'},
      SPIKE_EXPIRES_AT: {type: 'text', value: process.env.SPIKE_EXPIRES_AT ?? '0'},
      RUNTIME_SHA256: {type: 'text', value: runtimeSha256},
      // Staging only: when set, the supervisor fetches modules from this real
      // Bifrost API instead of an attempt-supplied store.
      API_ORIGIN: {type: 'text', value: process.env.STAGING_API_ORIGIN ?? ''},
      // Child SDK surface: 'sdk' (default, the narrow allowlist in
      // executor/policy.mjs) or 'all' (capability lab only).
      SDK_SCOPE: {type: 'text', value: process.env.SDK_SCOPE === 'all' ? 'all' : 'sdk'},
      // Engine child CPU budget per request; empty keeps the packaged default.
      ENGINE_CPU_MS: {type: 'text', value: process.env.ENGINE_CPU_MS ?? ''},
      // Engine child subrequests per attempt; empty keeps the packaged default.
      ENGINE_SUBREQUESTS: {type: 'text', value: process.env.ENGINE_SUBREQUESTS ?? ''},
      // Per-attempt deadline; empty keeps the Linux-matching 1800 s default.
      ATTEMPT_DEADLINE_MS: {type: 'text', value: process.env.ATTEMPT_DEADLINE_MS ?? ''},
      // Capability lab only: echo runtime error text in dynamic-runtime replies.
      LAB_ECHO_ERRORS: {type: 'text', value: process.env.LAB_ECHO_ERRORS === 'true' ? 'true' : 'false'},
    },
    limits: {cpuMs: 1000},
    observability: {enabled: false},
  },
});
