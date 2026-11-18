import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {supervisorDigest} from './supervisor-digest.mjs';
import {defineConfig, exports, triggers} from 'cf/config';

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
const launcherEnabled = process.env.BIFROST_CLOUDFLARE_LAUNCHER_ENABLED === 'true';
const managedLauncher = process.env.BIFROST_CLOUDFLARE_MANAGED === 'true';
if (managedLauncher && !launcherEnabled) throw new Error('Managed capacity requires the launcher entrypoint');
if (launcherEnabled && (!process.env.WORKER_NAME ||
    ['bifrost-pyodide-tenant-ddf316c1', 'bifrost-pyodide-canary-ddf316c1'].includes(workerName))) {
  throw new Error('Use a distinct WORKER_NAME for the Cloudflare launcher');
}
const entrypoint = launcherEnabled ? './launcher-entry.mjs' : './tenant-entry.mjs';

// Identity of the packaged Python runtime the engine child loads. Hashed here,
// once per deploy, so the supervisor can key child isolates by engine version
// without hashing 15 MB per request.
const runtimeSha256 = createHash('sha256')
  .update(readFileSync(new URL('../runtime/generated-runtime.mjs', import.meta.url)))
  .digest('hex');

// Identity of the supervisor itself: every module tenant-entry.mjs reaches by
// relative import, hashed by path and content. The engine digest above does
// not cover it, so without this a host- or gateway-only change was not
// attributable in canary evidence.
const supervisorSha256 = supervisorDigest(fileURLToPath(new URL(entrypoint, import.meta.url)));

export default defineConfig({
  worker: {
    name: workerName,
    entrypoint,
    compatibilityDate: '2026-10-30',
    workersDev: !launcherEnabled,
    previewUrls: false,
    exports: {
      // One host per tenant: owns the warm engine child, serializes the
      // tenant's attempts, and holds each run token in memory under an opaque
      // handle for the attempt only (tenant-api.mjs).
      TenantHost: exports.durableObject({storage: 'sqlite'}),
      // Retired per-attempt token broker (one DO created per attempt cost
      // 0.2-0.5 s on every run); tombstoned so its namespace is deleted.
      ...(launcherEnabled ?
        {CloudflareLauncher: exports.durableObject({storage: 'sqlite'})} :
        {RunTokenBroker: {type: 'durable-object' as const, state: 'deleted' as const}}),
    },
    ...(launcherEnabled && !managedLauncher ? {triggers: [triggers.scheduled({schedule: '* * * * *'})]} : {}),
    env: {
      LOADER: {type: 'worker-loader'},
      SPIKE_EXPIRES_AT: {type: 'text', value: process.env.SPIKE_EXPIRES_AT ?? '0'},
      RUNTIME_SHA256: {type: 'text', value: runtimeSha256},
      SUPERVISOR_SHA256: {type: 'text', value: supervisorSha256},
      // Deployment identity ({id, tag, timestamp}); reported by /__code_digest.
      VERSION: {type: 'version-metadata'},
      // Staging only: when set, the supervisor fetches modules from this real
      // Bifrost API instead of an attempt-supplied store.
      API_ORIGIN: {type: 'text', value: process.env.STAGING_API_ORIGIN ?? ''},
      ...(managedLauncher ? {BIFROST_CLOUDFLARE_MANAGED: {type: 'text' as const, value: 'true'}} :
        launcherEnabled ? Object.fromEntries([
        'BIFROST_CLOUDFLARE_LAUNCHER_ENABLED', 'BIFROST_WORKER_INCARNATION_ID', 'BIFROST_WORKER_LAUNCHER_ID',
        'BIFROST_WORKER_RESOURCE_UID', 'BIFROST_WORKER_VERSION',
      ].map((key) => [key, {type: 'text' as const, value: process.env[key] ?? ''}])) : {}),
      // Child SDK surface: 'sdk' (default, the narrow allowlist in
      // executor/policy.mjs) or 'all' (capability lab only).
      SDK_SCOPE: {type: 'text', value: !launcherEnabled && process.env.SDK_SCOPE === 'all' ? 'all' : 'sdk'},
      // Engine child CPU budget per request; empty keeps the packaged default.
      ENGINE_CPU_MS: {type: 'text', value: process.env.ENGINE_CPU_MS ?? ''},
      // Engine child subrequests per attempt; empty keeps the packaged default.
      ENGINE_SUBREQUESTS: {type: 'text', value: process.env.ENGINE_SUBREQUESTS ?? ''},
      // Per-attempt deadline; empty keeps the Linux-matching 1800 s default.
      ATTEMPT_DEADLINE_MS: {type: 'text', value: process.env.ATTEMPT_DEADLINE_MS ?? ''},
      // Capability lab only: echo runtime error text in dynamic-runtime replies.
      LAB_ECHO_ERRORS: {type: 'text', value: process.env.LAB_ECHO_ERRORS === 'true' ? 'true' : 'false'},
    },
    // The launcher decodes fragmented logical envelopes. Child CPU limits
    // remain per workflow, applied by TenantHost.
    limits: {cpuMs: launcherEnabled ? 30_000 : 1000},
    // Workers Logs and traces attribute where an attempt's time goes (host,
    // child, outbound gateway, API origin). Every request is sampled: the
    // canary sends ~100 attempts a day plus the keep-warm pings.
    // Query strings are redacted in traces; the supervisor logs nothing itself.
    observability: {
      enabled: true,
      headSamplingRate: 1,
      redactQueryString: true,
      logs: {enabled: true, invocationLogs: true},
      traces: {enabled: true, headSamplingRate: 1},
    },
  },
});
