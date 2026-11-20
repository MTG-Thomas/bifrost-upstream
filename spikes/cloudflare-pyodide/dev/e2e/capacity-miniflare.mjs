import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

export function capacityWorker({token, scope, version, maxWorkers, slots = 1, bundle}) {
  const source = new URL('../cloudflare-launcher/', import.meta.url);
  const modulesRoot = fileURLToPath(source);
  const exports = Object.fromEntries(['ScopeRegistry', 'WorkerLedger'].map((name) =>
    [name, {type: 'durable-object', storage: 'sqlite'}]));
  const env = Object.fromEntries(Object.entries({
    CAPACITY_TOKEN: token, SCOPE: scope, WORKER_VERSION: version,
    MAX_WORKERS: String(maxWorkers), WORKFLOW_SLOTS: String(slots), MAX_LIFETIME_SECONDS: '21600',
  }).map(([key, value]) => [key, {type: 'text', value}]));
  for (const [name, exportName, worker] of [
    ['CLOUDFLARE_LAUNCHER', 'CloudflareLauncher', 'launcher'],
    ['SCOPE_REGISTRY', 'ScopeRegistry', 'capacity'],
    ['WORKER_LEDGER', 'WorkerLedger', 'capacity'],
  ]) {
    env[name] = {type: 'durable-object', worker, exportName};
  }
  const modules = bundle
    ? {'capacity.mjs': {type: 'esm', contents: readFileSync(bundle, 'utf8')}}
    : Object.fromEntries(['capacity.mjs', 'capacity-core.mjs', 'protocol.mjs'].map((name) =>
      [name, {type: 'esm', contents: readFileSync(new URL(name, source), 'utf8')}]));
  return {config: {name: 'capacity', compatibilityDate: '2026-10-30', exports, env,
    manifest: {mainModule: 'capacity.mjs', modulesRoot, modules}}};
}
