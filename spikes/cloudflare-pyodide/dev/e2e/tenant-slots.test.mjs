// Run with the pinned local runtime. This bundles source for Miniflare without
// producing a qualified release or connecting to Cloudflare.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {after} from 'node:test';
import {bundleWorker} from '../../../../deploy/capacity/cloudflare/build-workers.mjs';

const runtime = await readFile(process.env.BIFROST_RUNTIME ?? new URL('../runtime/generated-runtime.mjs', import.meta.url));
assert.equal(createHash('sha256').update(runtime).digest('hex'),
  '11ade0a1ea1b117d577803cff87d1c3c4074d793df5bcb17a9f473923e55f254');
const directory = await mkdtemp(join(tmpdir(), 'bifrost-tenant-slots-'));
after(() => rm(directory, {recursive: true, force: true}));
for (const role of ['launcher', 'gateway']) {
  const path = join(directory, `${role}.mjs`);
  await writeFile(path, await bundleWorker(role, runtime));
  process.env[`BIFROST_QUALIFIED_${role.toUpperCase()}_BUNDLE`] = path;
}
process.env.BIFROST_LOCAL_TENANT_SLOTS = 'true';
await import('../tenant-workers/capacity-workerd.test.mjs');
