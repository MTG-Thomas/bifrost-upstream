import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {supervisorDigest, supervisorModules} from './supervisor-digest.mjs';

function tree(files) {
  const root = mkdtempSync(join(tmpdir(), 'supervisor-'));
  for (const [path, source] of Object.entries(files)) {
    mkdirSync(join(root, path, '..'), {recursive: true});
    writeFileSync(join(root, path), source);
  }
  return root;
}

test('follows relative static and dynamic imports, not platform ones, and skips the runtime', () => {
  const root = tree({
    'w/entry.mjs': `import a from './a.mjs';\nimport {x} from "../lib/b.mjs";\nimport 'node:fs';\n` +
      `import {DurableObject} from 'cloudflare:workers';\nconst c = await import('./c.mjs');\n`,
    'w/a.mjs': `export default 1;`,
    'lib/b.mjs': `import {runtimeCode} from '../runtime/generated-runtime.mjs';\nexport const x = 2;`,
    'w/c.mjs': `export const c = 3;`,
  });
  try {
    assert.deepEqual(supervisorModules(join(root, 'w/entry.mjs')).map(([path]) => path),
      ['lib/b.mjs', 'w/a.mjs', 'w/c.mjs', 'w/entry.mjs']);
  } finally {
    rmSync(root, {recursive: true, force: true});
  }
});

test('a change to any reached module changes the digest; an unreached file does not', () => {
  const root = tree({'w/entry.mjs': `import './host.mjs';`, 'w/host.mjs': `export const v = 1;`,
    'w/unrelated.mjs': `export const u = 1;`});
  try {
    const before = supervisorDigest(join(root, 'w/entry.mjs'));
    writeFileSync(join(root, 'w/unrelated.mjs'), `export const u = 2;`);
    assert.equal(supervisorDigest(join(root, 'w/entry.mjs')), before);
    writeFileSync(join(root, 'w/host.mjs'), `export const v = 2;`);
    assert.notEqual(supervisorDigest(join(root, 'w/entry.mjs')), before);
  } finally {
    rmSync(root, {recursive: true, force: true});
  }
});

test('the real supervisor closure includes the host and the dispatch helpers', () => {
  const paths = supervisorModules(new URL('./tenant-entry.mjs', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'))
    .map(([path]) => path);
  for (const expected of ['tenant-workers/tenant-api.mjs', 'tenant-workers/tenant-dispatch.mjs',
    'tenant-workers/generic-worker-code.mjs', 'executor/policy.mjs']) {
    assert.ok(paths.includes(expected), `${expected} missing from ${paths.join(', ')}`);
  }
  assert.ok(!paths.some((path) => path.endsWith('generated-runtime.mjs')));
});
