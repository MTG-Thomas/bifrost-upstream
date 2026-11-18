import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

test('census uses the approval analyzer to pin imports and reject a runtime trap', (t) => {
  const root = mkdtempSync(join(tmpdir(), 'bifrost-census-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  mkdirSync(join(root, '.bifrost'));
  writeFileSync(join(root, '.bifrost', 'workflows.yaml'),
    'first:\n  path: good.py\n  function_name: run\nsecond:\n  path: bad.py\n  function_name: run\n');
  writeFileSync(join(root, 'good.py'), 'from helper import url\ndef run():\n    return url\n');
  writeFileSync(join(root, 'helper.py'), 'url = "https://example.com/path"\n');
  writeFileSync(join(root, 'bad.py'), 'from time import process_time as clock\ndef run():\n    return clock()\n');
  const out = join(root, 'report.json');
  execFileSync(process.execPath, ['census.mjs', '--workspace', root, '--out', out], {
    cwd: new URL('.', import.meta.url),
  });
  const [good, bad] = JSON.parse(readFileSync(out, 'utf8')).workspaces[0].rows;
  assert.equal(good.backend, 'cloudflare');
  assert.match(good.module_sha256['helper.py'], /^[0-9a-f]{64}$/);
  assert.deepEqual(good.https_hosts, ['example.com']);
  assert.equal(bad.backend, 'linux');
  assert.match(bad.reasons[0], /runtime operation/);
});
