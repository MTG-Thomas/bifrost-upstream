import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';
import {verifyOfflinePyodide} from './offline-pyodide.mjs';

test('local provider rejects an unconfigured or missing offline bundle', async (t) => {
  await assert.rejects(verifyOfflinePyodide(undefined), /Set REHEARSAL_PYODIDE_CACHE_DIR/);
  const directory = await mkdtemp(join(tmpdir(), 'bifrost-pyodide-missing-'));
  t.after(() => rm(directory, {recursive: true}));
  await assert.rejects(verifyOfflinePyodide(directory), /is required before starting the local provider/);
});

test('local provider rejects a file that fails the offline bundle digest', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'bifrost-pyodide-corrupt-'));
  t.after(() => rm(directory, {recursive: true}));
  await writeFile(join(directory, 'pyodide_314.0.6_2026-08-17_6.capnp.bin'), 'not the bundle');
  await assert.rejects(verifyOfflinePyodide(directory), /failed SHA-256 verification/);
});

test('the combined provider fails before opening a listener without the offline cache', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'bifrost-provider-offline-'));
  t.after(() => rm(directory, {recursive: true}));
  const config = join(directory, 'config.json');
  await writeFile(config, '{}');
  const provider = fileURLToPath(new URL('./combined-provider.mjs', import.meta.url));
  const result = spawnSync(process.execPath, [provider, config], {
    env: {...process.env, REHEARSAL_PYODIDE_CACHE_DIR: directory},
    encoding: 'utf8',
    timeout: 3000,
  });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Offline Pyodide bundle .* is required before starting the local provider/);
});
