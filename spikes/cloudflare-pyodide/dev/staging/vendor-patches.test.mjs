import assert from 'node:assert/strict';
import test from 'node:test';
import {PATCHES, applyPatches} from './vendor-patches.mjs';

const [jsfetch] = PATCHES;
const upstream = `def x():\n${jsfetch.find}\n        status_code=1)\n`;

test('the jsfetch patch handles bodiless responses before touching getReader', () => {
  const packaged = {[jsfetch.file]: {base64: Buffer.from(upstream).toString('base64')}};
  assert.deepEqual(applyPatches(packaged), [{file: jsfetch.file, state: 'applied'}]);
  const text = Buffer.from(packaged[jsfetch.file].base64, 'base64').toString();
  assert.ok(text.indexOf('JsNull') < text.indexOf('getReader()'), 'null check precedes getReader');
  assert.match(text, /content=b""/);
});

test('patching is idempotent', () => {
  const packaged = {[jsfetch.file]: {py: upstream}};
  applyPatches(packaged);
  const once = packaged[jsfetch.file].py;
  assert.deepEqual(applyPatches(packaged), [{file: jsfetch.file, state: 'already-applied'}]);
  assert.equal(packaged[jsfetch.file].py, once);
});

test('a changed upstream fails the build instead of half-patching', () => {
  assert.throws(() => applyPatches({[jsfetch.file]: {py: 'body = response_js.body.getReader()\n'}}),
    /matched 0 times; upstream changed/);
  assert.throws(() => applyPatches({}), /patch target missing/);
});

test('source-only steps refuse a compiled runtime', async () => {
  const {assertSourceRuntime} = await import('./vendor-patches.mjs');
  assert.doesNotThrow(() => assertSourceRuntime({'a.py': {py: ''}, 'b.so': {base64: ''}}, 'source'));
  assert.throws(() => assertSourceRuntime({'a.py': {py: ''}, '__pycache__/a.cpython-314.pyc': {base64: ''}},
    'compiled'), /compiled runtime/);
});
