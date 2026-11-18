// Explicit, verified changes to third-party code vendored in the packaged
// runtime, plus whole packages the runtime adds. Applied in place:
//
//   node vendor-patches.mjs <dev/runtime/source-runtime.mjs> [--add-wheel <file.whl>]...
//
// Each patch names the exact upstream text it replaces and must match exactly
// once; already-patched files are left alone, and anything else fails the
// build, so an upgraded package can never be silently half-patched.
import {readFileSync, writeFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';
import {inflateRawSync} from 'node:zlib';

export const PATCHES = [
  {
    // httpx's Pyodide fetch transport assumes every response has a body. Under
    // the Fetch API a bodiless response (204, 304, HEAD) has body === null, so
    // every SDK write answered with 204 (config.set, deletes, file writes)
    // failed client-side with "'JsNull' object has no attribute 'getReader'"
    // AFTER the server had applied it.
    file: 'python_modules/httpx/_transports/jsfetch.py',
    find: `    # get a reader from the fetch response
    body_stream_js = response_js.body.getReader()
    return Response(`,
    replace: `    # A bodiless response (204/304/HEAD) has body === null in the Fetch API.
    if response_js.body is None or type(response_js.body).__name__ == "JsNull":
        return Response(status_code=status_code, headers=headers, content=b"")
    # get a reader from the fetch response
    body_stream_js = response_js.body.getReader()
    return Response(`,
  },
];

// The source runtime (dev/runtime/source-runtime.mjs) is the only thing the
// build and these patches edit; precompile-runtime.mjs derives the deployed
// generated-runtime.mjs from it. Compiled bytecode is shipped unchecked, so
// editing a compiled artifact would leave stale bytecode in front of new
// source. Every step that edits source refuses one.
export function assertSourceRuntime(packaged, label) {
  const compiled = Object.keys(packaged).filter((key) => key.endsWith('.pyc'));
  if (compiled.length > 0) {
    throw new Error(`${label} is a compiled runtime (${compiled.length} .pyc files, e.g. ${compiled[0]}); ` +
      'edit dev/runtime/source-runtime.mjs and re-run precompile-runtime.mjs instead');
  }
}

const textOf = (value) => value.py ?? (value.base64 !== undefined
  ? Buffer.from(value.base64, 'base64').toString('utf8') : null);

export function applyPatches(packaged) {
  const applied = [];
  for (const patch of PATCHES) {
    const entry = packaged[patch.file];
    if (!entry) throw new Error(`patch target missing: ${patch.file}`);
    const text = textOf(entry);
    if (text.includes(patch.replace)) {
      applied.push({file: patch.file, state: 'already-applied'});
      continue;
    }
    const count = text.split(patch.find).length - 1;
    if (count !== 1) throw new Error(`patch for ${patch.file} matched ${count} times; upstream changed`);
    const patched = text.replace(patch.find, patch.replace);
    packaged[patch.file] = entry.py !== undefined ? {py: patched}
      : {base64: Buffer.from(patched, 'utf8').toString('base64')};
    applied.push({file: patch.file, state: 'applied'});
  }
  return applied;
}

// Minimal wheel (zip) reader: stored and deflated entries only, which is all
// pure-Python wheels use. Package files land under python_modules/.
export function addWheel(packaged, wheelBytes) {
  const eocd = wheelBytes.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  if (eocd < 0) throw new Error('not a zip');
  const entries = wheelBytes.readUInt16LE(eocd + 10);
  let offset = wheelBytes.readUInt32LE(eocd + 16);
  const added = [];
  for (let i = 0; i < entries; i++) {
    const method = wheelBytes.readUInt16LE(offset + 10);
    const compressed = wheelBytes.readUInt32LE(offset + 20);
    const nameLength = wheelBytes.readUInt16LE(offset + 28);
    const extraLength = wheelBytes.readUInt16LE(offset + 30);
    const commentLength = wheelBytes.readUInt16LE(offset + 32);
    const local = wheelBytes.readUInt32LE(offset + 42);
    const name = wheelBytes.toString('utf8', offset + 46, offset + 46 + nameLength);
    offset += 46 + nameLength + extraLength + commentLength;
    if (name.endsWith('/')) continue;
    const dataStart = local + 30 + wheelBytes.readUInt16LE(local + 26) + wheelBytes.readUInt16LE(local + 28);
    const raw = wheelBytes.subarray(dataStart, dataStart + compressed);
    const data = method === 0 ? raw : method === 8 ? inflateRawSync(raw)
      : (() => { throw new Error(`unsupported zip method ${method} for ${name}`); })();
    const key = `python_modules/${name}`;
    if (packaged[key]) throw new Error(`wheel would overwrite ${key}`);
    packaged[key] = {base64: Buffer.from(data).toString('base64')};
    added.push(key);
  }
  return added;
}

export function serialize(packaged, note) {
  const entries = Object.entries(packaged).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => {
    const encoded = value.py !== undefined ? `{py:${JSON.stringify(value.py)}}`
      : value.js !== undefined ? `{js:${JSON.stringify(value.js)}}`
      : `{base64:${JSON.stringify(value.base64)}}`;
    return `${JSON.stringify(key)}:${encoded}`;
  });
  return `// ${note}\nexport default {\n${entries.join(',\n')}\n};\n`;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const target = process.argv[2];
  const wheels = process.argv.flatMap((arg, i) => (process.argv[i - 1] === '--add-wheel' ? [arg] : []));
  const packaged = {...(await import(pathToFileURL(target).href)).default};
  assertSourceRuntime(packaged, target);
  const applied = applyPatches(packaged);
  const added = [];
  for (const wheel of wheels) {
    const files = addWheel(packaged, readFileSync(wheel));
    added.push({wheel: wheel.split(/[\\/]/).pop(), files: files.length});
  }
  writeFileSync(target, serialize(packaged, 'Generated runtime with vendor-patches.mjs applied.'));
  console.log(JSON.stringify({applied, added, modules: Object.keys(packaged).length}));
}
