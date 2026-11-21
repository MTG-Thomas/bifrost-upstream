// Precompile the packaged Python runtime to bytecode.
//
// The Cloudflare child runs CPython 3.14.2 under Pyodide with
// dont_write_bytecode=True and no deploy-time snapshot for dynamic Workers, so
// every cold child recompiles every imported module from source. Shipping
// unchecked-hash .pyc files removes that compile step from the boot.
//
//   node precompile-runtime.mjs dev/runtime/source-runtime.mjs dev/runtime/generated-runtime.mjs \
//     [--mode sourceless-vendor|cache] [--python "py -V:Astral/CPython3.14.4"]
//
// The input is the source runtime, the output the deployed artifact. Its
// bytecode is unchecked, so the output must never be edited afterwards; the
// build steps refuse compiled input (assertSourceRuntime).
//
// cache              keep every .py and add __pycache__/<name>.cpython-314.pyc
// sourceless-vendor  third-party packages (python_modules/) ship .pyc only, in
//                    place of the .py; Bifrost's own modules keep their source
//                    plus a cached .pyc, so engine tracebacks keep source lines.
//
// The compiler's magic number must equal the runtime's (checked below against
// EXPECTED_MAGIC, read from the child: 2b0e0d0a, cache tag cpython-314).
import {execFileSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {assertSourceRuntime} from './vendor-patches.mjs';
import {trimRuntime} from './runtime-footprint.mjs';

const EXPECTED_MAGIC = '2b0e0d0a';
const CACHE_TAG = 'cpython-314';
// Where the Workers runtime mounts packaged modules (seen in child tracebacks).
const MOUNT = '/session/metadata/';

const [input, output] = process.argv.slice(2, 4);
const flag = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
};
const mode = flag('mode', 'sourceless-vendor');
const python = flag('python', 'py -V:Astral/CPython3.14.4').split(' ');
if (!input || !output || !['cache', 'sourceless-vendor'].includes(mode)) {
  throw new Error('usage: precompile-runtime.mjs <in> <out> --mode cache|sourceless-vendor');
}

const sourceRuntime = (await import(pathToFileURL(input).href)).default;
// Compiling a compiled runtime would re-emit bytecode with its sources gone.
assertSourceRuntime(sourceRuntime, input);
const packaged = trimRuntime(sourceRuntime);
const sourceOf = (value) => value.py ?? (value.base64 !== undefined
  ? Buffer.from(value.base64, 'base64').toString('utf8') : null);

const work = mkdtempSync(join(tmpdir(), 'pyc-'));
try {
  const sources = Object.entries(packaged).filter(([key]) => key.endsWith('.py'));
  for (const [key, value] of sources) {
    const path = join(work, 'src', key);
    mkdirSync(dirname(path), {recursive: true});
    writeFileSync(path, sourceOf(value));
  }
  writeFileSync(join(work, 'keys.json'), JSON.stringify(sources.map(([key]) => key)));
  // One interpreter run compiles everything; a module that does not compile
  // (e.g. syntax newer than 3.14) is reported and keeps shipping as source.
  const compiler = `
import importlib.util, json, py_compile, sys
work, mount = sys.argv[1], sys.argv[2]
assert importlib.util.MAGIC_NUMBER.hex() == "${EXPECTED_MAGIC}", importlib.util.MAGIC_NUMBER.hex()
failed = []
for key in json.load(open(work + "/keys.json")):
    try:
        py_compile.compile(work + "/src/" + key, cfile=work + "/pyc/" + key + "c",
            dfile=mount + key, doraise=True,
            invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
    except py_compile.PyCompileError as error:
        failed.append([key, str(error).splitlines()[-1][:200]])
json.dump(failed, open(work + "/failed.json", "w"))
`;
  execFileSync(python[0], [...python.slice(1), '-c', compiler, work, MOUNT], {stdio: 'inherit'});
  const failed = new Map(JSON.parse(readFileSync(join(work, 'failed.json'), 'utf8')));

  const out = new Map(Object.entries(packaged));
  let compiled = 0;
  let sourceless = 0;
  for (const [key] of sources) {
    const pycPath = join(work, 'pyc', `${key}c`);
    if (failed.has(key) || !existsSync(pycPath)) continue;
    const bytecode = readFileSync(pycPath).toString('base64');
    const dir = key.includes('/') ? key.slice(0, key.lastIndexOf('/') + 1) : '';
    const stem = key.slice(dir.length, -'.py'.length);
    compiled++;
    if (mode === 'sourceless-vendor' && key.startsWith('python_modules/')) {
      // SourcelessFileLoader picks up <name>.pyc when no <name>.py exists.
      out.delete(key);
      out.set(`${dir}${stem}.pyc`, {base64: bytecode});
      sourceless++;
    } else {
      out.set(`${dir}__pycache__/${stem}.${CACHE_TAG}.pyc`, {base64: bytecode});
    }
  }

  const entries = [...out].sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => {
    const encoded = value.py !== undefined ? `{py:${JSON.stringify(value.py)}}`
      : value.js !== undefined ? `{js:${JSON.stringify(value.js)}}`
      : `{base64:${JSON.stringify(value.base64)}}`;
    return `${JSON.stringify(key)}:${encoded}`;
  });
  writeFileSync(output, `// Precompiled (${mode}) from ${input}.\nexport default {\n${entries.join(',\n')}\n};\n`);
  console.log(JSON.stringify({mode, modules_in: Object.keys(sourceRuntime).length,
    typing_files_removed: Object.keys(sourceRuntime).length - Object.keys(packaged).length, modules_out: out.size,
    py_sources: sources.length, compiled, sourceless, compile_failed: [...failed]}, null, 2));
} finally {
  rmSync(work, {recursive: true, force: true});
}
