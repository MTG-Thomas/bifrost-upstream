import {createHash} from 'node:crypto';
import {createReadStream} from 'node:fs';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const bundleName = 'pyodide_314.0.6_2026-08-17_6.capnp.bin';
const bundleSha256 = '3c3fd5a4179230e21e018e28b0e735e7c4abd8fe8e4ff39d2fd6bb2ea3a2e260';

export async function verifyOfflinePyodide(cacheDir) {
  if (!cacheDir) throw new Error('Set REHEARSAL_PYODIDE_CACHE_DIR to the verified offline Pyodide bundle directory');
  const directory = resolve(cacheDir);
  const hash = createHash('sha256');
  try {
    for await (const chunk of createReadStream(join(directory, bundleName))) hash.update(chunk);
  } catch (error) {
    throw new Error(`Offline Pyodide bundle ${bundleName} is required before starting the local provider`, {cause: error});
  }
  if (hash.digest('hex') !== bundleSha256) throw new Error(`Offline Pyodide bundle ${bundleName} failed SHA-256 verification`);
  return directory;
}

export function useOfflineWorkerd(binary, directory) {
  process.env.REHEARSAL_PYODIDE_CACHE_DIR = directory;
  process.env.REHEARSAL_WORKERD_BINARY = binary;
  process.env.MINIFLARE_WORKERD_PATH = fileURLToPath(new URL('./workerd-offline.sh', import.meta.url));
}
