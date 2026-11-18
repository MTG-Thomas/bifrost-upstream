// Deploy-time identity of the supervisor's source: the entry module plus every
// module it reaches through relative static or dynamic imports, hashed as
// (path relative to the entry's parent directory, content sha256) pairs in
// sorted order. Bare and scheme specifiers (`cloudflare:workers`, `node:*`)
// belong to the platform, not to this code, and are not followed.
//
// The packaged Python runtime is excluded: RUNTIME_SHA256 already identifies
// it, and folding it in here would make a runtime-only change look like a
// supervisor change.
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {dirname, relative, resolve} from 'node:path';

const RELATIVE_IMPORT = /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+)(['"])(\.{1,2}\/[^'"]+)\1/g;
export const RUNTIME_ARTIFACT = 'generated-runtime.mjs';

export function supervisorModules(entryPath) {
  const root = dirname(dirname(resolve(entryPath)));
  const seen = new Map();
  const pending = [resolve(entryPath)];
  while (pending.length > 0) {
    const path = pending.pop();
    if (seen.has(path)) continue;
    const source = readFileSync(path, 'utf8');
    seen.set(path, source);
    for (const [, , specifier] of source.matchAll(RELATIVE_IMPORT)) {
      const target = resolve(dirname(path), specifier);
      if (!target.endsWith(RUNTIME_ARTIFACT)) pending.push(target);
    }
  }
  return [...seen].map(([path, source]) => [relative(root, path).replaceAll('\\', '/'), source])
    .sort(([a], [b]) => a.localeCompare(b));
}

export function supervisorDigest(entryPath) {
  const hash = createHash('sha256');
  for (const [path, source] of supervisorModules(entryPath)) {
    hash.update(`${path}\0${createHash('sha256').update(source).digest('hex')}\n`);
  }
  return hash.digest('hex');
}
