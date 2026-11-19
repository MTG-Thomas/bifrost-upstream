// Cloudflare approval host policy. The API port uses the same PSL and vectors.
import psl from './egress-psl.generated.mjs';

const icann = new Set(psl.icann);
const privateRoots = new Set(psl.privateRoots);
const label = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

function exactHost(host) {
  const labels = host.split('.');
  return host.length <= 253 && !/[^a-z0-9.-]/.test(host) && labels.length > 1 &&
    !/^\d+$/.test(labels.at(-1)) && labels.every((part) => label.test(part));
}

function wildcardBaseAllowed(base) {
  const labels = base.split('.');
  let registrable = false;
  for (let index = 0; index < labels.length; index++) {
    const suffix = labels.slice(index).join('.');
    if (privateRoots.has(suffix)) return false;
    if (registrable) continue;
    if (icann.has(`!${suffix}`)) registrable = true;
    else if (icann.has(suffix) || icann.has(`*.${labels.slice(index + 1).join('.')}`)) {
      if (index === 0) return false;
      registrable = true;
    }
  }
  return registrable;
}

export function normalizeEgressPattern(pattern) {
  if (typeof pattern !== 'string' || pattern.length > 253 || /[^\x00-\x7f]/.test(pattern)) return null;
  const normalized = pattern.toLowerCase();
  if (normalized.startsWith('*.')) {
    const base = normalized.slice(2);
    return exactHost(base) && wildcardBaseAllowed(base) ? normalized : null;
  }
  return exactHost(normalized) ? normalized : null;
}

export function egressHostAllowed(hostname, patterns) {
  if (typeof hostname !== 'string' || /[^\x00-\x7f]/.test(hostname)) return false;
  const host = hostname.toLowerCase();
  if (!exactHost(host)) return false;
  if (patterns.has(host)) return true;
  const wildcard = `*.${host.slice(host.indexOf('.') + 1)}`;
  return patterns.has(wildcard) && normalizeEgressPattern(wildcard) !== null;
}

export function isHttpsEgressUrl(url) {
  return url.protocol === 'https:' && !url.username && !url.password && !url.port;
}
