// Executor egress gate: per-attempt declared HTTPS allowlist enforced BEFORE
// any network effect. Declaration rides the attempt; no queue/scheduler/DB,
// no per-tenant persistence.
//
// Contract:
// - egress === undefined -> null (no declaration, old behavior unchanged)
// - egress.hosts === undefined -> null (no hosts declared, old behavior)
// - invalid egress shape -> {reason: 'egress'|'hosts'|'hostname'} (fail closed, no echo)
// - all checks pass -> null (admit)
// - Enforcement: test-api.invalid always allowed (SDK routes only, via apiTarget).
//   Declared hosts (lowercase normalized) allowed for direct HTTPS fetch.
//   Anything else -> egress-rejected, zero api_calls, no host/value echo.
import {egressHostAllowed, isHttpsEgressUrl, normalizeEgressPattern} from './egress-hosts.mjs';

export const MAX_EGRESS_HOSTS = 8;

export const VIRTUAL_TEST_HOST = 'test-api.invalid';

const isRecord = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

export function normalizedEgressHosts(egress) {
  if (egress === undefined) return [];
  if (!isRecord(egress)) return null;
  const hosts = egress.hosts;
  if (hosts === undefined) return [];
  if (!Array.isArray(hosts)) return null;
  const out = [];
  for (const h of hosts) {
    // This legacy declaration comes from the caller. Only reviewed workspace
    // approvals can supply a wildcard.
    const pattern = normalizeEgressPattern(h);
    if (pattern === null || pattern.startsWith('*.')) return null;
    out.push(pattern);
  }
  return out;
}

export function egressRejection(egress) {
  if (egress === undefined) return null;
  if (!isRecord(egress)) return {reason: 'egress'};
  const hosts = egress.hosts;
  if (hosts === undefined) return null;
  if (!Array.isArray(hosts) || hosts.length > MAX_EGRESS_HOSTS) return {reason: 'hosts'};
  return normalizedEgressHosts(egress) === null ? {reason: 'hostname'} : null;
}

export function isHostAllowed(hostname, egress) {
  if (typeof hostname !== 'string') return false;
  const lower = hostname.toLowerCase();
  if (lower === VIRTUAL_TEST_HOST) return true;
  const normalized = normalizedEgressHosts(egress);
  if (normalized === null) return false;
  return egressHostAllowed(hostname, new Set(normalized));
}

export function isUrlAllowed(urlString, egress) {
  let url;
  try { url = new URL(urlString); }
  catch { return false; }
  return isHttpsEgressUrl(url) && isHostAllowed(url.hostname, egress);
}
