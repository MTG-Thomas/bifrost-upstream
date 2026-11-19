import {request as httpRequest} from 'node:http';

const hopByHop = [
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
  'te', 'trailer', 'transfer-encoding', 'upgrade',
];

export function hostedRelayPath(method, path, qualification = false) {
  if (qualification) {
    const pathname = path.split('?', 1)[0];
    if (method === 'POST' && path === '/__fixture/v1/chat/completions') return true;
    if (method === 'GET' && ['/api/sdk/modules/workflows/phase6_linux_only.py',
      '/api/sdk/modules-resolve'].includes(pathname)) return true;
  }
  return method === 'POST' &&
    ['/api/sdk/tables/list', '/api/sdk/runtime/logs', '/auth/refresh'].includes(path);
}

export function relayHeaders(headers) {
  const outgoing = {...headers};
  const nominated = (headers.connection ?? '').toLowerCase().split(',').map((name) => name.trim());
  for (const name of [...hopByHop, ...nominated, 'host']) delete outgoing[name];
  return outgoing;
}

export function relayHttp(incoming, url) {
  return new Promise((resolve, reject) => {
    const upstream = httpRequest(url, {
      method: incoming.method,
      headers: relayHeaders(incoming.headers),
      signal: AbortSignal.timeout(10_000),
    }, resolve);
    upstream.on('error', reject);
    incoming.on('aborted', () => upstream.destroy(new Error('Relay client disconnected')));
    incoming.pipe(upstream);
  });
}
