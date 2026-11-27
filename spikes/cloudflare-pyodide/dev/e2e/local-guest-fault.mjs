import assert from 'node:assert/strict';
import {buffer} from 'node:stream/consumers';
import {createServer, request} from 'node:http';

export const faultDirectory = '/opt/bifrost-fault-hook';
export const faultMarker = 'bifrost-local-fault-hook: ' + faultDirectory + '/sitecustomize.py';

export function mountFaultHook(launch, {mode, image, source}) {
  if (mode === 'off' || launch.Image !== image) return false;
  const generation = mode === 'reconnect' ? '2' : '1';
  assert.ok(launch.Env.includes('REHEARSAL_FAIL_GUEST_AFTER_HELLO=' + generation));
  assert.ok(launch.Env.some((value) => value.startsWith('BIFROST_WORKER_RESOURCE_UID=')));
  const pythonPath = launch.Env.find((value) => value.startsWith('PYTHONPATH='))?.slice(11) || '/app';
  launch.Env = launch.Env.filter((value) => !value.startsWith('PYTHONPATH='));
  launch.Env.push('PYTHONPATH=' + faultDirectory + ':' + pythonPath);
  const host = launch.HostConfig ??= {};
  const mounts = host.Mounts ??= [];
  assert.ok(!mounts.some((mount) => mount.Target === faultDirectory));
  mounts.push({Type: 'bind', Source: source, Target: faultDirectory, ReadOnly: true});
  return true;
}

// Workerd exposes a Docker socket seam, but no bind-mount setting for a
// guest. Intercept only the selected local create and stream the rest.
export async function listenFaultDocker(socketPath, options, dockerSocket = '/run/docker.sock') {
  const server = createServer(async (incoming, reply) => {
    const headers = {...incoming.headers};
    let payload = null;
    try {
      if (incoming.method === 'POST' && /(?:^|\/)containers\/create(?:\?|$)/.test(incoming.url)) {
        payload = await buffer(incoming);
        const launch = JSON.parse(payload);
        if (mountFaultHook(launch, options())) {
          payload = Buffer.from(JSON.stringify(launch));
          delete headers['transfer-encoding'];
          headers['content-length'] = payload.length;
        }
      }
      const upstream = request({socketPath: dockerSocket, method: incoming.method, path: incoming.url, headers}, (response) => {
        reply.writeHead(response.statusCode, response.headers);
        response.pipe(reply);
      });
      upstream.on('error', () => reply.destroy());
      reply.on('close', () => upstream.destroy());
      if (payload === null) incoming.pipe(upstream);
      else upstream.end(payload);
    } catch {
      reply.writeHead(502).end();
    }
  });
  server.on('upgrade', (incoming, downstream, head) => {
    const upstream = request({
      socketPath: dockerSocket, method: incoming.method, path: incoming.url, headers: incoming.headers,
    });
    upstream.on('upgrade', (response, socket, rest) => {
      downstream.write('HTTP/1.1 ' + response.statusCode + ' ' + response.statusMessage + '\r\n'
        + response.rawHeaders.reduce((lines, value, index) => lines + value + (index % 2 ? '\r\n' : ': '), '')
        + '\r\n');
      if (rest.length) downstream.write(rest);
      if (head.length) socket.write(head);
      socket.on('error', () => downstream.destroy());
      downstream.on('error', () => socket.destroy());
      socket.pipe(downstream).pipe(socket);
    });
    upstream.on('response', () => downstream.destroy());
    upstream.on('error', () => downstream.destroy());
    upstream.end();
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, resolve);
  });
  return server;
}
