// Run the existing capacity HTTP suite against the Docker seam. This command
// uses the image of an already-owned disposable debug stack. The fixture
// process stays local and does not enroll or claim Bifrost work.
import {execFile, execFileSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {resolve} from 'node:path';
import {promisify} from 'node:util';
import {localLinuxProvider, serveContainersProviders} from './containers-local.mjs';

const [project] = process.argv.slice(2);
if (!/^bifrost-debug-[a-f0-9]+$/.test(project ?? '') || !process.cwd().split('/').at(-1).startsWith('containers-')) {
  throw new Error('Pass the project of the owned Containers debug archive');
}
const archive = process.cwd();
const worker = project + '-worker-1';
const image = execFileSync('docker', ['inspect', '--format', '{{.Config.Image}}', worker],
  {encoding: 'utf8', timeout: 5000}).trim();
const scope = 'containers-protocol';
const token = randomBytes(32).toString('hex');
const provider = localLinuxProvider({project, image, scope, token, version: 'protocol-fixture', archive, protocolOnly: true});
const host = readFileSync('.phase6-host', 'utf8');
const server = await serveContainersProviders(host, {[scope]: provider});
const controls = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://127.0.0.1');
    const workerId = url.searchParams.get('worker_id');
    if (url.pathname === '/restart' && url.searchParams.get('scope') === scope) {
      provider.restart();
    } else if (/^[0-9a-f-]{36}$/.test(workerId ?? '')) {
      if (url.pathname === '/observe') return res.end(JSON.stringify(provider.observe(workerId)));
      if (url.pathname !== '/stop') return res.writeHead(404).end();
      await provider.stop(workerId);
    } else {
      return res.writeHead(400).end();
    }
    res.end('{}');
  } catch {
    res.writeHead(502).end();
  }
});
try {
  await new Promise((ready, reject) => {
    controls.once('error', reject);
    controls.listen(0, '127.0.0.1', ready);
  });
  const suite = 'deploy/capacity/conformance';
  const {stdout} = await promisify(execFile)('python3', [
    suite + '/conformance.py',
    '--base-url', `https://${host}:${server.address().port}/${scope}`,
    '--scope', scope, '--hook-command', `python3 ${suite}/hooks/control_http.py`,
    '--deadline', '90', '--json-out', '.containers-conformance.json',
  ], {
    env: {
      ...process.env, CAPACITY_TOKEN: token, SSL_CERT_FILE: resolve('.phase6-ca.pem'),
      CONTROL_URL: `http://127.0.0.1:${controls.address().port}`,
    },
    timeout: 100_000,
  });
  process.stdout.write(stdout);
} finally {
  controls.close();
  server.close();
  provider.close();
}
