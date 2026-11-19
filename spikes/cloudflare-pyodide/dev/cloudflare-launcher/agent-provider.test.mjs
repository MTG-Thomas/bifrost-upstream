import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {Miniflare} from 'miniflare';

const endpoint = 'https://agent-provider.invalid/v1';
const module = (contents) => ({type: 'esm', contents});
const source = async (path) => module(await readFile(new URL(path, import.meta.url), 'utf8'));

test('workerd private AgentProvider streams chunks and aborts its outbound body', async () => {
  const mf = new Miniflare({cf: false, workers: [{
    config: {name: 'launcher', compatibilityDate: '2026-10-30',
      env: {AGENT: {type: 'worker', worker: 'agent-provider', exportName: 'AgentProvider'}},
      manifest: {mainModule: 'launcher.mjs', modules: {
        'launcher.mjs': module(`
          import {agentStreamEvents} from './agent-provider.mjs';
          export default {
            async fetch(_request, env) {
              let close;
              const cancellation = new ReadableStream({type: 'bytes',
                start(controller) { close = () => controller.close(); },
              });
              const response = await env.AGENT.stream({
                endpoint: '${endpoint}', api_key: 'only-fixture-key', remaining_ms: 5000,
                body: {model: 'stub', stream: true, store: false, messages: []},
              }, cancellation);
              const events = agentStreamEvents(response);
              const event = (await events.next()).value;
              close();
              try {
                await events.next();
                return Response.json({event, aborted: false});
              } catch {
                return Response.json({event, aborted: true});
              }
            },
          };
        `),
        'agent-provider.mjs': await source('./agent-provider.mjs'),
      }},
    },
  }, {
    config: {name: 'agent-provider', compatibilityDate: '2026-10-30',
      env: {BIFROST_CLOUDFLARE_AGENT_ENDPOINT: {type: 'text', value: endpoint}},
      exports: {AgentProvider: {type: 'worker'}},
      manifest: {mainModule: 'agent-worker/entry.mjs', modules: {
        'agent-worker/entry.mjs': await source('./agent-worker/entry.mjs'),
        'agent-provider.mjs': await source('./agent-provider.mjs'),
      }},
    },
    dev: {outboundService: {type: 'worker', worker: 'stub'}},
  }, {
    config: {name: 'stub', compatibilityDate: '2026-10-30',
      manifest: {mainModule: 'stub.mjs', modules: {
        'stub.mjs': module(`
          export default {
            fetch(request) {
              if (request.url !== '${endpoint}/chat/completions' ||
                  request.headers.get('Authorization') !== 'Bearer only-fixture-key') {
                return new Response(null, {status: 401});
              }
              const stream = new ReadableStream({
                start(controller) {
                  controller.enqueue(new TextEncoder().encode(
                    'data: {"choices":[{"delta":{"content":"prefix"}}]}\\n\\n'));
                },
              });
              return new Response(stream, {headers: {'Content-Type': 'text/event-stream'}});
            },
          };
        `),
      }},
    },
  }]});
  try {
    const launcher = await mf.getWorker('launcher');
    const response = await launcher.fetch('http://fixture.invalid/', {signal: AbortSignal.timeout(8000)});
    assert.deepEqual(await response.json(), {event: {kind: 'delta', text: 'prefix'}, aborted: true});
  } finally {
    await mf.dispose();
  }
});
