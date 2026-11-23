// Native workerd and Docker exercise the actual egress Worker and registry.
// The only fixture is the upstream worker ingress and the local image reference.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";

const [image, proxy] = process.argv.slice(2);
assert.match(image, /^bifrost-containers-test:[a-f0-9]{12}-\d+$/);
assert.match(proxy, /^bifrost-containers-proxy:[a-f0-9]{12}-\d+$/);
const origin = "https://ingress.example.test";
const scope = "containers-conformance";
const ingress = "test-ingress";
const installation = randomUUID();
const digest = "sha256:" + "a".repeat(64);

const registry = await readFile("/fixture/container-registry.mjs", "utf8");
const scopeBundle = await readFile("/fixture/container-scope.mjs", "utf8");
const policy = await readFile("/fixture-routes.json", "utf8");
const guest = `
import asyncio
import ssl
import sys
import httpx
import websockets

checkpoint = 10
async def exercise():
    global checkpoint
    origin = "${origin}"
    trust = ssl.create_default_context(cafile="/etc/cloudflare/certs/cloudflare-containers-ca.crt")
    async with websockets.connect("wss://ingress.example.test/api/worker-control", ssl=trust) as ws:
        await ws.send("fixture-frame")
        assert await ws.recv() == "fixture-frame"
    checkpoint = 11
    async with httpx.AsyncClient(verify=trust, trust_env=False, follow_redirects=False) as client:
        path = "/api/sdk/capacity-canary/00000000-0000-0000-0000-000000000001"
        response = await client.get(origin + path)
        assert response.status_code == 204 and response.headers["x-bifrost-worker-ingress"] == "v1; allowed"
        checkpoint = 12
        response = await client.get(origin + path, headers={"Host": "untrusted.example.test"})
        assert response.status_code == 403 and response.headers["x-bifrost-container-relay-rejection"] == "origin"
        checkpoint = 13
        response = await client.get(origin + "/unapproved")
        assert response.status_code == 403 and response.headers["x-bifrost-container-relay-rejection"] == "route"
        checkpoint = 14
        response = await client.get(origin + path, headers={"X-Bifrost-Canary-Redirect": "1"})
        assert response.status_code == 403 and response.headers["x-bifrost-container-relay-rejection"] == "redirect"
        assert "location" not in response.headers

try:
    asyncio.run(exercise())
except Exception:
    sys.exit(checkpoint)
`;
const source = registry + scopeBundle.replace("export default {", "const scopeControl = {") + `
export class Fixture extends DurableObject {
    async run() {
        const registry = this.env.CONTAINER_REGISTRY.getByName("scope");
        const uid = crypto.randomUUID().replaceAll("-", ""), instance = this.ctx.id.toString(), now = Date.now();
        const pin = {
            installation: this.env.CONTAINERS_INSTALLATION, revision: 1, worker: crypto.randomUUID(),
            uid, image: this.env.CONTAINERS_IMAGE, tenant: "qualification-canary", instance: "standard-1",
            credentialVersion: 1, reservedAt: now, startDeadline: now + 10_000, deadline: now + 123_000,
            seconds: 123, ingressBytes: 100_000, outboundBytes: 100_000,
        };
        if ((await registry.reserveForStart(pin, instance)).status !== "reserved"
            || (await registry.authorizeStart(uid, instance, Date.now(), pin.deadline)).status !== "authorized") {
            throw new Error("Fixture could not reserve the exact egress meter");
        }
        const container = this.ctx.container;
        await container.interceptOutboundHttps("ingress.example.test", this.ctx.exports.ContainerIngress({
            props: { uid, instance },
        }));
        container.start({ image: container.images.local, enableInternet: false,
            entrypoint: ["/usr/local/bin/python", "-c", ${JSON.stringify(guest)}] });
        try {
            await container.monitor();
            return await registry.meteredEgress(uid, instance);
        } finally {
            if (container.running) await container.destroy();
        }
    }
}
export default {
    async fetch(request, env) {
        if (new URL(request.url).pathname !== "/fixture") return scopeControl.fetch(request, env);
        return new Response(null, { status: await env.FIXTURE.getByName("native").run() ? 204 : 409 });
    }
};
`;
const upstream = `
export default { fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/api/worker-control" && request.headers.get("Upgrade") === "websocket") {
        const [guest, relay] = Object.values(new WebSocketPair());
        relay.accept();
        relay.addEventListener("message", (event) => relay.send(event.data));
        return new Response(null, { status: 101, webSocket: guest });
    }
    if (!path.startsWith("/api/sdk/capacity-canary/")) return new Response(null, { status: 404 });
    if (request.headers.get("x-bifrost-canary-redirect") === "1") {
        return new Response(null, { status: 307, headers: { Location: "/unapproved" } });
    }
    return new Response(null, { status: 204, headers: { "x-bifrost-worker-ingress": "v1; allowed" } });
} };
`;
const manifest = (contents) => ({
    mainModule: "fixture.mjs", modules: { "fixture.mjs": { type: "esm", contents } },
});
const mf = new Miniflare({
    cf: false, containerEngine: { localDocker: {
        socketPath: "unix:///run/docker.sock", containerEgressInterceptorImage: proxy,
    } },
    workers: [{
        dev: { outboundService: { type: "worker", worker: ingress } },
        config: {
            name: scope, compatibilityDate: "2026-10-30",
            exports: {
                ContainerScopeRegistry: { type: "durable-object", storage: "sqlite" },
                ContainerIngress: { type: "worker" },
                Fixture: { type: "durable-object", storage: "sqlite", container: { images: [{ name: "local", image }] } },
            },
            env: {
                ...Object.fromEntries(Object.entries({
                    CONTAINERS_INSTALLATION: installation, CONTAINERS_REVISION: "1", CONTAINERS_IMAGE: digest,
                    CONTAINERS_CAP: "2", CONTAINERS_INGRESS_BYTES: "100000", CONTAINERS_OUTBOUND_BYTES: "100000",
                    CONTAINERS_ORIGIN: origin, CONTAINERS_INGRESS_POLICY: policy,
                }).map(([name, value]) => [name, { type: "text", value }])),
                FIXTURE: { type: "durable-object", worker: scope, exportName: "Fixture" },
                CONTAINER_REGISTRY: { type: "durable-object", worker: scope, exportName: "ContainerScopeRegistry" },
            },
            manifest: manifest(source),
        },
    }, { config: { name: ingress, compatibilityDate: "2026-10-30", manifest: manifest(upstream) } }],
});
try {
    const worker = await mf.getWorker(scope);
    assert.equal((await worker.fetch("https://scope.example.test/fixture")).status, 204);
    console.log("Native Container HTTPS, WebSocket and rejection witnesses passed");
} finally {
    await mf.dispose();
}
