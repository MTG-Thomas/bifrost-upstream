import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { once } from "node:events";
import test from "node:test";

const require = createRequire(new URL("./registry-toolchain/package.json", import.meta.url));
const { Miniflare } = require("miniflare");
const { WebSocket } = require("ws");
const registry = await readFile(new URL("./container-registry.mjs", import.meta.url), "utf8");
const standard = [1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014];
const source = `
import { relayControlSockets } from "./container-registry.mjs";
const codes = [...${JSON.stringify(standard)}, ...Array.from({length: 2000}, (_, i) => i + 3000)];
export default { fetch(request) {
    if (new URL(request.url).pathname === "/validate") {
        const rejected = [];
        for (const code of codes) {
            const [client, server] = Object.values(new WebSocketPair());
            client.accept(); server.accept();
            try { server.close(code); } catch { rejected.push(code); }
            client.close(1000);
        }
        return Response.json(rejected);
    }
    const code = Number(new URL(request.url).pathname.slice(1));
    const [api, ingress] = Object.values(new WebSocketPair());
    const [client, guest] = Object.values(new WebSocketPair());
    ingress.accept(); guest.accept(); client.accept();
    relayControlSockets({relayDeadline: () => Date.now() + 15_000, chargeRelay: () => true},
        "uid", "instance", guest, ingress);
    client.addEventListener("message", () => client.close(code, "fixture-secret"));
    // Let the Node client trigger the peer's real workerd close event.
    ingress.addEventListener("message", () => guest.send("close"));
    return new Response(null, {status: 101, webSocket: api});
} };
`;

test("workerd accepts every relay code and forwards representative close frames", { timeout: 20_000 }, async () => {
    const miniflare = new Miniflare({
        cf: false,
        workers: [{ config: {
            name: "close-validation", compatibilityDate: "2026-10-30",
            manifest: {
                mainModule: "fixture.mjs", modules: {
                    "fixture.mjs": { type: "esm", contents: source },
                    "container-registry.mjs": { type: "esm", contents: registry },
                },
            },
        } }],
    });
    try {
        const url = await miniflare.ready;
        const worker = await miniflare.getWorker("close-validation");
        const accepted = await worker.fetch("https://fixture.example.test/validate");
        assert.deepEqual(await accepted.json(), []);
        for (const code of [...standard, 3000, 3999, 4000, 4999]) {
            const socket = new WebSocket(new URL(`/${code}`, url).href.replace(/^http/, "ws"));
            try {
                await once(socket, "open");
                const closed = once(socket, "close");
                socket.send("close");
                const [seen, reason] = await closed;
                assert.equal(seen, code);
                assert.equal(reason.toString(), "Container relay peer closed");
            } finally {
                socket.terminate();
            }
        }
    } finally {
        await miniflare.dispose();
    }
});
