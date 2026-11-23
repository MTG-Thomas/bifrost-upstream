import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(new URL("../tenant-workers/package.json", import.meta.url));
const { Miniflare } = createRequire(require.resolve("wrangler"))("miniflare");
const registry = readFileSync(new URL("../../../../deploy/capacity/cloudflare/container-registry.mjs", import.meta.url), "utf8");

test("Miniflare SQLite preserves an unobserved start fence and its retirement", async (t) => {
    const mf = new Miniflare({
        cf: false,
        workers: [{ config: {
            name: "container-registry-conformance",
            compatibilityDate: "2026-10-30",
            exports: { Scope: { type: "durable-object", storage: "sqlite" } },
            manifest: { mainModule: "fixture.mjs", modules: {
                "container-registry.mjs": { type: "esm", contents: registry },
                "fixture.mjs": { type: "esm", contents: `
                    import { DurableObject } from "cloudflare:workers";
                    import { ScopeRegistry } from "./container-registry.mjs";
                    export class Scope extends DurableObject {
                        async fetch(request) {
                            const { operation, installation, image, args } = await request.json();
                            const scope = new ScopeRegistry(this.ctx, {
                                CONTAINERS_INSTALLATION: installation,
                                CONTAINERS_REVISION: 1,
                                CONTAINERS_CAP: 1,
                                CONTAINERS_IMAGE: image,
                                CONTAINERS_INGRESS_BYTES: 4096,
                                CONTAINERS_OUTBOUND_BYTES: 4096,
                            });
                            return Response.json(scope[operation](...args));
                        }
                    }
                    export default {
                        fetch(request, _env, ctx) {
                            return ctx.exports.Scope.getByName("fixture").fetch(request);
                        },
                    };
                ` },
            } },
        } }],
    });
    t.after(() => mf.dispose());
    const worker = await mf.getWorker("container-registry-conformance");
    const installation = crypto.randomUUID();
    const image = "sha256:" + "a".repeat(64);
    const instance = "b".repeat(64);
    const now = 100_000;
    const pin = () => ({
        installation, revision: 1, worker: crypto.randomUUID(), uid: crypto.randomUUID().replaceAll("-", ""),
        tenant: crypto.randomUUID(), image, instance: "standard-1", credentialVersion: 1,
        reservedAt: now, startDeadline: now + 1000, deadline: now + 123_000, seconds: 123,
        ingressBytes: 4096, outboundBytes: 4096,
    });
    const call = async (operation, ...args) => {
        const response = await worker.fetch("http://fixture.test", {
            method: "POST",
            body: JSON.stringify({ operation, installation, image, args }),
        });
        assert.equal(response.status, 200);
        return response.json();
    };
    const first = pin();
    const other = pin();
    const seats = await Promise.all([call("reserve", first, now), call("reserve", first, now), call("reserve", other, now)]);
    assert.equal(seats[0].status, seats[1].status);
    const [winner, loser] = seats[0].status === "reserved" ? [first, other] : [other, first];
    assert.equal(await call("reserve", loser, now).then(value => value.status), "full");
    assert.equal((await call("authorizeStart", winner.uid, instance, now, winner.deadline)).status, "authorized");
    // The injected scheduler never creates a placement after the first effect.
    assert.equal((await call("authorizeStart", winner.uid, instance, now, winner.deadline)).status, "pending");
    const observedAt = winner.startDeadline + 60_000;
    const closed = await call("closeAbsent", winner.uid, instance, observedAt, observedAt);
    assert.equal(closed.status, "absent");
    assert.equal((await call("reserve", loser, now)).status, "full");
    assert.equal((await call("retire", winner.uid, instance, closed.closedAt)).status, "retired");
    assert.equal((await call("reserve", loser, observedAt)).status, "closed");
    const later = {
        ...pin(), reservedAt: observedAt, startDeadline: observedAt + 1000, deadline: observedAt + 123_000,
    };
    assert.equal((await call("reserve", later, observedAt)).status, "reserved");
    assert.equal((await call("reserve", winner, winner.deadline)).status, "closed");
    assert.equal((await call("authorizeStart", winner.uid, instance, winner.deadline, winner.deadline)).status, "closed");
});
