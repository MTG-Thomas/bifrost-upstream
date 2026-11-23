import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { ContainerInstanceLifecycle, forwardContainerHttp, relayControlSockets, ScopeRegistry } from "./container-registry.mjs";

const policy = JSON.parse(readFileSync(new URL("../../../api/src/services/worker_ingress_routes.json", import.meta.url)));

function fixture(cap = 2, byteCap = 100) {
    const db = new DatabaseSync(":memory:");
    const storage = {
        sql: {
            exec(query, ...params) {
                const statement = db.prepare(query);
                if (!query.startsWith("SELECT")) statement.run(...params);
                return { toArray: () => statement.all(...params), one: () => statement.get(...params) };
            },
        },
        transactionSync(action) {
            db.exec("BEGIN IMMEDIATE");
            try {
                const result = action();
                db.exec("COMMIT");
                return result;
            } catch (error) {
                db.exec("ROLLBACK");
                throw error;
            }
        },
    };
    const installation = randomUUID();
    const image = `sha256:${"a".repeat(64)}`;
    const env = {
        CONTAINERS_INSTALLATION: installation, CONTAINERS_REVISION: "2",
        CONTAINERS_IMAGE: image, CONTAINERS_CAP: String(cap),
        CONTAINERS_INGRESS_BYTES: String(byteCap), CONTAINERS_OUTBOUND_BYTES: String(byteCap),
    };
    return {
        db, registry: () => new ScopeRegistry({ storage }, env),
        pin: (tenant = "platform") => ({
            tenant, installation, revision: 2, image, worker: randomUUID(), uid: randomUUID().replaceAll("-", ""),
            instance: "standard-1", credentialVersion: 1,
            reservedAt: 100, startDeadline: 101, deadline: 123_100, seconds: 123,
            ingressBytes: byteCap, outboundBytes: byteCap,
        }),
    };
}

test("a lost reply replays its original reservation across registry reset", () => {
    const { db, registry, pin } = fixture();
    const request = pin();
    assert.deepEqual(registry().reserve(request), { status: "reserved" });
    assert.deepEqual(registry().reserve(request), { status: "reserved" });
    assert.deepEqual(registry().reserve({ ...request, uid: pin().uid }), { status: "conflict" });
    assert.deepEqual(registry().reserve({ ...request, uid: randomUUID() }), { status: "invalid" });
    assert.deepEqual(registry().reserve({ ...request, credentialVersion: 2 }), { status: "conflict" });
    assert.deepEqual(registry().reserve({ ...request, worker: randomUUID() }), { status: "conflict" });
    assert.deepEqual(registry().reserve({ ...request, revision: 3 }), { status: "invalid" });
    assert.deepEqual(registry().reserve({ ...request, ingressBytes: 101 }), { status: "invalid" });
    assert.deepEqual(registry().reserve({ ...request, outboundBytes: 101 }), { status: "invalid" });
    assert.deepEqual(registry().reserve({ ...request, deadline: request.deadline + 1000 }), { status: "invalid" });
    db.close();
});

test("a prepared reservation tolerates a bounded lag in the actor clock", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const request = { ...pin(), reservedAt: 10_000, startDeadline: 11_000, deadline: 133_000 };
    const instance = "b".repeat(64);
    const tooEarly = 6999;
    const withinSkew = 7000;
    const scope = registry();
    assert.equal(scope.reserve(request).status, "reserved");
    assert.equal(scope.reserveForStart(request, instance, tooEarly).status, "closed");
    assert.equal(scope.authorizeStart(request.uid, instance, tooEarly, request.deadline).status, "conflict");
    assert.equal(scope.reserveForStart(request, instance, withinSkew).status, "reserved");
    assert.equal(scope.authorizeStart(request.uid, instance, withinSkew, request.deadline).status, "authorized");
    assert.equal(scope.chargeRelay(request.uid, instance, "ingress", 10, withinSkew), true);
    assert.equal(scope.chargeRelay(request.uid, instance, "ingress", 10, request.deadline), false);
    const placementStartedAt = 9500;
    assert.equal(scope.observeStart(request.uid, instance, tooEarly).status, "conflict");
    assert.equal(scope.observeStart(request.uid, instance, placementStartedAt).status, "observed");
    assert.equal(scope.closeStart(request.uid, instance, 6499).status, "conflict");
    const closed = {
        status: "closed", instance, authorizedAt: request.reservedAt, closedAt: request.reservedAt,
        startedAt: placementStartedAt, outboundBytes: 0, meterComplete: false,
    };
    assert.deepEqual(scope.closeStart(request.uid, instance, 8000), closed);
    assert.deepEqual(registry().closeStart(request.uid, instance, 12_000), closed);
});

test("unconfirmed instances retain the tenant seat and the last fleet seat is exclusive", async () => {
    const { db, registry, pin } = fixture();
    assert.equal(registry().reserve(pin()).status, "reserved");
    db.exec("UPDATE reservations SET state = 'unconfirmed'");
    assert.equal(registry().reserve(pin()).status, "full");
    const independent = [pin(randomUUID()), pin(randomUUID())];
    const results = await Promise.all(independent.map((request) => Promise.resolve().then(() => registry().reserve(request))));
    assert.deepEqual(results.map(({ status }) => status).sort(), ["full", "reserved"]);
    db.close();
});

test("a reset with a pending start cannot issue another start or close its fence", () => {
    const { db, registry, pin } = fixture(1);
    const request = pin();
    const instance = "b".repeat(64);
    const other = "c".repeat(64);
    assert.equal(registry().reserve(request).status, "reserved");
    assert.equal(registry().authorizeStart(request.uid, instance, 100, request.deadline).status, "authorized");
    assert.equal(registry().authorizeStart(request.uid, instance, 101, request.deadline).status, "pending");
    assert.equal(registry().authorizeStart(request.uid, other, 101, request.deadline).status, "conflict");
    assert.equal(registry().authorizeStart(request.uid, instance, 101, request.deadline + 1).status, "conflict");
    assert.equal(registry().authorizeStart(request.uid, instance, request.deadline, request.deadline).status, "closed");
    assert.equal(registry().closeStart(request.uid, instance, 105).status, "pending");
    assert.equal(registry().observeStart(request.uid, instance, 104).status, "observed");
    assert.equal(registry().observeStart(request.uid, instance, 105).status, "conflict");
    const closed = {
        status: "closed", instance, authorizedAt: 100, closedAt: 106,
        startedAt: 104, outboundBytes: 0, meterComplete: false,
    };
    assert.deepEqual(registry().closeStart(request.uid, instance, 106), closed);
    assert.deepEqual(registry().closeStart(request.uid, instance, 180), closed);
    assert.equal(registry().authorizeStart(request.uid, instance, 180, request.deadline).status, "closed");
    assert.equal(registry().reserve(pin(randomUUID())).status, "full");
    db.close();
});

test("only acknowledgement of the exact settled fence releases the one-seat registry", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const second = pin();
    const instance = "b".repeat(64);
    assert.equal(registry().reserve(first).status, "reserved");
    assert.equal(registry().authorizeStart(first.uid, instance, 100, first.deadline).status, "authorized");
    assert.equal(registry().retire(first.uid, instance, 104).status, "pending");
    assert.equal(registry().observeStart(first.uid, instance, 103).status, "observed");
    const receipt = registry().closeStart(first.uid, instance, 104);
    assert.equal(registry().reserve(second).status, "full");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt + 1).status, "conflict");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt).status, "retired");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt).status, "retired");
    assert.equal(registry().reserve(first).status, "retired");
    assert.equal(registry().reserve(second).status, "reserved");
    assert.equal(registry().authorizeStart(second.uid, "c".repeat(64), 100, second.deadline).status, "authorized");
});

class Socket extends EventTarget {
    sent = [];
    closed = [];
    send(data) { this.sent.push(data); }
    close(code) { this.closed.push(code); }
    frame(data) { this.dispatchEvent(new MessageEvent("message", { data })); }
}

test("an expired unstarted seat is voided after a reset and can never start", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const next = pin();
    const missing = pin();
    const instance = "b".repeat(64);
    assert.equal(registry().reserve(first).status, "reserved");
    assert.equal(registry().reserve(next).status, "full");
    assert.equal(registry().voidUnstarted(first, instance, first.startDeadline - 1).status, "pending");
    const receipt = { status: "void", uid: first.uid, instance, startDeadline: first.startDeadline, voidedAt: first.startDeadline };
    assert.deepEqual(registry().voidUnstarted(first, instance, first.startDeadline), receipt);
    assert.deepEqual(registry().voidUnstarted(first, instance, first.startDeadline + 1), receipt);
    assert.equal(registry().voidUnstarted(first, "f".repeat(64), first.startDeadline + 1).status, "conflict");
    assert.equal(registry().authorizeStart(first.uid, instance, first.reservedAt, first.deadline).status, "closed");
    assert.equal(registry().reserve(next).status, "reserved");
    assert.equal(registry().authorizeStart(next.uid, instance, next.reservedAt, next.deadline).status, "authorized");
    assert.equal(registry().voidUnstarted(next, instance, next.startDeadline).status, "conflict");
    assert.equal(registry().voidUnstarted(missing, "d".repeat(64), missing.startDeadline).status, "void");
    assert.equal(registry().reserve(missing).status, "retired");
});

test("an allocation refusal is durable and cannot cross the start fence", async (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const rejected = pin();
    const instance = "b".repeat(64);
    assert.equal(registry().reserve(first).status, "reserved");
    const receipt = registry().reserveForStart(rejected, instance, rejected.reservedAt);
    assert.deepEqual(receipt, {
        status: "allocation-refused", uid: rejected.uid, instance,
        startDeadline: rejected.startDeadline, voidedAt: rejected.reservedAt,
    });
    assert.equal(registry().voidUnstarted(first, "d".repeat(64), first.startDeadline).status, "void");
    assert.deepEqual(registry().reserve(rejected), receipt);
    assert.equal(registry().authorizeStart(rejected.uid, instance, rejected.reservedAt, rejected.deadline).status, "closed");
    assert.equal(registry().reserve(pin()).status, "reserved");
});

test("the instance lifecycle pins the intercept and launch options, then stops at the durable deadline", async (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    t.mock.timers.enable({ apis: ["Date"], now: 100 });
    const slot = pin();
    const actor = "b".repeat(64);
    const stored = new Map();
    const events = [];
    const intercepted = {};
    const origin = "https://ingress.example.test";
    const image = `registry.cloudflare.com/${"a".repeat(32)}/bifrost-${slot.installation.replaceAll("-", "")}-r2@${slot.image}`;
    const container = {
        running: false,
        async interceptOutboundHttps(host, binding) {
            assert.equal(host, "ingress.example.test");
            assert.equal(binding, intercepted);
            events.push("intercept");
        },
        start(options) {
            assert.equal(options.image, image);
            assert.equal(options.instance, "standard-1");
            assert.equal(options.enableInternet, false);
            assert.equal(options.env.BIFROST_API_URL, origin);
            assert.equal(options.env.BIFROST_WORKER_CONTROL_SECRET, "f".repeat(64));
            assert.equal(options.env.BIFROST_WORKER_RESOURCE_UID, slot.uid);
            assert.equal(options.env.BIFROST_CONTAINER_PROBE_NONCE, "n".repeat(43));
            assert.equal(options.env.BIFROST_CONTAINER_DEADLINE_MS, String(slot.deadline));
            assert.equal(options.env.BIFROST_CONTAINER_IDLE_SECONDS, "120");
            assert.equal(options.env.BIFROST_SECRET_KEY, undefined);
            assert.equal(options.env.BIFROST_DATABASE_URL, undefined);
            assert.equal(stored.get("deadline"), slot.deadline);
            assert.equal(db.prepare("SELECT instance FROM start_fences WHERE uid = ?").get(slot.uid).instance, actor);
            this.running = true;
            events.push("start");
        },
        async setInactivityTimeout(ms) {
            assert.ok(ms > 420_000);
            assert.ok(Date.now() + ms > slot.deadline);
            events.push("idle");
        },
        async destroy() {
            assert.equal(this.running, true);
            this.running = false;
            events.push("destroy");
        },
    };
    const ctx = {
        container, id: actor, blockConcurrencyWhile: async (action) => action(),
        storage: {
            get: async (key) => stored.get(key), put: async (key, value) => stored.set(key, value),
            setAlarm: async (when) => stored.set("deadline", when),
        },
    };
    const controller = () => new ContainerInstanceLifecycle(ctx, registry(), {
        origin, image, release: "c".repeat(64), intercept: intercepted,
    });
    assert.deepEqual(await controller().start(slot, "f".repeat(64), "n".repeat(43)), { status: "invalid" });
    container.images = { "bifrost-linux": image.replace(slot.image, "sha256:" + "e".repeat(64)) };
    assert.deepEqual(await controller().start(slot, "f".repeat(64), "n".repeat(43)), { status: "invalid" });
    assert.equal(db.prepare("SELECT count(*) AS count FROM reservations").get().count, 0);
    container.images = { "bifrost-linux": image };
    assert.deepEqual(await controller().start(slot, "f".repeat(64), "n".repeat(43)), { status: "pending" });
    assert.deepEqual(await controller().start(slot, "f".repeat(64), "n".repeat(43)), { status: "pending" });
    assert.equal(JSON.stringify([...stored]).includes("f".repeat(64)), false);
    assert.deepEqual(await controller().stop(pin().uid), { status: "conflict" });
    await controller().restore();
    stored.delete("deadline");
    await controller().alarm();
    assert.equal(stored.get("deadline"), slot.deadline);
    t.mock.timers.setTime(slot.deadline);
    await controller().alarm();
    assert.deepEqual(await controller().stop(slot.uid), { status: "pending" });
    assert.deepEqual(events, ["intercept", "start", "idle", "idle", "destroy"]);
    assert.equal(registry().closeStart(slot.uid, actor, Date.now()).status, "pending");
});

test("a registry authorization survives a local deadline advance; a throwing start can close absent", async (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    t.mock.timers.enable({ apis: ["Date"], now: 100 });
    const slot = pin();
    const actor = "b".repeat(64);
    const stored = new Map();
    let starts = 0;
    const image = `registry.cloudflare.com/${"a".repeat(32)}/bifrost-${slot.installation.replaceAll("-", "")}-r2@${slot.image}`;
    const scope = registry();
    const controller = () => new ContainerInstanceLifecycle({
        id: actor,
        blockConcurrencyWhile: async (action) => action(),
        storage: {
            get: async (key) => stored.get(key), put: async (key, value) => stored.set(key, value),
            setAlarm: async () => {},
        },
        container: {
            running: false,
            images: { "bifrost-linux": image },
            interceptOutboundHttps() {},
            start() { starts++; throw new Error("injected allocation error"); },
            destroy() { assert.fail("No running placement to destroy"); },
        },
    }, {
        reserveForStart: (...args) => scope.reserveForStart(...args),
        async authorizeStart(...args) {
            const result = scope.authorizeStart(...args);
            t.mock.timers.setTime(slot.startDeadline + 1);
            return result;
        },
        closeAbsent: (...args) => scope.closeAbsent(...args),
    }, {
        origin: "https://ingress.example.test", intercept: {}, release: "c".repeat(64),
        image,
    });
    assert.equal((await controller().start(slot, "f".repeat(64))).status, "pending");
    assert.equal((await controller().start(slot, "f".repeat(64))).status, "pending");
    assert.equal(starts, 1);
    assert.equal((await controller().closeAbsent(slot.uid, Date.now())).status, "pending");
    t.mock.timers.setTime(slot.startDeadline + 60_000);
    const receipt = await controller().closeAbsent(slot.uid, Date.now());
    assert.deepEqual(receipt, {
        status: "absent", uid: slot.uid, instance: actor, authorizedAt: 100,
        startDeadline: slot.startDeadline, closedAt: Date.now(),
    });
    assert.equal(scope.observeStart(slot.uid, actor, 101).status, "conflict");
    assert.equal(scope.reserve(pin()).status, "full");
    assert.equal(scope.retire(slot.uid, actor, receipt.closedAt).status, "retired");
    assert.equal(scope.reserve(pin()).status, "reserved");
});

test("interceptors refuse asynchronous registry proofs", async () => {
    const guest = new Socket();
    const upstream = new Socket();
    const registry = { relayDeadline: async () => Date.now() + 1000, chargeRelay: async () => true };
    relayControlSockets(registry, "uid", "instance", guest, upstream);
    guest.frame("no relay");
    assert.deepEqual(guest.closed, [1008]);
    assert.deepEqual(upstream.sent, []);
    const response = await forwardContainerHttp(
        registry, "uid", "instance", new Request("https://ingress.example.test/api/sdk/config/get"),
        "https://ingress.example.test", {}, () => assert.fail("Asynchronous charges cannot authorize egress"),
    );
    assert.equal(response.status, 429);
});

test("the control relay preserves fragments and fences both directions across reconnects", (t) => {
    const { db, registry, pin } = fixture();
    t.after(() => db.close());
    t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 100 });
    const instance = "b".repeat(64);
    const fragments = ['{"fragment":0,"data":"é"}', '{"fragment":1,"data":"β"}'];
    const fragmentBytes = new TextEncoder().encode(fragments.join("")).byteLength;
    const uid = { ...pin(), ingressBytes: 3, outboundBytes: fragmentBytes };
    assert.equal(registry().reserve(uid).status, "reserved");
    assert.equal(registry().authorizeStart(uid.uid, instance, Date.now(), uid.deadline).status, "authorized");
    t.mock.timers.tick(1);
    let guest = new Socket();
    let ingress = new Socket();
    relayControlSockets(registry(), uid.uid, instance, guest, ingress);
    guest.frame(fragments[0]);
    assert.deepEqual(ingress.sent, [fragments[0]]);
    ingress.frame("é");
    assert.deepEqual(guest.sent, ["é"]);
    guest.dispatchEvent(new Event("close"));
    guest = new Socket();
    ingress = new Socket();
    relayControlSockets(registry(), uid.uid, instance, guest, ingress);
    guest.frame(fragments[1]);
    assert.deepEqual(ingress.sent, [fragments[1]]);
    ingress.frame("é");
    assert.deepEqual(guest.sent, []);
    assert.deepEqual(guest.closed, [1008]);
    assert.deepEqual(ingress.closed, [1008]);
    guest = new Socket();
    relayControlSockets(registry(), uid.uid, instance, guest, new Socket());
    assert.deepEqual(guest.closed, [1008]);
    assert.equal(registry().observeStart(uid.uid, instance, 101).status, "observed");
    assert.equal(registry().closeStart(uid.uid, instance, 102).outboundBytes, fragmentBytes);
});

test("a silent relay closes at the pinned deadline", (t) => {
    const { db, registry, pin } = fixture();
    t.after(() => db.close());
    t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 100 });
    const request = pin();
    const instance = "b".repeat(64);
    registry().reserve(request);
    registry().authorizeStart(request.uid, instance, Date.now(), request.deadline);
    const guest = new Socket();
    const ingress = new Socket();
    relayControlSockets(registry(), request.uid, instance, guest, ingress);
    t.mock.timers.tick(request.deadline - Date.now());
    guest.frame("late");
    assert.deepEqual(guest.closed, [1008]);
    assert.deepEqual(ingress.closed, [1008]);
    assert.deepEqual(ingress.sent, []);
});

test("HTTP interception forwards only the exact ingress and shares the bidirectional meter", async (t) => {
    const { db, registry, pin } = fixture(1, 4096);
    t.after(() => db.close());
    const reservedAt = Date.now();
    const slot = { ...pin(), reservedAt, startDeadline: reservedAt + 1000, deadline: reservedAt + 123_000 };
    const instance = "b".repeat(64);
    const origin = "https://ingress.example.test";
    const path = "/api/sdk/config/get";
    const scope = registry();
    assert.equal(scope.reserve(slot).status, "reserved");
    assert.equal(scope.authorizeStart(slot.uid, instance, reservedAt, slot.deadline).status, "authorized");
    const calls = [];
    const upstream = async (url, options) => {
        calls.push(String(url));
        assert.equal(options.redirect, "manual");
        assert.equal(options.headers.get("authorization"), "Bearer fixture");
        assert.equal(options.headers.get("x-forwarded-host"), null);
        assert.equal(await new Response(options.body).text(), "{}");
        return new Response("fixture response");
    };
    const visit = (target, host) => forwardContainerHttp(scope, slot.uid, instance, new Request(target, {
        method: "POST", headers: {
            host, authorization: "Bearer fixture", "x-forwarded-host": "unsafe",
            "x-fixture-extra": "x".repeat(128), "content-length": "2",
        }, body: "{}",
    }), origin, policy, upstream);
    assert.equal((await visit("https://elsewhere.example.test" + path, "ingress.example.test")).status, 403);
    assert.equal((await visit(origin + path, "elsewhere.example.test")).status, 403);
    assert.equal((await visit(origin + "/api/users/me/permissions", "ingress.example.test")).status, 403);
    assert.equal((await visit(origin + "/api/sdk/%63onfig/get", "ingress.example.test")).status, 403);
    assert.deepEqual(calls, []);
    const pending = db.prepare("SELECT outbound, ingress FROM start_fences WHERE uid = ?").get(slot.uid);
    assert.ok(pending.outbound >= 4 * 2 * (128 + 2));
    assert.ok(pending.ingress > 0);
    const reply = await visit(origin + path, "ingress.example.test");
    assert.equal(await reply.text(), "fixture response");
    assert.deepEqual(calls, [origin + path]);
    const tooLarge = await forwardContainerHttp(scope, slot.uid, instance, new Request(origin + path, {
        method: "POST", headers: { host: "ingress.example.test", "content-length": "4096" }, body: "{}",
    }), origin, policy, upstream);
    assert.equal(tooLarge.status, 429);
    assert.ok(db.prepare("SELECT outbound FROM start_fences WHERE uid = ?").get(slot.uid).outbound >= 2 * 4096);
    assert.equal(scope.chargeRelay(slot.uid, instance, "ingress", 1, Date.now()), false);
});

test("HTTP interception refuses redirects and stops streaming an oversized response", async (t) => {
    const { db, registry, pin } = fixture(1, 4096);
    t.after(() => db.close());
    const reservedAt = Date.now();
    const slot = { ...pin(), reservedAt, startDeadline: reservedAt + 1000, deadline: reservedAt + 123_000 };
    const instance = "b".repeat(64);
    const origin = "https://ingress.example.test";
    const scope = registry();
    scope.reserve(slot);
    scope.authorizeStart(slot.uid, instance, reservedAt, slot.deadline);
    const visit = (upstream) => forwardContainerHttp(scope, slot.uid, instance, new Request(
        origin + "/api/organizations", { headers: { host: "ingress.example.test" } },
    ), origin, policy, upstream);
    const redirect = await visit(async () => Response.redirect("https://elsewhere.example.test/"));
    assert.equal(redirect.status, 403);
    assert.equal(redirect.headers.get("location"), null);
    const large = await visit(async () => new Response("x".repeat(4097)));
    await assert.rejects(large.text(), /budget exhausted/);
    assert.equal(scope.relayDeadline(slot.uid, instance), null);
});
