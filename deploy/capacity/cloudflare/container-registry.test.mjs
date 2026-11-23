import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test, { mock } from "node:test";
import { ContainerInstanceLifecycle, dispatchContainerControl, forwardContainerHttp, relayControlSockets, ScopeRegistry } from "./container-registry.mjs";

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

function withClock(now, operation) {
    const clock = mock.method(Date, "now", () => now);
    try {
        return operation();
    } finally {
        clock.mock.restore();
    }
}

function reserve(scope, pin, now = pin.reservedAt) {
    return withClock(now, () => scope.reserve(pin));
}

test("the Container transport only dispatches a fresh, certified incarnation request", async (t) => {
    const { db, pin, registry } = fixture();
    t.after(() => db.close());
    const scope = generateKeyPairSync("ed25519");
    const incarnation = generateKeyPairSync("ed25519");
    const rawPublic = key => key.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    const key = rawPublic(incarnation.publicKey);
    const reservation = pin();
    const env = {
        CONTAINERS_INSTALLATION: reservation.installation,
        CONTAINERS_REVISION: String(reservation.revision),
        CONTAINERS_PUBLIC_KEY: rawPublic(scope.publicKey),
    };
    const body = JSON.stringify({ pin: reservation });
    const signValue = (identity, domain, value) =>
        sign(null, Buffer.from(domain + JSON.stringify(value)), identity.privateKey).toString("hex");
    const path = "/v1/stop";
    const now = Date.now();
    const nonce = "4".repeat(32);
    const expires = now + 60_000;
    const certificate = signValue(scope, "bifrost.containers.incarnation.v1\0",
        [Object.entries(reservation).sort(([a], [b]) => a < b ? -1 : Number(a > b)), key, expires]);
    const headers = (time, content = body, signingKey = incarnation, expiry = expires) => ({
        "X-Bifrost-Incarnation-Key": key,
        "X-Bifrost-Incarnation-Certificate": certificate,
        "X-Bifrost-Incarnation-Expires": String(expiry),
        "X-Bifrost-Request-Time": String(time),
        "X-Bifrost-Request-Nonce": nonce,
        "X-Bifrost-Request-Signature": signValue(signingKey, "bifrost.containers.request.v1\0",
            [path, time, expiry, nonce, createHash("sha256").update(content).digest("hex")]),
    });
    const changed = JSON.stringify({ pin: { ...reservation, deadline: reservation.deadline + 1 } });
    const request = (pathname, options) => new Request("https://containers.example.test" + pathname, options);
    let calls = 0;
    const invoke = (operation, data) => {
        assert.equal(operation, path);
        assert.deepEqual(data.pin, reservation);
        calls++;
        return new Response(null, { status: 202 });
    };
    const dispatch = input => dispatchContainerControl(input, env, (n, stamp) => registry().consumeControlNonce(n, stamp), invoke);
    assert.equal((await dispatch(request("/"))).status, 404);
    for (const [route, content, h] of [
        [path, body, {}],
        ["/v1/start", body, headers(now)],
        [path, body + " ", headers(now)],
        [path, body, headers(now - 60_000, body, incarnation, now)],
        [path, body, headers(now, body, generateKeyPairSync("ed25519"))],
        [path, changed, headers(now, changed)],
    ]) {
        assert.equal((await dispatch(request(route, { method: "POST", headers: h, body: content }))).status, 403);
    }
    assert.equal(calls, 0);
    assert.equal((await dispatch(request(path, { method: "POST", headers: headers(now), body }))).status, 202);
    assert.equal((await dispatch(request(path, { method: "POST", headers: headers(now), body }))).status, 403);
    assert.equal(calls, 1);
});

test("caller timestamps cannot prune reservations or void a seat early", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const next = pin();
    const scope = registry();
    const actor = "b".repeat(64);
    withClock(first.reservedAt, () => {
        assert.equal(scope.reserve(first).status, "reserved");
        assert.equal(scope.reserve(next, first.deadline).status, "full");
        assert.equal(scope.reserveForStart(next, actor, first.deadline).status, "allocation-refused");
        assert.equal(scope.voidUnstarted(first, actor, first.deadline).status, "pending");
        assert.equal(scope.reserve(first).status, "reserved");
    });
});

test("a lost reply replays its original reservation across registry reset", () => {
    const { db, registry, pin } = fixture();
    const request = pin();
    assert.deepEqual(reserve(registry(), request), { status: "reserved" });
    assert.deepEqual(reserve(registry(), request), { status: "reserved" });
    assert.deepEqual(reserve(registry(), { ...request, uid: pin().uid }), { status: "conflict" });
    assert.deepEqual(reserve(registry(), { ...request, uid: randomUUID() }), { status: "invalid" });
    assert.deepEqual(reserve(registry(), { ...request, credentialVersion: 2 }), { status: "conflict" });
    assert.deepEqual(reserve(registry(), { ...request, worker: randomUUID() }), { status: "conflict" });
    assert.deepEqual(reserve(registry(), { ...request, revision: 3 }), { status: "invalid" });
    assert.deepEqual(reserve(registry(), { ...request, ingressBytes: 101 }), { status: "invalid" });
    assert.deepEqual(reserve(registry(), { ...request, outboundBytes: 101 }), { status: "invalid" });
    assert.deepEqual(reserve(registry(), { ...request, deadline: request.deadline + 1000 }), { status: "invalid" });
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
    assert.equal(reserve(scope, request).status, "reserved");
    assert.equal(withClock(tooEarly, () => scope.reserveForStart(request, instance)).status, "closed");
    assert.equal(scope.authorizeStart(request.uid, instance, tooEarly, request.deadline).status, "conflict");
    assert.equal(withClock(withinSkew, () => scope.reserveForStart(request, instance)).status, "reserved");
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
    assert.equal(reserve(registry(), pin()).status, "reserved");
    db.exec("UPDATE reservations SET state = 'unconfirmed'");
    assert.equal(reserve(registry(), pin()).status, "full");
    const independent = [pin(randomUUID()), pin(randomUUID())];
    const results = await Promise.all(independent.map((request) => Promise.resolve().then(() => reserve(registry(), request))));
    assert.deepEqual(results.map(({ status }) => status).sort(), ["full", "reserved"]);
    db.close();
});

test("a reset with a pending start cannot issue another start or close its fence", () => {
    const { db, registry, pin } = fixture(1);
    const request = pin();
    const instance = "b".repeat(64);
    const other = "c".repeat(64);
    assert.equal(reserve(registry(), request).status, "reserved");
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
    assert.equal(reserve(registry(), pin(randomUUID())).status, "full");
    db.close();
});

test("only acknowledgement of the exact settled fence releases the one-seat registry", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const second = pin();
    const instance = "b".repeat(64);
    assert.equal(reserve(registry(), first).status, "reserved");
    assert.equal(registry().authorizeStart(first.uid, instance, 100, first.deadline).status, "authorized");
    assert.equal(registry().retire(first.uid, instance, 104).status, "pending");
    assert.equal(registry().observeStart(first.uid, instance, 103).status, "observed");
    const receipt = registry().closeStart(first.uid, instance, 104);
    assert.equal(reserve(registry(), second).status, "full");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt + 1).status, "conflict");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt).status, "retired");
    assert.equal(registry().retire(first.uid, instance, receipt.closedAt).status, "retired");
    assert.equal(reserve(registry(), first).status, "retired");
    assert.equal(reserve(registry(), second).status, "reserved");
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
    assert.equal(reserve(registry(), first).status, "reserved");
    assert.equal(reserve(registry(), next).status, "full");
    assert.equal(withClock(first.startDeadline - 1, () => registry().voidUnstarted(first, instance)).status, "pending");
    const receipt = { status: "void", uid: first.uid, instance, startDeadline: first.startDeadline, voidedAt: first.startDeadline };
    assert.deepEqual(withClock(first.startDeadline, () => registry().voidUnstarted(first, instance)), receipt);
    assert.deepEqual(withClock(first.startDeadline + 1, () => registry().voidUnstarted(first, instance)), receipt);
    assert.equal(withClock(first.startDeadline + 1, () => registry().voidUnstarted(first, "f".repeat(64))).status, "conflict");
    assert.equal(registry().authorizeStart(first.uid, instance, first.reservedAt, first.deadline).status, "closed");
    assert.equal(reserve(registry(), next).status, "reserved");
    assert.equal(registry().authorizeStart(next.uid, instance, next.reservedAt, next.deadline).status, "authorized");
    assert.equal(withClock(next.startDeadline, () => registry().voidUnstarted(next, instance)).status, "conflict");
    assert.equal(withClock(missing.startDeadline, () => registry().voidUnstarted(missing, "d".repeat(64))).status, "void");
    assert.equal(reserve(registry(), missing).status, "retired");
});

test("an allocation refusal is durable and cannot cross the start fence", async (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const first = pin();
    const rejected = pin();
    const instance = "b".repeat(64);
    assert.equal(reserve(registry(), first).status, "reserved");
    const receipt = withClock(rejected.reservedAt, () => registry().reserveForStart(rejected, instance));
    assert.deepEqual(receipt, {
        status: "allocation-refused", uid: rejected.uid, instance,
        startDeadline: rejected.startDeadline, voidedAt: rejected.reservedAt,
    });
    assert.equal(withClock(first.startDeadline, () => registry().voidUnstarted(first, "d".repeat(64))).status, "void");
    assert.deepEqual(reserve(registry(), rejected), receipt);
    assert.equal(registry().authorizeStart(rejected.uid, instance, rejected.reservedAt, rejected.deadline).status, "closed");
    assert.equal(reserve(registry(), pin()).status, "reserved");
});

test("registry prunes expired receipts across idle cycles and keeps unresolved start fences", (t) => {
    const { db, registry, pin } = fixture();
    t.after(() => db.close());

    const actor = "b".repeat(64);
    const unresolved = pin("platform");
    const initial = registry();
    assert.equal(reserve(initial, unresolved).status, "reserved");
    assert.equal(initial.authorizeStart(unresolved.uid, actor, unresolved.reservedAt, unresolved.deadline).status, "authorized");
    const at = (now, tenant) => ({
        ...pin(tenant), reservedAt: now, startDeadline: now + 1000, deadline: now + 123_000,
    });
    const count = (table) => db.prepare("SELECT count(*) AS count FROM " + table).get().count;
    const month = 31 * 24 * 60 * 60 * 1000;
    for (let cycle = 1; cycle <= 12; cycle++) {
        const scope = registry();
        const now = unresolved.deadline + cycle * month;
        const expiredUnstarted = at(now, "qualification-canary");
        const settled = at(expiredUnstarted.deadline, "qualification-canary");
        const denied = at(settled.reservedAt, "qualification-canary");
        assert.equal(reserve(scope, expiredUnstarted).status, "reserved");
        assert.equal(reserve(scope, settled).status, "reserved");
        assert.equal(scope.authorizeStart(settled.uid, actor, settled.reservedAt, settled.deadline).status, "authorized");
        assert.equal(withClock(denied.reservedAt, () => scope.reserveForStart(denied, actor)).status, "allocation-refused");
        const start = settled.reservedAt + 1;
        assert.equal(scope.observeStart(settled.uid, actor, start).status, "observed");
        const receipt = scope.closeStart(settled.uid, actor, start + 1);
        assert.equal(scope.retire(settled.uid, actor, receipt.closedAt).status, "retired");
        assert.equal(reserve(scope, settled, settled.deadline - 1).status, "retired");
        assert.equal(reserve(scope, settled, settled.deadline).status, "closed");
        assert.equal(withClock(settled.deadline, () => scope.reserveForStart(denied, actor)).status, "closed");
        assert.equal(scope.authorizeStart(settled.uid, actor, settled.deadline, settled.deadline).status, "closed");
        const recovered = {
            status: "void", uid: expiredUnstarted.uid, instance: actor,
            startDeadline: expiredUnstarted.startDeadline, voidedAt: expiredUnstarted.deadline,
        };
        assert.deepEqual(withClock(settled.deadline, () => scope.voidUnstarted(expiredUnstarted, actor)), recovered);
        assert.deepEqual(withClock(settled.deadline + 1000, () => scope.voidUnstarted(expiredUnstarted, actor)), recovered);
        assert.equal(withClock(settled.deadline, () => scope.voidUnstarted(unresolved, actor)).status, "conflict");
        assert.equal(count("reservations"), 1);
        assert.equal(count("start_fences"), 1);
    }
});

test("the registry limits recent receipts without dropping a live fence", (t) => {
    const { db, registry, pin } = fixture(1);
    t.after(() => db.close());
    const scope = registry();
    const actor = "b".repeat(64);
    const first = pin();
    assert.equal(reserve(scope, first).status, "reserved");
    assert.equal(scope.authorizeStart(first.uid, actor, first.reservedAt, first.deadline).status, "authorized");
    const rejected = pin();
    const receipt = withClock(rejected.reservedAt, () => scope.reserveForStart(rejected, actor));
    assert.equal(receipt.status, "allocation-refused");
    for (let index = 2; index < 1024; index++) {
        const pending = pin();
        assert.equal(withClock(pending.reservedAt, () => scope.reserveForStart(pending, actor)).status, "allocation-refused");
    }
    const overflow = pin();
    assert.equal(withClock(overflow.reservedAt, () => scope.reserveForStart(overflow, actor)).status, "receipt-limit");
    assert.deepEqual(reserve(scope, rejected), receipt);
    assert.equal(withClock(overflow.startDeadline, () => scope.voidUnstarted(overflow, actor)).status, "pending");
    assert.equal(db.prepare("SELECT count(*) AS count FROM reservations").get().count, 1024);
    assert.equal(withClock(overflow.deadline, () => scope.voidUnstarted(overflow, actor)).status, "void");
    assert.equal(db.prepare("SELECT count(*) AS count FROM reservations").get().count, 1);
    assert.equal(db.prepare("SELECT count(*) AS count FROM start_fences").get().count, 1);
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
    assert.deepEqual(events, ["intercept", "start", "idle", "idle", "idle", "destroy"]);
    assert.equal(registry().closeStart(slot.uid, actor, Date.now()).status, "pending");
});

test("a long-lived placement renews provider inactivity before six hours and hard-stops at its deadline", async (t) => {
    t.mock.timers.enable({ apis: ["Date"], now: 100 });
    const hour = 3_600_000;
    const ticket = { uid: "b".repeat(32), deadline: 100 + 8 * hour };
    const delays = [];
    const alarms = [];
    let running = true;
    const lifecycle = new ContainerInstanceLifecycle({
        id: "b".repeat(64), blockConcurrencyWhile: async (action) => action(),
        storage: {
            get: async () => ticket,
            setAlarm: async (at) => alarms.push(at),
            put: async (_key, value) => assert.equal(value.destroyed, true),
        },
        container: {
            get running() { return running; },
            async setInactivityTimeout(ms) { delays.push(ms); },
            async destroy() { running = false; },
        },
    }, {}, {
        origin: "https://ingress.example.test", release: "d".repeat(64), intercept: {},
        image: `registry.cloudflare.com/${"a".repeat(32)}/bifrost-${"b".repeat(32)}-r1@sha256:${"c".repeat(64)}`,
    });
    await lifecycle.restore();
    await lifecycle.alarm();
    assert.deepEqual(delays, [6 * hour, 6 * hour]);
    assert.deepEqual(alarms, [100 + 3 * hour]);
    t.mock.timers.setTime(alarms.at(-1));
    await lifecycle.alarm();
    assert.equal(delays.at(-1), 5 * hour + 1000);
    assert.equal(alarms.at(-1), 100 + 6 * hour);
    t.mock.timers.setTime(alarms.at(-1));
    await lifecycle.alarm();
    assert.equal(alarms.at(-1), ticket.deadline);
    assert.equal(running, true);
    t.mock.timers.setTime(ticket.deadline);
    await lifecycle.alarm();
    assert.equal(running, false);
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
    assert.equal((await controller().closeAbsent(slot.uid, Date.now() + 3001)).status, "invalid");
    const observedAbsentAt = Date.now() + 1500;
    const receipt = await controller().closeAbsent(slot.uid, observedAbsentAt);
    assert.deepEqual(receipt, {
        status: "absent", uid: slot.uid, instance: actor, authorizedAt: 100,
        startDeadline: slot.startDeadline, closedAt: observedAbsentAt,
    });
    assert.deepEqual(scope.closeStart(slot.uid, actor, Date.now()), receipt);
    assert.equal(scope.observeStart(slot.uid, actor, 101).status, "conflict");
    assert.equal(reserve(scope, pin()).status, "full");
    assert.equal(scope.retire(slot.uid, actor, receipt.closedAt).status, "retired");
    assert.equal(reserve(scope, pin()).status, "reserved");
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
    assert.equal(reserve(registry(), uid).status, "reserved");
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
    reserve(registry(), request);
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
    assert.equal(reserve(scope, slot).status, "reserved");
    assert.equal(scope.authorizeStart(slot.uid, instance, reservedAt, slot.deadline).status, "authorized");
    assert.equal(scope.meteredEgress(slot.uid, instance), false);
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
    const wrongHost = await visit(origin + path, "elsewhere.example.test");
    assert.equal(wrongHost.status, 403);
    assert.equal(wrongHost.headers.get("x-bifrost-container-relay-rejection"), "origin");
    const deniedRoute = await visit(origin + "/api/users/me/permissions", "ingress.example.test");
    assert.equal(deniedRoute.status, 403);
    assert.equal(deniedRoute.headers.get("x-bifrost-container-relay-rejection"), "route");
    assert.equal((await visit(origin + "/api/sdk/%63onfig/get", "ingress.example.test")).status, 403);
    assert.deepEqual(calls, []);
    const pending = db.prepare("SELECT outbound, ingress FROM start_fences WHERE uid = ?").get(slot.uid);
    assert.ok(pending.outbound >= 4 * 2 * (128 + 2));
    assert.ok(pending.ingress > 0);
    const reply = await visit(origin + path, "ingress.example.test");
    assert.equal(await reply.text(), "fixture response");
    assert.deepEqual(calls, [origin + path]);
    assert.equal(scope.meteredEgress(slot.uid, instance), true);
    assert.equal(scope.meteredEgress(slot.uid, "a".repeat(64)), false);
    const tooLarge = await forwardContainerHttp(scope, slot.uid, instance, new Request(origin + path, {
        method: "POST", headers: { host: "ingress.example.test", "content-length": "4096" }, body: "{}",
    }), origin, policy, upstream);
    assert.equal(tooLarge.status, 429);
    assert.ok(db.prepare("SELECT outbound FROM start_fences WHERE uid = ?").get(slot.uid).outbound >= 2 * 4096);
    assert.equal(scope.chargeRelay(slot.uid, instance, "ingress", 1, Date.now()), false);
    assert.equal(scope.meteredEgress(slot.uid, instance), false);
});

test("ingress canary headers are preserved only for the exact worker canary route", async (t) => {
    const { db, registry, pin } = fixture(1, 4096);
    t.after(() => db.close());
    const reservedAt = Date.now();
    const slot = { ...pin(), reservedAt, startDeadline: reservedAt + 1000, deadline: reservedAt + 123_000 };
    const instance = "b".repeat(64);
    const origin = "https://ingress.example.test";
    const scope = registry();
    reserve(scope, slot);
    scope.authorizeStart(slot.uid, instance, reservedAt, slot.deadline);
    for (const [path, generation] of [
        ["/api/sdk/capacity-canary/" + slot.worker, "3"],
        ["/api/organizations", null],
    ]) {
        const response = await forwardContainerHttp(scope, slot.uid, instance, new Request(origin + path, {
            headers: {
                host: "ingress.example.test", "x-bifrost-canary-generation": "3", "x-bifrost-canary-redirect": "1",
            },
        }), origin, policy, async (_url, options) => {
            assert.equal(options.headers.get("x-bifrost-canary-generation"), generation);
            assert.equal(options.headers.get("x-bifrost-canary-redirect"), generation ? "1" : null);
            return new Response(null, { status: 204, headers: { "x-bifrost-worker-ingress": "v1; allowed" } });
        });
        assert.equal(response.status, 204);
        assert.equal(response.headers.get("x-bifrost-worker-ingress"), generation ? "v1; allowed" : null);
    }
});

test("HTTP interception refuses redirects and stops streaming an oversized response", async (t) => {
    const { db, registry, pin } = fixture(1, 4096);
    t.after(() => db.close());
    const reservedAt = Date.now();
    const slot = { ...pin(), reservedAt, startDeadline: reservedAt + 1000, deadline: reservedAt + 123_000 };
    const instance = "b".repeat(64);
    const origin = "https://ingress.example.test";
    const scope = registry();
    reserve(scope, slot);
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
