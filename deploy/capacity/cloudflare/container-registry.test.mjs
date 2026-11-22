import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { forwardContainerHttp, relayControlSockets, ScopeRegistry } from "./container-registry.mjs";

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
    assert.equal(registry().observeStart(request.uid, instance, 99).status, "conflict");
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
    const { db, registry, pin } = fixture(1, 256);
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
        method: "POST", headers: { host, authorization: "Bearer fixture", "x-forwarded-host": "unsafe" }, body: "{}",
    }), origin, policy, upstream);
    assert.equal((await visit("https://elsewhere.example.test" + path, "ingress.example.test")).status, 403);
    assert.equal((await visit(origin + path, "elsewhere.example.test")).status, 403);
    assert.equal((await visit(origin + "/api/users/me/permissions", "ingress.example.test")).status, 403);
    assert.equal((await visit(origin + "/api/sdk/%63onfig/get", "ingress.example.test")).status, 403);
    assert.deepEqual(calls, []);
    const reply = await visit(origin + path, "ingress.example.test");
    assert.equal(await reply.text(), "fixture response");
    assert.deepEqual(calls, [origin + path]);
    assert.equal(scope.chargeRelay(slot.uid, instance, "outbound", 256, Date.now()), false);
    assert.equal(scope.chargeRelay(slot.uid, instance, "ingress", 1, Date.now()), false);
});

test("HTTP interception refuses redirects and stops streaming an oversized response", async (t) => {
    const { db, registry, pin } = fixture(1, 256);
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
    const large = await visit(async () => new Response("x".repeat(257)));
    await assert.rejects(large.text(), /budget exhausted/);
    assert.equal(scope.relayDeadline(slot.uid, instance), null);
});
