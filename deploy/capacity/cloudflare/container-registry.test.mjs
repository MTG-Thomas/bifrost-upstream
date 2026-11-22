import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { relayControlSockets, ScopeRegistry } from "./container-registry.mjs";

function fixture(cap = 2) {
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
    };
    return {
        db, registry: () => new ScopeRegistry({ storage }, env),
        pin: (tenant = "platform") => ({
            tenant, installation, revision: 2, image, worker: randomUUID(), uid: randomUUID().replaceAll("-", ""),
            instance: "standard-1", credentialVersion: 1,
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
    const bounds = { startDeadline: 101, deadline: 201, ingressBytes: 100, outboundBytes: 100 };
    assert.equal(registry().reserve(request).status, "reserved");
    assert.equal(registry().authorizeStart(request.uid, instance, 100, bounds).status, "authorized");
    assert.equal(registry().authorizeStart(request.uid, instance, 101, bounds).status, "pending");
    assert.equal(registry().authorizeStart(request.uid, other, 101, bounds).status, "conflict");
    assert.equal(registry().authorizeStart(request.uid, instance, 101, {
        ...bounds, ingressBytes: 101,
    }).status, "conflict");
    assert.equal(registry().closeStart(request.uid, instance, 105).status, "pending");
    assert.equal(registry().observeStart(request.uid, instance, 99).status, "conflict");
    assert.equal(registry().observeStart(request.uid, instance, 104).status, "observed");
    assert.equal(registry().observeStart(request.uid, instance, 105).status, "conflict");
    const closed = { status: "closed", instance, authorizedAt: 100, closedAt: 106, outboundBytes: 0 };
    assert.deepEqual(registry().closeStart(request.uid, instance, 106), closed);
    assert.deepEqual(registry().closeStart(request.uid, instance, 180), closed);
    assert.equal(registry().authorizeStart(request.uid, instance, 180, bounds).status, "closed");
    assert.equal(registry().reserve(pin(randomUUID())).status, "full");
    db.close();
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
    const uid = pin();
    const instance = "b".repeat(64);
    const fragments = ['{"fragment":0,"data":"é"}', '{"fragment":1,"data":"β"}'];
    const fragmentBytes = new TextEncoder().encode(fragments.join("")).byteLength;
    assert.equal(registry().reserve(uid).status, "reserved");
    assert.equal(registry().authorizeStart(uid.uid, instance, Date.now(), {
        startDeadline: 101, deadline: 200, ingressBytes: 3, outboundBytes: fragmentBytes,
    }).status, "authorized");
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
    registry().authorizeStart(request.uid, instance, Date.now(), {
        startDeadline: 101, deadline: 200, ingressBytes: 100, outboundBytes: 100,
    });
    const guest = new Socket();
    const ingress = new Socket();
    relayControlSockets(registry(), request.uid, instance, guest, ingress);
    t.mock.timers.tick(100);
    guest.frame("late");
    assert.deepEqual(guest.closed, [1008]);
    assert.deepEqual(ingress.closed, [1008]);
    assert.deepEqual(ingress.sent, []);
});
