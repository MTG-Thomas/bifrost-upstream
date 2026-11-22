// SQLite-backed scope seats and start fences. The controller records a start
// before calling the scheduler. A reset never grants that effect a second time.
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const UID = /^[a-f0-9]{32}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const MAX_CONTROL_FRAME_BYTES = 2 ** 20;

export class ScopeRegistry {
    constructor(ctx, env) {
        this.storage = ctx.storage;
        this.sql = this.storage.sql;
        const {
            CONTAINERS_INSTALLATION, CONTAINERS_REVISION, CONTAINERS_CAP, CONTAINERS_IMAGE,
            CONTAINERS_INGRESS_BYTES, CONTAINERS_OUTBOUND_BYTES,
        } = env;
        this.cap = Number(CONTAINERS_CAP);
        this.revision = Number(CONTAINERS_REVISION);
        this.installation = CONTAINERS_INSTALLATION;
        this.image = CONTAINERS_IMAGE;
        this.ingressCap = Number(CONTAINERS_INGRESS_BYTES);
        this.outboundCap = Number(CONTAINERS_OUTBOUND_BYTES);
        if (!UUID.test(this.installation) || !Number.isSafeInteger(this.revision) || this.revision < 1
            || ![1, 2].includes(this.cap) || !DIGEST.test(this.image)
            || ![this.ingressCap, this.outboundCap].every(
                (bytes) => Number.isSafeInteger(bytes) && bytes > 0 && bytes <= 10 ** 9,
            )) {
            throw new Error("Container reservation policy is invalid");
        }
        this.sql.exec(`CREATE TABLE IF NOT EXISTS reservations (
            worker TEXT PRIMARY KEY, uid TEXT NOT NULL UNIQUE, tenant TEXT NOT NULL,
            binding TEXT NOT NULL, state TEXT NOT NULL,
            reserved_at INTEGER NOT NULL, start_deadline INTEGER NOT NULL, deadline INTEGER NOT NULL,
            ingress_limit INTEGER NOT NULL, outbound_limit INTEGER NOT NULL
        )`);
        this.sql.exec(`CREATE TABLE IF NOT EXISTS start_fences (
            uid TEXT PRIMARY KEY REFERENCES reservations(uid), instance TEXT NOT NULL,
            authorized_at INTEGER NOT NULL, observed_at INTEGER, closed_at INTEGER,
            start_deadline INTEGER NOT NULL, deadline INTEGER NOT NULL,
            ingress_limit INTEGER NOT NULL, outbound_limit INTEGER NOT NULL,
            ingress INTEGER NOT NULL DEFAULT 0, outbound INTEGER NOT NULL DEFAULT 0,
            relay_blocked INTEGER NOT NULL DEFAULT 0
        )`);
    }

    reserve(pin) {
        const { reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes } = pin;
        if (pin.installation !== this.installation || pin.revision !== this.revision
            || pin.image !== this.image || pin.instance !== "standard-1"
            || !UUID.test(pin.worker) || !UID.test(pin.uid)
            || (pin.tenant !== "platform" && !UUID.test(pin.tenant))
            || !Number.isSafeInteger(pin.credentialVersion) || pin.credentialVersion < 1
            || ![reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes].every(Number.isSafeInteger)
            || reservedAt < 0 || startDeadline <= reservedAt || startDeadline - reservedAt > 3_600_000
            || seconds < 123 || seconds > 93_600 || deadline !== reservedAt + seconds * 1000
            || deadline <= startDeadline || deadline - startDeadline > 90_000_000
            || ingressBytes <= 0 || ingressBytes > this.ingressCap
            || outboundBytes <= 0 || outboundBytes > this.outboundCap) {
            return { status: "invalid" };
        }
        const binding = JSON.stringify([
            pin.installation, pin.revision, pin.worker, pin.uid, pin.tenant,
            pin.image, pin.instance, pin.credentialVersion,
            reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes,
        ]);
        return this.storage.transactionSync(() => {
            const matching = this.sql.exec(
                "SELECT binding, state FROM reservations WHERE worker = ? OR uid = ?", pin.worker, pin.uid,
            ).toArray();
            if (matching.length) {
                if (matching.length !== 1 || matching[0].binding !== binding) return { status: "conflict" };
                return { status: matching[0].state };
            }
            const { total, tenant } = this.sql.exec(
                "SELECT count(*) AS total, count(CASE WHEN tenant = ? THEN 1 END) AS tenant "
                + "FROM reservations WHERE state <> 'retired'", pin.tenant,
            ).one();
            if (total >= this.cap || tenant >= 1) return { status: "full" };
            this.sql.exec(
                "INSERT INTO reservations (worker, uid, tenant, binding, state, "
                + "reserved_at, start_deadline, deadline, ingress_limit, outbound_limit) "
                + "VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?, ?, ?)",
                pin.worker, pin.uid, pin.tenant, binding,
                reservedAt, startDeadline, deadline, ingressBytes, outboundBytes,
            );
            return { status: "reserved" };
        });
    }

    authorizeStart(uid, instance, now, deadline) {
        if (!UID.test(uid) || !/^[a-f0-9]{64}$/.test(instance)
            || ![now, deadline].every(Number.isSafeInteger) || now < 0) {
            return { status: "invalid" };
        }
        return this.storage.transactionSync(() => {
            const slot = this.sql.exec("SELECT * FROM reservations WHERE uid = ?", uid).toArray()[0];
            if (!slot || slot.state === "retired") return { status: "closed" };
            if (slot.deadline !== deadline || now < slot.reserved_at) return { status: "conflict" };
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (start) {
                if (start.instance !== instance) return { status: "conflict" };
                if (start.closed_at !== null || now >= deadline) return { status: "closed" };
                return { status: start.observed_at === null ? "pending" : "observed" };
            }
            if (now >= slot.start_deadline) return { status: "closed" };
            this.sql.exec(
                "INSERT INTO start_fences (uid, instance, authorized_at, start_deadline, deadline, ingress_limit, "
                + "outbound_limit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                uid, instance, now, slot.start_deadline, deadline, slot.ingress_limit, slot.outbound_limit,
            );
            return { status: "authorized" };
        });
    }

    // Caller has observed the named physical placement independently. A
    // timeout or missing placement leaves the effect pending.
    observeStart(uid, instance, startedAt) {
        return this.storage.transactionSync(() => {
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!start || start.instance !== instance || !Number.isSafeInteger(startedAt)
                || startedAt < start.authorized_at) return { status: "conflict" };
            if (start.observed_at !== null) {
                return { status: start.observed_at === startedAt ? "observed" : "conflict" };
            }
            this.sql.exec("UPDATE start_fences SET observed_at = ? WHERE uid = ?", startedAt, uid);
            return { status: "observed" };
        });
    }

    closeStart(uid, instance, now) {
        return this.storage.transactionSync(() => {
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!start || start.instance !== instance || !Number.isSafeInteger(now)) return { status: "conflict" };
            if (start.observed_at === null) return { status: "pending" };
            if (now < start.observed_at) return { status: "conflict" };
            if (start.closed_at === null) {
                this.sql.exec("UPDATE start_fences SET closed_at = ? WHERE uid = ?", now, uid);
            }
            return {
                status: "closed", instance, authorizedAt: start.authorized_at, closedAt: start.closed_at ?? now,
                startedAt: start.observed_at, outboundBytes: start.outbound, meterComplete: false,
            };
        });
    }

    // The authenticated controller acknowledges a committed stop settlement.
    // Keep the tombstone so neither prepare nor start can resurrect the UID.
    retire(uid, instance, closedAt) {
        return this.storage.transactionSync(() => {
            const start = this.sql.exec("SELECT instance, closed_at FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!start || start.instance !== instance) return { status: "conflict" };
            if (start.closed_at === null) return { status: "pending" };
            if (start.closed_at !== closedAt) return { status: "conflict" };
            this.sql.exec("UPDATE reservations SET state = 'retired' WHERE uid = ?", uid);
            return { status: "retired" };
        });
    }

    relayDeadline(uid, instance) {
        const row = this.sql.exec(
            "SELECT deadline FROM start_fences WHERE uid = ? AND instance = ? "
            + "AND closed_at IS NULL AND relay_blocked = 0",
            uid, instance,
        ).toArray()[0];
        return row?.deadline ?? null;
    }

    chargeRelay(uid, instance, direction, bytes, now) {
        if ((direction !== "ingress" && direction !== "outbound") || !Number.isSafeInteger(bytes) || bytes <= 0) {
            return false;
        }
        return this.storage.transactionSync(() => {
            const row = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!row || row.instance !== instance || row.closed_at !== null || row.relay_blocked
                || !Number.isSafeInteger(now) || now < row.authorized_at || now >= row.deadline) return false;
            if (row[direction] + bytes > row[`${direction}_limit`]) {
                this.sql.exec("UPDATE start_fences SET relay_blocked = 1 WHERE uid = ?", uid);
                return false;
            }
            this.sql.exec(`UPDATE start_fences SET ${direction} = ${direction} + ? WHERE uid = ?`, bytes, uid);
            return true;
        });
    }
}

// The outbound interceptor must validate the origin, Host and upgrade before
// opening these sockets. Keep protocol fragments intact and charge each one
// across reconnects before forwarding it. Settlement can use this meter only
// once the HTTP paths charge into the same budget.
export function relayControlSockets(registry, uid, instance, guest, ingress) {
    const deadline = registry.relayDeadline(uid, instance);
    let alarm;
    let closed = false;
    const close = (code, reason) => {
        if (closed) return;
        closed = true;
        clearTimeout(alarm);
        for (const socket of [guest, ingress]) {
            try { socket.close(code, reason); } catch { /* Socket already closed. */ }
        }
    };
    const now = Date.now();
    if (deadline === null || deadline <= now) {
        close(1008, "Container relay unavailable");
        return;
    }
    alarm = setTimeout(() => close(1008, "Container relay expired"), deadline - now);
    for (const [source, destination, direction] of [
        [guest, ingress, "outbound"], [ingress, guest, "ingress"],
    ]) {
        source.addEventListener("message", (event) => {
            if (closed) return;
            try {
                const data = event.data;
                // Bound allocations during UTF-8 sizing, then count actual bytes.
                const bytes = typeof data === "string" && data.length <= MAX_CONTROL_FRAME_BYTES
                    ? new TextEncoder().encode(data).byteLength : Infinity;
                if (bytes > MAX_CONTROL_FRAME_BYTES || !registry.chargeRelay(uid, instance, direction, bytes, Date.now())) {
                    close(1008, "Container relay limit");
                } else {
                    destination.send(data);
                }
            } catch {
                close(1011, "Container relay failed");
            }
        });
        source.addEventListener("close", () => close(1000, "Container relay closed"));
        source.addEventListener("error", () => close(1011, "Container relay failed"));
    }
}

// Operate on the exact ingress route matrix shipped with the Worker. This
// is an outer allowlist: Bifrost still authorizes the run credential and
// verifies the matched handler and route parameters on every request.
function allowsIngressRoute(policy, request, url) {
    let decoded;
    try {
        decoded = decodeURIComponent(url.pathname);
    } catch {
        return false;
    }
    if (url.pathname.length > 4096 || /[\\%?#\u0000-\u001f\u007f\ufffd]/.test(decoded)
        || decoded.slice(1).split("/").some(segment => !segment || segment === "." || segment === "..")) {
        return false;
    }
    if (/%2f/i.test(url.pathname)) {
        const prefix = "/api/sdk/modules/";
        if (!url.pathname.startsWith(prefix)
            || url.pathname.slice(prefix.length) !== encodeURIComponent(decoded.slice(prefix.length))) return false;
    } else if (Array.from(url.pathname.matchAll(/%([a-f0-9]{2})/gi)).some(([, code]) =>
        /[A-Za-z0-9._~-]/.test(String.fromCharCode(Number.parseInt(code, 16))))) {
        return false;
    }
    if (decoded.startsWith("/api/sdk/modules/") && !decoded.endsWith(".py")) return false;
    return Object.entries(policy.routes).some(([path, methods]) => {
        if (!methods.includes(request.method)) return false;
        const parts = path.split("/");
        const actual = decoded.split("/");
        return parts.length <= actual.length && parts.every((part, i) => {
            if (part === "{path:path}") return i < actual.length;
            if (i >= actual.length) return false;
            return part.startsWith("{") || part === actual[i];
        }) && (parts.at(-1) === "{path:path}" || actual.length === parts.length);
    });
}

function outboundHeaders(headers, names) {
    return new Headers(names.flatMap(name => {
        const value = headers.get(name);
        return value === null ? [] : [[name, value]];
    }));
}

function byteSize(headers, line) {
    const encoded = new TextEncoder();
    let bytes = encoded.encode(line).byteLength;
    for (const [name, value] of headers) bytes += encoded.encode(name + ": " + value + "\r\n").byteLength;
    return bytes + 2;
}

function meterBody(body, charge, signal) {
    if (!body) return null;
    const source = body.getReader();
    return new ReadableStream({
        async pull(controller) {
            try {
                signal.throwIfAborted();
                const { done, value } = await source.read();
                if (done) controller.close();
                else if (value.byteLength && !charge(value.byteLength)) {
                    await source.cancel();
                    controller.error(new Error("Container relay budget exhausted"));
                } else {
                    controller.enqueue(value);
                }
            } catch (error) {
                controller.error(error);
            }
        },
        cancel(reason) { return source.cancel(reason); },
    });
}

export async function forwardContainerHttp(registry, uid, instance, request, origin, policy, fetchUpstream = fetch) {
    const url = new URL(request.url);
    if (url.protocol !== "https:" || url.origin !== origin || url.username || url.password || url.hash
        || request.headers.get("host") !== url.host || request.headers.has("upgrade")
        || policy.version !== 1 || !allowsIngressRoute(policy, request, url)) {
        return new Response(null, { status: 403 });
    }
    const deadline = registry.relayDeadline(uid, instance);
    const remaining = deadline === null ? 0 : deadline - Date.now();
    const charge = (direction) => (bytes) => registry.chargeRelay(uid, instance, direction, bytes, Date.now());
    const sent = charge("outbound");
    const received = charge("ingress");
    const headers = outboundHeaders(request.headers, ["accept", "authorization", "content-type"]);
    if (remaining <= 0 || !sent(byteSize(headers, `${request.method} ${url.pathname}${url.search}\r\n`))) {
        return new Response(null, { status: 429 });
    }
    const signal = AbortSignal.timeout(remaining);
    try {
        const response = await fetchUpstream(url, {
            method: request.method, headers, body: meterBody(request.body, sent, signal),
            redirect: "manual", signal, duplex: "half",
        });
        if (response.status >= 300 && response.status < 400) {
            await response.body?.cancel();
            return new Response(null, { status: 403 });
        }
        const reply = outboundHeaders(response.headers, ["content-type", "content-disposition", "retry-after"]);
        if (!received(byteSize(reply, `${response.status}\r\n`))) {
            await response.body?.cancel();
            return new Response(null, { status: 429 });
        }
        return new Response(meterBody(response.body, received, signal), { status: response.status, headers: reply });
    } catch {
        return new Response(null, { status: 502 });
    }
}
