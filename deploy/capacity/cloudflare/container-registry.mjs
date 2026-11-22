// SQLite-backed scope seats and start fences. The controller records a start
// before calling the scheduler. A reset never grants that effect a second time.
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const MAX_CONTROL_FRAME_BYTES = 2 ** 20;

export class ScopeRegistry {
    constructor(ctx, env) {
        this.storage = ctx.storage;
        this.sql = this.storage.sql;
        const { CONTAINERS_INSTALLATION, CONTAINERS_REVISION, CONTAINERS_CAP, CONTAINERS_IMAGE } = env;
        this.cap = Number(CONTAINERS_CAP);
        this.revision = Number(CONTAINERS_REVISION);
        this.installation = CONTAINERS_INSTALLATION;
        this.image = CONTAINERS_IMAGE;
        if (!UUID.test(this.installation) || !Number.isSafeInteger(this.revision) || this.revision < 1
            || ![1, 2].includes(this.cap) || !DIGEST.test(this.image)) {
            throw new Error("Container reservation policy is invalid");
        }
        this.sql.exec(`CREATE TABLE IF NOT EXISTS reservations (
            worker TEXT PRIMARY KEY, uid TEXT NOT NULL UNIQUE, tenant TEXT NOT NULL,
            binding TEXT NOT NULL, state TEXT NOT NULL
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
        if (pin.installation !== this.installation || pin.revision !== this.revision
            || pin.image !== this.image || pin.instance !== "standard-1"
            || !UUID.test(pin.worker) || !UUID.test(pin.uid)
            || (pin.tenant !== "platform" && !UUID.test(pin.tenant))
            || !Number.isSafeInteger(pin.credentialVersion) || pin.credentialVersion < 1) {
            return { status: "invalid" };
        }
        const binding = JSON.stringify([
            pin.installation, pin.revision, pin.worker, pin.uid, pin.tenant,
            pin.image, pin.instance, pin.credentialVersion,
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
                "INSERT INTO reservations (worker, uid, tenant, binding, state) VALUES (?, ?, ?, ?, 'reserved')",
                pin.worker, pin.uid, pin.tenant, binding,
            );
            return { status: "reserved" };
        });
    }

    authorizeStart(uid, instance, now, { startDeadline, deadline, ingressBytes, outboundBytes }) {
        if (!UUID.test(uid) || !/^[a-f0-9]{64}$/.test(instance)
            || ![now, startDeadline, deadline].every(Number.isSafeInteger) || now < 0
            || deadline <= startDeadline || deadline - startDeadline > 86_400_000
            || ![ingressBytes, outboundBytes].every((size) => Number.isSafeInteger(size) && size > 0 && size <= 10**9)) {
            return { status: "invalid" };
        }
        return this.storage.transactionSync(() => {
            const slot = this.sql.exec("SELECT state FROM reservations WHERE uid = ?", uid).toArray()[0];
            if (!slot || slot.state === "retired") return { status: "closed" };
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (start) {
                if (start.instance !== instance || start.start_deadline !== startDeadline || start.deadline !== deadline
                    || start.ingress_limit !== ingressBytes || start.outbound_limit !== outboundBytes) {
                    return { status: "conflict" };
                }
                if (start.closed_at !== null) return { status: "closed" };
                return { status: start.observed_at === null ? "pending" : "observed" };
            }
            if (now >= startDeadline) return { status: "closed" };
            if (startDeadline - now > 3_600_000) return { status: "invalid" };
            this.sql.exec(
                "INSERT INTO start_fences (uid, instance, authorized_at, start_deadline, deadline, ingress_limit, "
                + "outbound_limit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                uid, instance, now, startDeadline, deadline, ingressBytes, outboundBytes,
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
                outboundBytes: start.outbound,
            };
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
