// SQLite-backed scope seats and start fences. The controller records a start
// before calling the scheduler. A reset never grants that effect a second time.
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;

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
            authorized_at INTEGER NOT NULL, observed_at INTEGER, closed_at INTEGER
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

    authorizeStart(uid, instance, now, deadline) {
        if (!UUID.test(uid) || !/^[a-f0-9]{64}$/.test(instance)
            || !Number.isSafeInteger(now) || !Number.isSafeInteger(deadline) || now < 0) {
            return { status: "invalid" };
        }
        return this.storage.transactionSync(() => {
            const slot = this.sql.exec("SELECT state FROM reservations WHERE uid = ?", uid).toArray()[0];
            if (!slot || slot.state === "retired") return { status: "closed" };
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (start) {
                if (start.instance !== instance) return { status: "conflict" };
                if (start.closed_at !== null) return { status: "closed" };
                return { status: start.observed_at === null ? "pending" : "observed" };
            }
            if (now >= deadline) return { status: "closed" };
            this.sql.exec(
                "INSERT INTO start_fences (uid, instance, authorized_at) VALUES (?, ?, ?)", uid, instance, now,
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
            return { status: "closed", instance, authorizedAt: start.authorized_at, closedAt: start.closed_at ?? now };
        });
    }
}
