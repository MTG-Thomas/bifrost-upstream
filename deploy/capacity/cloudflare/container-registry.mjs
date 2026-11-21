// SQLite-backed reservations. The owning launcher exchanges this durable
// reservation for its own start intent in the next protocol phase.
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
}
