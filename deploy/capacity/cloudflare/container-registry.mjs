// SQLite-backed scope seats and start fences. The controller records a start
// before calling the scheduler. A reset never grants that effect a second time.
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const UID = /^[a-f0-9]{32}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const MAX_CONTROL_FRAME_BYTES = 2 ** 20;
// Reserve both legs of the relay. The byte floor covers framing and an
// attempt whose body never reached the upstream. Provider billing still
// requires hosted measurement before this meter can settle spend.
const HTTP_RELAY_MULTIPLIER = 2;
const HTTP_REQUEST_BODY_FLOOR = 256;
// Independent inventory must postdate the last start window plus this boot
// observation bound. Hosted qualification owns validation of that bound.
const BOOT_OBSERVATION_MS = 60_000;
const LAUNCHER_DRAIN_GRACE_MS = 300_000;
// Permit a small difference between the API's database clock and the DO's
// clock at reservation. The pinned start and expiry deadlines stay absolute.
const RESERVATION_CLOCK_SKEW_MS = 3000;
const MAX_RESERVATION_RECORDS = 1024;

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
            ingress_limit INTEGER NOT NULL, outbound_limit INTEGER NOT NULL,
            voided_at INTEGER, void_instance TEXT
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
        return this.#storeReservation(pin, Date.now());
    }

    reserveForStart(pin, instance) {
        const now = Date.now();
        if (!/^[a-f0-9]{64}$/.test(instance)) return { status: "invalid" };
        if (now >= pin.startDeadline) return this.#storeReservation(pin, now);
        return this.#storeReservation(pin, now, { instance, onlyWhenFull: true });
    }

    // Accept the immutable API pin even if the controller crashed before prepare.
    // A void tombstone occupies no seat and denies all later prepare/start replays.
    voidUnstarted(pin, instance) {
        const now = Date.now();
        if (!/^[a-f0-9]{64}$/.test(instance)) return { status: "invalid" };
        if (!Number.isSafeInteger(now) || now < pin.startDeadline) return { status: "pending" };
        return this.#storeReservation(pin, now, { instance, onlyWhenFull: false });
    }

    #storeReservation(pin, now, observation = null) {
        const { reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes } = pin;
        if (pin.installation !== this.installation || pin.revision !== this.revision
            || pin.image !== this.image || pin.instance !== "standard-1"
            || !UUID.test(pin.worker) || !UID.test(pin.uid)
            || (!["platform", "qualification-canary"].includes(pin.tenant) && !UUID.test(pin.tenant))
            || !Number.isSafeInteger(pin.credentialVersion) || pin.credentialVersion < 1
            || ![now, reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes].every(Number.isSafeInteger)
            || reservedAt < 0 || startDeadline <= reservedAt || startDeadline - reservedAt > 3_600_000
            || seconds < 123 || seconds > 93_600 || deadline !== reservedAt + seconds * 1000
            || deadline <= startDeadline || deadline - startDeadline > 90_000_000
            || ingressBytes <= 0 || ingressBytes > this.ingressCap
            || outboundBytes <= 0 || outboundBytes > this.outboundCap) {
            return { status: "invalid" };
        }
        if (now < reservedAt - RESERVATION_CLOCK_SKEW_MS) return { status: "closed" };
        const closingUnstarted = observation !== null && !observation.onlyWhenFull;
        const binding = JSON.stringify([
            pin.installation, pin.revision, pin.worker, pin.uid, pin.tenant,
            pin.image, pin.instance, pin.credentialVersion,
            reservedAt, startDeadline, deadline, seconds, ingressBytes, outboundBytes,
        ]);
        return this.storage.transactionSync(() => {
            // An acknowledged fence and an expired pin that never crossed its
            // start fence can no longer authorize a placement. Unacknowledged
            // fences must survive expiry until the API settles them.
            this.sql.exec(
                "DELETE FROM start_fences WHERE uid IN ("
                + "SELECT uid FROM reservations WHERE deadline <= ? AND state = 'retired')", now,
            );
            this.sql.exec(
                "DELETE FROM reservations WHERE deadline <= ? AND NOT EXISTS ("
                + "SELECT 1 FROM start_fences WHERE start_fences.uid = reservations.uid)", now,
            );
            const matching = this.sql.exec(
                "SELECT binding, state, voided_at, void_instance FROM reservations WHERE worker = ? OR uid = ?",
                pin.worker, pin.uid,
            ).toArray();
            if (matching.length) {
                if (matching.length !== 1 || matching[0].binding !== binding) return { status: "conflict" };
                const recorded = matching[0];
                if (recorded.voided_at !== null && recorded.voided_at < startDeadline) {
                    if (observation && observation.instance !== recorded.void_instance) return { status: "conflict" };
                    return {
                        status: "allocation-refused", uid: pin.uid, instance: recorded.void_instance,
                        startDeadline, voidedAt: recorded.voided_at,
                    };
                }
                if (observation === null || observation.onlyWhenFull) return { status: recorded.state };
                if (recorded.void_instance !== null && recorded.void_instance !== observation.instance) {
                    return { status: "conflict" };
                }
                if (this.sql.exec("SELECT uid FROM start_fences WHERE uid = ?", pin.uid).toArray().length) {
                    return { status: "conflict" };
                }
                if (recorded.voided_at === null) {
                    this.sql.exec(
                        "UPDATE reservations SET state = 'retired', voided_at = ?, void_instance = ? WHERE uid = ?",
                        now, observation.instance, pin.uid,
                    );
                }
                return {
                    status: "void", uid: pin.uid, instance: observation.instance, startDeadline,
                    voidedAt: recorded.voided_at ?? now,
                };
            }
            if (closingUnstarted && now >= deadline) {
                // Even a controller that never prepared this pin can settle it
                // after expiry. No expired request can create a new fence.
                return { status: "void", uid: pin.uid, instance: observation.instance, startDeadline, voidedAt: deadline };
            }
            if (!closingUnstarted && now >= startDeadline) return { status: "closed" };
            const { tracked, total, tenant } = this.sql.exec(
                "SELECT count(*) AS tracked, count(CASE WHEN state <> 'retired' THEN 1 END) AS total, "
                + "count(CASE WHEN state <> 'retired' AND tenant = ? THEN 1 END) AS tenant FROM reservations", pin.tenant,
            ).one();
            const full = total >= this.cap || tenant >= 1;
            if (tracked >= MAX_RESERVATION_RECORDS) return { status: closingUnstarted ? "pending" : "receipt-limit" };
            if (observation === null && full) return { status: "full" };
            const tombstone = observation && (closingUnstarted || full) ? observation : null;
            const voidedAt = Math.max(now, reservedAt);
            this.sql.exec(
                "INSERT INTO reservations (worker, uid, tenant, binding, state, "
                + "reserved_at, start_deadline, deadline, ingress_limit, outbound_limit, voided_at, void_instance) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                pin.worker, pin.uid, pin.tenant, binding, tombstone === null ? "reserved" : "retired",
                reservedAt, startDeadline, deadline, ingressBytes, outboundBytes,
                tombstone === null ? null : voidedAt, tombstone?.instance ?? null,
            );
            return tombstone === null ? { status: "reserved" } : {
                status: tombstone.onlyWhenFull ? "allocation-refused" : "void",
                uid: pin.uid, instance: tombstone.instance, startDeadline, voidedAt,
            };
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
            if (slot.deadline !== deadline || now < slot.reserved_at - RESERVATION_CLOCK_SKEW_MS) {
                return { status: "conflict" };
            }
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
                uid, instance, Math.max(now, slot.reserved_at), slot.start_deadline, deadline,
                slot.ingress_limit, slot.outbound_limit,
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
                || startedAt < start.authorized_at - RESERVATION_CLOCK_SKEW_MS
                || (start.closed_at !== null && start.observed_at === null)) return { status: "conflict" };
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
            if (now < start.observed_at - RESERVATION_CLOCK_SKEW_MS) return { status: "conflict" };
            // Keep the durable closure causally after authorization and placement.
            const closedAt = start.closed_at ?? Math.max(now, start.observed_at, start.authorized_at);
            if (start.closed_at === null) {
                this.sql.exec("UPDATE start_fences SET closed_at = ? WHERE uid = ?", closedAt, uid);
            }
            return {
                status: "closed", instance, authorizedAt: start.authorized_at, closedAt,
                startedAt: start.observed_at, outboundBytes: start.outbound, meterComplete: false,
            };
        });
    }

    // The instance actor serializes this with start and supplies the time of
    // an exact, empty management inventory for this actor.
    closeAbsent(uid, instance, now, observedAbsentAt) {
        return this.storage.transactionSync(() => {
            const start = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!start || start.instance !== instance || start.observed_at !== null) return { status: "conflict" };
            if (![now, observedAbsentAt].every(Number.isSafeInteger) || observedAbsentAt > now) {
                return { status: "invalid" };
            }
            if (observedAbsentAt < start.start_deadline + BOOT_OBSERVATION_MS) return { status: "pending" };
            if (start.closed_at === null) {
                this.sql.exec("UPDATE start_fences SET closed_at = ? WHERE uid = ?", now, uid);
            }
            return {
                status: "absent", uid, instance, authorizedAt: start.authorized_at,
                startDeadline: start.start_deadline, closedAt: start.closed_at ?? now,
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

    chargeRelay(uid, instance, direction, bytes, now, arrived = false) {
        if ((direction !== "ingress" && direction !== "outbound") || !Number.isSafeInteger(bytes) || bytes <= 0) {
            return false;
        }
        return this.storage.transactionSync(() => {
            const row = this.sql.exec("SELECT * FROM start_fences WHERE uid = ?", uid).toArray()[0];
            if (!row || row.instance !== instance || row.closed_at !== null
                || !Number.isSafeInteger(now) || now < row.authorized_at - RESERVATION_CLOCK_SKEW_MS
                || now >= row.deadline) return false;
            const total = Math.min(Number.MAX_SAFE_INTEGER, row[direction] + bytes);
            if (row.relay_blocked || total > row[`${direction}_limit`]) {
                if (arrived) {
                    this.sql.exec(`UPDATE start_fences SET ${direction} = ?, relay_blocked = 1 WHERE uid = ?`, total, uid);
                } else if (!row.relay_blocked) {
                    this.sql.exec("UPDATE start_fences SET relay_blocked = 1 WHERE uid = ?", uid);
                }
                return false;
            }
            this.sql.exec(`UPDATE start_fences SET ${direction} = ${direction} + ? WHERE uid = ?`, bytes, uid);
            return true;
        });
    }
}

const CERTIFICATE_DOMAIN = "bifrost.containers.incarnation.v1\0";
const REQUEST_DOMAIN = "bifrost.containers.request.v1\0";
const CONTROL_REQUEST_ROUTES = new Set([
    "/v1/start", "/v1/stop", "/v1/egress-probe", "/v1/void", "/v1/observe-start",
    "/v1/close-absent", "/v1/close", "/v1/retire",
]);
const MAX_CONTROL_REQUEST = 8192;
const CONTROL_REQUEST_CLOCK_WINDOW = 30_000;

function hex(bytes) {
    return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

async function verifySignature(publicKey, signature, content) {
    const key = await crypto.subtle.importKey("raw", Uint8Array.fromHex(publicKey), "Ed25519", false, ["verify"]);
    return crypto.subtle.verify("Ed25519", key, Uint8Array.fromHex(signature), new TextEncoder().encode(content));
}

async function readControlBody(request) {
    const reader = request.body?.getReader();
    if (!reader) return null;
    const body = new Uint8Array(MAX_CONTROL_REQUEST);
    let length = 0;
    try {
        for (;;) {
            const { done, value } = await reader.read();
            if (done) return body.subarray(0, length);
            if (length + value.byteLength > body.length) {
                await reader.cancel();
                return null;
            }
            body.set(value, length);
            length += value.byteLength;
        }
    } finally {
        reader.releaseLock();
    }
}

// Authentication happens before a request can address a stateful actor. Only
// public verification material is installed in the Worker.
export async function dispatchContainerControl(request, env, invoke) {
    const url = new URL(request.url);
    if (request.method !== "POST" || url.search || !CONTROL_REQUEST_ROUTES.has(url.pathname)) {
        return new Response(null, { status: 404 });
    }
    const denied = () => new Response(null, { status: 403 });
    const root = env.CONTAINERS_PUBLIC_KEY;
    const publicKey = request.headers.get("X-Bifrost-Incarnation-Key");
    const certificate = request.headers.get("X-Bifrost-Incarnation-Certificate");
    const signature = request.headers.get("X-Bifrost-Request-Signature");
    const time = request.headers.get("X-Bifrost-Request-Time");
    if (!/^[a-f0-9]{64}$/.test(root) || !/^[a-f0-9]{64}$/.test(publicKey)
        || !/^[a-f0-9]{128}$/.test(certificate) || !/^[a-f0-9]{128}$/.test(signature)
        || !/^[0-9]{13}$/.test(time) || Math.abs(Date.now() - Number(time)) > CONTROL_REQUEST_CLOCK_WINDOW) {
        return denied();
    }
    let data;
    try {
        const body = await readControlBody(request);
        if (!body) return denied();
        const digest = hex(new Uint8Array(await crypto.subtle.digest("SHA-256", body)));
        const message = JSON.stringify([url.pathname, Number(time), digest]);
        if (!await verifySignature(publicKey, signature, REQUEST_DOMAIN + message)) return denied();
        data = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
        const pin = data.pin;
        if (pin?.installation !== env.CONTAINERS_INSTALLATION
            || pin?.revision !== Number(env.CONTAINERS_REVISION)
            || !UUID.test(pin?.worker) || !UID.test(pin?.uid)) return denied();
        const binding = JSON.stringify([pin.installation, pin.revision, pin.worker, pin.uid, publicKey]);
        if (!await verifySignature(root, certificate, CERTIFICATE_DOMAIN + binding)) return denied();
    } catch {
        return denied();
    }
    return await invoke(url.pathname, data);
}

// The per-incarnation Durable Object calls this before it exposes a control
// endpoint. The registry may be a remote DO stub. A running bit or the
// return from start/destroy never substitutes for independent inventory.
export class ContainerInstanceLifecycle {
    constructor(ctx, registry, { origin, image, release, intercept, idleSeconds = 120 }) {
        const target = new URL(origin);
        if (origin !== target.origin || target.protocol !== "https:" || target.port
            || !/^registry\.cloudflare\.com\/[a-f0-9]{32}\/bifrost-[a-f0-9]{32}-r[1-9]\d*@sha256:[a-f0-9]{64}$/.test(image)
            || !/^[a-f0-9]{64}$/.test(release)
            || !Number.isSafeInteger(idleSeconds) || idleSeconds < 1 || idleSeconds > 120
            || !ctx.container || !intercept) {
            throw new Error("Container lifecycle deployment is not pinned");
        }
        this.ctx = ctx;
        this.registry = registry;
        this.origin = origin;
        this.image = image;
        this.release = release;
        this.intercept = intercept;
        this.idleMs = idleSeconds * 1000;
        this.instance = ctx.id.toString();
    }

    async restore() {
        return this.ctx.blockConcurrencyWhile(async () => {
            const ticket = await this.ctx.storage.get("start-ticket");
            if (!ticket || !this.ctx.container.running) return;
            if (Date.now() >= ticket.deadline) await this.#destroy(ticket);
            else await this.#armProviderInactivity(ticket.deadline);
        });
    }

    async start(pin, controlKey, probeNonce = null) {
        const image = this.ctx.container.images?.["bifrost-linux"];
        if (!/^[a-f0-9]{64}$/.test(controlKey) || !UUID.test(pin.installation)
            || pin.instance !== "standard-1" || image !== this.image
            || !this.image.endsWith(`/bifrost-${pin.installation.replaceAll("-", "")}-r${pin.revision}@${pin.image}`)
            || (probeNonce !== null && !/^[A-Za-z0-9_-]{43}$/.test(probeNonce))) {
            return { status: "invalid" };
        }
        return this.ctx.blockConcurrencyWhile(async () => {
            const prepared = await this.registry.reserveForStart(pin, this.instance);
            if (prepared.status !== "reserved") return prepared;
            const ticket = await this.ctx.storage.get("start-ticket");
            if (ticket && (ticket.uid !== pin.uid || ticket.deadline !== pin.deadline)) return { status: "conflict" };
            if (ticket?.destroyed) return { status: "closed" };
            if (!ticket && this.ctx.container.running) return { status: "conflict" };
            if (!ticket) {
                if (Date.now() >= pin.startDeadline) return { status: "closed" };
                // Install the exact HTTPS intercept before crossing the launch fence.
                await this.ctx.container.interceptOutboundHttps(new URL(this.origin).hostname, this.intercept);
                await this.ctx.storage.setAlarm(pin.deadline);
                await this.ctx.storage.put("start-ticket", { uid: pin.uid, deadline: pin.deadline });
            }
            const now = Date.now();
            if (now >= pin.startDeadline) return { status: "pending" };
            const authority = await this.registry.authorizeStart(pin.uid, this.instance, now, pin.deadline);
            if (authority.status !== "authorized") return authority;
            const env = {
                BIFROST_WORKER_FORM: "external", BIFROST_WORKER_INCARNATION_ID: pin.worker,
                BIFROST_WORKER_RESOURCE_UID: pin.uid, BIFROST_WORKER_CONTROL_SECRET: controlKey,
                BIFROST_CONTAINER_RELEASE: this.release, BIFROST_API_URL: this.origin, HOSTNAME: `bifrost-${pin.uid}`,
                BIFROST_CONTAINER_DEADLINE_MS: String(pin.deadline),
                BIFROST_CONTAINER_IDLE_SECONDS: String(this.idleMs / 1000),
            };
            if (probeNonce !== null) env.BIFROST_CONTAINER_PROBE_NONCE = probeNonce;
            try {
                this.ctx.container.start({ image, instance: pin.instance, env, enableInternet: false });
            } catch {
                // A throw does not prove absence. Reconcile the durable fence.
                return { status: "pending" };
            }
            await this.#armProviderInactivity(pin.deadline);
            return { status: "pending" };
        });
    }

    async #armProviderInactivity(deadline) {
        // Local idle/drain decides graceful stop. This backup cannot preempt
        // a busy workflow before the independently armed absolute alarm.
        const delay = Math.max(this.idleMs + LAUNCHER_DRAIN_GRACE_MS, deadline - Date.now()) + 1000;
        await this.ctx.container.setInactivityTimeout(delay);
    }

    async closeAbsent(uid, observedAbsentAt) {
        return this.ctx.blockConcurrencyWhile(async () => {
            const ticket = await this.ctx.storage.get("start-ticket");
            if (!ticket || ticket.uid !== uid) return { status: "conflict" };
            if (this.ctx.container.running) return { status: "pending" };
            return this.registry.closeAbsent(uid, this.instance, Date.now(), observedAbsentAt);
        });
    }

    async stop(uid) {
        return this.ctx.blockConcurrencyWhile(async () => {
            const ticket = await this.ctx.storage.get("start-ticket");
            if (!ticket || ticket.uid !== uid) return { status: "conflict" };
            await this.#destroy(ticket);
            return { status: "pending" };
        });
    }

    async #destroy(ticket) {
        if (ticket.destroyed) return;
        if (this.ctx.container.running) await this.ctx.container.destroy();
        await this.ctx.storage.put("start-ticket", { ...ticket, destroyed: true });
    }

    async alarm() {
        return this.ctx.blockConcurrencyWhile(async () => {
            const ticket = await this.ctx.storage.get("start-ticket");
            if (!ticket || ticket.destroyed) return;
            if (Date.now() < ticket.deadline) await this.ctx.storage.setAlarm(ticket.deadline);
            else await this.#destroy(ticket);
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
    if (!Number.isSafeInteger(deadline) || deadline <= now) {
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
                if (bytes > MAX_CONTROL_FRAME_BYTES
                    || registry.chargeRelay(uid, instance, direction, bytes, Date.now()) !== true) {
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
    const now = Date.now();
    const rawLength = request.headers.get("content-length");
    const declared = rawLength !== null && /^\d+$/.test(rawLength) ? Number(rawLength) : HTTP_REQUEST_BODY_FLOOR;
    const prepaid = Math.min(Number.MAX_SAFE_INTEGER, declared);
    const headerBytes = byteSize(request.headers, `${request.method} ${request.url} HTTP/1.1\r\n`);
    const arrival = Math.min(Number.MAX_SAFE_INTEGER, HTTP_RELAY_MULTIPLIER * (headerBytes + prepaid));
    const local = (status) => {
        registry.chargeRelay(uid, instance, "ingress", HTTP_RELAY_MULTIPLIER * byteSize([], `HTTP/1.1 ${status}\r\n`),
            Date.now(), true);
        return new Response(null, { status });
    };
    if (registry.chargeRelay(uid, instance, "outbound", arrival, now, true) !== true) return local(429);
    if (url.protocol !== "https:" || url.origin !== origin || url.username || url.password || url.hash
        || request.headers.get("host") !== url.host || request.headers.has("upgrade")
        || policy.version !== 1 || !allowsIngressRoute(policy, request, url)) {
        return local(403);
    }
    const deadline = registry.relayDeadline(uid, instance);
    const remaining = Number.isSafeInteger(deadline) ? deadline - Date.now() : 0;
    const charge = (direction, bytes) =>
        registry.chargeRelay(uid, instance, direction, bytes * HTTP_RELAY_MULTIPLIER, Date.now(), true) === true;
    let unspent = prepaid;
    const sendBody = (bytes) => {
        const activeUntil = registry.relayDeadline(uid, instance);
        if (!Number.isSafeInteger(activeUntil) || Date.now() >= activeUntil) return false;
        const covered = Math.min(unspent, bytes);
        unspent -= covered;
        return covered === bytes || charge("outbound", bytes - covered);
    };
    const received = (bytes) => charge("ingress", bytes);
    const canaryPrefix = "/api/sdk/capacity-canary/";
    const canary = url.pathname.startsWith(canaryPrefix) && UUID.test(url.pathname.slice(canaryPrefix.length));
    const headers = outboundHeaders(request.headers, [
        "accept", "authorization", "content-type", ...(canary ? ["x-bifrost-canary-generation"] : []),
    ]);
    if (remaining <= 0) return local(429);
    const signal = AbortSignal.timeout(remaining);
    try {
        const response = await fetchUpstream(url, {
            method: request.method, headers, body: meterBody(request.body, sendBody, signal),
            redirect: "manual", signal, duplex: "half",
        });
        if (!received(byteSize(response.headers, `HTTP/1.1 ${response.status}\r\n`))) {
            await response.body?.cancel();
            return local(429);
        }
        if (response.status >= 300 && response.status < 400) {
            await response.body?.cancel();
            return local(403);
        }
        const reply = outboundHeaders(response.headers, [
            "content-type", "content-disposition", "retry-after", ...(canary ? ["x-bifrost-worker-ingress"] : []),
        ]);
        return new Response(meterBody(response.body, received, signal), { status: response.status, headers: reply });
    } catch {
        return local(502);
    }
}
