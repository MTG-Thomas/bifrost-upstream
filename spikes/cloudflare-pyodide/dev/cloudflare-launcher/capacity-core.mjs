// Shared capacity protocol engine (JavaScript).
//
// Fork protocol 2 retains the upstream lifecycle with caller-owned preparation.
//
// Deliberately pure: no fetch, no Durable Objects, no Cloudflare APIs. The
// transport (capacity.mjs) and the durable store are injected, which is what
// makes the lifecycle testable without a deployment.

export const STATE = Object.freeze({
    PREPARED: "prepared",
    STARTING: "starting",
    RUNNING: "running",
    STOPPED: "stopped",
    DELETED: "deleted",
});

const LIVE = new Set([STATE.PREPARED, STATE.STARTING, STATE.RUNNING]);
const RESERVATION_FIELDS = [
    "worker_id",
    "worker_version",
    "memory_limit_bytes",
    "max_lifetime_seconds",
    "drain_grace_seconds",
];
const PREPARE_FIELDS = [...RESERVATION_FIELDS, "resource_uid", "control_key"];

export class ProtocolError extends Error {
    constructor(status, message) {
        super(message);
        this.status = status;
    }
}

async function keyDigest(key) {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/**
 * @param {object} options
 * @param {object} options.profile   advertised hardware/build profile
 * @param {object} options.store     { read, write, reserve, release, discard } — all async
 * @param {object} options.runtime   { prepare, start, destroy, clearStorage, observe } — all async
 * @param {object} [options.clock]   { now } injectable for tests
 */
export function createEngine({ profile, store, runtime, clock = { now: () => Date.now() } }) {
    const chains = new Map();

    // Serialize mutations per worker: Durable Objects give us one isolate, but
    // concurrent requests can still interleave across awaits.
    function serialize(workerId, fn) {
        const prior = chains.get(workerId) ?? Promise.resolve();
        const next = prior.then(fn, fn);
        chains.set(workerId, next.catch(() => {}));
        return next;
    }

    function resource(row) {
        return {
            scope: profile.scope,
            worker_id: row.worker_id,
            worker_version: row.reservation.worker_version,
            resource_uid: row.resource_uid,
            state: row.state,
            out_of_memory: Boolean(row.out_of_memory),
            ...(row.state === STATE.DELETED && row.usage ? {usage: row.usage} : {}),
        };
    }

    function settle(row, observation) {
        let state = observation.state;
        if (state === STATE.RUNNING && row.state === STATE.STARTING) {
            const elapsed = clock.now() - (row.started_at ?? 0);
            if (elapsed < profile.starting_grace_ms) state = STATE.STARTING;
        }
        if ((state === STATE.STOPPED) && LIVE.has(row.state)) {
            row.stopped_at = row.stopped_at ?? clock.now();
        }
        row.state = state;
        row.out_of_memory = Boolean(observation.out_of_memory);
        row.intent = null;
        row.revision = (row.revision ?? 0) + 1;
        return row;
    }

    function validateReservation(workerId, reservation) {
        if (!reservation || Object.keys(reservation).some((key) => !PREPARE_FIELDS.includes(key))) {
            throw new ProtocolError(400, "invalid reservation fields");
        }
        if (typeof reservation.resource_uid !== "string" || !/^[0-9a-f]{32}$/.test(reservation.resource_uid) ||
            typeof reservation.control_key !== "string" || !/^[0-9a-f]{64}$/.test(reservation.control_key)) {
            throw new ProtocolError(400, "invalid managed prepare identity");
        }
        for (const field of RESERVATION_FIELDS) {
            if (reservation?.[field] === undefined) {
                throw new ProtocolError(400, `reservation misses ${field}`);
            }
        }
        if (reservation.worker_id !== workerId) {
            throw new ProtocolError(400, "reservation worker_id does not match the URL");
        }
        if (reservation.memory_limit_bytes !== profile.memory_limit_bytes) {
            throw new ProtocolError(409, "reservation memory does not match advertised capacity");
        }
        if (reservation.worker_version !== profile.worker_version) {
            throw new ProtocolError(409, "reservation build does not match advertised capacity");
        }
        if (!Number.isInteger(reservation.max_lifetime_seconds) || reservation.max_lifetime_seconds < 60 ||
            !Number.isInteger(reservation.drain_grace_seconds) || reservation.drain_grace_seconds < 0 ||
            reservation.drain_grace_seconds > 3600) {
            throw new ProtocolError(400, "invalid lifecycle bounds");
        }
        if (reservation.max_lifetime_seconds > profile.max_lifetime_seconds) {
            throw new ProtocolError(409, "reservation lifetime exceeds advertised capacity");
        }
    }

    async function finishPrepare(workerId, row, fresh = false) {
        if (!await store.reserve(workerId)) {
            if (fresh) await store.discard(workerId);
            throw new ProtocolError(409, "capacity ceiling reached");
        }
        // A confirmed ledger and scope slot precede enrollment. Replaying
        // prepare addresses the same launcher and uses the same key.
        try {
            await runtime.prepare(row);
        } catch (error) {
            // Enrollment cannot dial. Keep the documented UID for retry or
            // terminal cleanup, but give up the scope slot until the retry.
            await store.release(workerId);
            throw error;
        }
        row.intent = null;
        delete row.control_key;
        await store.write(workerId, row);
        return resource(row);
    }

    return {
        get(workerId) {
            return serialize(workerId, async () => {
                const row = await store.read(workerId);
                if (!row || row.scope !== profile.scope) return null;
                if (row.state === STATE.DELETED || row.intent === "prepare" || row.intent === "delete" ||
                    (row.state === STATE.PREPARED && !row.intent) || row.state === STATE.STOPPED) return resource(row);
                const observation = await runtime.observe(row);
                if (row.intent === "start" && observation.start_fact === "accepted") {
                    row.started_at ??= clock.now();
                }
                if (observation.state !== row.state) {
                    settle(row, observation);
                }
                return resource(row);
            });
        },

        prepare(workerId, reservation) {
            return serialize(workerId, async () => {
                validateReservation(workerId, reservation);
                const {resource_uid, control_key, ...bounds} = reservation;
                const digest = await keyDigest(control_key);
                const existing = await store.read(workerId);
                if (existing) {
                    if (existing.scope !== profile.scope) {
                        throw new ProtocolError(409, "worker belongs to another scope");
                    }
                    if (existing.state === STATE.DELETED) {
                        throw new ProtocolError(409, "worker incarnation already terminated");
                    }
                    if (existing.intent === "delete") {
                        throw new ProtocolError(409, "worker deletion is pending");
                    }
                    if (existing.resource_uid !== resource_uid || existing.control_key_digest !== digest ||
                        RESERVATION_FIELDS.some((key) => existing.reservation[key] !== bounds[key])) {
                        throw new ProtocolError(409, "conflicting reservation for this worker");
                    }
                    if (existing.intent === "prepare") return finishPrepare(workerId, existing);
                    return resource(existing);
                }
                // Commit the reservation before any provider work so an
                // interrupted prepare converges on readback.
                const row = {
                    scope: profile.scope,
                    worker_id: workerId,
                    reservation: bounds,
                    resource_uid,
                    control_key,
                    control_key_digest: digest,
                    handle: {},
                    state: STATE.PREPARED,
                    revision: 1,
                    created_at: clock.now(),
                    started_at: null,
                    stopped_at: null,
                    intent: "prepare",
                };
                await store.write(workerId, row);
                return finishPrepare(workerId, row, true);
            });
        },

        start(workerId, body) {
            return serialize(workerId, async () => {
                const row = await store.read(workerId);
                if (!row || row.scope !== profile.scope) throw new ProtocolError(404, "unknown worker");
                if (row.state === STATE.DELETED) {
                    throw new ProtocolError(409, "worker incarnation already terminated");
                }
                if (body?.resource_uid !== row.resource_uid) {
                    throw new ProtocolError(409, "resource UID does not match the prepared incarnation");
                }
                if (row.intent === "delete") {
                    throw new ProtocolError(409, "worker lifecycle transition is pending");
                }
                if (row.intent === "prepare") await finishPrepare(workerId, row);
                if (row.state === STATE.STARTING || row.state === STATE.RUNNING) {
                    const prior = await runtime.observe(row);
                    if (prior.state === STATE.STOPPED || prior.state === STATE.DELETED) {
                        settle(row, {...prior, state: STATE.STOPPED});
                        await store.write(workerId, row);
                        throw new ProtocolError(409, "worker incarnation already stopped");
                    }
                    return resource(row); // replay: never launch twice
                }
                if (row.state === STATE.STOPPED) {
                    throw new ProtocolError(409, "worker incarnation already stopped");
                }
                if (row.intent === "start") {
                    // A previous attempt may have launched a container whose
                    // response we never saw. Observe before acting.
                    let prior;
                    try {
                        prior = await runtime.observe(row);
                    } catch {
                        throw new ProtocolError(502, "start outcome unknown; provider unreachable");
                    }
                    if (prior.state === STATE.STARTING || prior.state === STATE.RUNNING) {
                        settle(row, prior);
                        await store.write(workerId, row);
                        return resource(row);
                    }
                    if (prior.state === STATE.STOPPED || prior.state === STATE.DELETED) {
                        settle(row, { ...prior, state: STATE.STOPPED });
                        await store.write(workerId, row);
                        throw new ProtocolError(409, "worker incarnation already stopped");
                    }
                    if (prior.state !== STATE.PREPARED || prior.start_fact !== "never_started") {
                        // A provider-side request can fail before its Durable
                        // Object handler runs, or after the handler accepted a
                        // physical start. Only that handler's durable
                        // never-started fact makes a replay safe; a bare
                        // stopped/not-running readback cannot prove it.
                        throw new ProtocolError(502, "start outcome unknown; physical launch is unproven");
                    }
                }

                row.intent = "start";
                row.revision = (row.revision ?? 0) + 1;
                await store.write(workerId, row);

                const startedAt = clock.now();
                let observation;
                try {
                    observation = await runtime.start(row);
                } catch (error) {
                    // Keep the intent: reconciliation decides from readback.
                    throw new ProtocolError(502, `start outcome unknown: ${error.name}`);
                }
                row.started_at = startedAt;
                if (observation.state === STATE.RUNNING) {
                    observation = { ...observation, state: STATE.STARTING };
                }
                settle(row, observation);
                row.handle = observation.handle ?? row.handle;
                await store.write(workerId, row);
                return resource(row);
            });
        },

        remove(workerId, body) {
            return serialize(workerId, async () => {
                const row = await store.read(workerId);
                if (!row || row.scope !== profile.scope) throw new ProtocolError(404, "unknown worker");
                if (body?.resource_uid !== row.resource_uid) {
                    throw new ProtocolError(409, "resource UID does not match this worker");
                }
                if (body.receipt_ack === true) {
                    if (row.state !== STATE.DELETED) throw new ProtocolError(409, "worker deletion is unconfirmed");
                    if (row.usage) {
                        const fence = {
                            scope: row.scope, worker_id: row.worker_id, resource_uid: row.resource_uid,
                            reservation: {worker_version: row.reservation.worker_version}, state: STATE.DELETED,
                            out_of_memory: row.out_of_memory,
                        };
                        await store.write(workerId, fence);
                        return resource(fence);
                    }
                    return resource(row);
                }
                if (row.state === STATE.DELETED) return resource(row);

                try {
                    if (!row.launcher_stopped) {
                        row.intent = "delete";
                        await store.write(workerId, row);
                        const observation = await runtime.destroy(row);
                        if (observation.state !== STATE.DELETED) throw new Error("launcher not stopped");
                        // A durable receipt must survive erasing the DO and a lost purge response.
                        row.launcher_stopped = true;
                        row.usage = observation.usage;
                        await store.write(workerId, row);
                    }
                    await runtime.clearStorage(row);
                    if (row.usage) {
                        row.usage.launcher_storage_cleared = true;
                        row.usage.ledger_receipt_pending = true;
                    }
                } catch (error) {
                    throw new ProtocolError(502, `delete outcome unknown: ${error.name}`);
                }
                // Until the slot release succeeds, readback still directs
                // the controller to retry DELETE.
                await store.release(workerId);
                row.state = STATE.DELETED;
                row.intent = null;
                delete row.launcher_stopped;
                delete row.control_key;
                delete row.control_key_digest;
                row.revision = (row.revision ?? 0) + 1;
                await store.write(workerId, row);
                return resource(row);
            });
        },
    };
}
