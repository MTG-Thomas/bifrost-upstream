// This module is appended to container-registry.mjs before signing the release.
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

export class ContainerScopeRegistry extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        this.registry = new ScopeRegistry(ctx, env);
    }

    reserveForStart(pin, instance) {
        return this.registry.reserveForStart(pin, instance);
    }

    authorizeStart(uid, instance, now, deadline) {
        return this.registry.authorizeStart(uid, instance, now, deadline);
    }

    closeAbsent(uid, instance, now, observedAbsentAt) {
        return this.registry.closeAbsent(uid, instance, now, observedAbsentAt);
    }

    voidUnstarted(pin, instance) {
        return this.registry.voidUnstarted(pin, instance);
    }

    observeStart(uid, instance, startedAt) {
        return this.registry.observeStart(uid, instance, startedAt);
    }

    close(uid, instance) {
        return this.registry.closeStart(uid, instance, Date.now());
    }

    retire(uid, instance, closedAt) {
        return this.registry.retire(uid, instance, closedAt);
    }

    async fetch(request) {
        // Only the bound egress Worker can invoke this fetch path. Use the
        // fetch transport so a WebSocket upgrade can cross the DO boundary.
        const uid = request.headers.get("x-bifrost-container-uid");
        const instance = request.headers.get("x-bifrost-container-instance");
        if (!UID.test(uid) || !/^[a-f0-9]{64}$/.test(instance)) return new Response(null, { status: 403 });
        request = new Request(request);
        request.headers.delete("x-bifrost-container-uid");
        request.headers.delete("x-bifrost-container-instance");
        const origin = this.env.CONTAINERS_ORIGIN;
        const policy = JSON.parse(this.env.CONTAINERS_INGRESS_POLICY);
        if (!request.headers.has("upgrade")) {
            return forwardContainerHttp(this.registry, uid, instance, request, origin, policy);
        }
        const url = new URL(request.url);
        const denied = () => new Response(null, { status: 403 });
        if (url.origin !== origin || url.pathname !== policy.control || url.search || url.hash
            || url.username || url.password || request.headers.get("host") !== url.host
            || request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
            return denied();
        }
        const now = Date.now();
        if (this.registry.chargeRelay(uid, instance, "outbound",
            byteSize(request.headers, `GET ${request.url} HTTP/1.1\r\n`), now, true) !== true) return denied();
        const upstream = await fetch(url, { headers: { Upgrade: "websocket" }, redirect: "manual" });
        if (upstream.status !== 101 || !upstream.webSocket) return denied();
        upstream.webSocket.accept();
        if (this.registry.chargeRelay(uid, instance, "ingress",
            byteSize(upstream.headers, "HTTP/1.1 101\r\n"), Date.now(), true) !== true) {
            upstream.webSocket.close();
            return denied();
        }
        const { 0: guest, 1: relay } = new WebSocketPair();
        relay.accept();
        relayControlSockets(this.registry, uid, instance, relay, upstream.webSocket);
        return new Response(null, { status: 101, webSocket: guest });
    }
}

export class ContainerIngress extends WorkerEntrypoint {
    fetch(request) {
        const { uid, instance } = this.ctx.props;
        const relayRequest = new Request(request);
        relayRequest.headers.set("x-bifrost-container-uid", uid);
        relayRequest.headers.set("x-bifrost-container-instance", instance);
        return this.env.CONTAINER_REGISTRY.getByName("scope").fetch(relayRequest);
    }
}

export class ContainerLauncher extends DurableObject {
    lifecycle(uid) {
        const registry = this.env.CONTAINER_REGISTRY.getByName("scope");
        return new ContainerInstanceLifecycle(this.ctx, registry, {
            origin: this.env.CONTAINERS_ORIGIN,
            image: this.env.CONTAINERS_PREPARED_IMAGE,
            release: this.env.CONTAINERS_RELEASE,
            idleSeconds: Number(this.env.CONTAINERS_IDLE_SECONDS),
            intercept: this.ctx.exports.ContainerIngress({ props: { uid, instance: this.ctx.id.toString() } }),
        });
    }

    async start(pin, controlKey, nonce = null) {
        const lifecycle = this.lifecycle(pin.uid);
        await lifecycle.restore();
        const result = await lifecycle.start(pin, controlKey, nonce);
        return { ...result, instance: lifecycle.instance };
    }

    async stop(uid) {
        const lifecycle = this.lifecycle(uid);
        return { ...await lifecycle.stop(uid), instance: lifecycle.instance };
    }

    async closeAbsent(uid, observedAbsentAt) {
        return this.lifecycle(uid).closeAbsent(uid, observedAbsentAt);
    }

    async alarm() {
        const ticket = await this.ctx.storage.get("start-ticket");
        if (ticket) await this.lifecycle(ticket.uid).alarm();
    }
}

export default {
    fetch(request, env) {
        return dispatchContainerControl(request, env, async (operation, data) => {
            const { pin, nonce, control_key: controlKey } = data;
            if (operation === "/v1/egress-probe" &&
                (pin.tenant !== "qualification-canary" || !/^[A-Za-z0-9_-]{43}$/.test(nonce))) {
                return new Response(null, { status: 403 });
            }
            const id = env.CONTAINER_INSTANCE.idFromName(pin.uid);
            const actor = () => env.CONTAINER_INSTANCE.get(id);
            const registry = () => env.CONTAINER_REGISTRY.getByName("scope");
            const instance = id.toString();
            const uid = pin.uid;
            switch (operation) {
                case "/v1/start": return Response.json(await actor().start(pin, controlKey));
                case "/v1/egress-probe": return Response.json(await actor().start(pin, controlKey, nonce));
                case "/v1/stop": return Response.json(await actor().stop(uid));
                case "/v1/void": return Response.json(await registry().voidUnstarted(pin, instance));
                case "/v1/observe-start": return Response.json(await registry().observeStart(uid, instance, data.started_at));
                case "/v1/close-absent": return Response.json(await actor().closeAbsent(uid, data.observed_absent_at));
                case "/v1/close": return Response.json(await registry().close(uid, instance));
                case "/v1/retire": return Response.json(await registry().retire(uid, instance, data.closed_at));
                default: return new Response(null, { status: 404 });
            }
        });
    },
};
