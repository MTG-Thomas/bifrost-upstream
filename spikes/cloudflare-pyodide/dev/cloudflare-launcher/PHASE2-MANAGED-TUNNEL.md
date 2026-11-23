# Phase 2 Managed Tunnel

Managed Tunnel uses an exact hostname in an active DNS zone owned by the
selected Cloudflare account. Preflight proposes the hostname and discovers
the zone. Review both before selecting **Scale Out with Cloudflare**.
For CLI or MCP, stage the scoped credential and use its reference for
preflight and activation. Confirm the discovered zone, proposed hostname,
consent and spend limit using the combined activation procedure.
[Combined activation procedure](PHASE1-PILOT.md#combined-hosted-qualification-plan)
The account token needs Account Settings Read, Workers Scripts Write and
Cloudflare Tunnel Write on that account. Grant Zone Read and DNS Write only
for the selected zone. Leave the dedicated hostname unoccupied.
Cloudflare Tunnel Write also authorizes the account-scoped `teamnet/routes`
list used to check for private-network IP routes. Phase 2 drift detection
covers those IP routes. Zero Trust hostname routes are outside its scope.
[Cloudflare list tunnel routes](https://developers.cloudflare.com/api/resources/zero_trust/subresources/networks/subresources/routes/methods/list/)

Every Compose installation includes a dormant connector with the API release
image. The connector can reach the API's main port on their dedicated bridge.
It can also reach host-published ports through the host gateway, including
the Bifrost client port. Host routing and firewall rules govern its reach to
the LAN, the instance metadata IP, and the outbound internet. The API owns
the private token volume. The connector mounts that volume read-only and runs
cloudflared with a token file. Kubernetes users add
`k8s/cloudflare-tunnel` to the consuming overlay's `components` and roll
out the API and connector with the matching release image. The component
gives the API ServiceAccount get and patch access to one pre-created Secret.
The connector mounts only that Secret and has no ServiceAccount token.
The component denies pod ingress and permits outbound worker ingress on TCP 8001, cluster DNS,
and documented global Cloudflare Tunnel endpoints on TCP/UDP 7844. Validate
Service and DNS handling with the installed CNI before enabling a Managed
Tunnel. Kubernetes can store a NetworkPolicy even when no CNI enforces it.
The API's read-only preflight verifies permission to project the connector
Secret. Prove enforcement with a live denied connection from a pod selected
by the connector policy and a successful connection to the same listening
target from an unselected pod. Check both Service IPs and direct endpoints
where applicable. Egress to the local node and host-network workloads
depends on the CNI and Service routing. Kindnet allowed both the Kubernetes
API Service and the direct same-node endpoint in the rehearsal. Require a
cluster-specific host firewall or CNI host policy when the connector must be
unable to reach the control plane, and probe both paths from the connector.
The component disables the connector's ServiceAccount token independently
of network enforcement.
[Kubernetes NetworkPolicy reference](https://kubernetes.io/docs/reference/kubernetes-api/networking/network-policy-v1/)

Cloudflared establishes outbound connections and listens for metrics on
loopback. The Deployment has no inbound health probes.
[Cloudflare Tunnel connections](https://developers.cloudflare.com/tunnel/configuration/)

For NodeLocal DNSCache or another
nonstandard resolver, patch the DNS egress rule in the consuming Kustomize
overlay to match the actual resolver destination and CNI behavior.
The global edge IPs are hard-coded in the component's `network-policy.yaml`.
Refresh the IPv4 and IPv6 entries against the published region1 and region2
addresses in that file or a consuming overlay, then render and validate
connectivity with the installed CNI.
[Cloudflare Tunnel firewall rules](https://developers.cloudflare.com/tunnel/configuration/#firewall-rules)

**Proposed separate hardening.** Loopback-bind backend ports is a separate
proposed change outside this branch. It requires a breaking-change Product
Updates disposition and an upgrade note showing how to replace port mappings
with `ports: !override`.

Activation saves consent and policy and queues one Enable job. That job
creates and verifies the tunnel, ingress and proxied CNAME, checks for a
live connector, then runs the signed Worker canary. If a connector is lost, Reconcile
closes admission when it observes the loss. A transient Cloudflare REST
outage leaves admission as observed before the failed reconcile. On ingress
drift, Reconcile closes admission before reasserting the approved config on
the exact owned tunnel. Private-network routes targeting the tunnel also close
admission and require operator removal. After the connector returns, run
Enable or Reconcile to verify ingress and the signed canary before reopening admission.
Use Scale In before Rotate Connector Token. The supervisor restarts its
cloudflared child when it receives the new token. Bifrost then evicts the
previous connector sessions by ID and verifies a replacement connection.
Disconnect removes the
owned DNS record, verifies zero connectors, removes the exact tunnel and
verifies deletion before retiring the installation. Complete Disconnect
before removing the Kustomize component or Compose token volume.

Account-token replacement proves write authority per owned resource class.
Workers retain the live-script endpoint write check. For the exact tunnel,
Bifrost writes its approved ingress and verifies the readback. On ingress
drift, it closes admission before using that fenced repair as the write proof.
It PATCHes the exact DNS record with its current owned body. DNS identity
uses the provider ID and approved body, including the installation and effect
IDs in the comment. This lets a lost same-body PATCH response advance the
provider timestamp without invalidating the cleanup fence. The final check
compares all relevant inventory receipts before switching credentials.
An absent class needs no write probe. Its first create reports missing
management permissions through Reconnect Cloudflare.

## Pending hosted qualification

Obtain explicit approval for the account, zone, time limit, Worker cap, and
spend envelope. Use fresh dedicated hostnames and separate disposable Compose
and Kubernetes installations. Start with read-only entitlement and grant
checks. Use the same packaged release and pinned cloudflared build in both
layouts. Record only sanitized resource receipts and job outcomes.

1. Preflight and activate Managed Tunnel once. Poll the returned Enable
   job. Independently verify exact account, zone, tunnel
   ID, CNAME and connector sessions. Exercise a signed hello, scoped SDK call
   and terminal receipt. Probe login, OpenAPI, wrong Host, wrong method,
   normalized path escapes and unsigned WebSocket hello for refusal.
2. Inject a DNS failure after tunnel creation and simulate a lost create
   response. Re-observe the same IDs and confirm one CNAME and one tunnel.
3. Scale In and rotate the connector token. Verify old live sessions are
   dropped and the old token cannot establish new sessions. Verify a
   replacement connection. Roll the Deployments or restart the Compose connector with
   retained configuration. Enable and repeat the signed canary.
4. Stop the connector during a harmless run. Observe admission closure and
   bounded reconnect or terminal recovery. Restore it and verify the same
   tunnel and DNS identities before resuming admission.
5. Disconnect and independently verify zero attached connector sessions,
   exact DNS absence, the tunnel tombstone or exact-ID absence, and an empty
   local credential projection. Preserve the cleanup receipt and revoke the
   temporary management token within the approved window.
6. Confirm the live `warp-routing` echo is disabled after Bifrost explicitly
   sends `enabled: false`. Record the normalised ingress GET shape and empty
   origin-request fields. Verify the Tunnel Write grant can list
   `teamnet/routes` for the owned tunnel. Confirm extra rules trigger admission
   closure and fenced repair. Qualify each account-token write probe and
   verify missing Tunnel Write or DNS Write preserves the incumbent token.

## Combined Hosted Qualification Disposition

The Existing HTTPS canary succeeded. The combined run stopped at the
Enable execution-class gate before Managed Tunnel Connect. The disabled
warp-routing echo, private-route list, and connector rotation with
old-session eviction remain unqualified on Cloudflare.

See the [sanitized combined hosted receipt](../../results/cloudflare-scaleout-combined-hosted.json).

## Combined Hosted Re-run Disposition

The Existing HTTPS worker preflight stopped on an IPv4 TLS handshake
timeout. The re-run ended before Managed Tunnel Connect. The warp-routing
echo, route-list grant and connector rotation with old-session eviction
remain unqualified on Cloudflare.

See the [sanitized re-run receipt](../../results/cloudflare-scaleout-combined-hosted-rerun.json).

## Combined Hosted Run With HTTP Connection Reuse

The 2026-11-22 UTC run passed Existing HTTPS Enable and stopped at the
local primary hold gate before Managed Tunnel Connect. The managed
`warp-routing` echo, route-list grant and live old-session eviction were
not exercised. The Existing HTTPS tunnel was independently verified
deleted with zero live sessions.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json).

## Observable-Gate Hosted Run

The 2026-11-22 UTC run passed Existing HTTPS Enable and stopped at the
cold latency measurement before Managed Tunnel Connect. The warp-routing
echo, route-list grant and old-session eviction were not exercised.
The separately owned Existing HTTPS tunnel was verified deleted with
zero live sessions.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).

## Hosted Cold and Warm Reuse Run

The run from `788ffc641` stopped after Existing HTTPS latency at a
post-reuse harness gate, before Managed Tunnel Connect. The
warp-routing echo, route-list grant and old-session eviction remain
unqualified on hosted. The Existing HTTPS tunnel had a deletion
tombstone with zero live sessions.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-latency-reuse.json).

### Combined Hosted Run at `db39a3698`

The Existing HTTPS Enable ownership gate stopped the run before the
Managed Tunnel leg. This run produced no hosted warp-routing echo,
route-list grant, or rotation evidence. The Existing HTTPS tunnel was
retired, with a deletion tombstone and zero sessions.

See the [sanitized ownership-stop receipt](../../results/cloudflare-scaleout-combined-hosted-ownership-stop.json).

### Hosted Preset B Stop at `e7c59788c`

Managed Connect accepted preset B with admission closed. Enable
returned `CF_OWNERSHIP_CONFLICT` with resource class `tunnel_ingress`,
check `ingress_occupied`, observed category `already_configured` and
expected category `empty_or_matching`. The stop preceded the signed
canary, warp-routing echo, route-list grant readback, and rotation.
Disconnect verified a deleted tunnel with zero sessions. The DNS
intent had no provider ID. Exact-name inventory and both authorities
verified its absence.

See the [sanitized preset B receipt](../../results/cloudflare-scaleout-preset-b-hosted.json).

### Catch-all during initial provisioning

Cloudflare documents `http_status:404` as a catch-all with no origin.
Its remote tunnel setup guide creates a tunnel before installing ingress
rules. The hosted stop retained only a categorized error. The public
examples and receipt leave the exact initial response body and version
unconfirmed. The single 404 rule is the narrow provider-default shape
modeled by the local fixture.
[Create a remote tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel-api/) · [Configuration API](https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/subresources/cloudflared/subresources/configurations/)

Bifrost accepts a single 404 catch-all regardless of version during
initial provisioning, including a retry after a lost create response.
The committed creation secret must prove the exact tunnel, and its
configuration must identify the same account and tunnel. The rule can
have only blank matching fields, empty origin options, and default or
disabled WARP routing. Once a receipt is recorded, the catch-all alone
is ingress drift and requires repair. A conflict reports the version,
recognized keys, rule count, and service categories. Unknown keys are
reported as `other`; provider IDs, hostnames and rule values are omitted.

The local provider starts with version 0 and a null configuration.
`combined-stack.py --default-tunnel-config` starts with the catch-all.
`combined-stack.py --missing-tunnel-config` returns HTTP 404 before
ingress is written. Use `combined-rehearsal.py --managed-only` to
exercise items 6–8 and final teardown with each response.

CT128 exercised combined items 6–8 at `a580a0b70` with both pinned
release packages and internal-only fixtures. The Managed Tunnel stub
started with the version 1 404 rule. Enable programmed the approved
route, then returned a signed canary receipt. Route listing returned
200 and the WARP echo was disabled. Rotation replaced one old session
with one new session and refused the old token. Upgrade reconnected the
original claim, delivered and acknowledged its terminal result after one
dispatch, and verified three old script and four old namespace 404s.
Final Disconnect removed the stub tunnel with zero sessions and no DNS
record. The hosted behavior of this change remains unverified.

### Hosted version-zero configuration envelope

The Managed-only run at `a9226cb62` stopped at
`tunnel_ingress / ingress_occupied / already_configured / empty_or_matching`.
The provider returned version 0 with the envelope keys `config`,
`created_at`, `source`, `tunnel_id`, and `version`. The missing
`account_id` key fails the exact configuration identity check before
Bifrost evaluates the empty-config or catch-all rule. Shape diagnostics
classify `config` as a non-object without recording its raw value, so
the run cannot establish that the value was JSON null.

The signed canary, disabled WARP echo, route-list grant and connector
rotation remain unqualified on this run. Disconnect verified the tunnel
tombstone, zero sessions, and DNS and token absence.

See the [sanitized Managed-only receipt](../../results/cloudflare-scaleout-managed-hosted-a922.json).

The configuration GET schema marks `account_id`, `tunnel_id`, `config`,
and `version` optional. The request path selects the exact account and
tunnel. Bifrost checks both identifiers when echoed, and initializes
version 0 with a null or absent config only during initial provisioning.
A recorded receipt must match the approved ingress or use the fenced
repair path. [Cloudflare configuration GET](https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/subresources/cloudflared/subresources/configurations/methods/get/)

The tunnel read accepts an omitted account tag on the scoped path and
uses `remote_config: true` only when `config_src` is absent. The
connector read can use one consistent nested `client_id` if the parent
ID is omitted. The filtered route read treats a row with no echoed
tunnel ID as a route on the selected tunnel and blocks admission.
[Tunnel GET](https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/subresources/cloudflared/methods/get/) ·
[Connections GET](https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/subresources/cloudflared/subresources/connections/methods/get/) ·
[Routes GET](https://developers.cloudflare.com/api/resources/zero_trust/subresources/networks/subresources/routes/methods/list/)

DNS retains its exact CNAME, record ID, modification time, and
ownership comment checks. The connector token endpoint returns a
string. The decoded token still proves the account, tunnel, and
creation or rotation secret. Preflight checks the account entitlement
with the supplied management token. [DNS record GET](https://developers.cloudflare.com/api/resources/dns/subresources/records/methods/get/) ·
[Connector token GET](https://developers.cloudflare.com/api/resources/zero_trust/subresources/tunnels/subresources/cloudflared/subresources/token/methods/get/) ·
[cloudflared token fields](https://github.com/cloudflare/cloudflared/blob/2026.11.1/connection/connection.go)

### Hosted Managed Enable and Rotation at `8712eb4c6`

The hosted preset B run passed Enable and the signed canary. The exact
owned tunnel, proxied CNAME and approved ingress matched independently.
The provider echoed disabled WARP routing and granted the scoped route
list with HTTP 200 and zero private routes. The approved configuration
read back at version 1. Successful Enable leaves the initial configuration
body unclassified in its receipt.

Rotation evicted the old connector session, refused its token, and
established one replacement session. Worker preflight and signed re-Enable
passed on `remote_launcher`. Disconnect verified exact DNS absence, the
tunnel tombstone, zero sessions, and an empty connector token projection.

See the [sanitized run receipt](../../results/cloudflare-scaleout-managed-hosted-8712.json).
