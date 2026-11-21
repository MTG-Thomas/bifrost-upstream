# Phase 2 Managed Tunnel

Managed Tunnel uses an exact hostname in an active DNS zone owned by the
selected Cloudflare account. Supply the HTTPS origin and zone ID at Connect.
The account token needs Account Settings Read, Workers Scripts Write and
Cloudflare Tunnel Write on that account. Grant Zone Read and DNS Write only
for the selected zone. Leave the dedicated hostname unoccupied.

Every Compose installation includes a dormant connector with the API release
image. The connector shares a dedicated bridge with the API. Its access to
the API's main port remains possible on that bridge. Database, connection
pooler, Redis and object-store host ports bind to loopback. The API owns
the private token volume. The connector mounts that volume read-only and runs
cloudflared with a token file. Kubernetes users add
`k8s/cloudflare-tunnel` to the consuming overlay's `components` and roll
out the API and connector with the matching release image. The component
gives the API ServiceAccount get and patch access to one pre-created Secret.
The connector mounts only that Secret and has no ServiceAccount token.
The component's egress policy permits worker ingress on TCP 8001, cluster DNS,
and documented global Cloudflare Tunnel endpoints on TCP/UDP 7844. Validate
Service and DNS handling with the installed CNI. Installations with a
nonstandard DNS resolver need a matching DNS policy.
[Cloudflare Tunnel firewall rules](https://developers.cloudflare.com/tunnel/configuration/#firewall-rules)

Use Enable to create and verify the tunnel, ingress and proxied CNAME before
running the existing signed Worker canary. If a connector is lost, Reconcile
closes admission when it observes the loss. A transient Cloudflare REST
outage leaves admission as observed before the failed reconcile. On ingress
drift, Reconcile closes admission before reasserting the approved config on
the exact owned tunnel. After the connector returns, run
Enable to verify ingress and the signed canary before reopening admission.
Use Scale In before Rotate Connector Token. The supervisor restarts its
cloudflared child when it receives the new token. Bifrost then evicts the
previous connector sessions by ID and verifies a replacement connection.
Disconnect removes the
owned DNS record, verifies zero connectors, removes the exact tunnel and
verifies deletion before retiring the installation. Complete Disconnect
before removing the Kustomize component or Compose token volume.

Account-token replacement proves write authority per owned resource class.
Workers retain the live-script endpoint write check. For the exact tunnel,
Bifrost reasserts the observed, approved ingress and verifies the readback.
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

1. Enable Managed Tunnel. Independently verify exact account, zone, tunnel
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
6. Record the normalised ingress GET shape, including default warp routing
   and empty origin-request fields. Confirm extra rules trigger admission
   closure and fenced repair. Qualify each account-token write probe and
   verify missing Tunnel Write or DNS Write preserves the incumbent token.
