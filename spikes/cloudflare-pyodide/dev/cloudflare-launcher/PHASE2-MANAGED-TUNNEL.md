# Phase 2 Managed Tunnel

Managed Tunnel uses an exact hostname in an active DNS zone owned by the
selected Cloudflare account. Supply the HTTPS origin and zone ID at Connect.
The account token needs Account Settings Read, Workers Scripts Write and
Cloudflare Tunnel Write on that account. Grant Zone Read and DNS Write only
for the selected zone. Leave the dedicated hostname unoccupied.

Compose installs a dormant connector with the API release image. The API owns
the private token volume. The connector mounts that volume read-only and runs
cloudflared with a token file. Kubernetes users add
`k8s/cloudflare-tunnel` to the consuming overlay's `components` and roll
out the API and connector with the matching release image. The component
gives the API ServiceAccount get and patch access to one pre-created Secret.
The connector mounts only that Secret and has no ServiceAccount token.

Use Enable to create and verify the tunnel, ingress and proxied CNAME before
running the existing signed Worker canary. If a connector is lost, Reconcile
closes admission when it observes the loss. After the connector returns, run
Enable to verify ingress and the signed canary before reopening admission.
Use Scale In before Rotate Connector Token. The supervisor restarts its
cloudflared child when it receives the new token. Disconnect removes the
owned DNS record, verifies zero connectors, removes the exact tunnel and
verifies deletion before retiring the installation. Complete Disconnect
before removing the Kustomize component or Compose token volume.

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
3. Scale In and rotate the connector token. Observe the replacement
   connector sessions and verify the old token cannot establish a new
   session. Roll the Deployments or restart the Compose connector with
   retained configuration. Enable and repeat the signed canary.
4. Stop the connector during a harmless run. Observe admission closure and
   bounded reconnect or terminal recovery. Restore it and verify the same
   tunnel and DNS identities before resuming admission.
5. Disconnect and independently verify zero attached connector sessions,
   exact DNS absence, the tunnel tombstone or exact-ID absence, and an empty
   local credential projection. Preserve the cleanup receipt and revoke the
   temporary management token within the approved window.
