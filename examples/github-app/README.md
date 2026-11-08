# GitHub App Integration Package

This package uses existing Bifrost Solutions, integration secrets, generic signed
webhooks, Tables, Forms and workflows. It requires the optional webhook delivery
identity capability in upstream PR #1285. The native architecture in #1274 stays
separate and unmerged. Existing PAT Git sync is unaffected.

Build the two install ZIPs with `python examples/github-app/build.py --output /tmp/github-app`.
The ZIPs contain definitions and source only. Build inputs are explicitly selected;
`.env`, credentials, runtime table rows, caches and generated dependencies are excluded.

## Manual Setup

1. Install `github-app-operator-0.1.0.zip` in the deployment's provider organization
   using Solutions. Install `github-app-consumer-0.1.0.zip` in each destination org.
   The operator install allows outbound calls and rejects inbound cross-Solution
   access. The consumer accepts inbound topic events and has no outbound fallback.
2. Review the requested workflow permissions. Assign the operator run identity
   explicitly: provider-org reach, Integrations Read, Secrets Read, both Configs
   Read and Configs Read/Write, both Events Read and Events Read/Write, Tables
   Read/Write, Solutions Read, and access to its private state table through the
   GitHub App Operators role. Grant the tenant
   receipt identity the GitHub App Consumers role and access only to its own
   receipt table. Package roles allow append/read, with maintenance restricted to
   admins; installing does not assign identities or grant cross-org reach.
3. An operator registers their own GitHub App manually for this deployment, with
   repository Metadata Read and the selected read-only event subscriptions. The
   package does not register, install or transfer an App. Use the App-level webhook
   URL from **GitHub App Signed Ingress**, not per-tenant callback URLs.
4. In the **GitHub App Operator** integration, configure `app_id` and `private_key`
   as overrides for the provider organization only. **Leave global/default values
   empty.** Existing tenant Secrets Read must never grant access to that mapping.
   The normal Solution connection UI supplies the encrypted secret field. No key
   or token belongs in a manifest, table, workflow input, issue or bundle.
5. Set the matching webhook signing secret with the existing **Rotate Secret**
   control or `POST /api/events/sources/<id>/rotate-secret` and on the GitHub App.
   This instance-only endpoint supports Solution-managed sources. Preserve
   `X-Hub-Signature-256`, `sha256=`, `X-GitHub-Event` and
   `X-GitHub-Delivery`. Verification is mandatory when delivery identity is enabled.
6. Open **GitHub App Setup And Health**. Run Health, then Verify And Bind with an
   installation ID and the explicitly selected destination organization and
   consumer Solution UUIDs. Binding verifies the installation through GitHub,
   reads its repository inventory and stores no credentials in the result. An
   installation cannot be rebound automatically; multiple installations may
   target one destination.
7. Verify the Event retention window covers the desired recovery/redelivery
   window. Deleting an Event or allowing it to expire ends ingress deduplication.
   Configure rate limits in the package for expected traffic. After setup checks
   pass, rebuild with `--enable-ingress` and optionally `--enable-reconciliation`
   and redeploy the operator ZIP to the same install. Activation is definition-owned;
   the normal redeploy preserves the encrypted signing secret. Both ship disabled.

## Behavior And Operations

The App key signs a short-lived App JWT. Read-only installation tokens are cached
as encrypted operator-org Config values and refreshed through shared durable
claims. Their metadata contains no plaintext credentials. A per-installation
request scheduler and persisted quota cooldown coordinate concurrent workflows;
creating more tokens does not create more installation quota. Recent Deliveries
lists the first 100 App-level records filtered to the bound installation; IDs are
returned as strings. Use GitHub history for older records. Requests never
follow redirects, use personal credentials or retry a write automatically.

Ingress persists an Event before asynchronous workflow execution. Repeated signed
deliveries reuse its EventDelivery and execution identity. Installation lifecycle
events invalidate inventory; repository events route only when their repository
is in a current verified inventory. Reconciliation checks revocation, suspension,
account identity and repository removal. Inventory cache age is at most 60 seconds;
this is point-in-time admission. Removed or revoked access fails closed. Failed
verification and stale work cannot reactivate a newer invalidation fence.

The tenant topic envelope includes a stable delivery GUID, Event ID, installation,
repository and payload. Topic publication is at least once after uncertain worker
handoffs. The consumer example inserts the complete receipt atomically by
installation/GUID and rejects conflicting content. Its receipt is its only effect.
Use it as a read-only PR/issue/check/release event example; external writes require
their own reviewed idempotency contract and are deliberately not enabled here.

Inspect Event history and workflow results for failures. Use the existing Event
retry/recovery controls for retained deliveries. GitHub does not automatically
retry failed webhook deliveries. Request Redelivery accepts the exact decimal
GitHub delivery ID (as text), verifies its installation, records intent, and sends
one request. After a timeout or other uncertain response, inspect GitHub delivery
history before acting again. The package refuses to resend an existing intent.

Run Reconcile Inventory for one installation or enable the periodic workflow.
Scans are bounded to 100 repository pages and 50 installations per schedule; split
larger schedules deliberately. Health returns current readiness, inventory counts
and quota cooldowns, without secrets. Table queries fail closed above 1,000
matching records; archive old telemetry/request slots and superseded snapshots
before that bound. Keep active inventory and token metadata,
bindings, redelivery intents and retained tenant receipts. Ingress retention and
state-table maintenance are explicit operator responsibilities in this release.
Archiving fences changes the epoch and requires a fresh reconciliation. Every
distinct retained invalidation participates in the epoch, regardless of host clock skew.

No production deployment or fresh live-provider qualification is implied by local
tests. Manifest App setup, GitHub write-back, App Git sync, ownership transfer and
fleet broker work remain staged later.
