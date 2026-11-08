# GitHub App Solution

Decision: deliver two portable Solutions. Install the operator package in the
deployment's provider organization. Its sealed code owns App authentication,
verified immutable installation bindings and private signed ingress. Install the
consumer example in each destination organization; explicitly allow inbound topic
events there. No package owns a platform worker, queue, credential resolver or Git
sync path.

The App key lives only in the operator organization's integration mapping, never
the global defaults. Existing `integrations.read` and `secrets.read` boundaries
apply. Tokens stay in encrypted operator-org Config values, never Tables, event
payloads, workflow results or bundles. API requests use the fixed GitHub.com host,
bounded timeouts, read-only metadata installation tokens and no credential fallback.

Bindings are insert-only: one installation belongs to one Bifrost organization
and one explicitly selected consumer Solution. Multiple installations may share
that destination. A bind verifies the installation through the App API. Lifecycle
events append invalidation fences; only a fresh successful API inventory can
produce a usable snapshot. Late token refreshes and inventories cannot override a
new fence. Tenant routing checks the current snapshot, repository membership and
binding before publishing an envelope to that tenant's topic.

The generic signed webhook boundary needs #1285: `X-GitHub-Delivery` claims a
retained Event before any workflow fan-out. Existing EventDelivery/run staging,
worker execution and recovery remain authoritative. Downstream topic publication
is at least once: recovery can repeat a publication after an uncertain handoff.
The consumer example atomically inserts the complete receipt by stable delivery
identity; its only effect is that receipt. Future external writes must establish
their own provider idempotency contract rather than claiming exactly once.

Package Tables hold append-only fences, request slots, cache metadata, inventory
snapshots and recovery receipts. Shared fixed time-slot admission bounds request
rate per installation and App; refresh claims coalesce token minting across
workflow processes. Quotas belong to the installation, not each minted token.
Rate-limit observations persist a cooldown, with no inline automatic HTTP retries.
Periodic reconciliation uses an ordinary scheduled workflow and bounded pages.
GitHub does not automatically retry failed webhooks. Explicit redelivery verifies
the exact provider delivery ID and installation, records intent before POST and
leaves uncertain outcomes for inspection before any further attempt.

Manual registration and secret configuration are operator steps. Packaged ingress
and schedules start disabled. Existing Solution setup, Forms, workflow results
and Event history supply the UI. Setup must verify a sufficient Event retention
window before enabling ingress; deletion or retention ends ingress deduplication.
Installation and repository revocation are checked on reconciliation and fenced
immediately when their lifecycle event is processed. Cached inventory is usable
for at most 60 seconds; this is point-in-time admission, not a distributed
transaction with GitHub.

Manifest onboarding, GitHub write-back, App-based Git sync, ownership transfer,
fleet brokers and live registration/deployment are outside this package release.
