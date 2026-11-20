# Phase 1 Product Pilot

**November 20, 2026 UTC · Base `510ff121e`**

## Usage-only limit

Bifrost's monthly limit now covers attributable usage. The fleet envelope
and admission meter exclude the $5 Workers Paid subscription. Status shows
the subscription separately as **Workers Paid Subscription (Billed by
Cloudflare)**. Using the same release bounds, the minimum fleet envelope is
$13.31 and the two-launcher, 300-second envelope is $14.51.

## First pilot

The product rejected Connect under the authorized $5 monthly limit. The
Connect REST endpoint returned HTTP 422 with a **$19.51 fleet spend envelope**
for two one-slot launchers with a 300-second lifetime. The same product
calculator returned **$18.31** for one one-slot launcher at the 60-second
minimum lifetime. No Workers were deployed.

The first-run limit covered the $5 Workers Paid subscription base and the release's
gross usage envelope. Cloudflare rounds billable Durable Object requests
and duration up to billing units. Bifrost prices this envelope without
relying on shared-account allowances. The first-run guard rejected the $5
policy.
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) ·
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/).

## Preflight

The trial reserved a 30-minute hosted window from connector startup, up to
two Workers launchers, zero Containers, and under $0.50 estimated
incremental spend. The connector never started. The existing Workers
spend notification guard was enabled. Only the existing Paid account
was visible to the operator.

The disposable stack started within the shared guest limit with no other
Bifrost stacks running and 15 GiB available on the host. The first
Compose build stalled during dependency installation. An isolated
install with the same pinned Node image and lockfile passed, and one
unchanged-source rebuild succeeded. The product release loader verified
the bundled launcher and capacity modules from the exact base. The
release manifest SHA-256 was
`c76e4de48b8c4c1c136efc3190556052fe90d735ef6d97c4f9370e4b40ea0da9`.

The dedicated, expiring, account-owned token had exactly **Workers Scripts
Write**, **Account Settings Read**, and **Account Analytics Read** on the
selected account. A remote-managed trial tunnel was created. The intended
Existing HTTPS Endpoint was a dedicated hostname under the environment's
`PHASE7_ZONE`, targeting only the loopback publication of worker ingress
port 8001. No DNS record or public route was created.

## Product checks

| Step | Result | Evidence |
| --- | --- | --- |
| 1. Connect | Fail | The product issued the staged credential reference. Connect submitted the current consent version, explicit consent, the Existing HTTPS Endpoint, and the $5 policy. The Paid entitlement check completed before policy pricing rejected the request. Product status afterward returned `installation: null`. No consent or installation was committed. |
| 2. Enable | Not run | No installation existed. There was no enable job, REST deploy, canary, or open admission. |
| 3. Burst | Not run | No enabled Workers scope or workflow executions. |
| 4. Spend | Not run | No usage ledger or trial Worker observations. No calibration ratio or admission toggle. |
| 5. Ingress refusal | Not run | The connector and public route stayed absent. |
| 6. Reconcile | Not run | No deployment or reconcile job. |
| 7. Scale In | Not run | No enabled scope or drain job. |
| 8. Disconnect | Not run | No installation for a Disconnect job or a product absence receipt. Independent cleanup is recorded below. |
| 9. Free-plan control | Not run | No available Free account. |

Estimated incremental trial spend was **$0.00**. No Workers, Durable
Objects, Containers, or model calls were used. This excludes the
account's existing subscription.

## Teardown

The product returned `installation: null` after Connect failed. The
product's Disconnect and absence-verification job therefore did not run.
The staged product credential was purged when the disposable database
volume was removed.

Independent `cf` readback found zero trial Workers, Durable Object
namespaces, DNS records, account tokens, and live connectors. The trial
tunnel had a deletion record with zero connections. No Container app or
registry image was created. Docker readback found zero containers,
networks, volumes, or images for the pilot project. The operator removed
the local token and tunnel credential files from the guest.
