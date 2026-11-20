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

## Usage-only rerun

**November 20, 2026 UTC · Product source `393037492`**

The rerun used a $25 monthly usage ceiling. The trial targeted less than
$0.50 in incremental spend and had a $1 abort threshold. The connector
window was capped at 25 minutes and ran from 15:02:03 to 15:08:37 UTC.
The account's $25 Workers notification was enabled. The product policy
allowed two one-slot launchers with a 300-second lifetime. No launcher
incarnation, Container, or LLM call was started. The shared host had one
other Bifrost test stack and 14 GiB available at debug startup.

The exact packaged Worker modules were unchanged from the first pilot.
The rerun loaded them with the tested product source revision and verified
the module hashes and release manifest digest
`54aa30998c0ebec57699991ea642f108fca739972aa96254240d3463f08b7292`.
The fresh, expiring account token had **Workers Scripts Write**,
**Account Settings Read**, and **Account Analytics Read**. The tunnel
routed the versioned worker-ingress paths and Enable probe paths to
the listener on port 8001. Other paths received 404.

| Step | Result | Evidence |
| --- | --- | --- |
| 1. Connect | Pass | HTTP 200. Installation `bea3fe96-d238-40fe-9c3d-ca07b13e1a24`, revision 1. Consent `existing-https-v1` recorded at 15:00:33 UTC, using staged credential `291f4c98-cfad-46e2-97b6-f5f7e09cb88d`. The Paid check passed. Status showed a $14.51 fleet envelope, $0.00 usage, and a separate $5.00 subscription. |
| 2. Enable | Fail | Job `d0d41ff0-4601-4258-8115-b16d73660b91` passed entitlement and deployed both packaged scripts. Both IPv4 addresses passed 100 route checks and the WebSocket refusal check. Neither advertised IPv6 address was reachable from the API runner. The job returned `CF_WORKER_INGRESS_UNVERIFIED` before the signed runtime canary, and admission stayed closed. Cloudflare rejected the trial-record-only IPv4 setting with error 9227. |
| 3. Burst | Not run | The product accepted the loose and Solution approvals. Admission never opened, so no workflows were submitted for placement and no execution IDs were issued. |
| 4. Spend | Fail | Self-meter status and the partial Analytics comparison were observed. Policy rejected a $0.01 limit with HTTP 422 because the static envelope was $14.51. At $0.00 estimated usage there was no valid positive limit below the estimate. Even with successful burst work, a limit below $1 could not satisfy the $13.31 minimum fleet envelope. Admission close and reopen remained untested. |
| 5. Ingress refusal | Pass | From outside the guest, login, admin, and OpenAPI each returned 404 with the ingress denial marker. SDK context returned 401 with the allowed marker, and the catch-all path returned 404. |
| 6. Reconcile | Pass | Job `0d5b5f69-8c23-40d2-9232-07a4b01d68e6` returned `verified` with no reported deployment drift. |
| 7. Scale In | Pass for an empty fleet | Job `0d406dd3-97f9-4a2d-87ed-4d2ecc9a8afc` returned `scaled_in`, revision 2 with admission closed. Independent readback showed both scripts and all four namespaces retained. Active drain was not exercised. |
| 8. Disconnect | Pass | Job `a118a6e9-1533-49de-a29d-c670e51b2a55` returned `disconnected`. REST returned `installation: null`. The journal had two verified-absent script records, zero scopes, and zero retained credential ciphertext or staged credentials. |
| 9. Free-plan control | Not run | Only the existing Paid account was available. |

### Spend calibration

Admission closure at the limit is covered by the existing admission E2E
tests, which put the ledger at or above the configured limit. The hosted
spend check calibrates the estimate against observed workload usage. The
static fleet envelope has a minimum of $13.31, so a below-spend limit is
outside the trial's $1 abort threshold.

The 15:02:00–15:05:51 UTC GraphQL sample returned **29 estimated capacity
Worker requests and 12,694 µs of CPU**, with a sample interval of 1 and no
launcher or Durable Object samples. The observed Workers-only marginal
price was about **$0.000009**. The Bifrost ledger showed $0.00 with no
launcher reservations or workflow assignments, so the displayed-estimate
to sampled Workers-only ratio was **0×**. This measures idle control-plane
traffic outside the ledger proxies. There was no workload calibration
ratio or complete provider cost comparison. A query following deletion
returned no rows. Estimated incremental trial spend was **under $0.01**,
excluding the existing Workers Paid subscription.
[Workers metrics](https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-workers-metrics/) ·
[Durable Object metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/).

### Verified cleanup

After product Disconnect, independent `cf` inventory found zero trial
Workers and zero Durable Object namespaces. The operator removed the
tunnel, DNS record, and account token. Readback showed zero live connectors,
tunnels, DNS records, and trial account tokens, with a tunnel deletion
record. Docker reported zero containers, networks, volumes, and images
for the trial debug project. The connector process and staged local
credential files were absent. No Container apps or registry images were
created.
