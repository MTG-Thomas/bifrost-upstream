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
| 4. Spend | Fail (hosted calibration incomplete) | Self-meter status and the partial Analytics comparison were observed. Policy rejected a $0.01 limit with HTTP 422 because the static envelope was $14.51. Admission closure at the limit is E2E-only because the $13.31 minimum fleet envelope exceeds the hosted trial's $1 abort threshold. |
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

## Dual-stack follow-up

The earlier probe used HTTPX with a validated literal address and the
WebSocket client with a socket connected to that same address. It
required every address to connect, so a later `ENETUNREACH` invalidated
successful IPv4 checks. The signed canary already opens its connections
by hostname from Cloudflare. The ingress probe now keeps looking after
unavailable addresses and requires HTTPS, the route matrix, and the
WebSocket checks on the addresses it reaches. Invalid TLS certificates
and nonpublic DNS answers still block Enable.

The live follow-up ran the fixed probe on a hostname with two IPv4 and
two IPv6 addresses. Both IPv6 connects returned errno 101, and IPv4
passed all 100 route checks and WebSocket validation. The probe reached
the canary callback boundary. Connect created installation
`91f2d69f-128c-40ad-a14d-879a7f21a6fa`, but the first Enable job
`3a582d41-3e16-4865-aea2-1c0bb80ed13e` stopped earlier with
`CF_DEPLOYMENT_UNVERIFIED`. Both scripts and all four namespaces
existed. The capacity script's journal remained in `creating`.

Cloudflare's audit log recorded HTTP 200 for the capacity script's
endpoint update at 15:26:38 UTC. The job stopped before its next
ownership read. A later read-only check found the exact namespace and
deployment, the endpoint enabled, and previews disabled. This places
the failure between the successful update and final ownership
verification, where Enable required an immediate GET of the new
subdomain settings. The fix validates the documented update response
for both settings and retains final ownership verification. The
regression fixture leaves the GET response stale after the update and
checks refusal when the update returns mismatched settings.
[Cloudflare update response](https://developers.cloudflare.com/api/resources/workers/subresources/scripts/subresources/subdomain/methods/create/).

Reconcile job `363f6e09-393b-49f5-981d-eb7d2d92a40a` could not verify
the incomplete receipt. Scale In job
`9bcd9a42-bd21-411f-afe4-ccb28a49cbc5` completed with an empty fleet.
Disconnect job `484ec7cb-991b-4e21-8e3e-94877e4dff45` retired the
installation. Independent readback found zero scripts and namespaces
for that installation.

### Product run with both fixes

The follow-up used product source `94b46d246` and release manifest
`d1b32e36bba0cf9ff63944d23ae311f114bec9284ded133d6073a73a24577ce2`.
The new Connect used the existing scoped pilot token, with Workers
Scripts Write, Account Settings Read, and Account Analytics Read.
Both attempts shared the same 25-minute connector envelope starting
15:25:48 UTC, the $25 usage ceiling and notification guard, the $0.50
target, and the $1 abort threshold. The guest started with one other
test stack and 15 GiB available on the host. The trial used one
canary launcher in total and zero Containers or LLM calls.

| Step | Result | Evidence |
| --- | --- | --- |
| 1. Connect | Pass | HTTP 200 for installation `e759aaf2-bab4-4a31-ae58-8af2e768d9a2` with explicit consent and staged reference `bbc642cc-24e2-4509-8765-3baebd7c2333`. The Paid check passed and the policy returned a $14.51 envelope. |
| 2. Enable | Fail (scoped SDK canary) | Job `9a17b796-7ebe-4170-9fcd-a9dfbfa1245f` confirmed two packaged scripts, four namespaces, and public ingress. Hosted canary `69c4010d-49f8-498f-8a14-4b0b3928f32a` recorded signed hello sequence 1 and session generation 1. The API logged `RuntimeError: Capacity canary failed`. The SDK receipt and acknowledgement remained empty. The launcher retired with a verified physical deletion at 15:37:35 UTC. Admission stayed closed and the job returned `CF_WORKER_INGRESS_UNVERIFIED`. |
| 3. Burst | Not run | The product approved the loose workflow and the Solution-installed workflow. No workloads were submitted because admission stayed closed. |
| 4. Spend | Fail (hosted calibration incomplete) | Bifrost estimated $0.07. The sample included 72 capacity Worker requests and 35,806 µs of Worker CPU, with sample interval 1. The launcher and Durable Object datasets returned no rows. No workload calibration ratio was available. The runtime threshold is E2E-only. Both the exact-limit reservation test and the claim test with the ledger pushed past the limit passed. |
| 5. Ingress refusal | Pass | The public login, admin and OpenAPI requests all returned 404 with the worker-ingress refusal marker. SDK context returned 401 with the allowed marker. |
| 6. Reconcile | Pass | Job `baca42f6-4246-4a82-8f10-11f05d4d6ef0` returned `verified`. |
| 7. Scale In | Pass for an empty fleet | Job `f89e3a19-b479-47db-bdd1-75fca58c52d4` returned `scaled_in`. Both scripts and all four namespaces were retained. Active drain was not exercised. |
| 8. Disconnect | Pass | Job `1b11cb19-a54b-4ad5-a20f-32fe4fe1e9dc` returned `disconnected`. Product status returned `installation: null`. Across both attempts, the journal recorded four verified-absent scripts, two retired installations, and zero remaining scopes or credential ciphertext. |
| 9. Free-plan control | Not run | The operator could see only the Paid account. |

The Worker Logs query returned zero events for the launcher during the
canary. The scoped pilot token could read account Analytics and had no
zone Analytics permission. An external test reached the scoped canary
path and returned the expected 401 with an invalid proof. The failing
hosted SDK request's HTTP status and reason remain unobserved.

The connector stopped at 15:44:32 UTC. Independent `cf` and Docker
readback found zero trial Workers, Durable Object namespaces, Container
apps, account tokens, DNS records, live tunnels, connections, or project
containers, networks, volumes, and images. The tunnel deletion record
had no connections. Operator credential files were removed. No registry
artifacts were created.

The focused ingress, canary, and persistence suite passed 81 tests.
The Worker release and provisioning suite passed all 21 tests,
including the stale-read and mismatched-update fixtures.

## Scoped SDK canary fix and full pilot

The local trace loaded the exact previously deployed release manifest
`d1b32e36bba0cf9ff63944d23ae311f114bec9284ded133d6073a73a24577ce2`
in Miniflare/workerd. A local HTTP relay preserved the worker-ingress
host and forwarded to a real Bifrost ingress listener on port 8001.
For local job `fcef5a67-74ee-4bed-9c93-ab7819af0f5b`, the launcher
recorded signed hello for `02b9d8a7-72a9-4f08-8e50-1f03c68d50da`.
The relay saw no SDK request. A diagnostic copy exposed a workerd
`TypeError` while constructing the `Request`. The Workers runtime
accepts `follow` and `manual` for `redirect`. The bundled code used
`error`, so the canary stopped before DNS, TLS, ingress, or proof
verification. [Workers Request API](https://developers.cloudflare.com/workers/runtime-apis/request/)

The launcher uses `manual` and demands HTTP 204 with the exact
worker-ingress allow marker. Enable's Action Required reports a
redacted stage, error class, and HTTP status when present. The API
binds failure to the current job lease and launcher session. The
acknowledgement requires the server's own persisted proof receipt.
Local job `6aef916b-875b-48c1-aff5-006f9588ba8a` used rebuilt manifest
`d8f997c86da0b66b441dfaf6a4438bf44f999fa2bbe60c92f0397b88e47e2b3d`
and worker `df887906-7b7d-47cf-9310-e346f26f64a1`. The relay recorded
the SDK GET and HTTP 204 with the ingress marker. Bifrost recorded the
SDK receipt and ACK, then verified physical retirement. The local
reproduction removed the need for a separate hosted diagnostic.

The product trial used source `c0aa26b6e` and the rebuilt manifest.
The host had one other Bifrost stack and 13 GiB available at boot.
The Existing HTTPS Endpoint was `https://<trial-ingress>.<zone>`,
with exact SDK and probe path rules to the dedicated port 8001
listener and a catch-all 404. The connector ran from 16:14:30 to
16:25:25 UTC within a 25-minute limit. The fleet allowed two one-slot
launchers concurrently, with a 180-second maximum lifetime and a
12-second idle policy. The guard and $25 monthly Bifrost usage limit
were in effect. The static envelope was $14.17. The run used zero
Containers and zero LLM calls.

| Step | Result | Evidence |
| --- | --- | --- |
| 1. Connect | Pass | HTTP 200 for installation `c2cba184-51be-4491-91ed-f3424d66a620`, with `existing-https-v1` consent and staged reference `149153d0-c0a8-46b7-b5d2-7585f0f96bf8`. The dedicated account token granted Workers Scripts Write, Account Settings Read, and Account Analytics Read. |
| 2. Enable | Pass on the follow-up | The initial job `2d7d9086-52fa-49e5-837e-0a929b222ef5` deployed the two scripts and returned Action Required at the earlier ingress check with `ingress / route_refused` and no HTTP status. Its exact transport error was unobserved. Independent probes from the API and job runner passed the complete matrix on the reachable IPv4 addresses. A single subsequent product Enable job `acc04d20-c423-4a2b-a411-7d62043362d3` passed the Paid check, verified the packaged REST deployment and opened admission. The reserved worker `34b698ed-b1cb-4ce4-9e83-6495b816fca7` recorded signed hello sequence and session generation 1, SDK receipt at 16:18:15.161 UTC, ACK at 16:18:15.183 UTC, and verified deletion at 16:18:15.796 UTC. |
| 3. Burst | Pass | Two primary hold executions `cae9d964-a27a-461d-ab0f-6bfa5a2d13dc` and `bff02e66-c61f-4efb-a30b-bac52080ed56` left 24 MiB on each primary worker. In-product approvals accepted revisions `6e6ca300c1ecd787517f51c1a8c2475b219b83fa8257f94fda0c0c5f9fc30d56` and `6ba9df7c15b6b76977ef17acf06941668324df46e6f2f800ea305f955ea5394f`. Loose execution `eff6afe9-83f0-476a-8151-252547c4803f` and Solution execution `2f1f41d5-4a1c-45de-b474-05fa60200b08` ran concurrently and settled `Success`. Their `workers` claim rows are `164aba52-e523-4a40-99ed-6af5d342686f` and `9ab0f6a8-b21d-49fc-b247-f2bd1caf3805`. Their distinct launchers `95109e0e-44c1-44f1-abe8-22924418357c` and `abdbfa3d-a6f9-4956-bf76-ef35d6d42b48` retired with reason `idle` and physical deletion at 16:20:35.124 and 16:20:34.889 UTC. |
| 4. Spend | Pass for Workers sampling | Bifrost estimated $0.11. The ledger recorded 69.876 launcher-seconds, 69.876 slot-seconds, three claims, and five scoped SDK requests. Cloudflare returned Worker request and CPU samples during the workload window. Both Durable Object datasets returned no rows. The observed Workers-only cost proxy and ratio are below. Admission closure at the limit remains E2E-only. The $13.31 minimum envelope exceeds the $1 hosted abort threshold. |
| 5. Ingress refusal | Pass | External login, admin, and OpenAPI requests returned HTTP 404 with the Bifrost deny marker. SDK context returned HTTP 401 with the allow marker. The unmatched-path request returned the tunnel's HTTP 404. |
| 6. Reconcile | Pass | Job `3024a3a8-2091-4853-9bd4-a86e9076e28a` returned `verified`. |
| 7. Scale In | Pass | Job `52bf8827-99cd-430f-98bf-c8f91139d434` closed admission while Workers execution `9b45fd63-d0f6-4dc0-bdd6-4e398d3c7e50` was in flight under claim `e289a8b4-eba1-42ff-994d-a3d95876d99d`. The execution settled `Success`. The launcher retired with `cloudflare_admission_closed` and verified physical deletion. The job returned `scaled_in`. Independent readback found both scripts and all four namespaces retained. |
| 8. Disconnect | Pass after product resume | Job `a8ff000b-ae6f-42e3-909b-d55d71a055bd` entered Cleanup Pending with one verified-absent journal row and one deleting row. Independent Cloudflare readback then found both scripts and all namespaces absent. Product resume `e00fb7af-bfbf-4bca-8ff8-cd1bd6a93c49` completed, verified both journal rows absent, returned `installation: null`, and left zero scopes, staged credentials, or credential ciphertext. The operator then removed the Existing HTTPS Endpoint resources and revoked the dedicated token. |
| 9. Free-plan control | Not run | Only the Paid account was available. |

The GraphQL window was 16:14:30–16:23:27 UTC. The launcher reported
18 requests and 14,348 µs CPU, and the capacity Worker reported 125
requests and 39,863 µs CPU. Both samples had interval 1. At the
published marginal request and CPU rates, 143 requests and 54,211 µs
CPU correspond to $0.00004398422. The ratio of Bifrost's $0.11
conservative estimate to that Workers-only proxy was about 2,501×.
Durable Object, storage, and log charges are outside this sample. The
cumulative estimated usage across the two pilot rounds was $0.18.
The account subscription remained separate. [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)

Independent `cf` readback found zero trial Workers, namespaces,
Container apps, tokens, DNS records, live tunnels, or connections.
The tunnel deletion record had zero connections. The registry had zero
images matching the trial. Docker showed zero project containers, networks,
volumes, and images, and the local test runtime and connector had
stopped. Trial operator credential files were removed.

The post-pilot ingress diagnostic change distinguishes bounded
timeout, protocol, and transport errors when a route produces no
HTTP response. The focused backend run passed 95 tests and the full
capacity workerd suite passed eight tests, including its 17-case
shared provider conformance run.
