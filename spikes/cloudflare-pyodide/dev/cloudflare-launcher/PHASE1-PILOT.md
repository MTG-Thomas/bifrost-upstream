# Phase 1 Product Pilot

**November 20, 2026 UTC · Base `510ff121e`**

## Usage-only limit

The pilot used a previous conservative fleet projection. The monthly
target applies to Bifrost's estimate of attributable usage. The fleet envelope
and admission meter exclude the $5 Workers Paid subscription. Status shows
the subscription separately as **Workers Paid Subscription (Billed by
Cloudflare)**. At the time of the pilot, the minimum fleet envelope was
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

The rerun used a $25 monthly usage target. The trial targeted less than
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

## Ingress review and deletion settlement

The retained records from the first pilot show a failed ingress route
without an HTTP response and a later successful independent check.
They do not retain the transport exception. The exact cause of that
first refusal remains unknown. The first Disconnect similarly left one
resource in `deleting`; the subsequent readback found both scripts
and all namespaces absent. The retained journal does not distinguish
stale script readback from delayed namespace disappearance.

The follow-up used release manifest
`b4d55400c080dc014b85054f1560e8a0e4da2470d597c71ab5c501790801f112`
and installation `55d6357a-a4c3-4f57-8369-572545515528`. Deliberately
returning 404 for one allowed route produced Action Required with
`ingress_http / unexpected_status / 404` in job
`e90690a7-4a38-44f5-988b-7cf314b3d5a7`. After restoring the route,
job `8039a014-8d90-4717-a0fb-08050726d142` recorded
`ingress_http / connect_timeout`. A diagnostic socket probe also
found IPv6 errno 101 on both advertised IPv6 addresses. HTTPX and
httpcore wrapped the error and suppressed its context. The probe
now checks that context for a missing-route errno. It still refuses
TCP timeouts, connection refusals, and TLS EOF.

After that fix, job `22529613-b865-4cab-9f7d-49361b3568a0` opened
admission. Worker `a4ff9d63-5dcf-4d0a-bbea-31eee300437b` recorded
hello, a generation-1 proof, a generation-1 ACK, and verified deletion.
The loose workflow `85d6948e-bb29-4513-b2f0-459f14b31c13` and the
Solution workflow `8694ea6d-d661-4d1e-a11e-5794e44f0e17` again ran
on separate Workers and settled `Success`.

One Disconnect job, `01b82b93-8d8e-47d2-8f5a-d56a4344ea7f`,
completed and verified both scripts absent. Product state showed no
installation or credential ciphertext. The regression simulates a
visible namespace after script DELETE and observes 2- and 4-second
backoffs within one job. The hosted run verifies one-job cleanup.
Independent `cf` and Docker readback found no trial resources.
The follow-up estimate was $0.08. [Measured inputs](PHASE1-USAGE.md#measured-release-calibration)

## Calibrated release verification

The packaged candidate `460a9e91d` used release manifest
`08cc31907f879f6208dbb5fed88dbf29e021ffc42a81b12e1691f5b17a0eca7e`.
Connect created installation `c26cd7bd-e84d-4b47-b756-13339c5f1332`
under the $25 usage limit. The envelope was $13.60, down from
$14.17 for the same two one-slot launchers. The connector ran from
17:20:36 to 17:26:05 UTC within a 25-minute window. The account's
$25 notification guard remained active. The pilot used no
Containers or LLM requests.

Enable `2b3bd935-7b00-4d96-819e-c59b77b6481a` opened admission.
Canary `23c640f1-ddc6-413c-ac2b-3475cac6bbd4` recorded its scoped
proof and ACK in generation 1 and retired with verified physical
deletion. Cloudflare's settings confirmed 425 ms CPU for the
capacity Worker, 30,000 ms for the launcher, and launcher bindings
of 40 gateway requests and 30,000 ms engine CPU.

With 24 MiB remaining on each primary worker, loose workflow
`c5ac8321-82ec-4b12-abb4-35dacb590f24` and Solution workflow
`303d966c-bd55-4dc4-899b-acaccf3d4009` settled `Success`.
Their Workers claims were `fe358e0b-c0a0-47c3-9043-4412312e0783`
and `327601eb-827c-44d1-a4bb-5d7f23653482`. The limit probe
`40e92637-3ee6-4458-bb73-1d6a7baa78da` had an approval allowance
of 50 and a loop of 43 SDK calls that caught SDK errors. It settled
`Failed` under Workers claim `75f55c6a-3fb5-4aaa-ad23-d5288e270029`
with the release SDK limit error. The API ledger recorded four
requests for the two successes and 39 for the probe. The launcher
budget also counts requests served by its local source snapshot.

For the 17:20:36–17:23:22 UTC sample, GraphQL reported 41 launcher
requests and 18,657 µs CPU, and 54 capacity requests and 47,534 µs
CPU. Sample intervals were 1.64 and 1, respectively. The marginal
Worker proxy was $0.00002982382. The product displayed $0.10, about
3,353× the proxy. On the same 79.356 launcher-seconds and slot-seconds,
three dispatches and 43 API SDK requests, the old formula gives
$0.14235 and the new one $0.09541, a 33% reduction. The workload mix
and sample differ from the earlier pilot. The Durable Object queries
returned no rows for this run or the delayed reference query.
SQLite and log enforcement remain unqualified, and their
theoretical bounds stay in the estimate. Total estimated pilot
spend across rounds was $0.36. [Cloudflare Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)

Scale In `ceb31900-03e4-4ad5-8e09-734a0c7a8aa7` succeeded.
Disconnect `a7d15a5b-edf6-4ab8-9702-b7906c99cbce` completed in
one job, verified both scripts absent, and cleared the installation,
scopes, staged credentials and credential ciphertext. Independent
`cf` readback found zero trial Workers, Durable Object namespaces,
Container apps, registry images, management tokens, DNS records,
live tunnels and connectors. Docker verified both hosted debug
projects and their test projects absent.

The focused backend run passed 82 tests, including the Alembic
single-head check. The two workerd suites passed 28 tests and the
release CI validation passed six tests. Focused Ruff and Pyright
also passed.

## Combined Bundle and Slot Pilot

**November 21, 2026 UTC · Release source `709e37795`**

Linux workerd requalified the exact three-module bundle. Two release builds
matched byte for byte, and the API, scheduler and worker loaded manifest
`5834840f2e6cd73f2f0c0ba8640b373a6ee477549cc61f47dc431024ea0e73fc`.
The pilot used one disposable stack on an existing Workers Paid account.
Before launch, the shared host had 9,403,130 KiB available memory, 15,899,108
KiB available disk and zero other Bifrost stacks. The existing Workers
notification remained enabled. The trial targeted $0.50 with a $1.00 hard
stop on the Bifrost usage meter. Its $415.59 modeled monthly fleet envelope
was a separate Connect policy check.

| Step | Result | Evidence |
| --- | --- | --- |
| Connect | Pass | HTTP 200 with a staged account token, explicit consent and admission closed. |
| Public ingress smoke check | Pass | The workstation saw login HTTP 404 with the deny marker, SDK context HTTP 401 with the allow marker, and HTTP 404 for the catch-all. |
| Enable and signed canary | Fail | Enable returned `CF_WORKER_INGRESS_UNVERIFIED` at `ingress_http` with `tls_error`. Admission stayed closed. The API's TLS probe failed before the canary. The specific handshake failure was unavailable in the job response. |
| Cold and warm workflows, slot burst and cancel | Not run | The attempt stopped at the failed Enable. The ledger contained zero launcher incarnations and zero workflow slots. |
| Reconcile and active Scale In | Not run | The attempt moved directly to teardown. |
| Spend comparison | Unavailable | Bifrost displayed $0.00 throughout the observed attempt. Cloudflare Workers Analytics returned zero rows for all three scripts. No settled workload or comparison ratio was available. |
| Disconnect | Pass after provider readback | The first job reported `CF_CLEANUP_PENDING`. Exact provider readback showed the capacity script and its namespaces had disappeared, and a cleanup-only follow-up completed. Status returned an absent installation and the journal marked all three script rows absent. |

Independent Cloudflare inventory returned zero matching scripts and
namespaces. Exact reads returned HTTP 404 for three Worker settings and four
Durable Object namespace object listings. Independent account and zone
checks confirmed token and DNS record absence. The tunnel deletion record
showed zero connections, and both authoritative nameservers confirmed the
trial hostname was absent. The connector process, disposable stack
containers, networks, volumes and project images, and the trial scratch
and secret files were removed and checked for absence. No Containers or LLM
calls were made.

See the [sanitized hosted attempt receipt](../../results/cloudflare-scaleout-hosted-attempt.json).

### TLS Follow-up

The trial stack and exported logs were removed during teardown. The job
receipt identified the HTTP ingress TLS probe, but did not preserve the
handshake reason. The two pilot revisions used the same API Dockerfile and
HTTP transport pins. The original project images were unavailable for a
package-by-package comparison.

A disposable CT128 API development image loaded 148 trusted CA certificates
with OpenSSL 3.5.7. Both tested Cloudflare IPv4 peers completed TLS with the
retired trial hostname as SNI. The same peers accepted the literal-IP HTTPX
probe with hostname SNI against existing first- and second-level names.
Their IPv6 peers returned `ENETUNREACH` before TLS. This matches the normal
unrouted-family behavior. These checks did not reproduce the original failure
or establish the edge certificate state at the time of Enable.

Enable now reports the IP family, symbolic OpenSSL reason when present,
and numerical certificate verification code when available. The TLS failure
still closes admission.

## Combined Bundle Hosted Retry

**November 21, 2026 UTC · Release source `7739c8e92`**

The package matched the locally qualified three-module fingerprint. All 11
packaging tests passed, and the API, scheduler and worker loaded the release.
The retry began with $0.00 observed cumulative spend. Before stack startup
the shared host had 10,025,901 KiB available memory, 14,746,796 KiB available
disk and zero other Bifrost stacks.

| Step | Result | Evidence |
| --- | --- | --- |
| Connect | Pass | HTTP 200 with a staged account token and admission closed. |
| TLS preflight | Pass | The running API container resolved four addresses and verified both IPv4 handshakes with hostname SNI. Both IPv6 sockets returned `ENETUNREACH`. The first pass took 0.51 seconds and needed no backoff. The literal-address HTTP SDK context check passed on both IPv4 peers. |
| Enable and signed canary | Fail | Enable returned `CF_WORKER_INGRESS_UNVERIFIED` at `ingress_http`, `tls_error`, family `ipv4`. The result provided neither a symbolic reason nor a certificate verification code. No launcher incarnation was recorded. |
| Cold and warm workflows, slot burst and cancel | Not run | Enable kept admission closed. No workflow slot rows were created. |
| Reconcile and active Scale In | Not run | The retry stopped after Enable and proceeded to Disconnect. |
| Spend comparison | Unavailable | Bifrost displayed $0.00 across both attempts, below the $0.50 target and $1.00 stop. Cloudflare returned zero Worker and Durable Object analytics rows for the retry's resources. |
| Disconnect and teardown | Pass | Disconnect succeeded on its first submission and the product no longer reported an installation. Independent inventories showed zero trial scripts and namespaces. Exact reads returned HTTP 404 for all three scripts and four namespaces. |

The scoped token and DNS record were absent by inventory and exact read.
The tunnel deletion record showed zero connections. Both authoritative
DNS servers confirmed the route was absent. The connector process, CT128
stack containers, volumes, networks, project images and trial credential
files were removed and checked for absence. The workstation scratch was
removed. Sanitized API, worker and scheduler log extracts were retained
locally outside the repository.

Enable reported an IPv4 probe failure after the preflight handshake and
initial HTTP check had passed. The job result did not identify the failing
HTTP request or the underlying TLS exception.

See the [sanitized hosted retry receipt](../../results/cloudflare-scaleout-hosted-retry.json).

### Local ingress diagnosis

Loopback TLS reproduced the two empty TLS diagnostic fields with both an
interrupted TLS wait and a stalled HTTP response. In each timeout, HTTPX and
httpcore reported `ConnectTimeout` or `ReadTimeout`, followed by
`TimeoutError`, cancellation, and `SSLWantReadError` in the exception chain.
AnyIO waits for socket I/O from inside the SSL want-read handler. The original
probe treated that internal wait signal as a TLS failure. A real certificate
hostname mismatch produced `SSLCertVerificationError` with
`CERTIFICATE_VERIFY_FAILED` and verification code 62. Closing during the
handshake produced `SSLEOFError` with `UNEXPECTED_EOF_WHILE_READING`.

The proposed post-handshake SSL closure explanation was **not confirmed**.
In the loopback tests a TLS close or TCP reset while reading response headers
surfaced as HTTPX and httpcore `RemoteProtocolError`. The old probe already
classified those particular errors as HTTP protocol failures. The hosted
receipts lack the underlying class and route, so either timeout phase remains
a candidate and the original trigger is unresolved.

Concurrent requests exposed an origin-cache defect. When its TTL expired,
every request waiting behind a failed DB lookup started a new lookup under
the same lock. Each lookup had a three-second bound, so an eight-request
batch could produce a series of three-second waits while using the API's
shared database pool. The listener watcher queries the same pool. A healthy
single-request check followed by a batch during a database stall fits this
sequence. The hosted receipts lack the timing needed to establish that this
was the trigger. Concurrent ingress requests now await one bounded lookup
and receive its shared result. A failed lookup returns HTTP 503 to its
waiters and leaves the cache expired for the next independent request.

The probe reads response headers and closes without consuming the body. In
the loopback test even a keep-alive response with `Content-Length: 0` led to
a fresh TLS handshake on the next probe. A separate test that consumed the
initial response did reuse the socket. The origin deliberately marks its
early denials `Connection: close`. Most allowed routes reject unsigned
requests in ingress before dispatching to the application. The refresh probe
sends a complete two-byte JSON object and takes the ordinary invalid-token
path. On HTTP/1.1 the eight-way batch can open eight separate edge connections.
The local TLS-to-API qualification exercises the same batch and route shapes.
The hosted receipts do not establish an edge or tunnel concurrency limit.

Enable now reports the method, route template, address family, handshake
completion, connection reuse, exception class, and last HTTP transport phase.
The phase distinguishes `tcp_connect`, `tls_handshake`, `request_write`, and
`response_headers`. Where the exception chain exposes a socket errno, the
receipt includes its symbolic name. TLS record failures preserve their
sanitized OpenSSL reason. An overall deadline reports the least-advanced
in-flight route on the current address. A deadline during the WebSocket
check reports the `websocket` phase and preserves the earlier HTTP TLS proof.
The result keeps certificate messages, peer names, and generated path values
out of diagnostics. Once a refusal or allowed batch fails, the probe returns
that result before issuing further probes. The certificate, route-witness,
WebSocket, and signed runtime gates still apply.

### Next hosted qualification

Obtain separate owner approval before provisioning or spending. After staging
Connect with admission closed, identify the platform-job execution backend
configured to claim Enable. The Compose `worker` service runs the physical
remote launcher (`python -m src.worker.main`). The API's remote host claims
platform jobs and the launcher runs them as subprocesses in that container,
so a standard Compose Enable records `remote_launcher`. `worker_pool` is
recorded only when an in-process pool runs the job. For a separately
enrolled physical Linux remote launcher, run the preflight in that launcher's
network namespace or container with the same Python environment, resolver,
trust store, and egress. For Kubernetes, use an approved
qualification pod with the same platform-job pod spec and replace the job
entrypoint with the preflight command. Match the worker image, trust store,
service account, DNS configuration, namespace, network policy, egress and
scheduling constraints. The single-host Compose qualification uses one
running `worker` replica and no separately enrolled remote launcher. From
the project root, pass the same project options used to start the stack:

```sh
sh spikes/cloudflare-pyodide/dev/cloudflare-launcher/preflight-compose-worker.sh \
  "$WORKER_INGRESS_ORIGIN" --project-name "$QUALIFICATION_PROJECT"
```

Add any `--file` and `--env-file` options used at startup after the origin.
The command calls `probe_public_worker_ingress` directly inside `worker`.
It uses Enable's resolver, trust store, address-family policy, literal-IP
SNI and Host headers, refusal sequence, ordered eight-request batches, HTTP
pool lifecycle, and final unsigned WebSocket check. For another approved
execution class, invoke the same Python function in that class's environment.

Retain the JSON result, including `preflight_execution_class`, and copy
Enable's `execution_class` into `enable_execution_class` in the hosted receipt.
The values are `worker_pool`, `kubernetes_platform_job`, and `remote_launcher`.
If the qualified topology lets multiple classes claim Enable, qualify each
eligible class or establish which class will claim the job before submitting.
If the preflight fails, retain the sanitized result, keep admission closed,
and tear down the trial. If it passes, submit Enable once and retain the signed
runtime receipt or the Enable failure details. Proceed to the approved
workload only after Enable succeeds, then verify cleanup by independent
readback. Local tests establish the diagnostic fix. They leave the next
hosted run's outcome uncertain.

## Combined Hosted Qualification Plan

### Local Rehearsal

Rehearse the combined gates in a private `combined-local-*` Git archive
on the Compose host. Use Node 26, the installed Phase 6 Wrangler and
Miniflare dependencies, and the same two pinned release packages. Build
both images as in item 1 and save each loader's release version and
deployment digest in `.release-1.checked` and `.release-2.checked`.
Check the host start condition before launching the stack.

```sh
python3 spikes/cloudflare-pyodide/dev/e2e/combined-stack.py "$PROJECT" "$IMAGE"
node spikes/cloudflare-pyodide/dev/e2e/combined-provider.mjs .rehearsal/config.json &
provider_pid=$!
# Wait for .rehearsal/ready before starting Compose.
docker compose -p "$PROJECT" -f rehearsal-compose.yml -f rehearsal-overlay.yml \
  up -d --pull never api worker cloudflare-connector
# Wait for the API and worker health checks.
python3 spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py
```

The driver checks the internal-only Docker networks and loaded fixture
hooks as the service user before submitting a job. Bifrost runs the API,
worker, scheduler, and connector supervisor. Miniflare runs the uploaded
Workers modules. The fixture serves the Cloudflare REST response shapes
and tracks exact script, namespace, tunnel, DNS, and session identities.
The local edge relays the actual HTTP and WebSocket probes to Bifrost.
An observed successful SDK read confirms entry before the fixture's
bounded sleep. Retain the persisted entry log after settlement.

**Hosted-only checks** include public DNS and IPv6 routing, live edge
certificates and connection reuse, real provider entitlements and grant
enforcement, live cloudflared sessions, authoritative DNS cleanup, and
Analytics attribution or billing. Local reads verify the corresponding
gate and parser against fixture responses. Always remove the project
containers, volumes, and internal network, and stop the local provider
process when the rehearsal finishes or fails.

### Hosted Run

Run only the steps and checks exercised by the committed local rehearsal.
Rehearse any new check locally and commit it to the harness before using
it on hosted. Record an unexpected finding outside the rehearsed checks
as a sanitized observation and continue. Stop on a rehearsed stop
condition or a real spend, ownership, isolation, or teardown violation.

The run needs explicit owner approval for the exact two release manifests,
existing Workers Paid account, active zone, two dedicated origins, temporary
grants, and a single disposable Compose project. Cap each deployment at three
Workers scripts and three concurrent launcher incarnations, with zero Cloudflare
Containers and zero model calls. Start with at least 8 GiB available and no
other Bifrost test stack. Allow 25 minutes from the first connector start,
with the last five minutes reserved for teardown. Build both release images
and stage the approved no-effect fixtures before starting the clock.

Use the two committed preparation revisions with distinct source SHAs and
increasing SemVer precedence. Validate both release manifests, the meter
bounds, and the release loader offline. Use the same qualified Workers
modules for the two releases. Confirm that their deployment digests differ
and that the replacement serves the previous ingress and worker-control
protocols. This exercises the upgrade fence and replacement under load
without introducing a runtime or wire change. Roll the API and worker
together with the database, storage, and connector projection retained.

For each release checkout, use Node 26 and the pinned runtime artifact. Build
into a new Workers artifact directory and require it in the local API image.
Keep the two images available by immutable local digest before provisioning.

```sh
npm ci --prefix api/src/services/app_bundler --omit=dev
export BIFROST_RELEASE_WORKERS_METER_BOUNDS="$(cat api/src/services/worker_pool/cloudflare_meter_bounds.json)"
node deploy/capacity/cloudflare/build-workers.mjs "$QUALIFIED_RUNTIME" \
  deploy/capacity/cloudflare/image-artifacts/workers "$RELEASE_SOURCE" \
  "$RELEASE_VERSION" "$RELEASE_SEQUENCE"
docker build --file api/Dockerfile --build-arg BIFROST_REQUIRE_WORKERS_RELEASE=1 \
  --build-arg BIFROST_VERSION="$RELEASE_VERSION" --tag "$LOCAL_RELEASE_IMAGE" .
```

Start with Existing HTTPS Endpoint to qualify the previously failing lane.
Use a dedicated route to worker ingress on port 8001. Use Account Settings
Read, Workers Scripts Write and Account Analytics Read. For Managed Tunnel
also grant Tunnel Write on the selected account and Zone Read and DNS Write
on the selected zone only. Complete the first Disconnect and verify resource
absence before connecting the second origin as a Managed Tunnel in the same
Compose project. Managed Tunnel provisions its route
during the first Enable. That job performs the full ingress probe after the
connector is ready. Run the operator command against the actual managed
origin before the post-rotation Enable. If the initial Managed Tunnel job
returns the retryable connector-readiness result, observe its existing job
until the connector is healthy or the phase deadline arrives. Preserve its
effect identities and avoid a second provisioning submission.

Retain the sanitized `http_connections` array from the worker preflight
and the Enable result. Use each result's own counts to report reuse.

| Order | Action and required evidence | Incremental budget |
| --- | --- | ---: |
| 1 | Requalify and build both releases offline. Verify the three module digests, bounds hash, deployment digests and image/manifest pairs. Bind each hosted job to the appropriate release. | $0.00 |
| 2 | Existing HTTPS Connect. Stage an expiring scoped token, record consent and the policy envelope, and verify admission is closed. From the sole worker replica, require the complete preflight to pass. Submit Enable once. Require `remote_launcher` (the Compose `worker` container that ran the preflight) and a passing signed hello, scoped SDK receipt and terminal ACK before admission opens. | $0.03 |
| 3 | Measure a cold approved loose workflow and a warm repeat on the same tenant, launcher and ordinal within linger. Record queue-to-entry and end-to-end latency, loader identity, generation, cold/warm flags and terminal receipts. Verify scoped SDK results for a Solution fixture in the burst. | $0.04 |
| 4 | Hold four Provider and four Global executions simultaneously at a no-effect barrier. Offer a ninth execution to a different tenant with zero slots, then prove a tenth from a fourth tenant stays queued through two controller observations without a ticket or entry. Cancel one running execution mid-burst. Require confirmed stop and exact fence release, unchanged sibling fences, and admission of the waiting execution only after release. Open the barrier and collect the remaining terminal ACKs. | $0.14 |
| 5 | Reconcile and verify the owned revision and canary without duplicate resources. Start one harmless hold, request Scale In, then let it finish. Verify admission and sends close, the original claim settles, launchers retire, and scripts remain. Disconnect, retain spend evidence, and verify the first installation's resource absence. | $0.04 |
| 6 | Managed Tunnel Connect and Enable. Independently read back the exact CNAME, tunnel and connector session. Require `remote_launcher`, the signed canary and ingress refusal checks. Verify normalized ingress GET rules, empty origin-request fields and the actual disabled `warp-routing` echo. With the same Tunnel Write grant, list account `teamnet/routes` and verify no private IP route targets the tunnel. | $0.04 |
| 7 | Scale In and rotate the connector token. Compare live session sets and require all prior session IDs absent and a replacement session present. With approved bounded tooling, check the old token cannot establish a session while keeping it out of the receipt. Run the worker preflight on the managed origin, Enable, and verify the signed canary on the same tunnel and CNAME. | $0.04 |
| 8 | Upgrade once with one harmless held execution. Stage the newer compatible release. Require the admission and send fence, then interrupt only the old attempt's control connection. Release the hold and verify replay and terminal ACK under the original claim. Require old runtime retirement, exact script and namespace absence before any replacement upload, then provider readback and a signed canary before reopening. | $0.07 |
| 9 | Capture the spend comparison. Scale In and Disconnect. Verify both installations' cleanup independently, including the replacement revision. Reserve for bounded cleanup and remaining observation. | $0.05 |
| | **Cumulative estimate with $0.00 carried from the two failed combined attempts** | **$0.45** |
| | Optional queue pre-warm with one extra parallel boot per tenant | **+$0.04** |

Use three launchers with four seats each, nine installation slots, four per
tenant, 120-second launcher lifetime, 90-second engine linger, and queue
pre-warming disabled. Occupy or cap the primary with controlled local
fixtures so it cannot consume the approved Workers dispatches. Confirm
each admission witness is an actual Workers claim. Set the monthly policy
at or above the $415.59 requalified fleet envelope with the owner's
approval. That modeled monthly capacity value and the existing Paid
subscription are separate from the trial cap. Use the packaged marginal
rates without assumed free allowances. The table allocates live estimate
headroom for at most 14 fixture attempts, canaries and their control and
retirement operations. For scale, ten new engine identities are $0.020 at
$0.002 each. Three full 120-second launcher lifetimes, even at the
qualified per-second CPU ceiling, contribute about $0.045 in CPU. The
remaining allocation covers temporary dispatch reserves, Durable Object
duration and storage, logging, and lifecycle operations. These are planning
allocations. Confirm the carry-in for the approved trial series and
recalculate before provisioning if it differs from the table. Record the
high-water live estimate and settled usage by phase.

### Observe Each Gate Before Advancing

Reserve each ready, normal-lane primary worker's memory budget in whole
MiB minus 24 MiB with a local hold fixture. Record the expected worker
count before submitting the holds. After the fixtures report Running,
wait for the durable reservations before submitting the cold workflow
or a burst. From the Compose project directory, run:

```sh
python3 spikes/cloudflare-pyodide/dev/e2e/wait-primary-hold.py \
  "$PRIMARY_WORKERS" -- -p "$PROJECT" -f "$COMPOSE_FILE"
```

The gate polls PostgreSQL every 250 ms between reads for up to 10
seconds. It requires exactly the expected number of ready primary workers
and 24 whole MiB free on each in one read. A timeout reports the last
observed free memory and stops the phase. A Running execution status can
precede the visible reservation. Run the same command before submitting
a hold in the local rehearsal and require the timeout, then submit the
actual hold and require the gate to pass. Use `--database bifrost_test`
before the worker count when rehearsing on the test Compose stack.

Use bounded read-only polling within each phase's deadline. Submit each
operation once. Keep hold fixtures live until their required reads
complete, and stop on a terminal failure, changed fence, or expired
rehearsed deadline. Managed Workers overflow uses a 2-second dispatch-age
floor and a deadline wake. A similar hosted cold run is expected to enter
in about 7–9 seconds under healthy conditions. Record the measured time
to entry and treat that range as an expectation.

| Gate | Required readback before advancing |
| --- | --- |
| Cold and warm | Use `executions.created_at` in PostgreSQL as the queue timestamp for latency. After each run settles, retain its released-slot snapshot before submitting another run. Match the terminal receipt and ACK to that execution's dispatch route and fence. Submit the warm repeat inside linger and use the two saved snapshots to verify reuse. |
| Eight and ninth executions | Within 28 seconds of submitting the burst, capture eight sent, unreleased slots. From that read, allow up to 28 seconds for live gateway or launcher entry witnesses for those same eight fences. The latter window covers engine boot after dispatch, including serialized boots in one launcher. Then offer the ninth. Wait for its entry and slot while confirming the original eight fences are still held. Read persisted entry logs after settlement because the runtime can batch them until the terminal receipt. |
| Tenth waits | After its dispatch row appears, use its PostgreSQL queue time plus 2 seconds as the first overflow-eligible instant. The timestamp in Redis `bifrost:worker-pool:controllers` advances when the controller identified by `launched_by` starts a pass. Observe its first advance at or after eligibility and use the next two advances to mark two completed eligible passes. Check the scope logs for reconciliation errors during both passes. At each completion, verify the same nine active fences and the tenth's unassigned dispatch, with no claim, slot, or entry. Record the observations without controller IDs. |
| Isolated cancel | Use a fixture that can finish within the launcher's stop-confirmation window when the terminate command is observed. Wait for a confirmed `stopped` receipt, terminal cancellation, and exact slot fence release. A Cancelled run with an `uncertain` receipt still owns its fence. In the same slot read verify the other eight fences remain held. Compare the released fence timestamp with the tenth execution's claim and entry evidence. |
| Active Scale In | Confirm the drain fixture has entered on Workers under a sent fence. Free the local primary if needed to run the platform job. After Scale In is accepted, wait for the admission and send fence while the original claim is still live. Then release the fixture or let its bounded hold finish. Verify its terminal ACK, the job result, retired launcher readback, and the three retained scripts. |
| Managed Tunnel rotation | Capture live session identities before submission. After rotation, poll the exact tunnel until the pre-rotation set is absent and a replacement session is live. Check the old token and run the worker preflight only after that readback. |
| Upgrade | Wait for the held execution's original claim and sent fence, then stage the replacement. Wait for the persisted admission and send fence before interrupting the old control connection. Verify terminal replay and ACK under the original claim, old runtime retirement, and provider absence before the first replacement upload. Wait for the replacement's signed canary and admission. |

Baseline pre-warm stays off. Drop the optional run first. Only add it if the
original burst, rotation and upgrade have passed and the observed high-water
plus remaining exposure and teardown still project below $0.50. If time or
spend tightens, drop Upgrade next and defer the entire Managed Tunnel pass
if it cannot be completed with cleanup headroom. Keep the original burst
shape and teardown intact. Record any dropped item as unqualified. Defer
Kubernetes, Containers, DNS failure injection, account-token replacement,
and a deliberately failed upgrade canary to separately approved runs.

Use the same UTC window and the exact script/namespace sets for the Bifrost
estimate and independent Cloudflare Analytics. Record the raw aggregate
request, CPU and Durable Object samples and attribution limits. Calculate
Bifrost divided by the nonzero provider cost proxy for each installation
and the combined window. Label a Workers-only denominator as such. If
provider rows or the denominator are missing, preserve the samples and
report the ratio unavailable. Preserve both run windows outside the stack so
read-only Analytics readback can finish after cleanup within the approved
window.

For the rehearsed gates, stop workload and begin teardown on failed preflight or canary, class
mismatch, unexpectedly open admission, cross-tenant access or release,
unproven stop or slot release, unexpected grant or route drift, duplicate
resource, unresolved receipt, or loss of spend visibility. Stop optional
work if the next phase and cleanup would put the projection at $0.50.
Close admission and abort all further workload at $1 cumulative observed
or projected usage, at the teardown cutoff, or before exceeding an approved
resource cap. Keep cleanup authorized through independent absence verification.
Read back journaled identities after an uncertain result before deciding
any cleanup action. Preserve sanitized job state and diagnostics before
removing the stack.

For each retired revision require the product Disconnect outcome and
absent installation, plus independent exact reads and inventory absence
for all three scripts and four namespaces. Check no live launcher, held
slot, pending cleanup effect, or staged credential remains. Check each
owned DNS record gone through the provider and both authoritative DNS
servers, zero live connector sessions, tunnel tombstone or exact absence,
and empty local credential projection. Revoke temporary management tokens
and verify absence with independent authority. Finally verify zero
project containers, networks, volumes and project images, and remove local
connector processes, scratch files and secrets. Retain only sanitized
digests, dispositions, counts, durations and aggregate costs. Escalate any
unverified teardown with its remaining cleanup witness before the approved
window closes.

## Combined Hosted Qualification Result

**November 22, 2026 UTC · Spike source `eb5399823`**

The two prepared manifests were rebuilt byte for byte from the qualified
bundle. Twelve packaging tests passed. Both production images accepted
their packaged release and the two deployment digests differed. The
temporary Compose project started with 9 GiB available and no test stack
running on the shared host.

| Item | Result | Evidence |
| --- | --- | --- |
| 1. Offline bundle and images | Pass | Both manifests matched the prepared artifacts. Image loader and qualified bundle checks passed for both release sequences. |
| 2. Connect, worker preflight, Enable and canary | Stopped at the execution-class gate | Connect returned HTTP 200 with admission closed. The sole Compose worker passed all 100 preflight route checks. Enable succeeded and opened admission after its runtime canary. Its receipt recorded `remote_launcher`. The plan required `worker_pool`. |
| 3. Cold and warm latency | Not run | The trial stopped before any workflow submission. |
| 4. Slot burst and isolated cancel | Not run | No workflow ran and no workflow slot was held. |
| 5. Reconcile, active Scale In and first Disconnect | Disconnect passed | The earlier actions were skipped under the stop rule. Disconnect succeeded on its first submission and status returned no installation. |
| 6–7. Managed Tunnel and connector rotation | Not run | The Existing HTTPS leg stopped at its execution-class gate. |
| 8. Upgrade and terminal replay | Not run | Both packages qualified offline. The trial did not activate the replacement. |
| 9. Spend and teardown | Teardown verified. Ratio unavailable. | The settled Bifrost ledger estimated $0.0001907223 for the run. Cloudflare returned zero attributable Worker or Durable Object analytics rows. |

The preflight command runs inside the Compose worker container. That
container's standard entry point invokes the physical remote launcher.
Its platform-job command records `remote_launcher`. This accounts for
the difference between the preflight label and the Enable receipt. The
receipt provides evidence of a successful ingress and canary path and
does not meet this run's execution-class requirement.

**Disposition.** The requirement itself was wrong, not the run. The
preparation brief named `worker_pool` for single-host Compose, but in that
topology the `worker` container hosts the remote launcher that runs platform
jobs, so `remote_launcher` is the correct class. The preflight and Enable
ran in the same container, which is the property the gate protects. The
runbook and the preflight label now require `remote_launcher` for Compose.
This is the first hosted run in which the ingress probe passed and Enable
opened admission with a signed canary, which supports the single-flight
origin-lookup fix as the cause of the two earlier `tls_error` stops.

The two previous attempts carried $0.00 of observed Bifrost spend. The
public meter displayed $0.00 during this trial. The retired ledger's
sub-cent estimate covers the executed provider lifecycle and canary.
A per-item cost split and final provider billing were unavailable.
The estimated cumulative spend was below the $0.50 target and $1.00
hard stop.

Product and independent readback found all three trial Workers and four
namespaces absent, with HTTP 404 on their exact provider interfaces.
The sole launcher incarnation was retired. No live launcher, held slot,
present resource journal row, staged credential, retained credential
ciphertext, or active capacity scope remained. The temporary account
token and DNS record were absent on exact readback. Both authoritative
DNS servers confirmed the origin was absent. The external tunnel's
deletion record had zero live sessions. Local processes, Compose services,
volumes, networks, trial image tags, scratch and secret files were removed
and checked for absence.

See the [sanitized combined hosted receipt](../../results/cloudflare-scaleout-combined-hosted.json).

## Combined Hosted Re-run

**November 22, 2026 UTC · Spike source `bb05eb595`**

The two prepared manifests matched their recorded hashes and their
distinct deployment digests. Both offline production-image loader
checks and all 12 packaging tests passed. The Compose project started
with 9 GiB available and no other Bifrost test stack running.

| Item | Result | Evidence |
| --- | --- | --- |
| 1. Offline release verification | Pass | The original prepared release pair used the qualified bundle and distinct deployment digests. |
| 2. Connect and worker preflight | Stop | Connect returned HTTP 200 and admission remained closed. The sole Compose worker reported `remote_launcher`. Its preflight recorded 29 route checks and failed on `GET /api/forms/{form_id}` with an IPv4 `ConnectTimeout` in `tls_handshake`. The connection was new and the handshake incomplete. No symbolic errno was returned. Enable and its canary were not submitted. |
| 3–4. Latency, slot burst and cancel | Not run | The preflight stop prevented workload submission. No launcher incarnation or held slot was recorded. |
| 5. Reconcile, active Scale In and Disconnect | Disconnect passed | Reconcile and Scale In were skipped at the stop. Disconnect completed on the first submission and status returned no installation. |
| 6–8. Managed Tunnel, rotation and Upgrade | Not run | The Existing HTTPS preflight stopped the re-run before either later leg. |
| 9. Spend and independent teardown | Teardown verified. Ratio unavailable. | Bifrost displayed $0.00 for this re-run. No Workers or Durable Object namespaces were provisioned for an attributable analytics comparison. |

The approved carry-in was approximately $0.000191 of modeled Bifrost
usage. The cumulative observed estimate remained below the $0.50 target
and $1.00 hard stop. There was no workload or canary spend to allocate.
Provider-billed tunnel costs were unavailable.

After Disconnect, readback showed zero resource journal rows, live launchers,
held slots, active scopes, staged credentials, and retained credential
ciphertexts. Exact provider reads and independent inventory confirmed
the temporary account token and DNS record were absent. Both authoritative
DNS servers confirmed the route was gone. The tunnel deletion record
showed zero live sessions. The connector process, project containers,
volumes, networks, trial image tags, scratch and secret files were
removed and checked for absence.

See the [sanitized re-run receipt](../../results/cloudflare-scaleout-combined-hosted-rerun.json).

## Combined Hosted Run With HTTP Connection Reuse

On 2026-11-22 UTC, spike revision `97ef3ae83` ran both prepared release
loader checks on the disposable Compose host. The Workers module and
meter-bounds digests matched the pinned evidence. Only the first release
was used on the hosted path.

| Runbook item | Observed result |
| --- | --- |
| 1. Release verification | Passed. Both image loaders accepted distinct deployment digests and all 12 Workers packaging tests passed. |
| 2. Existing HTTPS | Passed. Connect returned HTTP 200 with admission closed. The same Compose worker passed all 100 ingress route checks and Enable completed the signed canary as `remote_launcher`, then opened admission. |
| 3. Cold and warm latency | Stopped at the local primary hold gate. A subsequent read showed 1,408 of 1,432 MiB reserved, leaving the intended 24 MiB. Zero hosted workflow submissions were made. |
| 4. Slot burst and cancel | Not run. |
| 5. Reconcile, active Scale In and Disconnect | Reconcile and active Scale In were not run. The stop path submitted Disconnect once and succeeded. |
| 6–8. Managed Tunnel, rotation and Upgrade | Not run. |
| 9. Spend and teardown | Bifrost settled $0.000201831 incremental and about $0.000392553 cumulative. Cloudflare Analytics returned empty attributed Workers and Durable Object samples. The ratio is unavailable. Teardown passed. |

Each of the two reachable IPv4 addresses recorded eight TCP starts, eight
started and completed TLS handshakes, and 92 requests served over reused
HTTP connections. The two IPv6 addresses each recorded one TCP attempt
and zero TLS starts. Enable returned a successful receipt. At this
revision its connection counters appear only on ingress failure.

Independent readback found zero owned scripts and namespaces in provider
listings. All three script endpoints and four namespace endpoints
returned 404. Exact DNS and token readback reported absence, both
authoritative DNS checks agreed, and the tunnel tombstone showed zero
live sessions. The connector process, project containers, networks,
volumes, trial image tags and both scratch directories were removed.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json).

## Combined Hosted Run With Observable Gates

The 2026-11-22 UTC run from `58029df89` verified both pinned release
images, then ran Existing HTTPS from the first release.

| Runbook item | Result |
| --- | --- |
| 1. Release verification | Both deployment digests matched the pinned manifests and the loader accepted both images. All 12 Workers packaging tests passed. |
| 2. Connect, preflight, and Enable | Passed. Connect returned HTTP 200 with admission closed. The Compose worker passed 100 route checks. The signed Enable receipt reported `remote_launcher` and opened admission. Both receipts recorded 92 reused HTTP requests per IPv4 address. |
| 3. Cold and warm latency | The primary hold gate observed 24 whole MiB free. The cold workflow succeeded on slot 0 and released its sent fence. The measurement harness stopped when the execution detail API returned a null `created_at`. Durable readback measured 10.178 s from queue to entry and 10.261 s from queue to completion. No warm run was submitted. |
| 4. Slot burst and cancel | Not reached. |
| 5. Reconcile, active Scale In, and Disconnect | Reconcile and active Scale In were not reached. The stop path submitted Disconnect once and succeeded. |
| 6–8. Managed Tunnel, rotation, and Upgrade | Not reached. |
| 9. Spend and teardown | Bifrost settled $0.007125132 incremental and about $0.007517685 cumulative. Cloudflare Analytics returned empty attributed samples. The ratio is unavailable. Teardown passed. |

Both ingress receipts recorded eight TCP starts, eight started and
completed TLS handshakes, and 92 reused requests on each of two IPv4
addresses. Each of two IPv6 addresses recorded one TCP attempt and zero
TLS starts.

Provider inventories contained zero owned scripts and Durable Object
namespaces. All three exact script reads and all four namespace reads
returned 404. Exact DNS and token readback reported absence, both
authoritative DNS checks agreed, and the tunnel tombstone showed zero
live sessions. The connector process, Compose stack, trial image tags,
scratch directories and secret files were removed and checked for absence.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).

## Combined Local Rehearsal

On 2026-11-22 UTC the disposable Compose stack exercised items 1–9 from
spike source `5716151b0` with both pinned releases, real workerd, and the
local provider fixture. The clean run reached the final Disconnect and
exposed a product retirement block.

| Item | Result | Observation |
| --- | --- | --- |
| 1. Releases | Rehearsed-pass | The production image loader accepted both packages with distinct deployment digests. All 12 packaging tests passed. |
| 2. Connect, preflight, Enable | Rehearsed-pass | The worker passed 100 routes. Preflight and signed Enable both reported eight completed TLS handshakes and 92 reused requests on the local IPv4 address. Enable ran as `remote_launcher`. |
| 3. Primary hold and latency | Rehearsed-pass | The durable hold showed 24 MiB free. PostgreSQL queue-to-entry was 12.092 s cold and 0.295 s warm. Queue-to-completion was 12.128 s and 0.313 s. Both used slot 0 on the same launcher. |
| 4. Burst and cancel | Rehearsed-pass | Four Provider and four Global executions overlapped. The third tenant took the ninth slot and the tenth waited through two observed controller passes. A controlled stop released one fence, preserved eight sibling fences, and admitted the waiting run. The Solution fixture returned its scoped value. |
| 5. Reconcile, active Scale In, first Disconnect | Rehearsed-pass | Reconcile retained the deployment, Scale In fenced admission while the held claim was live, and the first Disconnect returned three script and four namespace 404s. |
| 6. Managed Tunnel | Rehearsed-pass | The fixture echoed `warp-routing.enabled = false`, accepted the route-list request, and reported the exact proxied CNAME. The signed Enable passed. |
| 7. Connector rotation | Rehearsed-pass | The actual supervisor rolled its token projection. The stub showed one previous session evicted and one replacement session alive. The old token was refused. Worker preflight and signed re-Enable passed. |
| 8. Upgrade | Rehearsed-pass | The replacement fenced the original live claim. The launcher reconnected after the targeted cut, delivered its terminal receipt, and received its ACK after exactly one dispatch. All three old scripts and four old namespaces were absent before replacement upload. |
| 9. Spend and final teardown | Partial. Product block | Final Scale In and spend parsing passed against a synthetic Analytics sample. All nine Managed scripts and twelve namespaces across the three revisions returned 404. The tunnel was deleted with zero connections and DNS returned 404. Final Disconnect returned `CF_CLEANUP_PENDING` and left the Bifrost installation unretired. The test project and provider fixture were removed. |

**Product defect.** Managed Tunnel cleanup sets the tunnel intent to
`absent` in the session and returns completion. The retirement query uses
a session with autoflush disabled and still reads `deleting` from
PostgreSQL. A rolled-back diagnostic replay reproduced the rejection.
Flushing the session before calling the same retirement code made it
succeed. Product code was unchanged.

**Fixed harness defects.** The fixture now uses internal-only networks
and checks deferred Python hooks under the service user. One preliminary
account verification GET escaped before isolation with synthetic
credentials and returned HTTP 400. It created no resources. The HTTP
and WebSocket relay now preserves the origin, probe witness, and close
code. Queue timing uses PostgreSQL, and burst entry uses a live SDK
witness because entry logs can be buffered. The controlled cancel
releases its held read after the launcher accepts terminate. An earlier
long sleep returned an uncertain stop and correctly retained its fence.
The harness also corrected the Compose image pull option and made the
connector stub detach its last session on projection withdrawal.

### Managed Disconnect retirement fix

The cleanup helper and retirement query used the same worker session and
transaction. Cleanup set the tunnel row to `absent` in memory. With
autoflush disabled, retirement still saw `deleting` in PostgreSQL and
raised before the transaction could commit. The rollback left the same
row to be retried even after the provider had deleted the tunnel.

DNS and tunnel cleanup now report progress when they observe absence.
The job commits each journal update before taking the next step and
rechecks the job lease and installation revision before retirement. A
runner lost after provider deletion can resume from the committed delete
intent and verify the provider tombstone again. A successor after the
absence commit can retire from the durable journal. Connector token
withdrawal already waits for the projection receipt. Workers scripts
already commit progress after verifying script and namespace absence.

The original Phase 2 lifecycle test used autoflush and committed between
direct cleanup helper calls. It never ran retirement in the transaction
that marked the tunnel absent. The new regression runs the Disconnect
job with fresh autoflush-disabled PostgreSQL sessions and checks both the
provider tombstone and the retired installation. It returned
`CF_CLEANUP_PENDING` on `0bbaa403e` and passed with this fix.

The local rehearsal checks that the API, worker and connector are on
internal IPv4 networks before each Connect. The API and worker must also
load the fixture hooks under the service user. The persistence and
browser fixtures block real Cloudflare endpoints at HTTPX's network
transport and continue to accept in-process mock responses.

The CT128 rerun passed all nine items with Miniflare/workerd, the Bifrost
Compose services, and both pinned packages loaded in offline images.
Removing the fixture hooks blocked Connect before the first API request.

| Item | Verified on the rerun |
| --- | --- |
| 1. Releases | The Bifrost loader accepted both pinned packages with distinct deployment digests. |
| 2. Existing HTTPS | Worker preflight passed 100 routes. Preflight and signed `remote_launcher` Enable each recorded eight completed TLS handshakes and 92 reused requests. |
| 3. Hold and latency | The primary hold left 24 MiB free. Queue-to-entry was 10.454 s cold and 0.321 s warm. Queue-to-completion was 10.507 s and 0.335 s. Both runs used slot 0. |
| 4. Burst and cancel | Four Provider and four Global runs overlapped. The third tenant used the ninth slot. The tenth waited across two controller passes and entered after a cancel released one fence while preserving eight sibling fences. |
| 5. Reconcile, Scale In and first Disconnect | Reconcile preserved the deployment. Active Scale In drained the workflow. The first Disconnect retired the installation with three script and four namespace 404s. |
| 6–7. Managed Tunnel and rotation | The route-list request returned HTTP 200 and `warp-routing.enabled` was `false`. Rotation evicted one old session. One replacement session connected and signed re-Enable passed. |
| 8. Upgrade | The original claim drained, reconnected and received a terminal ACK after one dispatch. Three old scripts and four old namespaces returned 404. |
| 9. Spend and final Disconnect | Spend readback was `ready` and parsed one synthetic Analytics sample. Disconnect retired the installation and cleared the live ledger, occupied slots and credential ciphertext. The final three script reads, four namespace reads and DNS read returned 404. The tunnel tombstone had zero live sessions. |

The rehearsal project, fixture process, networks, volumes and image tags
were removed and checked for absence. Cloudflare billing remains
hosted-only.

### Disconnect replay and mixed test isolation

The job regression also covers DNS and tunnel intents in `creating`
with old observations and no provider receipt. It runs Disconnect
twice using fresh autoflush-disabled sessions. The first call commits
both absence records and retires the installation. The replay returns
`disconnected` after checking the lease and revision, without needing the
erased management credential. Before the replay fix, both the recorded
and unrecorded tunnel cases returned `CF_MANAGEMENT_CREDENTIAL` on the
second call.

Two worker-pool tests originally failed with
`ValueError: not enough values to unpack (expected 1, got 0)` when
`launch_for_demand()` returned an empty list.

- `test_managed_prepare_uses_the_uid_committed_before_provider_io`
- `test_cloudflare_reserves_only_approved_fitting_dispatches_before_hello`

Both tests wrongly assumed there was no live claimer. A controlled
mixed run traced each synthetic dispatch through insert, assignment to a
live static Linux executor, and deletion before the fixture controller
could reserve a worker. The worker-pool fixtures now use a private clone
of the migrated PostgreSQL template and a separate Redis database for
controller telemetry. API, worker and scheduler services stay live for
the persistence E2E tests in the same invocation.

## Hosted Cold and Warm Reuse Run

The 2026-11-22 UTC run used spike `788ffc641`, the two pinned releases,
and the 120-second launcher policy. CT128 started with 8 GiB available
and no other running test stack.

| Item | Result |
| --- | --- |
| 1. Releases | Passed. Both image loaders accepted the pinned manifests and distinct deployment digests. All 12 packaging tests passed. |
| 2. Connect, preflight and Enable | Passed. Connect returned HTTP 200 with admission closed. The Compose worker passed 100 routes. Signed Enable reported `remote_launcher` and opened admission. Both checks recorded eight completed TLS handshakes and 92 reused requests per IPv4 address. Each of two IPv6 addresses recorded one TCP attempt and zero TLS starts. |
| 3. Hold, cold and warm latency | The primary hold left 24 MiB free. Queue-to-entry was 9.959 s cold and 0.582 s warm. Queue-to-completion was 10.032 s and 0.650 s. Both used slot 0 on the same launcher. A post-latency harness gate then failed. |
| 4. Burst and cancel | Unreached. This run did not qualify Provider and Global overlap, ninth-slot admission, tenth-run waiting, or isolated cancellation. |
| 5. Reconcile and active Scale In | Unreached. The stop path submitted one Disconnect and verified retirement. |
| 6–8. Managed Tunnel, rotation and Upgrade | Unreached. No Managed Tunnel token was created and the replacement image was never started. |
| 9. Spend and teardown | The settled estimate was $0.007799626 for the run and $0.015317311 cumulative. The largest observed cumulative estimate was $0.026077645. Exact Analytics samples were empty. Teardown passed. |

The hosted wrapper added a post-latency query for both execution IDs.
The durable slot row tracks its current holder. Warm reuse had already
replaced the cold execution ID, so the added lookup raised `KeyError:
'cold'`. The rehearsed gate reads each released row before submitting
the next execution. Preserve that per-run snapshot and join each terminal
receipt and ACK by its route. The relay confirmed one dispatch and a
terminal ACK for each execution, generation 0 booked for cold, and no
new generation booking for warm. The run stopped at the failed gate.

The canary incarnation lived 1.864 s. The shared cold and warm workflow
incarnation lived 54.074 s. Disconnect returned three exact script and
four exact namespace 404s, with no owned entries in either inventory.
Product readback showed no current installation, live incarnation, held
slot, staged credential, retained ciphertext or pending resource row.
DNS and token exact reads returned 404. Both authoritative servers
confirmed DNS absence and the tunnel tombstone had zero sessions.
The owned Compose resources, processes, image tags, scratch and secret
files were removed and checked for absence.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-latency-reuse.json).

## Local Rehearsal With the 2-Second Overflow Gate

On 2026-11-22 UTC, CT128 ran all nine items from
`00a8d6d99f709fb2f031dfcf1fc885c4855d73b7`, the harness commit directly
on `c932f55b3`. The archived source ran with real Bifrost API, worker,
scheduler, PostgreSQL and Miniflare/workerd. Both service images passed
the outbound guard, including the replacement at Upgrade. A control
with missing fixture hooks blocked Connect before its first API request.

| Item | Result | Observation |
| --- | --- | --- |
| 1. Releases | Rehearsed-pass | The loader accepted both pinned deployment digests, and all 12 offline Workers packaging tests passed. |
| 2. Connect, preflight, Enable | Rehearsed-pass | The Compose worker passed 100 route checks. The signed Enable used `remote_launcher`. Both receipts showed 8 completed TLS handshakes and 92 reused requests on local IPv4. Cloudflare edge behavior remains hosted-only. |
| 3. Primary hold and latency | Fixed and rehearsed-pass | PostgreSQL showed 24 MiB free and a 2.000 s cold overflow eligibility offset. Queue-to-entry and queue-to-completion were 9.799 s and 9.840 s cold, and 0.707 s and 0.730 s warm. Both executions had their own terminal ACK and reused the same launcher and slot 0. |
| 4. Burst and cancel | Fixed and rehearsed-pass | Four Provider and four Global executions entered on eight unchanged fences, with full entry 9.306 s after the last dispatch read. The third tenant took the reserved ninth slot. The tenth remained unassigned through two completed eligible passes. Confirmed cancel released one fence and preserved the other eight before the tenth entered. The Solution run returned its scoped value. |
| 5. Reconcile, active Scale In, first Disconnect | Rehearsed-pass | Reconcile retained the deployment. Active Scale In held the original fence through admission closure and kept all three scripts. Disconnect returned three script and four namespace 404s. |
| 6. Managed Tunnel | Rehearsed-pass at provider boundary | The stub echoed `warp-routing.enabled = false`, returned HTTP 200 with an empty private-route list, and reported the expected proxied CNAME. Signed Enable passed. Live tunnel and Cloudflare grant behavior remain hosted-only. |
| 7. Connector rotation | Rehearsed-pass at provider boundary | One old session was evicted, one replacement session appeared, and the old token was refused. The worker preflight and signed re-Enable passed. Cloudflare session eviction remains hosted-only. |
| 8. Upgrade | Rehearsed-pass | The replacement fenced the original claim. The old launcher reconnected after the transport cut, replayed the terminal result under that claim, and received the ACK after one dispatch. Three old scripts and four old namespaces returned 404, with the old script deletions preceding replacement upload. |
| 9. Spend, Disconnect, teardown | Rehearsed-pass | Spend readback was ready and ratio parsing used a synthetic Analytics sample. The real provider denominator remains hosted-only. Final Disconnect retired the installation with zero live ledger rows, three script and four namespace 404s, DNS 404, and zero tunnel sessions. The owned Compose resources, image tags, fixture processes and scratch were verified absent. |

The earlier single 28-second burst wait mixed launcher allocation with
engine entry. The timed trace showed one Global execution entering
18.8 seconds after dispatch, after the submission-based deadline. The
separate dispatch and entry windows in the runbook exercised the same
fences successfully. Four loose workflow proposals and one Solution
proposal were reviewed and accepted in the full rehearsal. There were
no Cloudflare calls or resource changes.

## Combined Hosted Run Stopped at Enable Ownership

**November 22, 2026 UTC · Spike source `db39a369874e71274244fdc85f5f4436db4ef9de`**

| Item | Result |
| --- | --- |
| 1. Release and meter | Passed. Twelve offline tests passed. The pinned bundle and meter bounds matched both packages. Both exact-source images loaded their distinct deployment digests. |
| 2. Connect, preflight and Enable | Connect left admission closed. The Compose worker preflight passed all 100 route checks. Enable ran as `remote_launcher` and returned `requires_action / CF_OWNERSHIP_CONFLICT` after 8.116 s. The job exposed the generic ownership code, with no successful canary receipt or Enable connection counters. |
| 3. Hold and latency | Unreached. The policy used a 2 s overflow gate and a 120 s launcher lifetime. No workflow entered, and there was no measured eligibility offset or cold or warm latency. |
| 4. Burst and cancel | Unreached. No ninth or tenth admission and no cancellation were submitted. |
| 5. Reconcile and active Scale In | Unreached. Disconnect ran through the stop path. |
| 6. Managed Tunnel | Unreached. No Managed Tunnel installation or credential was created. |
| 7. Connector rotation | Unreached. |
| 8. Upgrade | Unreached. |
| 9. Spend and teardown | Bifrost settled $0.00 incremental and $0.015317311140185837 cumulative. No launchers started. Analytics returned no attributed rows, so the spend ratio is unavailable. Teardown passed. |

The preflight recorded eight TCP starts, eight completed TLS handshakes,
and 92 reused requests per IPv4 address. It resolved two IPv4 addresses.
Each of two IPv6 addresses recorded one TCP attempt and no TLS start.
At the ownership stop, the journal held one present script and two
creating intents. Enable was submitted once. The result does not expose
which ownership predicate failed.

Disconnect retired the installation and marked all three script intents
absent. Independent exact reads returned 404 for all three script
targets. The provider inventories contained no matching scripts or
Durable Object namespaces. There were no recorded namespace IDs for
exact reads. The ledger showed zero live incarnations, held slots,
staged credentials, pending resource rows, or retained ciphertext.
The temporary token and DNS record returned 404, both authoritative DNS
servers confirmed absence, and the Existing HTTPS tunnel tombstone had
zero sessions. The owned Compose stack, image tags, processes, scratch
and secret files were removed and verified absent.

See the [sanitized ownership-stop receipt](../../results/cloudflare-scaleout-combined-hosted-ownership-stop.json).
