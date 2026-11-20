# Phase 1 Hosted Usage Qualification

**2026-11-20 UTC, Phase 1b baseline `ecf05ec77`**

**Verdict.** GraphQL Analytics fell short of the usage writer requirements.
The operator `cf` session independently verified Workers Paid. Account Settings
Read alone verified the Containers entitlement.

## Trial

The existing `cf` session selected the trial account. Its read-only preflight
confirmed an active Workers Paid period, from 2026-10-30 00:00 UTC inclusive
through 2026-11-30 00:00 UTC exclusive, and an enabled $25 Workers spend
notification. The notification is an alert. The trial bounded spend separately.

- Envelope of 38 minutes starting 09:36:54 UTC, one trial-prefixed Worker, one
  SQLite Durable Object namespace, one object, no Containers and no Bifrost
  stack. Workers Observability and preview URLs were disabled.
- At 09:37:27 to 09:37:36 UTC, 96 sequential authenticated POSTs returned HTTP 200
  with the expected SQLite round-trip value. Each request ran 250,000 integer
  hash iterations and made one Durable Object call. The Worker CPU cap was
  10 ms. Fixed iterations held the work constant while measured CPU varied
  across invocations. The fixture performed one KV write and read per request,
  plus a single 64 KiB write on the first request.
- The Worker accepted the synthetic bearer for three minutes at most. The run
  disabled the endpoint at 09:37:37 UTC, immediately after load. The fixture
  used no alarm, external service, LLM, platform secret or production canary.
- Management used a temporary account-owned token with **Account Settings Read**
  and **Workers Scripts Write** on the selected account. GraphQL data queries
  used account-owned tokens with **Account Analytics Read** alone on the same
  account. A separate **Account Settings Read** token isolated the entitlement
  and permission checks. Token administration used `cf 1.0.0-beta.31`. Each
  token had an expiry at or before the trial deadline.

## Item 1. Attributable usage

GraphQL endpoint `POST /client/v4/graphql`. All data queries pinned
`viewer.accounts(filter: {accountTag: ...})`, used a half-open UTC window, and
filtered for identities recorded by the trial. The schedule sampled about 1 second
after the load, then at 1, 2, 5, 8, 12, 15 and 20 minutes. The Worker and DO invocation
queries kept the load window fixed. Periodic and storage queries extended the
end to the time of the sample so they could see later reports. A supplemental
analytics-only query measured the additive Worker CPU field and tested the
storage dataset's namespace-list membership filter. It generated no load.
[Workers metrics](https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-workers-metrics/)
[Durable Object metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)

| Dataset | Identity and fields | Hosted outcome |
| --- | --- | --- |
| `workersInvocationsAdaptive` | `scriptName`, `sum { requests cpuTimeUs wallTime duration requestDuration }`, `avg { sampleInterval }` | Exact script dimension. The fixed load-window query estimated 96 requests, 71,952 µs of CPU, raw sums of 6,998,742 for `wallTime`, 6,993,714 for `requestDuration`, and 0.87484275 for `duration`. `avg.sampleInterval` was 3.84. |
| `durableObjectsInvocationsAdaptiveGroups` | `namespaceId`, `sum { requests wallTime }`, `avg { sampleInterval }` | Exact namespace dimension. The load-window query reported 1 request at 15 minutes and 21 at 20 minutes, of 96 verified calls. Final raw `wallTime` was 1,316,232 and `avg.sampleInterval` was 1.91. |
| `durableObjectsPeriodicGroups` | `namespaceId`, `sum { cpuTime activeTime duration rowsRead rowsWritten storageReadUnits storageWriteUnits }` | Exact namespace dimension. First row at 20 minutes. Raw `activeTime` was 5,273,979, `cpuTime` was 25,642 and `duration` was 0.675069312. It reported 96 rows read and 97 written, matching the fixture. The trial did not establish invoiced duration units. |
| `durableObjectsStorageGroups` | `namespaceIds_has`, `max { storedBytes }`, `dimensions { namespaceIds }` | The schema exposes namespace-list membership and a maximum stored-byte value. The membership query returned zero rows through 20 minutes despite the 64 KiB write. Neither a namespace-exclusive storage value nor a GB-month total was observed. |

The first Worker data appeared between the approximately 1-second and 60-second
samples. The earliest namespace-attributed invocation row appeared between 12
and 15 minutes. It reported one request at a sample interval of 1 after the
96 confirmed DO round trips. A wider exact-namespace and script-name control
at about 10 minutes had also returned empty results.

**Billing-period query.** Account dataset settings reported a 32-day maximum
range, 90-day retention and 10,000 rows per query for all four datasets. The
requested period to date fit within that range. The period query reproduced 96
Worker requests and 71,952 microseconds of Worker CPU. It returned 20 estimated DO requests versus 21 in the fixed load window,
and no DO periodic or storage rows even though the short-window periodic
query had data. The period can be queried, but the trial could not reconcile
complete request, duration and storage totals. Worker totals held at each
nonempty sample apart from floating-point serialization. The random nonexistent
script and namespace controls returned empty arrays.
[Dataset settings](https://developers.cloudflare.com/analytics/graphql-api/features/discovery/settings/)

**Grants and errors.** Account Analytics Read alone was sufficient for data
reads. The management credential also received a successful settings response.
A data query with only Account Settings Read returned HTTP 200 with
`authorization denied` and no data. The deliberate unknown-field query
returned HTTP 200 with an `errors` entry and no data. The response exposed
`ratelimit` and `ratelimit-policy` headers. The trial stayed below the shared
quota and observed no HTTP 429.
Cloudflare documents 300 queries per five minutes by default and HTTP 429 for
quota or query-resource limits. A reader must reject GraphQL errors even when
HTTP succeeds, preserve evidence age through transient failures and respect
backoff without writing replacement zeroes.
[Analytics API token](https://developers.cloudflare.com/analytics/graphql-api/getting-started/authentication/api-token-auth/)
[Error responses](https://developers.cloudflare.com/analytics/graphql-api/errors/)
[Query limits](https://developers.cloudflare.com/analytics/graphql-api/limits/)

**Product decision.** Bifrost meters its own durable ledger. It prices
incarnation lifetimes, provisioned slot-seconds, committed cloud dispatch
assignments and SDK gateway requests authenticated to a live workflow run
currently assigned to a managed Cloudflare Worker. It also prices sealed
per-incarnation launcher counters. The deployed release determines which rule
prices each incarnation. Admission uses
the current UTC calendar month and closes at the
budget or if ledger evidence is unavailable or inconsistent. Policy save
also validates the static envelope. Both checks cover Bifrost-attributable
usage. Status shows the $5 monthly Workers Paid subscription separately
as billed by Cloudflare. The usage estimate can differ from the invoice.

GraphQL Analytics is an advisory comparison of sampled Worker requests
and the Bifrost request proxy. The incomplete Durable Object data prevents
a full spend reconciliation. Missing or divergent samples never gate admission.

### Measured release calibration

The current release keeps the hosted SDK sample of four API gateway requests
as the input to the 40-request attempt limit. Snapshot reads are served by
the pinned module store.
Third-party egress and snapshot reads consume the workflow's approved
subrequest allowance. Gateway fetches and refreshes count against both
allowances. The workflow can handle a 429 at its approval limit. A 429 at
the release gateway limit marks the attempt failed and reports the limit.

Local CPU qualification `46eb01a8-f3d7-415f-8092-be70e73ed6a0` measured
sequential requests using the exact bundled scripts and Linux process CPU.
The manifest pins the sample count, nearest-rank p99 and 10× margin.
With four cold samples, the nearest-rank p99 is the observed maximum.
End-to-end samples include local routing, DO work and runtime overhead.

| CPU path | Local CPU p99 | Deployed invocation limit or pricing reserve |
| --- | ---: | --- |
| Capacity capabilities and observe | 1.355 ms / 2.273 ms, 100 each | 87 ms capacity Worker limit, shared with lifecycle calls |
| Capacity prepare, start and delete | 8.658 / 2.547 / 5.643 ms, four each | 87 ms, the largest capacity sample × 10 rounded up |
| SDK gateway and launcher script | 109.638 ms loose / 85.358 ms Solution, four each | 1,097 ms script limit. The full workflow samples include the real SDK traffic and engine CPU, giving a conservative measurement for the gateway. |
| Warm priming | 43.918 ms, 100 | 440 ms on the Dynamic Worker entrypoint |
| Idle exercise | 61.295 ms, 100 | 613 ms and one subrequest on the Dynamic Worker entrypoint |
| Cold boot and first prime | 7,314.646 ms, four | 30,000 ms on the Dynamic Worker entrypoint. A 10× margin would exceed the existing ceiling. |
| Arbitrary approved workflow | 109.638 ms loose / 85.358 ms Solution inclusive, four each | The smaller of the reviewed workflow budget and 30,000 ms. The reference fixtures do not establish a ceiling for other approved Python. |
| Dynamic isolate startup | Cold exercise 4,852.122 ms inclusive, four. Cold boot 7,314.646 ms inclusive. | Startup is billed separately and WorkerLoader only exposes invocation CPU limits. The ledger retains the 1,000 ms published standard Worker startup reserve. These local measurements do not isolate billed Dynamic Python startup or verify that the standard limit applies to its Python bootstrap. |

The workerd tests verify the exact limit supplied to WorkerLoader for each
child path and the release tests verify the CPU limits uploaded for capacity
and gateway. Local workerd 1.20261023.1 completed a test invocation at HTTP
200 with a configured 1 ms limit despite about 518 ms of local workerd CPU.
The hosted runtime supplies CPU termination.
[Cloudflare Dynamic Worker limits](https://developers.cloudflare.com/dynamic-workers/usage/limits/) ·
[Cloudflare Worker startup limits](https://developers.cloudflare.com/workers/platform/limits/#worker-startup-time)

Bifrost prices each capacity request at its deployment limit. Before dispatch,
it reserves the approved execution limit, up to three boot or prime
invocations, each approved gateway call and the keep-warm tail at the
shipped limits. Idle alarms can only use an already confirmed generation.
Failure stops the managed warm tail until real work confirms a child again.
Reserve up to four initial Dynamic Worker identities and one stable idle
identity per day touched by the tail. The identity fee is separate from
CPU and Cloudflare bills it once per ID and code per UTC day.
The qualified CPU rate removes the old 30,400 ms per-launcher-second CPU
proxy and the duplicate 30,000 ms dispatch and SDK proxies. It leaves
DO lifetime charges and historical rate versions intact.
[Cloudflare Dynamic Worker pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)

The exact packaged launcher completed loose Python workflow
`7953faa1-0bfc-4866-b6a3-4f53e3345d1f`, remained running for 170 real
provider GETs over 340.854 seconds, then completed Solution snapshot
workflow `9b068376-8b10-48e8-b016-d3e99ea24bb7`. The workerd harness
used the controller's two-second minimum interval, which is faster than
the five-second idle sweep. It sent the same resource GET through the
real capacity Worker, WorkerLedger and launcher, with the launcher alarm
and control WebSocket active. The test supplied approved module snapshots,
verified both Python results and witnessed both terminal ACKs. It
exercised the provider path at controller cadence with a local relay
and fixtures for the API peer and placement.

The run used Bifrost's production wake scheduler and the packaged caps.
Both workflows settled successfully and the receipt recorded
`exhausted: false`.

| Launcher meter | Loose terminal | Solution terminal | Sealed incarnation | 10× cap |
| --- | ---: | ---: | ---: | ---: |
| SQLite row writes | 15 | 43 | 62 | 620 |
| DO request units | 3 | 6 | 6 | 60 |
| Application log events | 1 | 2 | 2 | 20 |

Terminal reports are cumulative snapshots. The sealed count includes
terminal and ACK storage, the launcher's meter journal, index writes,
and workload release bookkeeping. Polling, the control socket and
watchdog alarms are outside the workload cap. Exhaustion declines new
work before engine entry and leaves completed results intact. The
launcher can reconnect while results await ACK. Excess diagnostic logs
are suppressed independently. ACK clears each route tombstone after
committing a compact replay fence. A terminal, inventory or heartbeat
can close admission for its incarnation before another route is claimed.
The launcher also refuses stale attempts before engine entry. Settlement
headroom follows the configured slot count. The 620-write cap leaves
532 workload writes with one slot or 292 with four. The reference used
about 31 writes per route, so a four-slot launch can retire after about
nine similar workflows and be replaced when demand remains.

For each receipt meter, Bifrost reserves the largest of the pinned cap,
the sealed count, and the highest terminal count. Missing counts still
reserve the cap and over-cap cleanup remains billable. Incarnations
retain the rate and bounds of their deployed release. Admission closes
if a receipt-priced release has missing bounds.

Cloudflare bills DO duration by non-hibernating wall time at 128 MB per
object, with concurrent calls sharing an object's billed duration.
Bifrost charges the launcher's entire reservation-to-verified-deletion
lifetime at 0.128 GB, using Cloudflare's published conversion and an upper
bound on active time during ownership. Each two-second control poll reserves two DO requests
for WorkerLedger and Launcher. The lifetime rate adds a cadence allowance
for the watchdog, hello/reconnect, control frames, SQLite and logs, plus
one lifecycle allowance for startup and release. It reserves WorkerLedger
and ScopeRegistry residency at 128 MB each for the launcher lifetime.
Managed workflow dispatch
reserves TenantHost residency through its finite route deadline, the
20-minute warm window, a final 45-second alarm interval, 20 seconds
for an abandoned warm RPC and 17 seconds for the final exercise and
origin check. Bifrost rounds up each dispatch's ping count per UTC month.
Provider request and CPU reservations are committed immediately before
each request, including capability checks with zero live launchers.
Hosted capacity scripts reserve the CPU limit recorded with their deployment.

The estimate uses marginal rates. The static policy envelope retains wider
fleet reserves. Provider behavior described in the exclusions below can
affect the invoice.
[Cloudflare Durable Object billing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

Repricing the previous pilot as a 60-second-deadline scenario gives
**$0.11761008**, or **3,943.5×** its observed Worker request and CPU cost
of `$0.00002982382`. The ledger has 79.355808 launcher- and slot-seconds,
3 dispatches, 43 SDK requests and 3 incarnations. The historical capture
has no workload receipts or route-deadline reservations. This comparison
uses the 620/60/20 floor, three 60-second deadlines, a 1,000 ms approval
per workflow, subrequest allowances of 10, 12 and 50, a same-day warm tail,
and 54 capacity requests at 87 ms each. Cloudflare sampled 54 capacity requests, but the
pilot did not retain a Bifrost request counter. Stored historical charges keep their
deployed release pricing. The Cloudflare sample covers Worker requests
and CPU only. The three largest reserves per replay dispatch are retained
SQLite storage **$0.02000** (one incarnation per dispatch in this sample),
Dynamic Worker identity fees **$0.01000**, and all reserved Worker CPU
**$0.00408** (including **$0.00180** for up to three cold boot calls).
The previous replay's creation reserve alone was **$0.068 per dispatch**.
This run incurred no hosted spend.

The release manifest pins the exact Worker bundle fingerprint, the
measured capacity module digest, both measurement sources, the measured
inputs and the margin. The builder and API verify these against the
shipped bytes and enforced limits. Re-qualify changed bundles before
packaging. The rate version is
`workers-paid-2026-11-20-qualified-cpu`.

For a new qualification, build both candidate modules with `bundleWorker`
from `deploy/capacity/cloudflare/build-workers.mjs` and the qualified runtime.
Use `node dev/tenant-workers/qualify-cpu.mjs <package-directory> 4` on Linux
to measure the capacity and engine paths. Then run the lifecycle test in
`dev/tenant-workers/capacity-workerd.test.mjs` with
`BIFROST_QUALIFIED_CAPACITY_BUNDLE` and `BIFROST_QUALIFIED_LAUNCHER_BUNDLE`
to run the two Python references and seal the counters. Run only the
`prepare, start, signed hello` test. Set `BIFROST_QUALIFIED_CONTROLLER_POLLS=170`
for the cadence run and supply the packaged `BIFROST_LAUNCHER_METERS` JSON
as `BIFROST_QUALIFIED_LAUNCHER_METERS` for its acceptance check. Pass the
release child limits as `BIFROST_QUALIFIED_BOOT_CPU_MS`,
`BIFROST_QUALIFIED_WARM_CPU_MS` and `BIFROST_QUALIFIED_EXERCISE_CPU_MS`.

### Packaged Workers Meter Inventory

The deployment consists of the capacity script and its WorkerLedger and
ScopeRegistry DOs, plus the private launcher, CloudflareLauncher,
TenantHost and TenantApi exports. The engine is a Dynamic Worker. The
release builder checks the esbuild input graph and each input's reviewed
source hash against `deploy/capacity/cloudflare/meter-inventory.json`.
The Node test checks each classification below and rejects changed or
added inputs. The embedded Python and JS runtime archive uses its pinned
artifact digest. Changes within these inputs require review of their
billing classifications before another package can ship.

| Classification | Billable units and consuming paths | Bifrost accounting |
| --- | --- | --- |
| `capacity-http` | Worker request and CPU for `capacity.mjs` health, auth/refusal, capabilities, GET, prepare, start and delete. Enable canary and supervisor use the same client. The REST management and Analytics APIs are outside the Worker invocation path. | Durable request and measured 87 ms deployment CPU limit reserved before every managed client call, including idle capabilities. Public requests made outside Bifrost are excluded. |
| `capacity-state` | WorkerLedger HTTP, Launcher RPC and ScopeRegistry HTTP on reserve/observe/release. WorkerLedger reads and updates its intent row; ScopeRegistry reads and updates the slot set. Both DOs have duration and retained KV backed by SQLite. | Poll and lifecycle reserve for requests, row operations and duration. Scope state and journal retention have storage reserves. |
| `launcher-control` | Launcher enrollment, topology GET/RPC, 15-second alarm, alarm deletion, hello/welcome, inventory, control/ACK, 1-second outbound heartbeat, reconnect logs and control journal reads/writes. A workload command can fragment into up to 22 inbound frames, with terminate and ACK messages in addition. | Cadence, lifecycle and lifetime reserves. Workload frames receive a separate 26-unit dispatch reserve, conservatively pricing each as a full DO request. Outgoing WebSocket messages are free. Reconnect backoff resets on acknowledged progress. |
| `launcher-workload` | TenantHost run/warm/stop RPCs, route and slot journal, terminal persistence, ACK cleanup, SQLite meter row and application terminal logs. | Receipt for writes, DO calls and log events, with an admission cap and overage pricing. Lifetime and dispatch reserves cover reads. The stop wait is bounded, and a continuation after seal cannot issue a new charged RPC. |
| `gateway` | TenantApi and TenantHost route SDK fetch, refresh, diagnostics, snapshot reads and third-party egress. Authentication denials can still enter the gateway. | Reserve approved gateway CPU at the measured 1,097 ms launcher limit and up to 100 TenantHost RPCs per dispatch, including refresh diagnostics. WorkerLoader enforces the approved subrequest limit and the SDK gateway has a separate release cap. External fetch subrequests carry no additional standard Worker request fee. |
| `tenant-state` | TenantHost generation/prime/inflight/stop state, warm metadata, storage sync, deletion, 45-second alarm and rearm. Late hosts can outlive their launcher. | Per-dispatch storage/read/log/RPC reserve and full 128 MB duration through the deadline and extended warm tail. Each reserved alarm includes two DO units, eight reads and two writes for a failed ping and rearm. |
| `dynamic-engine` | WorkerLoader lookup, engine boot and warm retry, workflow, alarm exercise, import and SDK code, generation replacement. Dynamic Workers bill requests, CPU and unique creations per day. | Reserve up to three warm calls at the maximum of 30,000 ms for cold boot or 440 ms for priming, the approved execution cap, and each idle exercise at 613 ms. Carry the separate 1,000 ms startup reserve and up to four initial identities plus one reused identity per day covered by the tail. |
| `storage-logs` | SQLite bytes for route chunks, journals, provider tombstones and tenant state. Cloudflare Observability can charge for logging volume and retained data, including tenant stdout. | Active storage allowance and 0.1 GB-month per retained incarnation per UTC month until installation retirement. Managed uploads disable provider Observability and Logpush. Bifrost keeps the receipt and conservative log reserves. Exact provider allocation, external logging configuration and legacy retained logs remain excluded. |
| `policy-helpers` | Protocol parsing, path validation, egress host matching, response shaping and output limits inside the calling invocation. | Covered by the caller's CPU and storage bounds. These helpers use no separately billed service. |
| `inactive-features` | Agent and summary providers, manual scheduling, diagnostics and runtime probe paths reachable only in other spike configurations. | Outside the packaged managed workload surface. External traffic, provider-generated retries or overages, and account allowances and invoice rounding remain explicit exclusions. |

[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) ·
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) ·
[Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/) ·
[Workers resource limits](https://developers.cloudflare.com/workers/platform/limits/) ·
[Observability pricing](https://developers.cloudflare.com/observability/pricing/)

#### Hosted SDK Measurement

The hosted reference used manifest
`b4d55400c080dc014b85054f1560e8a0e4da2470d597c71ab5c501790801f112`,
Enable job `22529613-b865-4cab-9f7d-49361b3568a0`, and workflow
executions `85d6948e-bb29-4513-b2f0-459f14b31c13` and
`8694ea6d-d661-4d1e-a11e-5794e44f0e17`. The fixed workload sample
covered 17:01:25–17:08:40 UTC. Cloudflare reported 103 capacity Worker
requests, 42,499 µs CPU and 1,105 µs p99. The sample interval was
1.17. The Bifrost ledger counted four SDK requests across the two
completed workflows. That release used a 425 ms capacity CPU ceiling
and the `workers-paid-2026-11-20-calibrated` rate.

## Item 2. Workers Paid and Containers entitlement

The independent Workers Paid preflight succeeded through the existing
operator session. Bind the command to the intended account and read it from a
directory without a project config.

```sh
CLOUDFLARE_ACCOUNT_ID='<account-id>' cf accounts subscriptions get -q
```

Require exactly one matching `rate_plan.id == "workers_paid"`,
`state == "Paid"`, and `current_period_start <= now < current_period_end`.
The subscription's timestamps give the observed billing period for Item 1.
Cloudflare documents **Billing Read** for the REST equivalent,
`GET /client/v4/accounts/{account_id}/subscriptions`. Phase 0 (`2dadd5f0e`) observed
HTTP 403 with the provisioning token. This run reproduced HTTP 403, code 10000, with
an Account Settings Read-only token. It resolved the account-scoped Billing
Read permission group, but account-owned token creation with that grant alone
returned HTTP 400 and the token list showed no matching creation. Billing Read is outside the product grant.
[Account subscriptions API](https://developers.cloudflare.com/api/resources/accounts/subresources/subscriptions/methods/get/)

`GET /client/v4/accounts/{account_id}/entitlements` returned HTTP 200 with
active boolean-true `containers.enabled` and `workers.enabled` allocations
under a temporary token with **Account Settings Read alone**. For a Containers
feature check, require `containers.enabled`, a boolean-true allocation and an
empty deletion date. Cloudflare lists **Account Read** for this endpoint.
`workers.enabled` establishes feature access. Connect and Enable require active boolean-true `containers.enabled` as evidence of Workers Paid. Account Settings Read alone is sufficient.
[Account entitlements API](https://developers.cloudflare.com/api/resources/accounts/subresources/entitlements/methods/list/)

The trial covered one Paid account with Containers access. Paid or Enterprise
accounts without that entitlement will receive **Action Required** and cannot
enable this release. The trial did not test a Free account. A Free-account
negative control and any alternative Enterprise entitlement need separate
hosted qualification.

Account token replacement checks the same entitlements and reasserts the
current endpoint settings of exactly owned Workers with the candidate token.
The documented endpoint update requires Workers write authority.
Bifrost keeps the active credential until it verifies a write and confirms
the same ownership and deployment. With no live owned deployment to check,
use the current token to Enable, or Disconnect and connect with the new
credential. An unconsumed staged credential can survive a failed Connect
until it expires after ten minutes.
[Update Worker Script Subdomain](https://developers.cloudflare.com/api/resources/workers/subresources/scripts/subresources/subdomain/methods/create/)

## Spend and teardown

Estimated incremental spend was under **$0.03**. The estimate used one object
at 128 MB across the whole 38-minute envelope, at most 960 Worker CPU-ms from
96 requests at the configured limit, 96 DO calls and a small SQLite footprint.
It used published usage rates with margin and excluded the existing subscription
fee. The invoice was unavailable at teardown.
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

Scoped REST verified Worker and namespace absence at 09:57:40 UTC. The
independent `cf` session had found both identities during the trial. At
09:57:42 UTC it found zero Worker search matches and no namespace under the
exact script or namespace ID. All five issued account tokens returned absence
on exact-ID readback after deletion and were absent from the independent token
list. The last token readback was 09:57:48 UTC. The rejected Billing Read token
also had zero matches. The trial created no other hosted resources.
