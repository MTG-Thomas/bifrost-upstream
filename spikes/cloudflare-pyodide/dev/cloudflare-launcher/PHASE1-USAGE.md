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
as billed by Cloudflare. Amounts are order-of-magnitude estimates with operational backstops.
The estimate uses gross usage rates, including SQLite storage. Bifrost cannot
claim any part of the shared account's included allowances.

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

The local CPU qualification runs measured sequential requests
using the exact bundled scripts and Linux process CPU. The second run used
the three-script package. Pin the higher observed p99 for each path.
The manifest pins the sample count, nearest-rank p99 and 10× margin.
With four cold samples, the nearest-rank p99 is the observed maximum.
End-to-end samples include local routing, DO work and runtime overhead.

| CPU path | Local CPU p99 | Deployed invocation limit or pricing reserve |
| --- | ---: | --- |
| Capacity capabilities and observe | 1.691 ms / 2.273 ms, 100 each | 87 ms capacity Worker limit, shared with lifecycle calls |
| Capacity prepare, start and delete | 8.658 / 2.547 / 5.643 ms, four each | 87 ms, the largest capacity sample × 10 rounded up |
| Private SDK gateway | 109.638 ms loose / 85.358 ms Solution, four each | 1,097 ms on the gateway script. The full workflow samples include the SDK traffic, engine, and relay. |
| Launcher, TenantHost and Fetcher relay | Separate billing for Worker CPU and DO duration | The launcher script retains 30,000 ms. Price relay and gateway CPU together using the inclusive gateway sample. Price DO residency by duration. |
| Warm priming | 43.918 ms, 100 | 440 ms on the Dynamic Worker entrypoint |
| Idle exercise | 61.295 ms, 100 | 613 ms and one subrequest on the Dynamic Worker entrypoint |
| Cold boot and first prime | 7,314.646 ms, four | 30,000 ms on the Dynamic Worker entrypoint. A 10× margin would exceed the existing ceiling. |
| Arbitrary approved workflow | 109.638 ms loose / 85.358 ms Solution inclusive, four each | The smaller of the reviewed workflow budget and 30,000 ms. The reference fixtures do not establish a ceiling for other approved Python. |
| Dynamic isolate startup | Cold exercise 4,852.122 ms inclusive, four. | **Unenforced.** Reserve 48,522 ms per possible isolate load. After a verified seal, price each observed foreground load at the expected 4,852.122 ms (local sample × 1). The deployment startup check does not apply to WorkerLoader, and its CPU option limits invocation CPU. An evicted isolate can incur startup again. |

The workerd tests verify the exact limit supplied to WorkerLoader for each
child path and the release tests verify the CPU limits uploaded for capacity
and gateway. Local workerd 1.20261023.1 completed a test invocation at HTTP
200 with a configured 1 ms limit despite about 518 ms of local workerd CPU.
The hosted runtime supplies CPU termination with variable enforcement delay.
In termination probes, an 87 ms limit stopped at 2,347 ms and a 1,097 ms
limit stopped at 3,097 ms. Overruns of roughly 2–2.3 seconds per terminated
invocation can make Bifrost's estimate low.
[Cloudflare Dynamic Worker limits](https://developers.cloudflare.com/dynamic-workers/usage/limits/) ·
[Cloudflare Worker startup limits](https://developers.cloudflare.com/workers/platform/limits/#worker-startup-time)

New release snapshots price capacity calls from the largest capacity sample
and use measured boot, prime, workflow, gateway and idle-exercise CPU.
Round measured CPU up to milliseconds. Use the inclusive workflow sample
as the proxy for variable approved work and for the combined gateway and
Fetcher path. The reviewed workflow CPU limit still caps the pricing
input and the deployed invocation limit. Dispatch planning counts one
workflow, up to three boot or prime calls, and each approved gateway call.
Admission can overlap one separately reserved boot with dispatch preparation.
A ready host skips the admission ping. Readiness is held in memory for a
default of 90 seconds after the last attempt (policy range 0–300 seconds).
There are no idle alarms. Workerd can evict an idle child earlier.

The loader authorization books each distinct loader identity, including its
engine code digest once per UTC day. A tenant-day forecast for the dispatch
pays for the first identity. Preparation books the identity before Loader entry.
Additional identities pay separately, even on the same day. A seal can
also witness a new UTC day after the loader authorization. Count a dispatch
once in its starting UTC month and place linger duration in each month touched.
[Cloudflare Dynamic Worker pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)

The foreground receipt uses the engine isolate's first-request signal.
Counting WorkerLoader calls or code callbacks would miscount when an
existing identity starts another isolate from cached code. The TenantHost
returns the identity, request class, UTC timestamp, and first-request flag
for each observed boot, prime, and workflow invocation. Workflow entries
also carry the TenantHost's wall time across the child request and response.
The launcher records workflow evidence after Bifrost acknowledges the durable
terminal. Admission evidence is independent and includes an empty list for a
reused engine and a boot receipt when the workflow never arrives. A verified
deletion receipt settles child request counts and unused boot or prime CPU.
Live workflows reserve their approved CPU up to the engine ceiling. Settlement
prices workflow CPU from observed child wall time up to that reservation.
Wall time includes I/O, so I/O-heavy work may cost less than estimated.
Missing or uncertain evidence keeps the corresponding reservations.
[WorkerLoader API](https://developers.cloudflare.com/dynamic-workers/api-reference/)

The exact packaged launcher completed loose Python workflow
`0b9c414b-d42c-4454-bb88-42abbeb2b578`, remained running for 170 real
provider GETs over 340.861 seconds, then completed Solution snapshot
workflow `fe3b6d8f-5515-4c25-8c80-aff9c55b8bbf`. The workerd harness
used the controller's two-second minimum interval, which is faster than
the five-second idle sweep. It sent the same resource GET through the
real capacity Worker, WorkerLedger and launcher, with the launcher alarm
and control WebSocket active. The test supplied approved module snapshots,
verified both Python results and witnessed both terminal ACKs. It
exercised the provider path at controller cadence with a local relay
and fixtures for the API peer and placement.

The run used Bifrost's production wake scheduler and the packaged caps.
Both workflows settled successfully. Each sealed route reported two
child calls under one identity. The boot loaded an isolate and the
workflow reused it. The run witnessed two distinct generation bookings.
The seal recorded 20,480 SQLite bytes and `exhausted: false`. A separate
workerd test replaced the runtime and observed another first request
under the same identity.

| Launcher meter | Loose terminal | Solution terminal | Sealed incarnation | 10× cap |
| --- | ---: | ---: | ---: | ---: |
| SQLite row writes | 15 | 44 | 64 | 640 |
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
headroom follows the configured slot count. The 640-write cap leaves
552 workload writes with one slot or 312 with four. The reference used
32 writes per route on average. A four-slot launcher has room for about
nine similar workflows and can be replaced if demand remains.

While a launcher is live or lacks a trustworthy seal, Bifrost reserves
the greater of the pinned cap and the highest observed count. A verified
deletion with a complete launcher meter settles writes, request units,
and log events at their sealed counts, including over-cap cleanup. A
regressing count keeps the reserve. The packaged private launcher has
no default fetch or scheduled handler, so its theoretical outer Worker
requests and CPU are released on the same seal. The named service-binding
relay retains its per-dispatch reserve. Incarnations retain the rate and
bounds of their deployed release. Admission closes if a receipt-priced
release has missing bounds.

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
reserves TenantHost residency through its finite route deadline plus the
configured linger. Preparation prices one possible engine request, its
generation control, and up to 20 seconds of boot residency. It carries no
second linger, workflow gateway allowance, or workflow journal proxy.
The idle tail adds only a duration allowance and does not schedule keep-alive
traffic. Historical releases retain their deployed alarm pricing.
Provider request and CPU reservations are committed immediately before
each request, including capability checks with zero live launchers.
Managed capacity requests use the measured estimate recorded with their deployment.

The estimate uses marginal rates. The static policy envelope retains wider
fleet reserves. Provider behavior described in the exclusions below can
affect the invoice.
[Cloudflare Durable Object billing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

### Historical Workflow Cost and Receipt Replay

The examples below record the earlier limit-priced model. New policy
snapshots use the expected-usage model described above.

| Local reference | Live dispatch reservation | Settled dispatch estimate |
| --- | ---: | ---: |
| Approved loose workflow | $0.07374581 | $0.06076049 |
| Approved Solution snapshot workflow | $0.07374581 | $0.06076049 |

These are direct dispatch costs from the local receipts and production
pricing formula. Each fixture had a 60-second deadline, a 1,000 ms approval,
12 allowed subrequests and one SDK request. Shared launcher lifetime
and capacity polling belong to the fleet estimate. The seal released
$0.00498472 in foreground CPU, $0.00000060 for two unused child calls and
$0.008 for unused identity capacity per workflow. The remaining direct
reserve includes $0.04813920 for the 30 possible idle pings, including
their potential loads and relay calls. Foreground gateway and relay CPU
contribute another $0.00746328. Shared launcher counters price at
$0.00006610 for the sealed incarnation, down from the $0.00066100 floor.

The earlier hosted pilot had 79.355808 launcher- and slot-seconds,
3 dispatches, 43 SDK requests and 3 incarnations. It recorded no sealed
workload receipts or route deadlines. Keep its recorded release pricing
at $0.34698373 in the prior 60-second scenario, **11,634.4×** the observed
$0.00002982382 for Worker requests and CPU. Repricing those inputs with
the then-current 640/60/20 floor and no receipts gives **$0.34704373
(11,636.5×)**. The scenario uses three 60-second deadlines, 1,000 ms
approvals, subrequest allowances of 10, 12 and 50, three distinct cold
tenants with same-day warm tails, and 54 capacity requests at 87 ms each.
Cloudflare supplied the sampled capacity request count.

For comparison, applying the local receipt shape to that historical
workload gives **$0.21092935 (7,072.5×)**. It assumes one cold boot,
one warm workflow, one identity, and half of the local two-workflow seal's
launcher counts per historical incarnation. The replay removes $0.09527458
in private launcher outer requests and CPU, $0.024 in identities,
$0.01495415 in foreground CPU, $0.00188385 in launcher counters and
$0.00000180 in unused child requests. This is a receipt scenario. The
actual hosted rows still lack those receipts. The denominator covers
sampled Worker requests and CPU. It excludes DO charges and does not
separately attribute Dynamic Worker consumption.

The release manifest pins the exact Worker bundle fingerprint, the
measured capacity module digest, both measurement sources, the measured
inputs and the margin. The builder and API verify these against the
shipped bytes and enforced limits. Re-qualify changed bundles before
packaging. The rate version is
`workers-paid-2026-11-20-settled-usage`.

The latest exact package CPU check, `d04fa065-1b6a-4c52-bcf4-704265e50c5c`,
observed 4,613.784 ms on cold exercise and 85.218 ms on warm exercise.
The release retains the expected 4,852.122 ms per-load startup price and the
613 ms invocation cap for warm exercise.

For a new qualification, build the candidate modules with `bundleWorker`
from `deploy/capacity/cloudflare/build-workers.mjs` and the qualified runtime.
Use `node dev/tenant-workers/qualify-cpu.mjs <package-directory> 4` on Linux
to measure the capacity and engine paths. Then run the lifecycle test in
`dev/tenant-workers/capacity-workerd.test.mjs` with
`BIFROST_QUALIFIED_CAPACITY_BUNDLE`, `BIFROST_QUALIFIED_LAUNCHER_BUNDLE`
and `BIFROST_QUALIFIED_GATEWAY_BUNDLE`
to run the two Python references and seal the counters. Run only the
`prepare, start, signed hello` test. Set `BIFROST_QUALIFIED_CONTROLLER_POLLS=170`
for the cadence run and supply the packaged `BIFROST_LAUNCHER_METERS` JSON
as `BIFROST_QUALIFIED_LAUNCHER_METERS` for its acceptance check. Pass the
release child limits as `BIFROST_QUALIFIED_BOOT_CPU_MS`,
`BIFROST_QUALIFIED_WARM_CPU_MS` and `BIFROST_QUALIFIED_EXERCISE_CPU_MS`.

### Packaged Workers Meter Inventory

The deployment consists of the capacity script and its WorkerLedger and
ScopeRegistry DOs, the private launcher, CloudflareLauncher, TenantHost
and its Fetcher relay, and the separate private gateway. The engine is a Dynamic Worker. The
release builder checks the esbuild input graph and each input's reviewed
source hash against `deploy/capacity/cloudflare/meter-inventory.json`.
The Node test checks each classification below and rejects changed or
added inputs. The embedded Python and JS runtime archive uses its pinned
artifact digest. Changes within these inputs require review of their
billing classifications before another package can ship.

| Classification | Billable units and consuming paths | Bifrost accounting |
| --- | --- | --- |
| `capacity-http` | Worker request and CPU for `capacity.mjs` health, auth/refusal, capabilities, GET, prepare, start and delete. Enable canary and supervisor use the same client. The REST management and Analytics APIs are outside the Worker invocation path. | Book one request and 9 ms estimated CPU before each managed capacity call. The deployed CPU backstop remains 87 ms. Public requests made outside Bifrost are excluded. |
| `capacity-state` | WorkerLedger HTTP, Launcher RPC and ScopeRegistry HTTP on reserve/observe/release. WorkerLedger reads and updates its intent row; ScopeRegistry reads and updates the slot set. Both DOs have duration and retained KV backed by SQLite. | Poll and lifecycle reserve for requests, row operations and duration. Scope state and journal retention have storage reserves. |
| `launcher-control` | Launcher enrollment, topology GET/RPC, 15-second alarm, alarm deletion, hello/welcome, inventory, control/ACK, generation grants, 1-second outbound heartbeat, reconnect logs and control journal reads/writes. | Cadence, lifecycle and lifetime reserves. Release the private script's outer Worker request and CPU reserve after verified deletion with its seal. The script has no default fetch or scheduled entrypoint. Per dispatch, reserve 26 DO units for command fragments, terminate and ACK, plus four for generation replies. Outgoing WebSocket messages are free. Reconnect backoff resets on acknowledged progress. |
| `launcher-workload` | TenantHost prepare/run/warm/stop RPCs, route and slot journal, terminal persistence, ACK cleanup, SQLite meter row and application terminal logs. | Live admission cap and settled counts for writes, DO request units, and log events. Missing or uncertain seals preserve the cap. Lifetime and dispatch reserves cover reads. The launcher counts each TenantHost preparation RPC and its receipt write. |
| `gateway` | The launcher Fetcher passes the request and a tenant-scoped DO stub to the private SDK gateway. Routing covers SDK fetch, refresh, diagnostics, snapshot reads and third-party egress. | Price the gateway and relay at the inclusive 110 ms sample proxy per planned call, with up to 100 TenantHost RPCs per route. The deployed CPU backstops remain 1,097 ms and 30,000 ms. The service-binding invocation adds CPU but no standard Worker request fee. API counters miss snapshot, external, and failed calls. The terminal cannot attest every relay or gateway arrival, so the reserve remains. |
| `tenant-state` | TenantHost generation, prime, in-flight and stop state. Linger is an in-memory readiness window. | Per-dispatch storage/read/log/RPC reserve and 128 MB duration allowance for each operation and its idle tail. No ping or alarm reserve for the bounded linger. |
| `dynamic-engine` | WorkerLoader lookup, admission boot, dispatch boot or prime retry, workflow, import and SDK code, generation replacement. Dynamic Workers bill requests, CPU and unique creations per day. | Reserve 48,522 ms for each possible load and settle startup at the expected 4,852.122 ms per observed load. Missing receipts keep their startup reserves. Daily creation pricing deduplicates by loader identity with its engine code digest. |
| `storage-logs` | SQLite bytes for route chunks, journals, provider tombstones and tenant state. Cloudflare Observability can charge for logging volume, including tenant stdout. | Price the active launcher's high-water `databaseSize` with a 10× sizing margin, or 100 MB before a receipt. Reserve 10 MB for associated active state. At retirement the capacity ledger keeps the seal while the launcher database is cleared. After Bifrost commits its settlement it acknowledges the receipt, and the ledger replaces the receipt with a small replay fence. Estimate 128 KB for an unacknowledged receipt and 16 KB for the fence. Add 4 KiB per retired incarnation for retained TenantHost state, including route-stop markers. Price uncleared launcher residue at its measured high-water size or 1 MB when unknown, and shared capacity scope state at 1 MB. Prorate residue through deletion of its namespace. Operator-enabled logs remain outside the estimate. |
| `policy-helpers` | Protocol parsing, path validation, egress host matching, response shaping and output limits inside the calling invocation. | Covered by the caller's CPU and storage bounds. These helpers use no separately billed service. |
| `inactive-features` | Agent and summary providers, manual scheduling, diagnostics and runtime probe paths reachable only in other spike configurations. | Outside the packaged managed workload surface. External traffic, provider-generated retries or overages, and account allowances and invoice rounding remain explicit exclusions. |

[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) ·
[Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) ·
[Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/) ·
[Workers resource limits](https://developers.cloudflare.com/workers/platform/limits/) ·
[Observability pricing](https://developers.cloudflare.com/observability/pricing/)

The monthly target applies to the Bifrost estimate used for admission.
Provider overrun, detached work, usage outside Bifrost, and billing adjustments
can change the invoice. Cloudflare Analytics remains advisory.

At seal, compare independent Postgres workflow and preparation counts with
the corresponding receipt counts, capping each class at 128. Report
differences greater than 4� with a two-route noise floor. A receipt claiming
over 4� as many workflows as either reported DO calls or SQLite writes keeps
the full launcher reserve. Skipped preparations can record an empty receipt
without a TenantHost RPC. Unmatched routes keep their reserve.
Discard sealed route keys and prune older deduplication and incarnation
records after the previous UTC billing month. Roll up storage residue by
installation and deployment revision so admission can price it without reading each
retired incarnation.

The final launcher seal reserves writes for the ledger rows, their index
entries, and the meter row removed by `deleteAll()`. Cloudflare documents
SQLite deletes as billed writes and does not distinguish bulk clearing in
that pricing rule. [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/#sqlite-storage-backend)

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
