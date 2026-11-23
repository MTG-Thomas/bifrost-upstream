# Preset B settlement and cold-path follow-up

## Confirmed burst cancellation

The global-4 fixture in Existing HTTPS produced a signed stopped terminal
on the reused Global launcher. The fixture trace tied the result to the
original dispatch and slot fence. The baseline API at 9e3998067 marked
that launcher's usage row uncertain while it was still live and had a
terminal counter snapshot. This happened before Scale In, the Managed
Tunnel canary, and the Upgrade drain.

A targeted baseline replay followed the same row through Scale In.
The row was retired and had a final receipt. Its reconciliation drift
flag was false, and no terminal counter exceeded its final value.

In the managed workflow protocol, stopped means the launcher received stop
confirmation from TenantHost. TenantHost revokes the attempt's credentials,
fences the old generation, and waits for child termination evidence when
an invocation may have entered. A stop without that evidence returns
uncertain. The API had grouped both states into the same sticky accounting
flag. The final receipt could be stored and acknowledged, while the price
query still required the full counter cap because the flag was set.

Commit 47e9ecf3e records genuine attempt uncertainty on each foreground
booking. Confirmed stops add no uncertainty. Counter settlement still requires
verified retirement and a valid final seal that covers every observed terminal
counter. Missing or invalid receipt evidence, counter drift, and historical
incarnation uncertainty can still keep the full reserve. CPU and engine-request
credits require matching invocation evidence for each route. An uncertain
booking or a missing canceled invocation earns no foreground credit.
The API records the route flag before acknowledging the terminal. The
launcher journals its invocation proof on that acknowledgment, so the
final seal cannot credit a recorded uncertain booking ahead of the flag.

At 3,600 seconds the old full counter reserve was $0.6336. The same
per-second allowances at 120 seconds total $0.02112. The earlier fixed
680/60/20 caps total $0.000701. The full-lifetime reserve is 30 times the
120-second scaled reserve and about 904 times the original fixed reserve.
Receipt-level uncertainty can still reserve the full $0.6336.

The canary's ledger now uses its admitted 120 seconds. CT128 read back
15,360 SQLite writes, 7,680 DO requests and 7,680 log events. The
installation's 3,600-second workflow policy and its envelope stay in force.

## Wake and decision

The original rehearsal logged 1,949 ms from eligibility to decision.
The repeat instrumented the same controller helpers on CT128. The fresh
delay was 1,776 ms, split as follows.

| Stage | Milliseconds after queueing |
| --- | ---: |
| Previous pass starts | 1,673 |
| Age timer armed | 1,784 |
| Eligibility | 2,000 |
| Age timer fires | 2,001 |
| Next pass starts | 3,673 |
| Primary-pool lookup begins and ends | 3,707–3,709 |
| Launch planning begins | 3,729 |
| Decision | 3,776 |
| Launch planning returns | 3,800 |

The wake was 1 ms after eligibility. The 2-second minimum spacing deferred
the pass another 1,672 ms and planning took 103 ms from pass start to
decision. The primary-pool lookup took 2 ms. The age timer and the ordinary
workflow wake share the same coalescing event. Preserving the qualified
maximum reconciliation frequency preserves the control-request pricing
assumption. The trace confirmed both the timer and spacing worked as designed.

A second trace reproduced a 1,965 ms eligibility-to-decision delay. The
prior pass started at 1,874 ms, the timer fired at 2,001 ms, and the next
pass began at 3,874 ms. Decision followed 91 ms later. Clock rounding
accounts for about 1 ms in both runs.

## Cold engine readiness

Dispatch can arrive while its admission preparation is warming the engine.
The launcher joins that preparation before the run enters. TenantHost
records engine-ready after the child returns its boot ping. That path
starts the local workerd/Pyodide child and imports its engine modules.
The GC cadence applies after workflow attempts and the entry callback
runs after readiness. Neither runs during this cold ping.

The final CT128 replay measured 43 ms from Prepare to ping start and
3,850 ms from ping start to ready. Dispatch arrived 89 ms into the ping,
leaving 3,761 ms between Dispatch and engine-ready. Entry followed 86 ms
after readiness. The earlier 3,430 ms sample measured the same wait
boundary. The time is concentrated in the local cold boot and ping. The
roughly 2-second hosted reference lacks a matched host/runtime A/B, so
the local evidence does not resolve how much of the difference comes from
the host or the environment.

## Maximum receipt CPU

Four samples used the original packaged capacity Worker and its real
SQLite ledger. The local launcher stub supplied a 1,111,140-byte synthetic
receipt with 512 workflow and 512 preparation routes and four invocation
entries per route. The probe read back every route through DELETE and GET.

| Capacity operation | Four workerd CPU samples (ms) | Package limit |
| --- | --- | ---: |
| DELETE with full receipt | 38.037, 18.506, 22.451, 17.707 | 112 ms |
| GET with full receipt | 9.970, 12.196, 9.801, 7.213 | 112 ms |

The process CPU measurements cover the local fixture, including the
launcher stub and SQLite work. The manifest limit and Worker digests
were retained. The current builder also reproduced the original manifest
and all three module bytes with the original package inputs.

## Trial planning

For this workload, use the post-retirement Bifrost estimate as the
expected settled estimate. An additional uncertain workflow route can
retain at most the full credit that would otherwise have been released.
With the pinned limits, that is four starts at 48,522 ms, three warm-up
allowances at 7,315 ms, 30,000 ms of workflow CPU, and four Worker requests.
At the checked-in rates the maximum incremental reserve is $0.00492186 per
route. This assumes consistent final launcher receipts and the same
workload and retirement schedule. A missing launcher seal is a separate
case at up to $0.6336 in launcher counter reserve alone.

All nine local preset B items passed at 47e9ecf3e using the original two
Worker packages. Upgrade kept the original claim, delivered one dispatch,
reconnected and acknowledged the terminal. The item-9 estimate was
$0.0748921399957893 after eight retirements with no uncertain
incarnations. The display rounded to $0.07. The $0.50 final estimate
guard passed, and Disconnect verified absence of scripts, namespaces,
tunnel, sessions and DNS.

| Uncertain workflow routes | Conservative estimate for the same retired workload | Round up to cents |
| ---: | ---: | ---: |
| 0 | $0.074892 | $0.08 |
| 1 | $0.079814 | $0.08 |
| 2 | $0.084736 | $0.09 |

### Nine-item trial budget

Use a 30-minute trial window within one UTC day, with the last five
minutes reserved for cleanup. The model assumes 14 workflow dispatches
with 110-second timeouts, four Enable or Upgrade canaries, and queue
pre-warming disabled. The account's opening estimate is carry-in. All
values here are trial increments.

| Reserve | Calculation | USD |
| --- | --- | ---: |
| Active launchers | Three × $0.6336. Each 3,600-second pin prices 460,800 writes, 230,400 DO requests and 230,400 log events. | $1.900800 |
| Revision or Scale In transition | One additional full reserve while a draining launcher's final meter clears. Scale In drains the existing fleet. Upgrade requires old script and namespace absence before replacement upload, so there is no second live three-launcher fleet. | $0.633600 |
| Canaries | Four × $0.02112. Each has 120 seconds of counter caps. This allows all four reserves to overlap the regular fleet estimate. | $0.084480 |
| Launcher elapsed time | `(4 × 1,800 + 4 × 120) s × $0.000072778/s`, the policy's pinned `second_usd`. | $0.558935 |
| Slot residence | `7,680 s × 4 × $0.0000016/s`, including the inventory slot correction and full occupancy. | $0.049152 |
| Workflow bookings | Nine concurrent × $0.007541438 = $0.067872942. Budget all 14 × $0.007541438 to include uncleared bookings from previous steps. | $0.105580 |
| Lifecycle and remaining work | Eight × $0.00004395 for launcher setup, cleanup and acknowledgement, plus $0.152811 of checked-in fixed meter allowances for control, storage and other trial work. | $0.153163 |
| **Planned live estimate** | **Sum before rounding** | **$3.485710** |

The per-workflow allowance prices up to `4 × 48,522 + 3 × 7,315 +
30,000 + 50 × 214 = 256,733 ms` of CPU at $0.02 per million ms,
$0.000062228 of dispatch and generation-control requests, reads, writes
and logs, `51 × $0.00000045` for the request portion of one dispatch
plus 50 SDK activity entries, 201 seconds of TenantHost duration at
$0.0000016/s including the 90-second linger and one second for rounding,
and a $0.002 engine identity. The fixed reserve uses the gross checked-in
limits, including the full fixed CPU allowance. The measured nine-slot
live sample was $1.99.

Propose a **$3.60 live-estimate target**, **$4.50 live abort**, and
**$0.15 settled-spend target** for owner approval. The $0.90 gap covers
one further $0.6336 full-size unsealed launcher and up to
`1,800 × ($0.000072778 + 4 × $0.0000016) = $0.1425204` if it also
remains resident for the full window. Carry the opening account
estimate into each threshold and check remaining work before advancing.
At abort or when the cleanup window starts, stop admitting new work and
continue authorized cleanup.

The two rehearsal reads put expected settled spend at **$0.072–$0.075**
for this workload. If one full-size launcher is confirmed retired with
its seal missing, the estimate can reach about **$0.706–$0.709** from
the counter reserve alone. Allow **$0.78** including the foreground
credits that would also be withheld. The upper bound for those credits is
`14 × $0.00492186 = $0.06890604`, even if all 14 routes belonged to
the same launcher. The precise increment depends on the missing
launcher's final counts and booked routes. Provider billing was
unmeasured.

## Verification

The protected fixture used internal-only networks, local provider hooks
and the pinned cached Pyodide bundle. The baseline reproduced the $0.73
stop and the counter-only replay verified the exact sealed row. The
intermediate a48ba4882 run received an unconfirmed stop and kept its
slot fenced, which stopped the harness at cancellation. The first
47e9ecf3e run found a diagnostics-only lookup after worker retirement.
The final run retained the cold fence before Scale In and passed.
The product timeouts and stop checks stayed unchanged.

```sh
node --test deploy/capacity/cloudflare/build-workers.test.mjs
node spikes/cloudflare-pyodide/dev/tenant-workers/qualify-cpu.mjs <pinned-package> 4 --max-receipt-only
BIFROST_QUALIFIED_CONTROLLER_POLLS=170 node --test --test-name-pattern='prepare, start, signed hello' spikes/cloudflare-pyodide/dev/tenant-workers/capacity-workerd.test.mjs
python3 spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py
```

The build tests passed 13 of 13. The targeted cadence test passed and
sealed 68 SQLite writes, six DO requests and two log events with 170
observations and 182 capacity requests.

The first backend suite found a storage test still passing the removed
keyword. Commit dd1fa2785 updates that fixture. CT128 then passed all
875 tests under the exact command below, including the stopped and
uncertain route cases, receipt-level failures, all three fleet envelopes,
and the single Alembic head.

```sh
BIFROST_SKIP_BUILD=1 ./test.sh tests/unit/worker/remote tests/unit/services/worker_pool tests/unit/test_alembic_single_head.py tests/e2e/platform/test_cloudflare_capacity_persistence.py tests/e2e/platform/test_cloudflare_workflow_approval.py
BIFROST_SKIP_BUILD=1 ./test.sh quality api
```

API quality passed with zero Pyright diagnostics and a clean Ruff check.
The follow-up logs and diagnostic fixture are retained on CT128 under
/opt/bifrost-tests/requal-dd1fa2785c60ee0a9e4e0ab6d171344fc9f47265/verification.
The owned test stacks, fixture processes and image tags were removed.

### Unbooked and stopped terminals

Commit `1d48409c4` restores the incarnation-wide reserve when a stopped
or uncertain terminal cannot be tied to an exact workflow booking.
This includes summary and autonomous agent terminals. A booked stop
retains its own foreground reserve, so its receipt cannot claim credit
if a launcher reports partial invocations. Sibling routes still settle
with a consistent final receipt.

CT128 ran the same backend and quality commands above against that
commit. All 880 cases passed, including 139 capacity persistence tests
and six workflow approval tests. Pyright reported zero diagnostics and
Ruff passed. The canary regression exercised the production pin path
against PostgreSQL and verified 15,360 writes, 7,680 DO requests and
7,680 log events under the 3,600-second policy.

The full local rehearsal used both original Worker release packages,
with the API rebuilt from `1d48409c4`. The diagnostic copy of
`combined-rehearsal.py` retained the original gates and recorded the
database spend readback. During the burst, the confirmed stop left one
flagged booking and zero incarnation-wide flags. Item 9 passed at
$0.0720068330304167, with eight retired incarnations and zero uncertain
incarnations. Upgrade kept its original claim, reconnected and
acknowledged the single dispatch. Disconnect verified both installations'
resource absence. CT128 evidence is in the `verification` directory
under `/opt/bifrost-tests/requal-1d48409c46996ae73003867a62d3851e1cf75d6e`.

### Hosted readback at `e7c59788c`

Preset B passed Existing HTTPS Enable, cold and warm latency, the
nine-slot burst, isolated cancellation, Reconcile and active Scale In.
Its peak observed increment was $2.02 versus the approved $3.60 target
and $4.50 abort. Both installations were retired, settling the trial at
$0.061801362912 incremental against the $0.15 target. Managed Enable
stopped on the tunnel ingress ownership check. Rotation and Upgrade
were unreached. The Workers-only Analytics proxy was $0.00006021396 and
the Bifrost-to-proxy ratio was 1,026.36.

See the [sanitized hosted receipt](cloudflare-scaleout-preset-b-hosted.json).
