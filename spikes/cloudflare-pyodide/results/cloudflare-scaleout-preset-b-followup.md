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

The nine-slot live sample was $1.99 while launcher and foreground
reservations were still outstanding. For owner review, the proposed
thresholds are $2.25 for planned live Bifrost exposure and $2.50 for
abort, with a $0.15 settled estimate target. Include account carry-in
and remaining cleanup before each phase. The fixture exercised the
original $0.50 post-retirement check. Provider billing was unmeasured.

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
