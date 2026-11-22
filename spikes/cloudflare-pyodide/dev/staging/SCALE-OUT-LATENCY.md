# Cloudflare Workers scale-out latency

The first overflowed workflow reached user code in **10.178 s** and completed
in **10.261 s** with the primary held full. The largest identifiable
contributor is the **5 s dispatch-age threshold for creating managed Workers
capacity**. The next substantial contributor is cold engine startup, with
**2.035 s** as a separate hosted benchmark for a first synthetic workflow.
The managed receipt does not isolate engine startup or launcher setup.

This analysis starts at spike commit `5716151b0`. The combined hosted receipt
names `58029df89` and the qualified bundle. The API source in this path is
identical between those commits. The trial held one primary worker at 24
whole MiB free, enabled the managed scope before submission, and ran one
approved workflow on a fresh incarnation at ordinal 0 with queue pre-warming
off. The harness stopped when execution detail returned a null `created_at`.
The measurements come from PostgreSQL and execution-log readback. No warm
repeat or burst was submitted.
[Hosted receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json)
· [Pilot record](../cloudflare-launcher/PHASE1-PILOT.md#combined-hosted-run-with-observable-gates)

## Budget for the observed run

| Interval | Time | Evidence |
| --- | ---: | --- |
| Execution row creation → API running-row stamp | 6.558653 s | **Measured.** Includes demand recognition, launch, session registration, claim, and initial consumer work. |
| API running-row stamp → workflow entry log | 3.619677 s | **Measured.** Includes the remaining API prep, transport, any unfinished preparation or engine boot, and entry. Preparation can already be in flight at the stamp. |
| Execution row creation → entry | 10.178330 s | **Measured.** Sum of the preceding two intervals. |
| Entry → persisted completion | 0.082999 s | **Measured.** Includes the fixture remainder and result handling. |
| Completion → slot release | 0.063166 s | **Measured.** Outside queue-to-entry and queue-to-completion. |

The receipt uses `executions.created_at` as its queue clock. The planner uses
`work_dispatches.created_at`, which normally defaults to PostgreSQL transaction
time. The two clocks can differ, and neither field records commit visibility.
The claim first writes `executions.started_at` while moving Pending to Running.
The workflow consumer then calls `create_execution(check_existing=True)`,
which updates `started_at` again on the existing row. The durable readback
therefore marks an API update **after** claim commit and initial consumer
work. It cannot time claim commit, transport send, or child entry.
[Dispatch staging](../../../../api/src/services/execution/workflow_dispatch.py)
· [Claim transition](../../../../api/shared/run_cancellation.py)
· [Execution row update](../../../../api/src/repositories/executions.py)
· [Workflow consumer](../../../../api/src/jobs/consumers/workflow_execution.py)

| Stage | Bound or estimate | What is on the path |
| --- | --- | --- |
| Overflow detection | **Code-derived for the observed revision:** 5 s minimum dispatch age for a managed launch. Nominally another 0–5 s until an eligible pass on a healthy, promptly executing controller. In this sample, cadence, launcher setup, claim, and early consumer work share roughly **1.56 s** above the age gate if the queue timestamps nearly coincide. | The supervisor listens for workflow and Cloudflare capacity wakes. Passes start at least 2 s apart on a wake and sweep at 5 s from the last pass start without one. The observed revision cleared an early wake after the pass and relied on the next event or sweep. Slow passes or unavailable capacity can exceed the nominal band. |
| Launcher incarnation | **Unmeasured:** shares the roughly 1.56 s pre-stamp remainder with cadence and claim work under that same clock assumption. No independent lower bound beyond zero at receipt precision. | Bifrost commits a worker reservation and revision/control-root pin. The controller includes new rows in the same pass, observes the resource, prepares and binds it, then sends start. The capacity Worker delegates to its ledger/registry and Launcher DO. The Launcher DO durably accepts start and schedules an immediate alarm to dial the API. The WebSocket upgrade, signed hello, owner lease, welcome, and reconciled inventory open the claim gate. There is no required second supervisor sweep to make that gate open. |
| Claim and slot assignment | **Unmeasured:** shares the same pre-stamp remainder. The ready executor checks immediately on its first claim loop. Existing launchers check on a wake, with a 1 s busy or admission-blocked poll and a 15 s idle fallback sweep. | PostgreSQL rechecks scope and spend, fences the owner, locks the selected dispatch and serializes slot admission for the installation. The canonical claim and slot ordinal/fence commit together. The 30 s head-of-line guard applies to an older request that is temporarily too large to fit. It is not a minimum age to claim an eligible workflow. |
| Admission prepare and engine boot | **Estimated from separate hosted data:** about **2 s** for first-workflow engine startup, with synthetic candidate samples spanning 1.485–4.028 s. The share exposed on this managed path is unknown. | After the claim commit, the API schedules the best-effort `prepare_workflow` task and starts the normal consumer alongside it. The prepare rechecks the fence, books usage and sends a hint. TenantHost confirms readiness, reserves the generation through the API if required, and uses `LOADER.get` to launch or reuse the Dynamic Worker. The initial ping/prime brings up Pyodide. This load is on the tenant-slot path after claim, separate from the Launcher DO handshake. |
| Dispatch → entry | **Unmeasured separately:** constrained by the observed 3.620 s post-stamp interval together with the tail of API prep and any remaining boot. The send-to-entry slice is a subset of that interval. | The API resolves metadata and authority, issues route-bound run credentials, validates and packages the approved workspace, writes Redis context, and commits usage plus the sent fence before sending the command. The launcher journals the route, joins a matching in-flight prepare or warms after an explicit pre-entry readiness refusal, then calls TenantHost. TenantHost persists entry-safety markers before child fetch. The engine loads the workflow and reaches the fixture entry marker. |

The exact pre-stamp remainder after subtracting the age gate is
`1.558653 s - (dispatch_created_at - execution_created_at)`. That remainder
covers elapsed cadence, post-cutoff planning, provider startup, readiness,
claim, and initial consumer work together. The receipt lacks the dispatch
clock and the internal markers, so the approximate 1.56 s is a shared
constraint under a near-coincident-clock assumption, not an allocation
to any one stage. Likewise, subtracting the 2.035 s benchmark from the
10.178 s total leaves about **8.14 s** in a serial model. It is an
order-of-magnitude guide to the time outside the engine, not a measured
decomposition. If all 2.035 s were exposed after the running-row stamp,
about 1.59 s of that interval would remain for the rest. Overlap or an
earlier prepare increases that modeled residual.

### Why the controller starts Workers

`PrimaryPool.saturated()` checks the configured Linux cohort's readiness
and heartbeat freshness. A static cohort requires all its normal workers to
be ready. A job or external cohort must reach its effective ceiling. Those
conditions establish a healthy primary at its capacity limit. The planner
then packs demand against existing handler slots and unreserved memory,
including in-flight capacity. Uncovered demand can start a managed launcher
only after the age gate and if the Normal-lane `workflow.execute` v1
dispatch has an approved Cloudflare revision and fits the 128 MiB declared
slot. Installation admission checks the deployed revision, release bounds,
work-kind policy and spend. Shared, launcher and tenant/installation slot
limits constrain the reservation. A ready launcher can claim a compatible
dispatch without the launch-age gate, subject to its own admission and
fenced slot checks. The slot chooser favors reuse on the same launcher.
[Controller](../../../../api/src/services/worker_pool/controller.py)
· [Cadence](../../../../api/src/services/pool_cadence.py)
· [Supervisor](../../../../api/src/services/worker_pool/cloudflare_supervisor.py)
· [Admission](../../../../api/src/services/worker_pool/cloudflare_admission.py)
· [Eligibility](../../../../api/src/services/worker_pool/eligibility.py)
· [Claims](../../../../api/src/services/worker_pool/claims.py)

The launch path makes provider calls for capabilities, observe, prepare and
start. Start acceptance can precede alarm execution, and ready capacity
also depends on API inventory reconciliation. Measure both. The Enable
canary and ingress checks completed before the workload clock began. The
recorded TLS reuse counts belong to preflight and Enable, so they do not
time the workflow's launcher dial.
[External controller](../../../../api/src/services/worker_pool/external_controller.py)
· [Capacity Worker](../cloudflare-launcher/capacity.mjs)
· [Launcher DO](../cloudflare-launcher/launcher-do.mjs)
· [API host](../../../../api/src/worker/remote/host.py)
· [TenantHost runner](../tenant-workers/launcher-host.mjs)
· [TenantHost engine](../tenant-workers/tenant-api.mjs)
· [API dispatch](../../../../api/src/worker/remote/remote_pool.py)

### Cross-checks from the existing receipts

- **Hosted synthetic startup:** across 12 paired rounds, first-workflow
  median fell from 3.618 s to 2.0345 s. Bare Pyodide was 0.9405 s median
  (0.545–2.903 s), an empirical baseline. The synthetic timer includes
  request and response, engine initialization, and fixture execution. It
  uses an in-process gateway and excludes managed controller, launcher
  handshake and PostgreSQL. Stage medians from separate child samples
  cannot be subtracted to get import or boot costs in the managed run.
  [Cold-start study](COLD-START.md#hosted-results)
  · [Hosted synthetic receipt](../../results/cloudflare-cold-start-hosted.json)
- **Local admission overlap:** with a preconnected launcher, a simulated
  1 s API prep, and eight trials per arm, claim-to-result medians were
  4.170 s cold without admission boot, 3.1665 s cold with it, and 1.013 s
  on a warm repeat. The paired median saving was 1.012 s. This demonstrates
  overlap under controlled timing. It supplies no hosted managed claim,
  origin or provider wall times.
  [Admission experiment](COLD-START.md#admission-boot-and-short-linger)
- **Local meter qualification:** the network-disabled receipt with the
  hosted bundle hash has four-sample capacity prepare/start CPU values of
  about 8.7/2.7 ms, and a boot CPU value of about 5.7 s. These are local
  process CPU samples used to size reserves. They supply neither a hosted
  launcher wall bound nor a substitute for the synthetic hosted boot proxy.
  Its two workflow fetches took 91 and 70 ms after separate cold boots.
  The earlier local meter receipt shows 98 and 84 ms under the same
  cold-boot/warm-workflow pattern. These are child workflow wall times only.
  The two-session local spend fixture observed 323 ms from the activity
  upsert returning through commit for the winning booking. That makes
  metering worth timing without assigning 323 ms to any hosted booking.
  [Matching local package receipt](../../results/cloudflare-scaleout-meter-hosted-prep.json)
  · [Earlier local meter receipt](../../results/cloudflare-scaleout-meter-local.json)
  · [Usage qualification](../cloudflare-launcher/PHASE1-USAGE.md)

## What users should expect

For the first overflow on a fresh launcher with the primary held full,
**about 10 s to entry** is the observed reference from one run, not a
percentile or an upper bound. The new managed Workers launch-age gate is
**2 s** from the dispatch clock. The supervisor arms a one-shot wake for
the next compatible dispatch to reach that age. It uses the same event as
the existing workflow and capacity hints, with the same 2 s minimum pass
spacing and 5 s recovery sweep. An intervening pass can therefore add up
to roughly 2 s of spacing under healthy conditions.

**Budget about 7–9 s to entry** for a similar cold run on a healthy
controller. Replacing the observed run's 5 s age floor with 2 s gives
roughly 7.2 s if the rest of the path takes the same time, with up to
roughly 2 s of spacing allowance. This is a planning estimate. Contention,
admission limits, a missed wake, or a long pass can extend the wait.

A subsequent admissible workflow can use the same launcher and warm ordinal
within the **90 s linger**. It avoids the launch-age gate, launch/hello and
any exposed cold boot. As a planning expectation for a similar small
workflow with a prompt wake and an idle warm seat, allow **roughly 1–4 s
queue to entry** until a hosted repeat is measured. This is a broad
estimate based on the managed post-stamp budget and local overlap fixture,
not a hosted result. API validation, credentials, spend and the sent fence
still run. A missed wake, busy seat, fair-share constraint or a different
ordinal can increase the wait. Same launcher with a cold ordinal skips
launcher startup and still pays engine preparation. Linger is a readiness
window, not an isolate residency guarantee. Eviction, DO reset, retirement,
or a new incarnation can require reconfirmation or a fresh boot. There are
no idle engine pings during linger.
[Slot reuse and readiness](../cloudflare-launcher/PER-TENANT-SCRIPTS.md#engine-identity-and-readiness)

## Ranked reductions

1. **Implemented** — Separate the managed Workers launch age from
   reconciliation and set it to 2 s. Shorter patience can start a launcher
   for work the primary would have absorbed, adding creations and DO
   residency.
2. **Implemented** — Wake the supervisor as the next compatible dispatch
   crosses its age threshold. Each pass evaluates queue fit, primary
   saturation, installation admission and spend. Keep-warm and queue
   pre-warming remain off by default.
3. **Product change** — Measure the post-claim branches before shortening
   them. If API prep dominates, reuse a digest-keyed immutable workspace
   bundle or parallelize independent reads while rechecking the current
   approval, source pins, tenant and authority on every dispatch. If
   preparation arrives late, shorten the time to the fenced prepare frame
   and generation grant and compare against the current best-effort hint.
   If the pre-claim remainder proves launcher-heavy, target measured
   serial RPC, alarm or inventory delay. Preserve durable start acceptance,
   signed ownership, generation and sent fences, and spend checks.
   These options share evidence and offer no additive savings estimate yet.
4. **Product change** — Continue reducing the Python import and loader
   footprint if the engine remains exposed on the critical path. The
   earlier package work already removed about 1.58 s between hosted
   medians. The bare-to-candidate median gap is about 1.1 s across two
   different probes, useful as scale rather than a savings forecast.
   Preserve package availability and SDK behavior. Confirm with paired
   cold and warm managed samples before trading footprint for complexity.
5. **Config/policy · needs owner decision** — Keep
   `workers.queue_prewarm_per_tenant` at **0 by default**. Consider
   opt-in **1** for recurring same-tenant bursts that need another ordinal.
   It stages an extra prepare when a managed launcher already claims a
   workflow, ahead of a later compatible claim. It cannot remove first
   overflow detection or first launcher startup. Per-launcher cold
   boots are serialized, so a speculative boot can delay an admitted one.
   Compare queue wait and admitted-boot tails with pre-warming off and on
   before expanding. Keep-warm traffic is opt-in at most and is outside
   the baseline. Prefer passive reuse in the existing 90 s window.

The trial's Bifrost estimate was about **$0.007 settled** for the run through
retirement (roughly **$0.012 live** after the cold attempt). It combines
workflow and lifecycle costs. At the rates archived in the study, a new
billable daily loader identity costs about **$0.002** plus CPU, requests and
TenantHost duration. Budget **a few tenths of a cent per extra cold slot** and
**roughly a cent for three extras** before reuse. Unused prepares can incur
those costs. Included usage and actual residency affect the bill. Hosted
Analytics returned no attributed samples for the combined run, so there
is no provider cost or reconciliation ratio for this workflow.
[Bifrost meter readback and pricing model](../cloudflare-launcher/PHASE1-USAGE.md)

## Next hosted receipt

Read `executions.cloudflare_timing` for `queued_at`, `gate_eligible_at`,
`claim_at`, `prepare_sent_at`, `dispatch_sent_at`, `engine_ready_at`, and
`entry_at`. Join via the existing workflow claim to
`cloudflare_worker_reservations.launch_timing` for `controller_decision_at`,
`launcher_reserved_at`, `launcher_start_sent_at`,
`launcher_start_accepted_at`, and `session_ready_at`. The existing
`execution_workers.ready_at` records initial owner admission after hello.
The launcher timestamps describe the incarnation's initial launch and
should be attributed to the cold sample only when it uses the new
incarnation. A ready launcher can claim before the launch-age threshold.

These are UTC observations. Queue time comes from the dispatch row.
Reservation and claim stamps are inside their transactions. The prepare
and dispatch stamps record completion of the API socket sends. Engine
readiness records the TenantHost's confirmed ping for the generation it
selected, which can predate a warm run. Entry records the engine's
invocation of the loaded workflow and is returned with the terminal
receipt. Rebuild the Workers runtime from the updated engine sources to
collect it. Missing keys indicate a stage that did not occur or report a
timestamp, for example when a prepare hint was skipped, a terminal
receipt was lost, or an older bundle ran the workflow. Keep the fixture's
entry log as an independent observation.

Repair the measurement harness to use durable dispatch timing even if the
detail API omits `created_at`. Measure paired runs under the same primary
hold, first with a fresh launcher and then with a verified same-ordinal
repeat after ACK and release, inside linger. Add a ready-launcher/cold-slot
case to separate launcher from engine cost. Sweep submission phase against
the actual controller passes. Keep the baseline pre-warm setting at zero.
Collect several pairs and preserve timeouts, skipped hints, retries and
evictions alongside successes.

| Receipt group | Sanitized timing fields |
| --- | --- |
| Queue and decision | `dispatch_created_at`, `queue_commit_at`, `execution_created_at`, `wake_received_at`, `eligible_pass_at`, `queue_age_at_decision_ms`, `reservation_committed_at`. Include wake or sweep, primary fit and eligibility outcomes, configured spacing and age threshold, and time waiting for planner/admission locks. |
| Launcher startup | `capabilities_ms`, `observe_ms`, `prepare_rpc_ms`, `provider_bind_committed_at`, `start_rpc_ms`, `start_accepted_to_alarm_ms`, `alarm_to_upgrade_ms`, `hello_received_at`, `owner_lease_acquired_at`, `welcome_sent_at`, `inventory_reconciled_at`, `claim_gate_open_at`. Include connection attempt count and coarse result. |
| Claim and API branch | `claim_begin_at`, `claim_commit_at`, `api_running_row_at`, `credentials_ms`, `workspace_validation_ms`, `context_persist_ms`, `usage_and_sent_fence_commit_at`, `workflow_frames_sent_at`, `launcher_workflow_received_at`. Keep the existing consumer milestones and split send from ACK. |
| Prepare and engine branch | `prepare_scheduled_at`, `prepare_booking_ms`, `prepare_sent_at`, `launcher_prepare_received_at`, `warm_queue_wait_ms`, `generation_grant_rtt_ms`, `loader_first_fetch_to_ready_ms`, `prepare_observed_at`. Include sent/skipped/joined/reconfirmed, child first-request/cold bit, and same-launcher/ordinal/generation booleans to establish reuse. Export the existing TenantHost `child_ms` over the boot/prime RPC. Capture origin probe duration separately. |
| Entry and settlement | `tenant_run_begin_to_child_fetch_ms`, `child_fetch_to_workflow_entry_ms`, `workflow_entry_at`, `completed_at`, `terminal_ack_at`, `released_at`. Derive total queue-to-entry and the boot time exposed after dispatch is ready. |

Use elapsed milliseconds from a monotonic clock where available, or label
the local clock source and resolution. Keep absolute UTC anchors for
durable events and identify their clock domains. Join the API and Worker
branches with a receipt-local sample label and paired send/receive markers.
Report cross-host deltas only with a clock uncertainty bound. Use null
with a reason for an unavailable span. Record bundle provenance, ordinal,
policy values and outcome enums. Omit internal identifiers, URLs, identity
material, payloads and raw logs. Keep metered CPU, request and residency
counts alongside wall time so the receipt can show overlap without
counting concurrent spans twice.
