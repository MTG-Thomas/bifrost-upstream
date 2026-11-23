# Phase 3 — Containers Overflow

Design proposal for [Scale Out With Cloudflare](SCALE-OUT.md), based on spike
`d3533d6d1`. The first release adds an independently qualified, opt-in Linux
overflow scope to the existing worker pool. It uses ordinary Workers, SQLite
Durable Objects and Cloudflare Containers. The primary continues to serve all
work it serves today. Phase 3 introduces no Workers for Platforms dependency.

## Starting evidence and integration points

| Source | Evidence and implication |
| --- | --- |
| [Phase 0 provisioning](PHASE0-PROVISIONING.md) | A disposable fixture copied a Linux/amd64 image without a Docker daemon, verified the destination digest, prepared it, created a namespace-bound application with `durable_object` scheduling, ran one `lite` instance, refused a second reservation, and verified zero live instances before deleting the app. Registry listing and tag deletion used the pinned `cf` adapter. The product release image, parallel create races and the full Linux lifecycle still need qualification. |
| [Linux workflow path and hosted Phase 7](DESIGN.md#linux-workflow-path) | Phase 7 ran a signed Linux workflow with a non-root subprocess, denied access to the supervisor environment and observed requirements readiness and inactive readback after explicit idle release. Reservation to ready was 4.52 s and enqueue to start was 44.10 s in that trial. The contemporaneous Workers run failed before engine entry. The separate expiry attempt had a healthy application and no allocation. Reaching an application, running Linux, overlapping three successful lanes and observing natural expiry are separate witnesses. |
| [Linux startup candidate](../../results/startup-linux/v1.json) and [local three-scope harness](../e2e/README.md#three-scope-containers-overflow) | The local signed startup candidate demonstrated one configure and workflow command, actual Linux fork, an independently checked result, terminal ACK, revoked runtime access and template retirement. The local harness exercises Linux package/bootstrap and the existing Linux launcher. Treat these as protocol and process evidence. Measure cold image start, template launch, package generation, first execution and stop separately on the release image. |
| [Phase 1 slots](PER-TENANT-SCRIPTS.md), [usage](PHASE1-USAGE.md) and [pilot](PHASE1-PILOT.md) | Reuse the installation lock, original-claim terminal replay, usage reservations and independent readback. Hosted usage samples are incomplete. The combined Workers slot trials stopped at ingress TLS and do not qualify the merged artifact. |
| Phase 2 Managed Tunnel, Phase 4 drain/upgrade and queue pre-warming | Reviewed `t3/managed-tunnel-phase2` at `976d895a0`, `t3/cloudflare-drain-upgrade-repair` at `c13c306c6` and `t3/cloudflare-queue-prewarming` at `3f236bf8b`. Integrate on their merged schema and interfaces. Reuse Phase 2's connector and canary, Phase 4's pinned reconcile and drain checkpoint, and the pre-warming branch's shared claim/preparation accounting. |

## Release, registry and provisioning

Use the existing Linux API-only launch path and its unprivileged workflow
template as the image recipe. Package the Linux/amd64 child manifest and OCI
layers in a signed release artifact accessible to the trusted runner. Pin
base image, dependencies and platform. Record source revision, immutable OCI
manifest digest, signature and provenance, approved instance types, startup
entrypoint, control/SDK protocol versions, ingress contract, meter bounds and
provider module digests in the matched release descriptor. Build and sign once
in release CI. Verify signer, source and digest before reading registry
credentials. The API image records its release repository, workflow and
tag as one exact signer identity at build time. That identity determines the
asset URL and the Sigstore verification policy. A temporary asset or trust
metadata fetch failure retries through the existing PlatformJob policy and
preserves previously verified admission. A bad signature or mismatched
release closes joint admission. Keep builds, pushes and signing authority
outside Cloudflare.

Extend `cloudflare_release` and the existing provisioning adapter. Use the
qualified daemonless OCI client with a pinned version and short-lived
Cloudflare registry credentials acquired by the trusted runner. Copy by
manifest digest into an installation-owned repository and revision tag.
Verify destination digest and platform, then call image preparation and wait
for the exact digest to reach ready. The tag is an inventory and cleanup
handle. The start configuration always resolves to the prepared digest.
Inventory the source and destination digest, repository, tag and preparation
receipt. Journal the effect before the first push. A collision or changed
digest closes provisioning. After an ambiguous push, inspect that exact tag
before another write. Use the pinned `cf` path established in Phase 0 for
registry tag list and delete. Retire only digest-matched owned tags after
all app and deployment references are gone. Leave shared registry
configuration and content referenced by other tags alone. Report tag
absence separately from provider-internal blob or image-cache reclamation.
[Image management](https://developers.cloudflare.com/containers/guides/image-management/) ·
[Registry credentials API](https://developers.cloudflare.com/api/resources/containers/subresources/registries/) ·
[Image preparation API](https://developers.cloudflare.com/api/resources/containers/subresources/images/methods/prepare/)

Deploy the matched capacity Worker with SQLite registry and launcher classes,
its distinct scope bearer, and the named digest in the multipart image map.
Resolve the exact launcher namespace and explicitly create the associated
application with `scheduling_policy: "durable_object"`. Preserve its opaque
application identifier verbatim. Verify ownership, namespace association and
digest mapping before enabling starts. The owning DO passes its qualified
`ctx.container.images` reference and named `instance` to
`ctx.container.start()`. The DO policy is a hosted qualification dependency.
Keep the scope closed if the supported API, bindings or permissions fail.
[Application create API](https://developers.cloudflare.com/api/resources/containers/subresources/applications/methods/create/) ·
[Scheduling policies](https://developers.cloudflare.com/containers/configuration/scheduling-policy/) ·
[Durable Object Container API](https://developers.cloudflare.com/containers/api/durable-object-container/)

## Policy, inventory and operator surfaces

Extend the existing installation policy and revision inventory, with
Containers disabled for existing installations until explicit enablement.
Persist desired scope enablement, installation and tenant instance caps,
workflow slots per instance, a release-qualified size, idle and absolute
lifetime bounds, queue wait/depth and cooldown, and egress limits. Ship
`standard-1` (0.5 vCPU, 4 GiB, 8 GB), two live instances installation-wide,
one workflow per instance, one instance per tenant, and a two-minute idle
stop as the pilot policy defaults. Provider and Global can each hold one
instance, subject to the shared admission and spend gates. Reuse the
existing primary, aggregate worker cap, shared cloud slot caps, reachability
choice and monthly target. Size and digest are pinned by the admitted
release and policy revision. Check image plus scratch and package footprint,
memory headroom for supervisor/template/child, and account entitlement and
resource limits. Verify image and runtime fit on `standard-1` before hosted
qualification.
Current published account defaults include 60 TiB concurrent memory,
15,000 concurrent vCPU, 300 TB concurrent disk and 50 GB of image storage.
These are ceilings shared with other workloads. They provide no allocation
guarantee. Default placement applies without location hints, regional
constraints or Smart Placement. Under this scheduling policy the Container
runs where its DO runs. Keep residency-bound work on the primary.
[Limits and instance types](https://developers.cloudflare.com/containers/platform/limits/) ·
[Placement](https://developers.cloudflare.com/containers/concepts/placement/)

Reuse the resource journal keyed by installation, account, scope, kind,
exact target and effect. Add owned image/tag, preparation and application
records, and attach runtime identity and observations to existing worker
reservations. Persist desired and deployed revision, artifact and policy
pins, opaque app/namespace/instance identities, expected UID, start
deadline, credential version, verified process time boundaries and last
independent observation. Distinguish reserved, starting, ready, busy,
draining, refused, unconfirmed and retired. Send only redacted status and
remedies on the existing Capacity surface. Include scope-local capacity
unavailable/cooldown, remaining reserved exposure and unresolved cleanup.
Read entitlement before enable and honor the scoped Containers management
grant. A replacement management credential must prove authority for exact
owned Container resources as well as applicable Workers and Phase 2
resources before switching. Qualify a supported, ownership-checked
write-authority probe for the app and registry. Keep the incumbent on an
unverified replacement.

Extend the existing capacity GET/PATCH and lifecycle actions,
`bifrost capacity cloudflare`, and thin REST-bridge MCP tools from shared
DTOs. Use the registered `capacity.cloudflare_enable`,
`capacity.cloudflare_reconcile` and `capacity.cloudflare_disable`
PlatformJobs, their installation resource protection, and shared status and
notification transports. Payloads carry revision and identifiers. Execution
continues through the workflow worker path. Gate lifecycle operations on
the existing Global capacity permission and workflow approval on the
applicable operation and persisted workload scope. Provider organization
membership grants no authority. Keep credentials and infrastructure
inventory installation-local and outside portable manifests.
[Platform jobs](../../../../docs/architecture/platform-jobs.md) ·
[Repository authorization](../../../../api/src/repositories/README.md)

## Placement, slots and parallel starts

1. Reuse the healthy primary's normal packing and provisioning. For VM or
   Kubernetes require a reconciled effective ceiling with no fitting
   headroom. For in-process use healthy registered static slots and memory
   fit. Hold new overflow on unknown primary health. Require compatible
   backlog to persist past the configured thresholds. The primary can win
   when it regains fit. Keep existing Fast Path, privileged jobs, agents,
   chat, services, and isolated builds on their existing paths.
2. Use one prospective placement predicate in the planner, claim and final
   send fence. For approved Normal workflows that fit Pyodide, choose
   Workers and prefer a ready engine. A Workers shortage, expired approval,
   analysis contention or Workers-specific limit such as `CF_CPU` or
   `CF_SUBREQUESTS` grants no Linux approval. `CF_NATIVE` and
   `CF_PROCESS` identify Linux candidates. `CF_PACKAGE` qualifies as a
   Linux reason only after exact approved Linux/amd64 package and dependency
   pins resolve to wheels baked into the signed release image. Approved
   loose and Solution-installed Normal workflows can use this lane.
   Other incompatible or unresolved findings, including `CF_URL_REVIEW`,
   go to the primary.
   Require a current Linux-specific reviewed digest binding source, import
   and package closure, organization, install generation, the exact
   worker-ingress-only egress envelope, and resource/deadline limits.
   Reuse the existing approval/reapproval workflow and recheck the digest
   and consent at send.
3. Plan Containers from eligible backlog still uncovered by the primary.
   Bind demand to the selected incarnation for a bounded placement window
   using the existing queue/claim fences. Start with unassigned queued work.
   Claim only after authenticated Linux readiness. Allow the primary to
   claim once an unstarted placement expires or is explicitly returned.
   Prefer ready compatible capacity and atomically recheck org, release,
   cost, occupancy and runtime capability before claiming or sending.
4. Reuse the installation-wide tenant slot ticket, claim fence, sent marker,
   fairness rule and release machinery in `cloudflare_slots`. Route from
   persisted execution organization, including the existing Global tenant
   key. One Container exposes a single physical seat and stays pinned to a
   tenant for the incarnation, including during linger. Reuse it only for
   that tenant after the old child, runtime credentials and per-run state
   are gone. If cleanup cannot be attested, retire the instance. If older
   eligible demand from another tenant cannot fit because an idle instance
   holds the physical cap, shorten that idle linger and wait for verified
   stop before allocating a fresh incarnation. The default
   Workers online engine layout and linger remain Workers-specific.
   Count occupied Container seats and Workers pre-warm reservations together
   against tenant and installation limits. Count a starting Container's
   promised seat as preparation, bound to eligible same-tenant demand.
   Exchange it for the dispatch ticket under the same lock. A prepared
   Workers engine is adoptable only by its owning Workers launcher.
   Apply the existing fair last-seat rule across both lanes. Container
   launches remain demand-driven. The queue-depth knob continues to
   authorize only extra Workers engines.
5. Before network effects, serialize **reserve** under the existing pool and
   installation fences using the established lock order. In the transaction
   allocate unique worker and resource UID, pin tenant/scope/revision,
   reserve spend and enforce shared worker, installation-wide Container
   instance and per-tenant instance limits. On Cloudflare, one persistent
   SQLite ScopeRegistry per scope atomically reserves against its pinned
   installation and tenant physical caps. Its transaction
   counts prepared, starting, running, stopping and unconfirmed rows.
   The owning instance DO records one start intent for that UID before the
   start effect. Identical replay observes the same reservation, while a
   changed UID, digest, size or credential binding conflicts. Preserve the
   registry reservation through lost replies, DO reset, controller takeover
   and stop uncertainty. Re-observe exact state before another mutation.
   The DO-managed app has no `max_instances` ceiling. A shared-cloud
   failure closes peer starts only when shared headroom or spend cannot be
   proven. Both provider and API caps must hold at the same time.
   [Scheduling policies](https://developers.cloudflare.com/containers/configuration/scheduling-policy/)

## Bootstrap, network and process lifetime

1. Reuse the managed protocol-2 prepare request. Bifrost derives one key
   under the pinned derivation-root version for installation, Container
   scope, worker identity and resource UID. Transfer it over the
   authenticated scope channel. Identical prepare replay carries the same key. The
   scope bearer is an independent provider Worker secret. No Container or
   Worker receives the derivation root, `BIFROST_SECRET_KEY`, primary
   worker capacity token, management token, tunnel connector token or
   database credentials. Deliver the derived key and release/identity pins
   to the Container supervisor in start-time environment. Bind the short
   lived prepare payload to the UID and purge key bytes when the effect
   settles. Keep a digest for idempotency and exclude key bytes from
   inventory/readback. Same-incarnation supervisor reconnect uses its
   in-memory key. A stopped process requires a fresh incarnation.
   [Start environment](https://developers.cloudflare.com/containers/configuration/environment-variables/)
2. The supervisor dials the approved dedicated worker-ingress origin over
   TLS and opens the existing signed control WebSocket with replay ordering.
   Phase 2 owns Managed Tunnel DNS, connector and ingress. Existing HTTPS
   uses the same API ingress contract. Neither path exposes an inbound
   public Container port. Qualify the proposed exact-origin outbound
   handler with `enableInternet: false`, matched HTTPS interception and
   Cloudflare CA trust. The handler validates the destination and Host,
   pins the upstream origin, accepts only the versioned route/method and
   WebSocket matrix, forbids redirects and meters forwarded bytes. The
   WebSocket relay must account for frames and close the stream on its
   reserved byte or time limit. Prove
   the upgraded stream, byte accounting, reconnect and denial of raw TCP,
   alternate origins and generic API routes on the hosted release. Treat
   intercepted WebSocket forwarding as an explicit qualification gate.
   The earlier direct Internet-enabled Linux trial did not qualify this
   network boundary. Keep admission closed if the bounded path fails.
   [Outbound traffic](https://developers.cloudflare.com/containers/configuration/outbound-traffic/)
3. Admit only after signed hello matches the exact live reservation, image
   and negotiated `linux-process-v1` Normal workflow capability. Require
   package-generation heartbeat and template ready, then a no-effect
   scoped SDK canary before opening the scope. Reuse the Linux explicit
   configure/bootstrap content, fragmenting control frames as needed.
   Scope module resolution and source bytes to the live run, caller and
   approved digest. Resolve approved packages from pinned Linux wheels
   baked into the signed image. Disable runtime package downloads and
   refuse readiness if required wheels are missing or mismatched.
   Children receive only their scoped run capability and pinned
   inputs. Strip inherited secrets and supervisor authority, run non-root,
   verify dump protection and absence of ptrace capability, and require
   a kernel boundary that blocks guest access to the supervisor and its
   signals, such as separate unprivileged identities set by the trusted
   image init before privilege drop. Qualify that boundary and cross-attempt
   scratch isolation as part of the release image. A child may use only
   the qualified SDK contract through the exact worker ingress. Initial
   qualification uses synthetic fixtures with baked wheels.
4. `start()` returning proves acceptance of options. Use bounded monitor
   and exact management instance observation to distinguish pending,
   refused and allocated. Publish ready only on signed application
   bootstrap. On a definitive allocation refusal and verified no live or
   pending start, release capacity, settle the spend reservation, remove
   the unstarted placement and let the primary claim. Back off that
   scope persistently with a bounded cooldown and a single demand-driven
   probe afterward. An unknown start retains capacity and cost. A proven
   unsent workflow can return to primary once the send fence is closed.
   Preserve the original claim and existing uncertainty semantics for a
   possibly sent attempt. Provider health or a control timeout alone
   cannot prove a refusal or a stopped process.
5. Cancel via existing attempt controls. Before send, release the ticket and
   revoke its claim. After send, revoke the run capability, stop the
   workflow process group and reap the child and descendants. Verify stop
   separately from the durable result/cancel and terminal ACK. Test detached
   descendants as well as group members. If the launcher cannot prove all
   descendants exited, drain and stop the whole one-seat Container. Stop
   failure keeps the seat charged and occupied. ACK replay and liveness
   use the original identity, including during connector or API interruption.
6. After two minutes idle by default, stop new claims, seal terminal and
   usage evidence, then request graceful shutdown. Use bounded
   signal/destroy when needed. Bifrost
   provides a renewable runtime lease and an absolute deadline. The
   supervisor exits when authority expires, and the provider enforces a
   one-shot hard-stop deadline. Arm the provider inactivity backup after
   start and on DO reinitialization for a still-running instance. Its
   deadline follows both the absolute alarm and the full local idle and
   drain window. The launcher owns graceful idle stop.
   Completion of a bounded startup/exit monitor, lack of traffic and
   reset have distinct meanings. At idle, stop DO pings and long-lived
   monitoring that holds the DO active. Observe provider instances via
   REST and journal-backed reads that do not wake the owning DO. Qualify
   natural expiry in an isolated mode that permits the inactivity
   deadline to win before the explicit cleanup backstop. Never autostart
   on GET, alarm recovery, reconnect or expired UID. Restore capacity
   only after exact inactive or absent provider evidence plus the closed
   start fence, with no pending creating effect. A receipt pairs those
   facts with the resource UID and, where observed, stop time.
   [Container lifecycle](https://developers.cloudflare.com/containers/concepts/architecture/) ·
   [Monitor, destroy and inactivity timeout](https://developers.cloudflare.com/containers/api/durable-object-container/)

## Spend enforcement and estimate

Extend `cloudflare_spend` with versioned per-type instance-seconds and
outbound bytes. Keep meter keys idempotent by account, installation,
revision, scope, UID and charge interval/effect. Reserve in the same
transaction as admission before permitting a start. Cover start-to-stop,
boot, idle, deadline and cleanup margin, plus worst-case allowed egress
and measured control Worker/DO/log/storage allowances. Size against the
maximum configured fleet and time/egress envelope at policy save. Price
both scopes in the same monthly ledger. Admission holds when the estimate
reaches the target, available balance cannot cover the next reservation,
or history is incomplete. Runtime deadline, instance cap, byte cap,
bounded requests/logs and fail-closed ledger backstop the estimate. Apply
the existing cleanup allowance to retirement. Alert on accrued cleanup
exposure. Provider spending notifications are advisory.

Use gross rates for estimates, without subtracting shared included usage.
Charge elapsed live time at provisioned memory and disk and conservatively
at provisioned vCPU until calibrated active CPU evidence is trustworthy.
Settlement uses independently witnessed time bounds and measured egress
where complete. Bound an unsettled billing reserve at the absolute deadline
formed by the start deadline, maximum lifetime and cleanup margin. An unresolved
stop keeps its seat occupied after the billing horizon. Track scoped source reads and
outbound ACK, heartbeat, log, SDK and response bytes separately. Bound both
directions through the relay and reserve for overhead and failed or retried
transfers. Price egress at the maximum published regional rate until a
qualified attribution supports another rate. Pin rate and envelope version
by release, split intervals across UTC months and preserve commitments
through retirement, rollover and same-account reconnect. Use exact-scoped
Analytics samples for comparison only. Report the `$5/month` Workers Paid
base separately. The ledger enforces the Bifrost admission target. Provider
billing, shared account traffic and delayed cleanup can raise actual cost.
[Container pricing](https://developers.cloudflare.com/containers/platform/pricing/) ·
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)

At published gross rates, `standard-1` costs approximately
`4 × $0.0000025 + 0.5 × $0.000020 + 8 × $0.00000007 = $0.00002056`
per second at full CPU. That is about $0.074 per instance-hour and $0.0025
per instance for a full two-minute idle window, before egress and control.
The two-instance pilot has a 30-day always-live compute envelope of
**$106.58** (5,184,000 instance-seconds). Forecast gross monthly usage as
`$106.58 × average billable duty cycle + $0.05 × outbound GB + control costs`.
Billable duty cycle includes boot, execution, idle and stop time relative
to both instances running continuously. For illustration, 10% yields
$10.66 of compute and 25% yields $26.65. Add the measured control budget and
bounded ingress-route egress, and show the $5 Workers Paid fee separately.
The monthly target and default forecast duty cycle remain open. Enforce
the reserved ceiling on each start.

## Joint drain, replacement and teardown

Extend Phase 4's **one** installation revision barrier. The desired release
pins both scope bundles, image, ingress contract and meter bounds. A
Containers-only image change triggers the same barrier when both scopes are
enabled. Under the installation lock close both lanes to new reservations,
pre-warms, claims and sends. Fence already queued Enable/prepare work and
record each retiring deployed revision and its full inventory before
releasing the lock. Keep the preceding control and ingress contracts,
provider bearers and connector usable for bounded drain. Require verified
Scale In on the old build before rolling an API that cannot serve its wire.
Primary placement continues during the cloud pause.

For each retiring deployment, await original claims, terminal ACK/replay,
preparation expiry and physical stop. Persist separate stop and deletion
witnesses in the checkpoint per lane. Close every unresolved capacity effect.
For Containers, require exact application instance readback with zero live
or pending processes while the application still exists, then verify app
deletion independently of the Worker and namespace. For Workers, require
the Phase 4 stop/settlement and exact retired script and namespace readback.
An application tombstone cannot substitute for documented process
retirement in the normal replacement path. If a resource is already absent
and prior stop evidence is missing, hold the checkpoint until independent
authoritative evidence covers the former instance and any late start.
Keep the open checkpoint when readback is ambiguous or a credential lacks
authority. Freeze the matched replacement through repair. Re-read journaled
effects and retained witnesses after ownership loss. Use fresh same-account
authorization when required.

After both lanes retire, remove old references and retire old owned registry
tags. Complete the common retirement checkpoint **before staging either
replacement**. Stage both scopes on the pinned revision, recompute the
combined spend envelope, and verify each enabled scope's exact resources
and signed no-effect canary through the retained ingress. Commit one admitted
revision only when all selected scopes pass. Failed readiness keeps both
lanes closed, with compatible work served by the primary. Repair resumes
the same pinned joint revision. Scope-local status remains visible in
steady state. The first release has no independent per-lane repair.

Scale In drains both lanes to zero compute and retains provisioned resources,
meter history and ingress. Require verified stop before reporting completion.
Disconnect extends the same drain to delete all owned resources. Disable
start triggers and remove public bindings after drain. Verify zero live or
pending instances, delete the exact app, remove dependent bindings and
owned scripts, then verify retired DO namespaces independently. Delete only
digest-matched image tags with no remaining application/deployment
references. Retire Phase 2 DNS and connector/tunnel resources in the
established order, verifying zero sessions, exact DNS absence and tunnel
absence or tombstone. Retain credentials and restartable cleanup inventory
until verified absence. Records retain sanitized ownership, timing and
usage evidence after credential purge. A denied, stale, malformed or
incomplete list is unconfirmed. Only authenticated exact readbacks and
closed effects complete teardown.
[Containers REST resources](https://developers.cloudflare.com/api/resources/containers/)

## Verification and hosted qualification

| Layer | Acceptance |
| --- | --- |
| Unit | Exercise artifact signature/digest/platform mismatch, named prepared image and app ownership, exact opaque IDs, provider/token/revision fences, ambiguous copy/create and digest collision, stale policy/approval and permission checks, all primary saturation predicates, combined slot and spend math, UTC rollover, unconfirmed charges, cooldown and exact absence. Race independent API claimers for the last installation instance and for the sole tenant instance and verify Workers pre-warms participate. |
| Miniflare with Docker | Use the pinned Worker toolchain, real Linux release recipe and conformance harness. Prove protocol-2 prepare, same-UID duplicates and cap under concurrent distinct creates, reset during a pending start, signed bootstrap/refresh, correct bytes/fragmentation, scoped source, baked-wheel resolution and blocked package downloads, successful native import and subprocess, denial of supervisor and prohibited network access, cancel with descendants, ACK replay, clean idle exit and stop timeout. Inject scheduler and inactivity transitions at the provider boundary for deterministic coverage. Local Docker lifecycle does not establish hosted scheduling, network or provider expiry behavior. [Cloudflare local development](https://developers.cloudflare.com/containers/guides/local-dev/) |
| API e2e with stubs | Use isolated repository Docker tests and the real PostgreSQL installation/effect, queue, approval and meter paths. Hold a healthy primary and overlap supported Workers and Linux jobs through scoped fixtures. Check eligible and excluded findings, parallel claims/preparations, one Container per tenant and two per installation, exhausted shared budget, injected allocation refusal and uncertain send, runner loss, both ingress modes, joint drain and failed canary. Prove that neither upload happens while either lane lacks retirement witnesses. Exercise credential denial/replacement, late effects and disconnect absence. Verify UI shared job notifications and regenerated REST/CLI/MCP parity, OpenAPI, DTO flags, CLI contract and operation catalog. |

Hosted qualification needs explicit approval of the disposable account lane,
installation, grants, time window and spend bounds, with an independent
readback operator. Use the exact signed release, a held primary, synthetic
no-effect workflows and no live model or production credentials. Run
preflight for entitlement, registry room, scoped grants, supported scheduling,
ingress TLS, baked-wheel inventory, scoped source access and cleanup
authority. Stop on unexpected ownership, cap, identity, ingress or
teardown evidence. Do not
blindly retry allocation. Reserve cleanup time before starting an instance.

1. Prove release OCI copy/preparation, namespace/app join, successful
   signed Linux canary and negative routes over both supported ingress
   choices. Record startup stages, type, digest, process readiness,
   origin RTT and byte counters. Verify child environment, outbound and
   process restrictions.
2. With the primary held, overlap successful Workers and Linux execution.
   Hold each fixture at a deterministic barrier so all three active
   claims overlap. Verify only accepted Linux findings enter Containers,
   exercise scoped source and baked native wheels for approved loose and
   Solution-installed workflows, then correlate results and terminal ACKs.
   Verify package downloads and egress outside the exact ingress fail.
3. Race distinct creates through parallel controllers, including same-UID
   replay and a lost reply. With eligible Provider and Global work, require
   one instance each, refusal of another instance for either tenant, and
   refusal of a third concurrent instance installation-wide. Verify
   independently through application-wide instance readback.
   Exercise post-send cancellation and exact process retirement.
4. Prove explicit idle stop and natural expiry on separate fresh
   incarnations. Keep a workflow busy past the local idle window and
   verify that the provider inactivity timeout does not interrupt it.
   For expiry require prior running and signed ready,
   quiesce the owning DO and the supervisor's activity, and observe
   inactive with no destroy, alarm-driven stop or supervisor deadline
   beforehand. An allocation failure cannot pass this check.
5. Prove allocation-refusal handback on the hosted product path. Use a
   deterministic, disposable-only refusal injection at the scheduling
   adapter to prove cooldown, primary claim and exactly one execution.
   A passing hosted injection satisfies the refusal gate for admission.
   Record genuine provider refusal as pending until observed in a later
   approved run. Watch for a healthy application with a genuine refusal,
   exact-identity confirmation of no pending or live instance and the
   same handback evidence. Avoid stressing the account to force scarcity.
6. With active work in both cloud lanes, interrupt control during upgrade
   and prove original terminal replay and both physical retirements before
   either replacement is staged. Check failed readiness leaves both
   closed. Exercise same-account token replacement after a journaled effect
   and complete one admitted generation after readback.
7. Disconnect and independently verify all owned classes, including
   application and registry tag absence, namespaces, connector, DNS and
   tunnel where used. Preserve meter, effect and cleanup receipts and
   revoke the temporary management token after final absence. Record
   sanitized evidence and separate each passed criterion from pending
   evidence. No hosted criterion is satisfied by an emulator or stub.

Proposed approval envelope for the combined trial is 60 minutes, including
at least 20 minutes reserved for cleanup, with up to two live
`standard-1` Containers, 7,200 aggregate instance-seconds including
cleanup exposure, and 1 GB outbound through the dedicated worker ingress.
Budget roughly $0.30 for Workers, DO, logs and residue by doubling the
one-instance allowance based on the small Phase 1 trial. Verify packaged
rates and invocation bounds before approval. Estimated gross incremental
usage at that envelope is about **$0.50**, including $0.148 compute,
$0.05 egress and $0.30 control.
Propose a **$1 incremental abort threshold**, separate from the $5 monthly
base. Preflight the existing monthly policy validator and combined minimum
fleet envelope independently from the run abort threshold. If the package
cannot admit this bounded exercise under the approved policy, revise the
trial proposal before use. If entitlement, allocation, network qualification
or time is unavailable, capture the partial outcome and finish teardown.

## Implementation sequence and effort

Estimated **3–4 engineer-weeks** after the three dependent branches converge,
excluding owner approvals, provider availability and any Cloudflare API
blocker. The first commit is this design. Proposed implementation commits
carry their focused tests and keep Containers gated until release qualification.

1. Release manifest and Linux image build/signing, OCI runner and exact
   registry/application inventory adapters (3–4 days).
2. Policy migration, joint budget/slot admission, Linux eligibility and
   placement, capacity registry and concurrent reservation tests (4–5 days).
3. Signed Linux bootstrap, bounded egress, source/package handling, stop,
   cancellation, expiry and Docker conformance (4–5 days).
4. Joint Phase 4 retirement and teardown, generated REST/CLI/MCP/UI
   contracts, and API e2e on provider stubs (3–4 days).
5. Run owner-approved hosted qualification, record evidence, and enable only
   the independently verified release envelope (2 days active work).

The seams staged in steps 1–2 have these first callers:

| Seam | First production use |
| --- | --- |
| Release CI Linux image, baked wheel manifest and signature | Step 1 release packaging. Make the GHCR `…-cloudflare-linux` package public so the trusted runner can check the source anonymously before obtaining Cloudflare registry credentials. |
| `verify_containers_release`, `ContainerRegistry.copy` and `ContainersRest.create_application` | Step 4 joint provisioning verifies the pinned release, copies the owned revision tag and inventories the application. |
| `ScopeRegistry.reserve` | Step 3 authenticated capacity controller reserves before it requests an instance start. |
| `reserve_container`, Linux review/source predicates and joint slot and spend accounting | Step 3 admission and bootstrap. Use the execution tenant for capacity and the workflow owner for approval and source scope. |

Step 3 must ship verified stop and settlement before adding any production
caller of `reserve_container`. A start requires independent evidence that
the exact worker ingress is allowed and every other egress destination is
denied. Pass the release-bound `VerifiedEgress` from the qualification
probe to the reservation seam. Provider readiness and successful image
preparation alone cannot satisfy that precondition.

The scope controller must persist a start fence before requesting a start
and supply its closure to settlement. Pending effects keep their seats
across controller resets. The control relay charges each fragment in the
original direction against the incarnation's durable byte budget. Before
using those bytes for settlement, charge the scoped HTTP paths to the same
budget and verify that the scope has fenced all relays. The closure must
explicitly attest complete metering before settlement can reduce the fixed
hold. Pair the closure with a singleton management inventory result for
the fenced actor and exact placement start. Read complete
[`instances-v2` inventory](https://developers.cloudflare.com/api/resources/containers/subresources/applications/subresources/instances/methods/list/)
and match the exact actor ID locally. The provider prefix filter can hide a
named placement from an actor-ID search. Application health counts can veto
deletion when the provider reports activity or an incomplete sweep. Verify
inventory behavior during hosted qualification. Missing or ambiguous evidence
keeps the seat unresolved. The controller
acknowledges the exact closed fence after the stop transaction commits.
Only that acknowledgement retires the provider seat.
For an expired reservation without a start fence, the registry first
records a permanent void tombstone. This also covers a lost prepare.
Bifrost then requires empty inventory for the fenced actor before it
settles zero instance runtime and leaves the fixed control hold intact.
A recorded start fence without an observed placement can close after a
60-second boot observation window following the start deadline. The
controller must obtain exact empty inventory and serialize closure with
the instance actor. Bifrost independently repeats the inventory check
before settlement and retains the full reserved charge. Hosted
qualification must establish the observation window.
A witnessed absent ordinary launch drops its placement pointer. The
original queued dispatch can be claimed by the primary without creating
a second dispatch. The scope waits one minute before permitting one
demand-driven placement. Further launches wait for its signed enrollment
or a new settlement. The pilot applies the cooldown installation-wide.
The receipt does not distinguish the one-instance tenant limit from the
two-instance installation limit. Either refusal after API admission can
indicate a stale provider seat. Pausing the installation bounds repeated
starts while the registry and API settle the placement.
A committed settlement can replay after provider inventory is collected.

Egress readiness expires after five minutes. The authenticated scope
challenge reports the network observations. The API independently verifies
that it accepted the fresh, signed canary nonce via worker ingress. The
proof binds the exact application ID and provider version from inventory.
An overdue unresolved placement blocks Container admission for its
account with `CF_CONTAINERS_CLEANUP_PENDING`. Disconnect keeps cleanup
and its credentials available until the stop is settled.
The qualification canary holds a real account budget and installation seat
under the Enable or reconcile job lease. It uses a short start window,
its own tenant key, and no dispatch. It can obtain the first egress proof
before admission opens.

The API sends a relative lease derived from the database clock, subtracting
the elapsed send preparation time and a three-second transit margin.
The margin matches the admission telemetry freshness window and leaves
seven seconds after a halfway renewal of a twenty-second lease.
Hosted qualification shall measure API-to-guest one-way frame delay
through the exact ingress relay during startup, normal renewal, and
reconnect, and confirm the margin against the delay tail. A
supervisor needs its first grant within 15 seconds. It uses its own
monotonic clock for later grants and keeps the pinned absolute deadline.
Losing control while the guest is initializing ends the launch before
workflow dispatch. Ready guests retain the fenced reconnect path.
An API crash ends any in-flight Container workflows at lease expiry unless
a fenced API owner reconnects and renews first. A reconnect cannot revive
an expired process.

The signed Linux recipe uses its private PID namespace's init to preopen
the one-workflow guest's pipes. Init starts the guest as UID 1001 with no
capabilities and then runs the supervisor as UID 1000 with only the signal
capability needed to stop and reap the dedicated guest. Both drop privilege
escalation. The return pipe is bounded JSON and cannot carry Python objects.
The supervisor reaps all adopted guest descendants before a terminal result.
If it cannot prove they exited, it stops the whole namespace and leaves
settlement to independent provider observation. The Docker fixture exercises
the real guest launch, execution engine, native package import, subprocess,
UID denial and detached-descendant cleanup. Run it on CT128 with
`./test.sh cloudflare-containers <local-api-image>` using Buildx v0.21.3 and
a local API test image with the matched dependencies.
Hosted qualification must confirm the private
PID namespace and capability behavior with the signed release.

HTTP arrival accounting reserves the request line, all headers exposed by
the Fetch API, and the announced body length or a 256-byte floor. It
includes rejected requests, failed attempts, and repeated transfers.
The provisional two-times multiplier reserves both relay legs. Fetch's
normalized headers, transport framing, and Cloudflare's intercepted
traffic billing still require hosted measurement. Stop receipts keep
`meterComplete` false until that qualification.

The future Linux review writer must require platform-level permission to
accept a Global Linux approval because the approval can authorize runs for
every tenant. Provider-organization membership grants no such permission.

## Owner decisions

Decided on 2026-11-21

- Ship `standard-1`, two live instances per installation, one workflow
  per instance, one instance per tenant and two-minute idle stop as
  pilot policy defaults.
- Admit approved loose and Solution-installed workflows with wheels baked
  into the signed image. Runtime package downloads are disabled and the
  exact worker ingress is the only egress destination.
- Use one admitted revision across both lanes. Failed joint readiness
  closes both lanes, returns compatible work to the primary and requires
  joint repair.
- Use hosted injected-refusal handback as the refusal admission gate.
  Genuine provider-refusal evidence remains pending and subsequent runs
  continue watching for it.

Still open

- Consent wording and Linux workflow review fields.
- Monthly target and forecast duty-cycle defaults.
- Hosted trial spend, account lane and window. A hosted run requires
  separate explicit approval.
