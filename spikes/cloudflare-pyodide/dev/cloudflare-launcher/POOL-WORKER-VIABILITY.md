# Cloudflare Workers viability beyond workflow execution

Source baseline `1bd47b0c8611767ae72fa8acb7253d7a14df871e` from
`t3/cloudflare-pyodide-workflow-spike`. Containers reference
`origin/pr-1425` at `bdbe07f934a09dd79a169b09d881671fee526959`.
All file and line references refer to the source baseline unless marked
#1425. Estimates are engineering time, including focused tests, for one
engineer with the existing launcher in place. Hosted qualification and
provider rollout are separate.

## Placement decision

The catalog has **27 job kinds**. One is a good Workers pilot, five have
plausible Workers slices after a split, nine should retain their effects on
the Bifrost side, and twelve should keep a Linux runner. Five of the twelve
are build-class jobs with an existing Kubernetes isolation option.

| Verdict | Count | Meaning |
| --- | ---: | --- |
| Pilot | 1 | Bounded work in the isolate, canonical validation and completion in Bifrost. |
| Split | 5 | An isolate can do a bounded computation or I/O step after the job is redesigned around fenced batches. |
| API | 9 | Keep the existing handler in a trusted Bifrost runner near the API and its data. API mediation is possible for small operations, with little execution payoff. |
| Linux | 12 | Keep the existing handler on Linux. Qualify Containers for the required runner or isolated-build contract. |

**Realistic ceiling.** Workers can supply real Normal capacity for explicitly
eligible, bounded, HTTP-shaped work, with declared admission, pinned artifacts,
and a supervised attempt. That includes the existing reviewed workflow subset
and potentially selected agent/summary and PlatformJob slices. Normal is a lane,
so each kind still needs its own eligibility and resource policy. Keep the API
as the owner of the claims, schedule, leases, and database effects. Use
Containers for ordinary Linux process semantics, native libraries, mutable
environments, persistent service processes, builds, and large whole-job working
sets. Keep existing trusted runners and build pods for kinds whose Container
contract remains unqualified.

## PlatformJob inventory

Each row gives the catalog deadline and **minimum admission headroom**, not
measured working set or typical duration. These values come from
`api/src/jobs/platform/catalog.py:54–351` and the 256 MiB default at
`api/src/jobs/platform/policy.py:16`. The runner also has job-specific leasing,
resource limits, and observed memory requirements. DB means direct PostgreSQL
in the current handler. Object storage means the installation's configured
storage adapter. The shared control path uses DB for claims/progress and Redis
for wakeups and notifications for all kinds. Each row calls out additional
dependencies in the job body. CPU and memory descriptions are source-based
estimates. A row without a toolchain callout has no identified effect-critical
subprocess or native workload in the described path.

Handler filenames without a directory are under `api/src/jobs/platform/`.
Effort for Split rows is incremental after the common opaque-route contract.
Effort for Linux rows assumes the Linux handler is retained and measures
adapter and qualification work, with the Container prerequisites below.
“Keep” represents no migration engineering.

| Job kind | What it does and what it needs today | Placement, effort, payoff |
| --- | --- | --- |
| `workflow.cloudflare_reapproval` | Re-analyzes changed source pins and carries approval forward only if the envelope still qualifies. DB revision/pin checks and lease-fenced approval, repo object reads, Python source analysis. 60 s / 128 MiB. `cloudflare_reapproval.py:88,114,145`. | **API.** Keep the authorization decision and source verification together. A pure source analysis step could be advisory only. Keep, low remote payoff. |
| `application.deploy` | Downloads a staged ZIP, sanitizes/retains source in temporary files, rebuilds and activates an independent app. DB, object storage, ZIP and Linux npm/Vite toolchain. Build CPU and dependency memory can dominate. 20 min / 512 MiB. `application_deploy.py:35`, `api/src/services/application_build.py:36`, `api/src/services/solutions/app_build.py:328`. | **Linux.** Container build runner or existing build pod. About 2–4 weeks for shared isolated-build placement, then 3–5 days to qualify this kind. High payoff. |
| `application.sdk_update` | Resolves retained app source, checks SDK target/binding, rebuilds and atomically activates. DB, object storage, temp files and same npm/Vite path. 20 min / 512 MiB. `application_sdk_update.py:24`, `api/src/services/application_build.py:36,120,337`. | **Linux.** Reuse the isolated-build path, 3–5 days after shared work. High payoff, including old payload versions. |
| `application.publish` | Rebuilds the app from its current source and promotes published storage/snapshot state. DB, object storage, Redis publish event, materialized source and Node/esbuild subprocess. 20 min / 256 MiB. `application_publish.py:45`, `api/src/repositories/applications.py:573–649`, `api/src/services/app_bundler/__init__.py:323,736`. | **Linux.** Normal Container after a trusted publish adapter or qualified runner, 3–5 days per kind after common access work. High payoff. |
| `oauth.refresh` | Refreshes expiring provider tokens and reports per-token outcomes. DB credential reads/writes, outbound token endpoints, secret rotation; wall time and memory scale with the sweep. 15 min / 128 MiB. `system_maintenance.py:30`, `api/src/jobs/schedulers/oauth_token_refresh.py:32,46`. | **API.** Keep provider credentials and rotations with the trusted handler. An edge driver calling an API per token mostly adds hops and duplicates failure modes. Keep, low remote payoff. |
| `webhook.renew` | Finds subscriptions and renews or recreates provider webhooks. DB rows and provider credentials, HTTPS mutations and corresponding state updates; installation-wide I/O sweep. 30 min / 128 MiB. `system_maintenance.py:54`, `api/src/jobs/schedulers/webhook_renewal.py:59,84`. | **API.** Keep provider effect and record reconciliation together. Keep, low remote payoff. |
| `solution.update_check` | Compares descriptor versions across Git-connected installs and emits update availability. DB, private source credentials, shallow Git clones into temp dirs, YAML and PEP 440 parsing, event emission. 30 min / 256 MiB. `api/src/jobs/schedulers/solution_update_check.py:27`, `api/src/services/solutions/update_check.py:37`, `api/shared/solution_git_sources.py:173,194`. | **Linux.** Clone on Container; an HTTPS descriptor reader would require provider/ref/path parity before it could replace cloning. 3–5 days for existing handler qualification; low incremental payoff. |
| `workspace.file_index_reconcile` | Compares workspace/Solution object lists against search index rows and repairs drift. DB and object storage, listing/diff memory scales with scope and object count. 60 min / 256 MiB. `system_maintenance.py:92`, `api/src/services/file_index_reconciler.py:45,94`. | **Split.** API issues immutable, paged inventories and commits a version-checked diff. Worker can compare bounded pages. 1–2 weeks; medium payoff only if profiling shows the diff is material. |
| `artifact.retention_cleanup` | Finds expired chat artifacts and deletes them through file storage. DB policy and metadata plus object deletes; I/O bound, memory follows candidate count. 30 min / 128 MiB. `system_maintenance.py:114`, `api/src/services/artifact_retention.py:78`. | **API.** Keep deletion and metadata reconciliation under existing authority. Keep, low remote payoff. |
| `audit.archive` | Selects audit rows in batches, builds and verifies deterministic gzip JSONL segments, commits manifests/deletes source, expires archives/exports. DB, object storage, in-memory gzip and buffers, potentially long scans. 2 h / 256 MiB. `audit_archive.py:45`, `api/src/services/audit_retention/archiver.py:93,119,151,165`, `api/src/services/audit_retention/format.py:142`. | **Linux.** Keep trusted access, retention fences and byte-level archive format intact. A separately qualified compression accelerator would leave most effects local. 1–2 weeks after common access work; medium payoff. |
| `audit.query` | Exports matching live and archived rows under the stored reach snapshot. DB, object storage, segment decode/filter and gzip output streaming, data volume drives CPU/wall time. 2 h / 256 MiB. `audit_query.py:27`, `api/src/services/audit_retention/export.py:89,101`. | **Linux.** Keep tenant reach and export verification with the trusted runner. 1–2 weeks alongside archive; medium payoff. |
| `run.retention` | Plans/deletes old runs and events, checkpoints counts, reclaims execution log streams. DB plus Redis streams, batched destructive work with potentially long sweeps. 2 h / 256 MiB. `run_retention.py:48`, `api/src/services/run_retention/deleter.py:81,96,134,246`. | **API.** Keep deletion, cancellation and checkpointing under the same trusted authority. Keep, low remote payoff. |
| `model_catalog.refresh` | Conditional public models.dev GET, trim/validate response and update last-good catalog. DB row and legacy bundled gzip baseline when needed; HTTP/JSON buffering is the scale risk. 5 min / 256 MiB. `system_maintenance.py:142`, `api/src/services/model_catalog.py:343,432,493`. | **Pilot.** PID-less bounded HTTPS read, API validates and commits. 1–2 weeks for common route plus 2–4 days for the adapter. Modest workload payoff, high contract payoff. |
| `files.move` | Moves scoped objects with conflict handling, checkpoints and verified copies, metadata updates, source cleanup, recovery. DB authorization/locks, object storage and publish/invalidation; batch and object counts can be large. 4 h / 128 MiB. `file_move.py:76,129`, `api/src/services/file_storage/move.py:366,392,454,593,615,727`. | **API.** Keep copy/drop/metadata ownership in the trusted mover. Replaying a partial move needs its existing recovery path. Keep, low remote payoff. |
| `solution.export` | Builds a Solution backup, including encrypted full-backup material, uploads and updates the export row. DB, object storage, temporary archive and compression/crypto work; archive size drives disk/CPU. 60 min / 512 MiB. `solution_export.py:18`, `api/src/jobs/schedulers/solution_export_jobs.py:85`. | **Linux.** Isolated Container build-class adapter or existing build pod. 3–5 days after common isolated-build work; high payoff. |
| `solution.deploy` | Restores staged input and orchestrates install/deploy plus app compiles. DB, object storage, temporary archive, encrypted payload, native build toolchain and substantial working set. 60 min / 512 MiB. `solution_deploy.py:63`, `api/src/services/solutions/app_build.py:328`. | **Linux.** Isolated Container or existing build pod; 1–2 weeks after shared work to qualify staging, child tracking and finalization. High payoff. |
| `solution.git_sync` | Pulls Solution Git source, updates entities and compiles apps. DB and credentials, object storage, Git checkout, build toolchain and localized filesystem. 60 min / 512 MiB. `solution_git_sync.py:26`, `api/shared/solution_git_sources.py:173,194`, `api/src/services/solutions/app_build.py:328`. | **Linux.** Isolated Container or existing build pod, about 1 week after shared work. High payoff. |
| `embedding.reindex` | Re-embeds knowledge rows, grouped by provider, and updates vectors/progress. DB, Redis cancellation, provider/network and credentials; batch memory and rate limits dominate. 4 h / 512 MiB. `embedding_reindex.py:17`, `api/src/services/embeddings/reindex.py:58,90,138`. | **Split.** Per-batch Worker fetch against scoped provider mediation or isolated provider adapter, with API-side vector writes and checkpoints. Queue only as a wake hint. 2–4 weeks; medium payoff if provider throughput permits. |
| `workspace.reimport` | Materializes the repo from object storage, imports entities/indexes, and syncs app previews. DB, object storage, local checkout, parsing/compilation; size scales with workspace. 60 min / 512 MiB. `reimport.py:18`, `api/src/services/github_sync.py:1917,1969`. | **Linux.** Container with qualified storage/DB integration and checkout resources, about 1 week after common access work. High payoff for large workspaces. |
| `workspace.git` | Connect/fetch/status/commit/sync/resolve/diff/discard operations with worktree and source synchronization. DB, object storage, Redis dirty flag, local Git/credentials and possible preflight subprocess, variable checkout/build cost. 60 min / 512 MiB. `git_operation.py:60,128`, `api/src/services/github_sync.py:1517,1751,2175`. | **Linux.** Container runner; about 1–2 weeks after common access work for Git and recovery parity. High payoff. |
| `workspace.bundle_import` | Restores and verifies a reviewed ZIP, applies entity imports, promotes files, regenerates manifests. DB, object storage, Redis invalidation/dirty state, temp filesystem and encrypted payload. 60 min / 512 MiB. `workspace_bundle_import.py:180`. | **Linux.** Container with staged archive and trusted finalize access; about 1 week after common access work. Medium payoff. |
| `agent.summary_backfill` | Stages summary membership/dispatches then returns waiting while separate summaries execute. DB, Redis wake hints and durable child aggregation. 15 min / 128 MiB. `summary_backfill.py:14,53`. | **API.** Keep fan-out and aggregate coordination near DB. The individual `agent.summarize_backfill` handler is the compute opportunity. Keep, low parent-job payoff. |
| `chat.video_generation` | Submits provider job, polls, buffers video bytes, then stores a chat attachment and records usage. DB, provider credentials, object storage, 2 s polling, unbounded-by-isolate video body; non-retryable runner loss. 30 min / 512 MiB. `video_generation.py:34`, `api/src/services/media_generation.py:290,353`. | **Split.** Cloudflare Workflows can wait and poll explicit provider references. API owns submit receipt, usage and chat attachment, with a streamed download. 3–5 weeks for shared video work; medium payoff for long waits. |
| `sdk.video_generation` | Same provider submit/poll/download, stores a scoped SDK artifact and usage without a chat message. DB, provider credentials, object storage and bytes in memory; non-retryable runner loss. 30 min / 512 MiB. `video_generation.py:164`, `api/src/services/media_generation.py:290,353`. | **Split.** Reuse video polling and streaming, qualify SDK reach and artifact settlement separately. 3–5 days after shared video work; medium payoff. |
| `workflow.permissions.analyze` | Reads workflow sources and observed usage, resolves imports and assembles a reviewed conversion report. DB authority and object source reads, AST/digest CPU grows with source graph. 60 min / 384 MiB. `workflow_conversion.py:20`, `api/shared/workflow_conversions.py:117,148,473,789`. | **Split.** Only bounded syntax/source facts on Workers. The API verifies them or produces the policy report from authoritative inputs. 2–3 weeks; medium payoff if analysis is CPU-significant. |
| `workflow.permissions.apply` | Revalidates reviewed state and writes grants/rules under lease and Solution locks. DB and source/policy evidence, audit, reanalysis can grow with selected workflows. 60 min / 384 MiB. `workflow_conversion.py:33`, `api/shared/workflow_conversions.py:670,720,737,909`. | **API.** Keep authorization, mutation, and audit in Bifrost. Keep, low remote payoff. |
| `workflow.permissions.cutover` | Verifies readiness and switches permission mode under current authority. DB policy/locks and audit; evidence traversal rather than toolchain workload. 15 min / 384 MiB. `workflow_conversion.py:46`, `api/shared/workflow_conversions.py:720,737,1138`. | **API.** Keep permission-mode cutover and lease fence together. Keep, low remote payoff. |

## Pool worker and service loops

`api/src/worker/pool_main.py:342–408` starts heartbeat and wake handling for
both runtime profiles. Diagnostics, package reconciliation, memory calibration,
and Normal platform-job/service work have explicit Linux profile gates.
`api/src/worker/remote/host.py:751–795,830–860,1326` leaves controller startup
to the API lifespan, skips Linux requirements/bootstrap status for Cloudflare,
and accepts exactly the workflow handler for that profile. The Cloudflare
launcher currently implements only the workflow's opaque route and shared
protocol controls. `launcher-core.mjs:167,210,284–356,417`.

| Worker responsibility | Dependencies and lifecycle | Placement, effort, payoff |
| --- | --- | --- |
| Process startup/registration, readiness, budget calibration | Schema/ORM startup and worker incarnation in DB. Linux measures cgroups after the template is up and reports requirements generation. `pool_main.py:268,342,420–477`. | **API + Workers already.** Remote API host owns registration/readiness; DO reports declared slot readiness. Keep Linux calibration with the Linux runner. No migration; high payoff from common admission. |
| Main claim/start/sweep and unstarted-claim recovery | PostgreSQL fenced selection, handler caps, shared memory admission, Redis wake hint and periodic sweep; Normal workflows and agents, live and backfill summaries. Releases stranded claims after errors. `pool_main.py:253,481,542,579,618–699`. | **API + Workers already for approved workflows.** Keep claims in API; extend exact handler capabilities for qualified agents/summaries in the sibling studies. Linux Container remains the process path. No second dispatcher; high payoff. |
| Workflow process supervisor or opaque host | Linux fork template and child IPC with monitor, heartbeat, cancel and command tasks; cgroup pressure, process exit/kill, Redis diagnostics. Worker DO owns isolated tenant generations instead. `api/src/services/execution/process_pool.py:835–880`, `api/src/worker/remote/launcher.py:394,675,730`. | **Workers for bounded opaque workflows; Linux for process parity.** Existing opaque mechanics are reusable. Porting Linux loops has no payoff. |
| Pool wake listener | Direct Redis pub/sub from the API-side host; dispatch, service, platform job and package wake hints, followed by DB reads. `pool_main.py:760–778`. | **API.** Keep current transport. Cloudflare Queue can wake already admissible work only if useful and deduped; adding it now has low payoff. |
| Worker heartbeat, handoff, retirement and diagnostics | DB heartbeat and incarnation/lease fences; Redis scheduler replica diagnostics. DO outbound socket heartbeat/inventory, freshness and drain together with API lease renewal. Boot-to-retire lifecycle. `pool_main.py:365–408,811,880`, `api/src/scheduler/control.py:93`, `api/src/worker/remote/host.py:1118,1220`. | **API + Workers already for execution liveness.** For PlatformJobs, include their retained route occupancy in the existing heartbeat. Cosmetic diagnostics parity can use API-reported platform/kind fields, about 1–2 days; low incremental payoff. |
| Platform-job claim/recovery/supervision | Normal Linux loop recovers expired leases, takes a local DB job with capacity/resource/concurrency gates, starts separate subprocess, renews, observes deadline/cancel and holds memory until exit. API leader also sweeps leases independently of connected workers. `pool_main.py:701–758`, `api/src/jobs/schedulers/platform_jobs.py:66,195,569`, `api/src/scheduler/control.py:196`, `api/src/worker/remote/launcher.py:451–474`. | **API scheduler plus Workers pilot or Linux runner.** Add per-kind opaque dispatch below. Keep canonical progress, cancellation, notifications and recovery, even at zero capacity. High contract payoff. |
| Service claim/supervision tick | General always-on service-workflow attempts, driven by DB eligibility rather than a fixed list of named daemons. Claims due attempts, renews lease, enforces startup grace/drain/stop, flushes logs and telemetry, rotates attempt token, releases/reschedules, and hands over. Needs DB and Redis plus SDK network and runnable source in its child. Long-lived memory and CPU depend on user code. `pool_main.py:855–878,891`, `api/src/services/service_claim.py:144,167,195,244,292,380,466,572,661,806,946`. | **Linux for current contract.** Container under controlled lifecycle with adopted processes, real stop and log parity. At least 2–4 weeks to qualify. Workers/Workflows could only support a separately declared, resumable bounded service model, with persisted cursor and lease checks per callback. High parity payoff on Containers. |
| Package-generation loop and Linux `package_tool` | Startup requirements install, DB package change generations, Redis wake and requirements cache, repo storage snapshot, local venv/temp files, pip install/uninstall/requirements/inventory and template recycle. Native wheels/build scripts and resolver CPU, memory and minutes of wall time. `pool_main.py:420,780`, `api/src/services/package_generation.py:25,43,48`, `api/src/core/requirements_cache.py:52,78,157,192`, `api/src/jobs/consumers/package_install.py:46,77,111,163,174`, `api/src/worker/remote/host.py:668–698`, `api/src/worker/remote/launcher.py:475–489`. | **Linux for live mutation.** Container with API-delivered requirements and disposable, generation-fenced venv; qualify inventory/recycle across restart. About 1–2 weeks. **Workers** can consume prebuilt, hash-pinned Pyodide bundles via approved runtime versions, with inventory of supported packages and child replacement. About 1–2 weeks for generation integration, separate from wheel porting. High payoff for a curated subset. |
| Static compatibility capacity controllers | Only static local executor with remote work can start this path. Imports policy from DB/environment, may start external-capacity reconciliation; Kubernetes permission check can enable pool controller (if no external provider) and isolated-build controller. Remote `PoolWorker` override starts none. `pool_main.py:300`, `api/src/services/kubernetes_control.py:39,66–113`, `api/src/worker/remote/host.py:773`. | **API.** Workers DOs can implement the capacity provider lifecycle only. Kubernetes and its DB-owned build controller stay in the authorized control host. No migration; high payoff from existing API placement. |

APScheduler triggers, including due service eligibility and periodic job enqueue,
belong to the API scheduler leader. They are distinct from supervised service
children. Keep the fenced trigger lease and deduplicated occurrences in
`api/src/scheduler/control.py:149,243,300` and
`docs/architecture/platform-jobs.md:119–198`.

## Pilot contract for `model_catalog.refresh`

Start with the one public, read-only conditional fetch. Split
`api/src/services/model_catalog.py:493` at the fetch boundary. This preserves
the 304 semantics, last-good snapshot validation and current typed result,
and exercises the same claim/occupancy/recovery machinery needed by more
valuable jobs.

1. **Capabilities and claims.** Introduce a versioned job-kind allowlist in
   the API's prepared profile/build record and authenticate the hello against
   it. For the pilot admit only `model_catalog.refresh@1` on external Normal.
   Keep platform-job capability separate from the workflow/agent handler
   list. Filter both the candidate selection and locked reread in
   `api/src/jobs/schedulers/platform_jobs.py:195–255` on kind, payload
   version, placement and an explicit isolate-only resource profile. Keep
   priority, concurrency, resource locks, lease and cancellation intact.
   The existing 256 MiB Linux floor cannot serve as the isolate request.
   Set a separate measured-and-capped isolate request after qualification,
   reserve one existing slot plus bytes in the claim transaction, and share
   this eligibility with demand planning. Initially use one already enrolled
   fork launcher for the pilot. Keep queued ineligible jobs claimable by their
   existing trusted Linux backend.
2. **Command, execution and data boundary.** Add a versioned
   `platform_job` action for the opaque profile carrying route ID, job ID,
   kind/version, attempt identity, absolute deadline and declared bytes. The
   API keeps the real lease token. Send only the current conditional ETag
   and bounded parameters, using an attempt-bound API capability for large
   response transfer if needed. The dedicated trusted handler performs
   `GET https://models.dev/api.json` with fixed host/path/method, bounded
   bytes and time, and either rejects redirects or revalidates every permitted
   redirect hop. It sends no Bifrost or provider credential. Measure realistic
   catalog sizes before setting the cap. Use a test-owned size cap and body
   validation in the implementation tests. Prefer scoped staging and a small
   content receipt if data approaches the 16 MiB logical envelope or the
   isolate allocation. Do not route this privileged job through workspace
   Python, give the isolate DB/Redis/S3 credentials, or accept a Worker-supplied
   URL or arbitrary SQL.
3. **Opaque acknowledgement and inventory.** Journal the attempt before
   sending `{runtime_handle}` without a PID. Persist live and terminal route
   records with job/attempt identity and original resource request. Keep the
   slot until the canonical terminal is durably accepted. After reconnect
   reconcile against the exact durable job lease, then replay the retained
   result with a stable event ID. Add an opaque backend branch to the API
   supervisor. The Linux process supervisor expects the child to commit its
   result using DB credentials and then reports child exit. A Worker supplies
   the read result for API validation and fenced settlement instead.
   References: `api/src/worker/remote/host.py:356–421,918–957`,
   `api/src/jobs/schedulers/platform_jobs.py:569`,
   `api/src/jobs/platform/base.py:57,73,98`.
4. **Commit and recover.** On result, under the exact current lease and
   owner fence, compare the starting catalog generation/ETag to current
   state. Run existing trim and plausibility checks with the last-good
   baseline, preserve `fetched_at` on a valid 304, and update the catalog
   and canonical terminal transactionally or with a durable idempotent receipt
   tied to the attempt. ACK only after durable acceptance. Report refused,
   completed or uncertain attempts explicitly. API cancellation/expiry
   revokes the capability first, and late results cannot commit. Retry a
   lost read only through the existing recovery policy after reconciliation
   establishes the prior route cannot commit. Never resubmit a mutating
   provider request on transport ambiguity. Test 200/304, bad or oversized
   input, incompatible kind/version, duplicate terminal, lost reply/ACK,
   owner takeover, cancellation and stale ETag/lease against real local DB.

**Prototype.** `../tenant-workers/launcher-workerd.test.mjs` exercises the
actual TenantHost, Dynamic Worker loader and gateway in pinned
Miniflare/workerd. A test-only JS child rides the existing opaque workflow
carrier and performs the read step against an in-process fixture origin.
A fresh path returns 200 with the payload and ETag. A second opaque route
returns 304 with no payload. The origin checks GET with no Authorization
header, and exactly two requests reach it with an allowance of one per route.
This establishes the carrier's conditional HTTP behavior. The genuine
PlatformJob command, API SQL claim, catalog-size cap, durable settlement,
and hosted behavior remain implementation work.

**Local verification.** Node v26.13.1 ran `node --test` in all requested
directories. The launcher passed 98, tenant-workers 97, e2e relay 2,
routing 1, executor 61, and backend Node 122. The tenant suite was initially
missing its ignored runtime bundle. It passed after rebuilding the five
runtime seam modules from the baseline checkout, precompiling the retained
vendor-patched source bundle, and staging the checked-in loader.
Generated bundle SHA-256
`6237f17dc96f3c98048c2b36322be5db0cdba75bfe3f0412661dcd39692a14d8`.
The catalog coverage check matched all 27 source kinds with one row each.
No backend Python suite or Docker integration stack was run.

## Workers and Containers boundary

- **Workers execution.** Keep the DO control socket, signed enrollment,
  opaque routes and bounded fetch/gateway. CPU and subrequest allowances
  apply separately from wall-clock deadlines. The platform's isolate memory
  ceiling is 128 MB and includes runtime overhead. Declared slot bytes are
  an admission model and require representative hosted bounds. Workflows
  can checkpoint long waits but each step still inherits a CPU budget and
  result storage limits. Queue delivery is at least once, so use a queue
  only for wake or fenced attempt references with idempotent application
  effects. PostgreSQL remains the only claim/status source. See
  [Workers limits](https://developers.cloudflare.com/workers/platform/limits/),
  [Workflows limits](https://developers.cloudflare.com/workflows/reference/limits/),
  and [Queues delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/).
- **Linux execution on Cloudflare.** Jack's #1425 is a capacity provider.
  `git show origin/pr-1425:deploy/cloudflare/worker/capacity.mjs` defines
  ScopeRegistry (159), WorkerContainer (253, start at 382), and WorkerLedger
  (439). It provisions and observes a digest-pinned launcher. The provider
  receives no job payloads. The API-only reference explicitly rejects
  direct DB, Redis, S3 and installation signing credentials in the
  container environment. Sharing an image does not imply support for
  privileged PlatformJobs, agents or summaries, and the provider has no
  Fast Path or isolated-build implementation. See
  `git show origin/pr-1425:docs/runbooks/cloudflare-containers.md`, lines
  9–33 and 281. Cloudflare Containers supply Linux CPU, disk, memory and
  subprocess semantics. [Cloudflare Containers overview](https://developers.cloudflare.com/containers/).
- **Path to parity.** First qualify a Normal Container with digest-bound
  capabilities, requirement bootstrap, recoverable package generation,
  correct process stop/drain and reconnect adoption. Add scoped API-only
  adapters for platform DB, Redis and object effects by kind while preserving
  the existing authorization, leases and transactions. Whole-runner trusted
  data-plane access would require an explicitly reviewed trust/deployment
  model beyond #1425. Qualify service uptime across idle detection, replacement
  and provider lifetime limits. Add a separately sized, isolated-build
  Container placement adapter with UID-bound attempts and authoritative
  cleanup, or retain Kubernetes for those jobs. Fast Path retains its
  separate isolation contract. Full Linux coverage is a property of those
  qualified runners and placements together. The current provider and the
  current isolate launcher cover only the capabilities each advertises.
