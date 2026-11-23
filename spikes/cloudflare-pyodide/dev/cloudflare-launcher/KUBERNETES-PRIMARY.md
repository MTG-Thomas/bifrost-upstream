# Kubernetes Primary With Cloudflare Workers Overflow

## Source audit

| Stage | Finding at the starting revision |
| --- | --- |
| Preflight and activation | Preflight identifies the Job primary by service-account access review. Activation rechecks the signed preflight choices and commits one installation and Enable job. On Kubernetes, Managed Tunnel preflight only reads the connector Secret. A read-only Role or an immutable Secret passes this check and fails during token projection, after provisioning has begun. [Preflight](../../../../api/src/services/worker_pool/cloudflare_activation.py#L158) · [Activation](../../../../api/src/services/worker_pool/cloudflare_activation.py#L287) · [Projection readiness](../../../../api/src/services/worker_pool/cloudflare_connector_channel.py#L27) |
| Enable and leadership | Enable uses the existing local PlatformJob runner. Each API competes for the scheduler lease and can run the Job controllers. Job reservations serialize through a PostgreSQL advisory transaction lock. The elected API runs the DB-backed Workers supervisor. The base scheduler Deployment is a physical launcher with no service-account token. A token-bearing scheduler, including the isolated-build overlay, can run the legacy Job helper without competing for the trigger lease. An elected API with no Job access holds new Workers starts as unverified. [API host](../../../../api/src/scheduler/api_host.py#L99) · [Legacy helper](../../../../api/src/scheduler/kubernetes_host.py#L32) · [Leader services](../../../../api/src/scheduler/control.py#L307) · [Supervisor](../../../../api/src/services/worker_pool/cloudflare_supervisor.py#L98) |
| Placement | Workers demand is launchable after 2 seconds once the healthy Job cohort reaches its ceiling and no eligible Linux budget fits. The base also supplies static Linux executors, which the controller can pack. The primary erroneously deducts pinned Workers launchers from its own Max Workers when an installation has no shared cap. If the Job retires while a Workers launcher is live, the primary can lose the slot needed to recover. [Packing and ceiling](../../../../api/src/services/worker_pool/controller.py#L402) |
| Placement reasons | The status read packs only the Job cohort when a Job primary is selected. A ready static executor can suppress an actual Workers launch while the read reports `eligible_for_overflow` or `waiting_for_launch_gate`. The installation slot limit uses the shared DB slot calculation and applies regardless of the primary form. [Status](../../../../api/src/services/worker_pool/cloudflare_status.py#L292) |
| Tunnel delivery and rotation | The production component projects the one resource-version-fenced Secret read-only. The connector watches kubelet's atomic symlink swap and stops its old child before starting a new one. Rotation snapshots old connector sessions, evicts them by ID after Secret projection, and verifies a replacement session. Immediate retry eligibility can exhaust rotation attempts before kubelet refreshes the mount or a replacement session attaches. The local harness lacks the dedicated ingress Service and a component overlay. [Component](../../../../k8s/cloudflare-tunnel/connector.yaml) · [Supervisor](../../../../api/scripts/cloudflare_connector.py#L53) · [Rotation](../../../../api/src/services/worker_pool/cloudflare_tunnel.py#L265) |
| Scale In, Disconnect and Upgrade | These operations use durable installation and release revisions. Scale In removes admission before drain. Disconnect withdraws the token, verifies owned resource absence, and then retires the installation. Upgrade drains the old revision before replacement Enable, with action dispatch remaining on the local Linux execution lane. [Capacity jobs](../../../../api/src/jobs/platform/cloudflare_capacity.py) |

Workers launch age and pool reconciliation cadence are independent. At the
starting revision, existing Linux primary Jobs are wake-driven with 2-second
pass spacing and a 5-second recovery sweep. A 5-second minimum age on new
Linux Jobs would be a separate change in primary capacity behavior. Warm
claimants are independent of provisioning age. A ready Workers launcher
and a ready Linux executor can both claim eligible work. Primary fit
controls when Bifrost provisions new Workers capacity. [Claim selection](../../../../api/src/services/worker_pool/claims.py#L226)

Warm claims do not renew a Workers launcher. The reservation fixes the
deadline at creation. Bifrost checks the remaining lifetime before claiming
and again before sending the workflow, using database time and the
workflow's bounded timeout. An active route can use the fixed drain
allowance. At expiry the controller stops admission and reconciles
retirement independently of primary fit. Releases with the deadline on
the wire also enforce it inside the launcher. [Reservation and send fence](../../../../api/src/services/worker_pool/cloudflare_admission.py#L63) · [Claim window](../../../../api/src/services/worker_pool/eligibility.py#L42) · [Retirement](../../../../api/src/services/worker_pool/external_controller.py#L209) · [Launcher alarm](launcher-do.mjs#L391)

Claiming on a warm launcher cannot extend its lifetime reservation or send
an unbooked workflow. The send path checks the existing ledger against
the spend guard and books qualified activity before delivery. That
workflow adds a separate reservation and can bring the estimate to or
above the limit.
The ledger also accounts for the launcher through verified deletion and
retains uncertain usage. The estimate is an admission guard. Actual
provider charges can exceed it, for example during delayed cleanup.
[Send path](../../../../api/src/worker/remote/host.py#L314) · [Activity booking](../../../../api/src/services/worker_pool/cloudflare_spend.py#L652) · [Duration accounting](../../../../api/src/services/worker_pool/cloudflare_spend.py#L355)

The persistence suite covers retirement at the pinned deadline and refusing
work that does not fit the remaining claim window.
[Retirement test](../../../../api/tests/e2e/platform/test_cloudflare_capacity_persistence.py#L2356) · [Claim-window test](../../../../api/tests/e2e/platform/test_cloudflare_capacity_persistence.py#L5303)

Record the Enable runner class in the Kubernetes rehearsal. A physical Linux
launcher reports `remote_launcher`, including a launcher hosted in a reusable
worker Job. An in-process executor reports `worker_pool`. Isolated build Jobs
report `kubernetes_platform_job`. Enable is a default-class PlatformJob and
does not switch to the build class when the isolation setting is enabled.
[Job catalog](../../../../api/src/jobs/platform/catalog.py#L58) · [Runner receipt](../../../../api/src/jobs/platform/runner.py#L81)

The production component assumes compatible DNS resolution and NetworkPolicy
enforcement. Kubernetes API discovery and readback can establish that a
NetworkPolicy object exists, but cannot determine whether the installed CNI
enforces it. Use a selected pod and an unselected control to verify that
disallowed traffic is blocked. For NodeLocal DNS, the consuming overlay
must allow the actual resolver destination. Confirm TCP/UDP 7844 to the configured edge addresses
and TCP 8001 to the ingress Service on the installed CNI. The provisioned
connector receives the tunnel token file. Workers receives scoped credentials
and derived incarnation keys. Bifrost platform secrets stay on Bifrost.

## Implementation and qualification

- Managed Tunnel preflight checks Secret identity, mutability, and permission
  to patch that exact Secret. The authorization review has no persistent
  write. Projection uses the API pod namespace even when Jobs run in another
  namespace.
- Activate, Enable, Upgrade, and connector rotation share a readiness
  wait. It begins after the first not-ready observation and checks again
  every five seconds for up to 180 seconds, with a final observation at
  the deadline. A probe uses the job lease and current revision, and
  commits before sleeping so Secret projection can take the installation
  lock. The budget allows 60 seconds for the default kubelet sync,
  60 seconds for caching when the TTL strategy uses its default TTL, and
  60 seconds for the connector child to start and attach, including
  polling margin. A cluster with longer sync or cache intervals can
  exceed the window. The job returns `CF_CONNECTOR_UNAVAILABLE` when
  readiness is still missing at the deadline. Provider-error retries
  retain the 15- and 45-second delays.
  [Secret projection](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod) · [Kubelet defaults](https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/#kubeletconfiguration) · [Secret cache TTL](https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/kubelet/secret/secret_manager.go)
- The primary keeps its own ceiling while pinned Workers launchers or
  Containers instances are present. An explicit shared ceiling counts
  each live resource once and retains pending provider deletions while
  admission is closed. Job and VM-provider status pack the same Linux
  executor budgets as the controller, including static executors. For a
  pending worker the forecast uses calibration from its launch build,
  package generation, memory threshold, and provider scope. Cohort health
  still uses heartbeat freshness.
  [Containers ceiling regression](../../../../api/tests/e2e/platform/test_cloudflare_capacity_persistence.py#L2306)
- The [local tunnel overlay](../../../../k8s/local-cloudflare-tunnel/kustomization.yaml)
  adds the existing connector component to the local manifests. It binds
  the Role to the local API ServiceAccount and uses the same image as the
  API. For an offline rehearsal, use a release image with the Workers
  bundle and replace the connector and provider endpoints with local fixtures.

Static validation used `registry.k8s.io/kubectl:v1.32.2` for
`kubectl kustomize` and `ghcr.io/yannh/kubeconform:v0.6.7` with
`-strict -summary -kubernetes-version 1.32.2`. A temporary consuming
overlay added the production component and exercised namespace
transformation.

| Manifest | Valid resources |
| --- | ---: |
| Production base | 12 |
| Production with Tunnel | 17 |
| Isolated-build overlay | 17 |
| Isolated-build overlay with Tunnel | 22 |
| Local base | 20 |
| Local overlay with Tunnel | 25 |
| Local optional isolated-build resources | 2 |

Rendered RoleBindings in both tunnel layouts target the API ServiceAccount in
the connector's namespace.

The disposable kind v0.24.0 test on the shared LXC test host reached kubeadm
initialization on Kubernetes v1.31.0. Kubelet exited because the LXC lacks
`/dev/kmsg`.
The control plane never became ready, so the Kubernetes workload rehearsal
did not start. The disposable node, network, and kind scratch directory
were removed. Live qualification needs an approved disposable Kubernetes
host with a functional kubelet and an enforcing CNI, plus a Kubernetes
adapter for the Compose-oriented Miniflare/provider fixture.

### Node-only retry

Kind v0.24.0 reached a Ready Kubernetes v1.31.0 control plane with
`KubeletInUserNamespace` enabled and a read-only `/dev/null` bind at
`/dev/kmsg` inside the node container. During both bootstrap and a later
restart, a peer test stack appeared after the host was initially clear.
The disposable node was stopped both times before launching the fixtures.
[Kind configuration](https://kind.sigs.k8s.io/docs/user/configuration/) · [Kubelet feature gate](https://v1-34.docs.kubernetes.io/docs/tasks/administer-cluster/kubelet-in-userns/)

In a later clear window, the release loader accepted the pinned Workers
bundle and the Bifrost image imported into the node. A manifest separator
in the temporary rehearsal adapter blocked application startup. Importing
the image left 159 MiB free on the shared Docker filesystem. After fixing
the separator, the attempt stopped at the disk headroom check before
launching Bifrost. The release image and its node were removed to recover
space. The rehearsal did not reach Job placement, Scale In, or Disconnect.

A fresh lightweight node ran the rendered connector Deployment and the
repository's connector supervisor with a local cloudflared stand-in. A
client impersonating the API ServiceAccount read and resource-version-patched
the real Kubernetes Secret. The first projected value started the child.
The rotated value stopped the old child and started its replacement with
zero container restarts. Clearing the Secret stopped the replacement.
Sampling put the lower bounds from successful patch to child start at
about 66 seconds for initial projection and 78 seconds for rotation.
These observations exceeded the former 60 seconds of scheduled retry delays.
The follow-up below exercised the full rotation job and provider session
eviction.
The connector namespace, disposable nodes, fixture containers, and
rehearsal networks were removed and their absence verified.

### Full local rehearsal

On 2026-11-23, the follow-up ran Bifrost at `ebfaea092bd4979101b4a890c289203c13db6979`
on a single Kind v0.24.0 node with Kubernetes v1.31.0. It used one API
replica, one warm Kubernetes Job at a one-Job ceiling, and the repository's
[local provider](../e2e/combined-provider.mjs) with Miniflare, Workerd, and a
checksum-verified offline Pyodide cache. The Bifrost connector supervisor
started a local cloudflared stand-in from the projected Kubernetes Secret.

The run waited while peer Compose stacks used the host and resumed with
at least 8 GiB of available RAM. The Docker filesystem had 85 GiB free
before the final node boot and 77 GiB with workloads running. The
temporary adapter split the Kustomize output on document separator lines
and wrote each document with its own separator and newline. Strict
kubeconform validation passed all 23 adapted resources. The first import stopped at the
isolation check. The replacement node showed both IPv4 and IPv6 default
routes and removed them before workloads started. The adapter bound the fixture's HTTP server for kubectl port
forwarding, installed the local CA for Workerd, and allowed connector
egress only to the fixture on its local port. The first overflow reached
the Workers claim but stopped before Python entry. The replay supplied the
pinned bundle from the existing offline cache.

A fresh Activate request created one installation and queued Enable.
Attempt three failed with `CF_CONNECTOR_UNAVAILABLE` 69.3 seconds after job
creation. The connector started 81.3 seconds after the last successful
Secret patch and 15.8 seconds after the job failed. Status showed
`connector_unavailable` and `setup_required`. After the local connector
session attached, Enable on the same installation succeeded in 4.6
seconds and recorded `remote_launcher`. This run established the initial
Enable timing failure that the shared connector-readiness wait addresses.

| Check | Observed result |
| --- | --- |
| Primary with room | A Kubernetes Job held the execution claim and completed it in 4.354 s from queue entry. The execution had no Workers slot. Idle Workers reported `waiting_for_demand`. |
| Primary at its ceiling | The sole Job's reservation left no room for the next approved workflow. Status reported `waiting_for_launch_gate` then `eligible_for_overflow` while the primary was saturated and primary fit was zero. The controller decided at 2.125 s after the 2.000 s gate. Workers claimed at 2.429 s, reached engine entry at 6.602 s, and completed at 24.671 s, including the fixture's 18 s hold, and received a terminal ACK. |
| Connector rotation | The Secret resource version advanced. The new child started 39.8 s after the rotation patch. Rotation completed on attempt three in 188.2 s, evicted one old session, attached one replacement, and the old token returned HTTP 403. The connector container had zero restarts. |
| Scale In | Admission closed while the original Workers claim kept its fence and drained successfully. Scale In and readback took 32.8 s. Three current scripts remained, with zero live usage incarnations. Status showed `setup_required` and `waiting_for_demand`. |
| Disconnect | The cleanup status showed `cleanup_pending` and `setup_required`. Full product readback took 7.6 s. The Secret was empty, the child was stopped, DNS returned 404, the tunnel was deleted, and no connector sessions remained. Across three revisions, nine journaled scripts and twelve namespaces returned 404. No live usage rows, unreleased workflow slots, stored credentials, or live ownership receipts remained. Final status showed `setup_required`. |

The rehearsal namespace and connector Deployment were deleted before Kind
teardown. Cleanup verified zero trial containers, zero recorded node volumes,
and absence of the disposable bridge and both trial image references. The
removed node volumes contained the six imported images. The kubeconfig,
binary, archive, local fixture cache copy, and scratch directory were removed.
Docker reported 84 GiB free after teardown. The rehearsal made zero hosted
Cloudflare requests and incurred zero model spend.

### First Activate with the readiness wait

The follow-up tested `2351e6d1934b405912d7b2e3f03b4461b5a3ca83` on a fresh Kind v0.24.0 node
running Kubernetes v1.31.0, with one API replica and one warm Job worker.
The adapter validated all 23 rendered resources and used the local provider
fixture with node egress isolated. One Activate request created one Enable
job and one connector credential. Enable succeeded on attempt one in
51.5 seconds and recorded `remote_launcher`. The Kubernetes Secret advanced,
and its connector child started about 38.8 seconds after the last patch.
The provider reported one attached session and the connector pod recorded
zero container restarts. The persistence tests exercise readiness at 140
simulated seconds and terminal `CF_CONNECTOR_UNAVAILABLE` at 180 seconds.

Disconnect was still pending when the rehearsal's 90-second observation
ended. The teardown deleted the disposable node and verified the absence
of its volume, its six imported images, both trial image references, the
bridge, the kubeconfig, and the source and fixture files. The local provider
state existed only in that cluster. Hosted Cloudflare requests and model
calls were both zero.

### Disconnect settlement after first Activate

The first-Activate log captured the capacity reasons `cleanup_pending` and
`setup_required` at submission. The rehearsal reached its 90-second
observation cutoff with the job still active. That log lacked the job phase
and connector readbacks needed to identify its last step.

The instrumented Kind replay at `d3d64529aeaef4a1968f49bb56ca85448b74f72c`
started Disconnect immediately after the first successful Enable. There were
zero live Workers usage incarnations and zero unreleased slots throughout.
The job ran on attempt one. The timings below are sampled from the
rehearsal's first job poll.

| Elapsed | Observed state |
| --- | --- |
| 3.2 s | All three Workers scripts were absent. The database wanted no connector token. The job was `running` in `Verifying Cloudflare Deletion`. |
| 5.7 s | The Kubernetes Secret object was empty, DNS was absent, and the tunnel receipt was `deleting`. The connector pod still had the previous mounted token and one provider session. |
| 57.3 s | The token mount and the attached session were empty. The tunnel still awaited a deletion check. |
| 66.0 s | Attempt one was `succeeded` with outcome `disconnected`. The tunnel was deleted, three script and four namespace reads returned HTTP 404, and there were no live usage rows, unreleased slots, stored credentials, or non-absent ownership receipts. |

The wait was for the mounted Secret to refresh and the connector to detach,
followed by the next deletion poll. The cleanup journal had already verified
Workers and DNS absence before that wait. Shared-cap persistence could
affect placement but did not participate in this path, and the connector
readiness wait runs in Enable, Upgrade, Reconcile when it provisions, and rotation. While the job was
`running`, its error and result codes were empty. The capacity status reason
was `cleanup_pending`, with `setup_required` also present. The earlier
7.6-second run submitted Disconnect after Scale In and did not capture when
the Secret mount refreshed. Its successful readback shows that the wait can
finish on an early deletion poll.

A controlled replay changed only the disposable node's kubelet sync period
to 105 seconds. The Secret object, DNS, and Workers scripts were clear by
5.6 seconds. At 90.4 seconds the pod still held the token and the provider
still reported one session. The job was `running` in `Verifying Cloudflare
Deletion`, with no PlatformJob error or result code. The installation
still reported `cleanup_pending`. At 100.5 seconds the mounted token and
session had cleared. Disconnect succeeded on attempt one at 125.6 seconds
and verified the same absence checks as the default replay.

Kubernetes documents that a mounted Secret update is eventually consistent.
The delay includes kubelet synchronization and cache propagation. Kubernetes
v1.31 also jitters regular pod resync by up to 50 percent. An empty Secret
object and a still-present token in the pod can therefore coexist, especially
when Disconnect follows shortly after the token was first projected. [Kubernetes Secret volume updates](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod). [Kubernetes v1.31 pod worker resync](https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/kubelet/pod_workers.go).

The rehearsal now waits up to 16 minutes for Disconnect to reach a terminal
PlatformJob state. This covers the 15-minute attempt limit and one minute to
claim the job. It logs status, attempt, phase, and code on transitions and
still requires verified absence. Within the attempt, deletion checks back
off from 2 seconds to at most 30 seconds. If deletion is still settling at
the first unresolved check after the 10-minute drain deadline, the job
requires action with `CF_CLEANUP_PENDING`. Both live jobs finished as
`succeeded` with the final progress message `Completed`.

A managed primary can still be constrained by an explicit shared cap while
Cloudflare deletions remain unverified. Status reports
`primary_limited_by_cloudflare_cleanup` when completing those deletions
would open primary room for queued demand. Live workers finishing work
after admission closed report `primary_limited_by_cloudflare_drain`, even
when a policy change closed admission without a job. Routine lifetime
retirements continue to consume the cap while outstanding. The status
uses the job's title only if the current lifecycle action covers those
resources. The diagnostic reasons appear alongside any Cleanup Pending
blocker.

Reconcile uses the Enable connector wait when it enters the provisioning
path. This is intentional. Reconcile can admit a new deployment, so it must
verify the same projected token and attached session. A connector that
remains unready after the 180-second wait ends that job with non-retryable
`CF_CONNECTOR_UNAVAILABLE`. A later explicit replay can reuse the
journaled connector credential. The `_wait_for_connector` loop releases its database
transaction before reading connector sessions, so a slow provider read
cannot block Secret projection. It rechecks the job, installation, and
token versions before recording the observation. Rotation evicts superseded
sessions and reads them back under the existing fence after the new token
is projected. Reconcile verification of an admitted deployment still reads
the provider under the installation row lock.

Strict kubeconform validated all 23 adapted manifests in each replay. Both
replays removed the namespace and connector workload, then verified that
the trial containers, node volumes containing the six imported images,
bridge, and two trial image references were absent. The source, fixtures,
local bundle copy, kubeconfig, and Kind binary were removed. Only the
rehearsal evidence was retained. Both runs used the isolated local provider
with zero hosted Cloudflare requests or model calls.

### Connector NetworkPolicy qualification

Kind v0.24.0 shipped kindnet with a bundled `kube-network-policies`
controller. On the pinned Kubernetes v1.31.0 node image it started the
controller and synchronized nftables rules. Kindnet
`v20240813-c6f155d6` had manifest digest
`sha256:e59a687ca28ae274a2fc92f1e2f5f1c739f353178a43a23aafc71adb802ed166`.
[Kind v0.24.0 release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.24.0)

The connector used the rendered component policy. Listening fixtures used
the rendered Service selectors and target ports. An unselected pod in the
same namespace reached all 22 targets. The node had no IPv4 or IPv6 default
route. A route inside the disposable node sent the allowed edge address and
a separate test address to a local listener.

| Connector destination | NetworkPolicy result | Unselected control |
| --- | --- | --- |
| Cluster DNS via Service and direct endpoint, TCP and UDP 53 | Reachable | Reachable |
| Worker ingress via Service and direct endpoint, TCP 8001 | Reachable | Reachable |
| Local Cloudflare edge fixture, TCP and UDP 7844 | Reachable | Reachable |
| Edge fixture on TCP 443 and arbitrary destination on TCP 443 and TCP/UDP 7844 | Blocked | Reachable |
| Bifrost main API, Postgres, Redis and SeaweedFS, each via Service and direct endpoint | Blocked | Reachable |
| Kubernetes API via Service and same-node control-plane endpoint | Reachable | Reachable |

Standard NetworkPolicy exempts traffic to the node running the connector.
In this single-node layout, the Kubernetes API Service also led to that
node. A temporary node-local firewall rule scoped to the connector's
source address blocked both API paths. The same rule left edge and worker
ingress reachable from the connector and left both API paths reachable
from the control pod. The rule was removed after the check. Deployments
that require this boundary need a host firewall or CNI-specific host policy.
The portable component retains the connector's token-free ServiceAccount
configuration. The probe verified the token was absent and an uncredentialed
Kubernetes API request returned HTTP 403. [Kubernetes NetworkPolicy scope](https://kubernetes.io/docs/concepts/services-networking/network-policies/#the-two-sorts-of-pod-isolation)


With the shipped policy still applied, the full Bifrost replay used one
Kubernetes Job worker and the offline Miniflare provider. The connector's
local cloudflared stand-in attached through the allowed edge route on TCP
7844 without an additional connector NetworkPolicy. The actual API pod
still reached Postgres, Redis, SeaweedFS, the Kubernetes API, and the local
provider. All four PlatformJobs succeeded on attempt one.

| Live action | Time | Readback |
| --- | ---: | --- |
| Enable | 60.7 s | One attached connector session and a successful `remote_launcher` canary |
| Scale In | 2.7 s | Admission closed and no live usage rows remained |
| Rotate connector token | 73.0 s | The Secret advanced, one old session was evicted, one replacement attached, and the old token returned HTTP 403 |
| Disconnect | 66.0 s | The Secret was empty, sessions and tunnel DNS were absent, the tunnel was deleted, and three scripts and four namespaces returned HTTP 404 |

The namespace, connector, edge fixture, and Kind node were removed. The
verification checked the node volume containing the imported and bundled CNI
images, the test image references, and the disposable bridge. The kubeconfig,
Kind binary, and fixture files were removed. No separate CNI image pulls,
hosted Cloudflare requests, or model calls were needed.
