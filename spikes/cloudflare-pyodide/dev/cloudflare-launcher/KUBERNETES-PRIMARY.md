# Kubernetes Primary With Cloudflare Workers Overflow

## Source audit

| Stage | Finding at the starting revision |
| --- | --- |
| Preflight and activation | Preflight identifies the Job primary by service-account access review. Activation rechecks the signed preflight choices and commits one installation and Enable job. On Kubernetes, Managed Tunnel preflight only reads the connector Secret. A read-only Role or an immutable Secret passes this check and fails during token projection, after provisioning has begun. [Preflight](../../../../api/src/services/worker_pool/cloudflare_activation.py#L158) · [Activation](../../../../api/src/services/worker_pool/cloudflare_activation.py#L287) · [Projection readiness](../../../../api/src/services/worker_pool/cloudflare_connector_channel.py#L27) |
| Enable and leadership | Enable uses the existing local PlatformJob runner. Each API competes for the scheduler lease and can run the Job controllers. Job reservations serialize through a PostgreSQL advisory transaction lock. The elected API runs the DB-backed Workers supervisor. The base scheduler Deployment is a physical launcher with no service-account token. A token-bearing scheduler, including the isolated-build overlay, can run the legacy Job helper without competing for the trigger lease. An elected API with no Job access holds new Workers starts as unverified. [API host](../../../../api/src/scheduler/api_host.py#L99) · [Legacy helper](../../../../api/src/scheduler/kubernetes_host.py#L32) · [Leader services](../../../../api/src/scheduler/control.py#L307) · [Supervisor](../../../../api/src/services/worker_pool/cloudflare_supervisor.py#L98) |
| Placement | Workers demand is launchable after 2 seconds once the healthy Job cohort reaches its ceiling and no eligible Linux budget fits. The base also supplies static Linux executors, which the controller can pack. The primary erroneously deducts pinned Workers launchers from its own Max Workers when an installation has no shared cap. If the Job retires while a Workers launcher is live, the primary can lose the slot needed to recover. [Packing and ceiling](../../../../api/src/services/worker_pool/controller.py#L402) |
| Placement reasons | The status read packs only the Job cohort when a Job primary is selected. A ready static executor can suppress an actual Workers launch while the read reports `eligible_for_overflow` or `waiting_for_launch_gate`. The installation slot limit uses the shared DB slot calculation and applies regardless of the primary form. [Status](../../../../api/src/services/worker_pool/cloudflare_status.py#L325) |
| Tunnel delivery and rotation | The production component projects the one resource-version-fenced Secret read-only. The connector watches kubelet's atomic symlink swap and stops its old child before starting a new one. Rotation snapshots old connector sessions, evicts them by ID after Secret projection, and verifies a replacement session. Immediate retry eligibility can exhaust rotation attempts before kubelet refreshes the mount or a replacement session attaches. The local harness lacks the dedicated ingress Service and a component overlay. [Component](../../../../k8s/cloudflare-tunnel/connector.yaml) · [Supervisor](../../../../api/scripts/cloudflare_connector.py#L53) · [Rotation](../../../../api/src/services/worker_pool/cloudflare_tunnel.py#L265) |
| Scale In, Disconnect and Upgrade | These operations use durable installation and release revisions. Scale In removes admission before drain. Disconnect withdraws the token, verifies owned resource absence, and then retires the installation. Upgrade drains the old revision before replacement Enable, with action dispatch remaining on the local Linux execution lane. [Capacity jobs](../../../../api/src/jobs/platform/cloudflare_capacity.py) |

Workers launch age and pool reconciliation cadence are independent. At the
starting revision, existing Linux primary Jobs are wake-driven with 2-second
pass spacing and a 5-second recovery sweep. A 5-second minimum age on new
Linux Jobs would be a separate change in primary capacity behavior. Warm
claimants are independent of provisioning age. A ready Workers launcher
and a ready Linux executor can both claim eligible work. Primary fit
controls when Bifrost provisions new Workers capacity. [Claim selection](../../../../api/src/services/worker_pool/claims.py#L226)

Record the Enable runner class in the Kubernetes rehearsal. A physical Linux
launcher reports `remote_launcher`, including a launcher hosted in a reusable
worker Job. An in-process executor reports `worker_pool`. Isolated build Jobs
report `kubernetes_platform_job`. Enable is a default-class PlatformJob and
does not switch to the build class when the isolation setting is enabled.
[Job catalog](../../../../api/src/jobs/platform/catalog.py#L58) · [Runner receipt](../../../../api/src/jobs/platform/runner.py#L81)

The production component assumes compatible DNS resolution and NetworkPolicy
enforcement. For NodeLocal DNS, the consuming overlay must allow the actual
resolver destination. Confirm TCP/UDP 7844 to the configured edge addresses
and TCP 8001 to the ingress Service on the installed CNI. The provisioned
connector receives the tunnel token file. Workers receives scoped credentials
and derived incarnation keys. Bifrost platform secrets stay on Bifrost.

## Implementation and qualification

- Managed Tunnel preflight checks Secret identity, mutability, and permission
  to patch that exact Secret. The authorization review has no persistent
  write. Projection uses the API pod namespace even when Jobs run in another
  namespace.
- Worker credential rotation uses the existing Cloudflare retry schedule to
  give asynchronous Secret projection and session replacement time to settle.
  The attempt limit and per-attempt timeout are unchanged.
- The primary keeps its Job ceiling while Workers launchers are present.
  An explicit shared ceiling still includes the launchers and unsettled
  reservations. Job-primary status accounts for fit on static Linux
  executors in the same installation.
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
