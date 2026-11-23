# Kubernetes Primary With Cloudflare Workers Overflow

## Source audit

| Stage | Finding at the starting revision |
| --- | --- |
| Preflight and activation | Preflight identifies the Job primary by service-account access review. Activation rechecks the signed preflight choices and commits one installation and Enable job. On Kubernetes, Managed Tunnel preflight only reads the connector Secret. A read-only Role or an immutable Secret passes this check and fails during token projection, after provisioning has begun. [Preflight](../../../../api/src/services/worker_pool/cloudflare_activation.py#L158) · [Activation](../../../../api/src/services/worker_pool/cloudflare_activation.py#L287) · [Projection readiness](../../../../api/src/services/worker_pool/cloudflare_connector_channel.py#L27) |
| Enable and leadership | Enable uses the existing local PlatformJob runner. Each API competes for the scheduler lease and can run the Job controllers. Job reservations serialize through a PostgreSQL advisory transaction lock. The elected API runs the DB-backed Workers supervisor. The scheduler Deployment is a physical launcher with no service-account token in the supplied manifests. A legacy scheduler with a token can host control, so its Job access matters if it wins leadership. A leader without Job access in a cluster holds new Workers starts as unverified. [API host](../../../../api/src/scheduler/api_host.py#L99) · [Scheduler entry point](../../../../api/src/scheduler/main.py#L29) · [Leader services](../../../../api/src/scheduler/control.py#L307) · [Supervisor](../../../../api/src/services/worker_pool/cloudflare_supervisor.py#L98) |
| Placement | Workers demand is launchable after 2 seconds once the healthy Job cohort reaches its ceiling and no eligible Linux budget fits. The base also supplies static Linux executors, which the controller can pack. The primary erroneously deducts pinned Workers launchers from its own Max Workers when an installation has no shared cap. If the Job retires while a Workers launcher is live, the primary can lose the slot needed to recover. [Packing and ceiling](../../../../api/src/services/worker_pool/controller.py#L402) |
| Placement reasons | The status read packs only the Job cohort when a Job primary is selected. A ready static executor can suppress an actual Workers launch while the read reports `eligible_for_overflow` or `waiting_for_launch_gate`. The installation slot limit uses the shared DB slot calculation and applies regardless of the primary form. [Status](../../../../api/src/services/worker_pool/cloudflare_status.py#L325) |
| Tunnel delivery and rotation | The production component projects the one resource-version-fenced Secret read-only. The connector watches kubelet's atomic symlink swap and stops its old child before starting a new one. Rotation snapshots old connector sessions, evicts them by ID after Secret projection, and waits for a replacement session. The local Kubernetes harness lacks the dedicated ingress Service and a component overlay. [Component](../../../../k8s/cloudflare-tunnel/connector.yaml) · [Supervisor](../../../../api/scripts/cloudflare_connector.py#L53) · [Rotation](../../../../api/src/services/worker_pool/cloudflare_tunnel.py#L265) |
| Scale In, Disconnect and Upgrade | These operations use durable installation and release revisions. Scale In removes admission before drain. Disconnect withdraws the token, verifies owned resource absence, and then retires the installation. Upgrade drains the old revision before replacement Enable, with action dispatch remaining on the local Linux execution lane. [Capacity jobs](../../../../api/src/jobs/platform/cloudflare_capacity.py) |

Workers launch age and pool reconciliation cadence are independent. At the
starting revision, existing Linux primary Jobs are wake-driven with 2-second
pass spacing and a 5-second recovery sweep. A 5-second minimum age on new
Linux Jobs would be a separate change in primary capacity behavior. Warm
claimants are independent of provisioning age.

The production component assumes compatible DNS resolution and NetworkPolicy
enforcement. For NodeLocal DNS, the consuming overlay must allow the actual
resolver destination. Confirm TCP/UDP 7844 to the configured edge addresses
and TCP 8001 to the ingress Service on the installed CNI. The provisioned
connector receives the tunnel token file. Workers receives scoped credentials
and derived incarnation keys. Bifrost platform secrets stay on Bifrost.
