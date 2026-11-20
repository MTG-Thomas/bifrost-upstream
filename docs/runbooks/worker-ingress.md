# Worker Ingress

Point the dedicated public HTTPS hostname at TCP port **8001** on Bifrost. The
operator's proxy terminates TLS, preserves the public `Host`, and forwards
HTTP/1.1 WebSocket upgrades and the original path. Keep WebSocket connections
open through idle periods and disable caching for this hostname. The API
replica checks the hostname against the non-retired installation's HTTPS
origin and caches the result for three seconds. The listener remains available during
Scale In and Disconnect so launchers can drain.

The API runner needs outbound HTTPS and WebSocket connectivity to the
worker hostname. The probe accepts an IPv4-only runner for a dual-stack
hostname when every IPv4 address passes and the runner lacks an IPv6
route. The probe validates the entire DNS answer. It skips a family only
when every address in that family reports a missing TCP route or address
family. Connection refusal, timeouts, TLS failures, and ingress-policy
failures all fail Enable.
The Linux API containers classify POSIX socket errors. Windows network
errors 1231 and 1232 are outside that runtime.

- **Docker Compose** — Attach the proxy to the Bifrost network and target
  `http://api:8001`. Compose exposes the port to the network without publishing
  it on the host.
- **Kubernetes** — Route to the `worker-ingress` ClusterIP Service on
  port 8001 in the API namespace. The Service selects the existing API replicas.
  A Helm-managed deployment can use the same container port and separate
  Service. The repository ships Kustomize manifests.

A separate socket keeps proxy routing independent of the main API hostname.
The listener runs in the API process and reuses its database pools, signed
control host, permission checks, and handlers. Never route the worker hostname
to the normal API service or client web proxy.

## Allowed Operations

The [versioned route matrix](../../api/src/services/worker_ingress_routes.json)
lists exact router templates and methods. The test compares it with the
normal API registrations and checks the runtime-specific calls at their SDK call sites. The
[Cloudflare SDK route policy](../../api/bifrost/cloudflare_sdk_routes.json)
supplies the explicit methods for roles, users, job polling, and refresh. The
matrix narrows its prefix families to specific methods and registered handlers.

| Family | Runtime contract |
| --- | --- |
| WebSocket `/api/worker-control` | Signed launcher hello, inventory, commands, claims, results, and terminal ACKs. Linux requirements bytes arrive in the bootstrap message. Package install, uninstall, requirements updates, and inventory use the `package_tool` command on this socket. The helper can fetch distributions from its configured package origins. |
| SDK context, config, and integrations | Caller context, typed config reads and writes, integration mapping operations, and integration OAuth refresh. The Python SDK uses the `/api/sdk/` paths for config and integrations. |
| Organizations, files, tables, and events | SDK organization CRUD, file operations, table definition and document operations, and event emission. |
| Forms, agents, and agent runs | Form and agent lookup plus agent-run enqueue and status. |
| Roles, users, and PlatformJobs | Explicit SDK methods from the Cloudflare policy, including role assignments and single-job polling. Role and job identifiers must be UUIDs. User identifiers may also be email addresses. |
| Knowledge, artifacts, and AI | Existing SDK request, polling, and streaming contracts under normal caller authorization. |
| Source and runtime | Module resolution, scoped Python source reads, runtime log writes and reads, and run-token refresh. The run refresh contract accepts a signed `run_refresh` token in the body. |

User, organization, and role creation via `POST`, updates via `PATCH`,
and deletion via `DELETE` are reachable by design, as are role-user and
role-form assignment via `POST`. The listener requires a signed workflow
run token for these mutations. The API still checks live run state and the
original caller's permissions for each operation.

The tenant gateway encodes the entire module storage key as one component.
Only the source GET route permits that encoding, and only when the decoded
path has no empty or traversal segments. The listener rejects path aliases,
unsupported methods, and redirects. It does not expose login, OpenAPI, test
fixtures, package administration, or unrelated administrative routes. The
listener strips cookies and forwarding headers before dispatch. A signed bearer subject
gets 40 requests per second and eight concurrent SDK requests. The refresh
route accepts 8 KiB with eight concurrent bodies before verification. Other
request bodies are limited to 4 MiB. Headers and bodies have a three-second
inactivity timeout and a ten-second total deadline. Eight pending
WebSocket hellos per replica have five seconds to authenticate. Verified
incarnations use separate quotas with two concurrent sockets per incarnation.
WebSocket messages use the control protocol's 1 MiB limit.

## Enable Checks

`probe_worker_ingress(installation, runtime_canary=...)` validates the
persisted HTTPS origin, pins public DNS addresses to verified TLS connections,
checks credential-free SDK responses and explicit refusals, and verifies
WebSocket upgrade and rejection of an invalid hello. It does not follow
redirects. Each request needs the ingress response marker and its fresh random
nonce, so a stale response cannot satisfy the probe. These checks provide
defence in depth for proxy configuration. The listener and API authorization
enforce the boundary on every request.

The Phase 1b enable job is responsible for calling this interface and supplying
a canary callback for its reserved launcher. The callback verifies a signed
hello and a no-effect SDK read from the runtime on the same origin.
`reachable` reports the transport and policy checks. `ready` additionally
reports both canary observations.

## Usage Estimate Coverage

Managed Workers price launcher lifetime and control traffic separately
from workflow receipts. Provider polling reaches both WorkerLedger and
the launcher and is budgeted at the fastest controller cadence. Startup,
alarm, reconnect and cleanup traffic have control reserves. The workload
receipt covers launcher storage, TenantHost dispatch RPCs and application
logs. Bifrost charges at least the pinned workload caps and retains
observed usage above them. Exhaustion closes workload admission while
terminal ACK recovery continues.

Bifrost reserves the Worker request and configured CPU budget before each
managed capacity call, including idle capability polling. Each workflow
reserves control fragments, gateway calls, and TenantHost residency
through its finite deadline and the extended warm tail. CPU reserves use
the packaged limits for cold boot, warm prime, idle exercise and gateway,
plus the approved workflow CPU budget up to the release ceiling. Idle
alarms can use only a confirmed engine generation. The separate identity
reserve covers initial Dynamic Workers and reuse across UTC days. Managed
uploads disable provider Observability and Logpush.

Capacity Settings lists the coverage of each rate contributing to the
current month. External traffic, provider retries and overages, exact
retained bytes, operator-enabled logging, and account billing adjustments
can affect the invoice. [Cloudflare Durable Object pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) · [Cloudflare Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)
