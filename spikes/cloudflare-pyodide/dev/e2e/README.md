# Phase 5a local launcher qualification

Use a private, disposable Git archive on the Linux host running Docker. The
suite binds HTTP to loopback and uses local Miniflare/workerd with the real
launcher and Python child. It rejects outbound origins other than its local
API relay and held-response fixture. No Cloudflare account is required.

## Run on CT128

Archive the desired local commit into a fresh `/opt/bifrost-tests/phase5-<sha>`
directory. Create the directory with `umask 077`. Inside it, run `git init`,
`git add -A`, and make an initial local commit without adding a remote.
Use Node 26 on PATH and the existing Wrangler version:

```sh
(cd spikes/cloudflare-pyodide/dev/tenant-workers && npm install --no-audit --no-fund)
umask 077
python3 spikes/cloudflare-pyodide/dev/e2e/phase5a.py prepare
BIFROST_FORCE_PORT=1 nohup setsid ./debug.sh up >.phase5-debug.log 2>&1 </dev/null &
# Wait for "Stack is up." in the private boot log.
BIFROST_RUNTIME=/absolute/path/generated-runtime.mjs nohup setsid sh -c '
  python3 spikes/cloudflare-pyodide/dev/e2e/phase5a.py run "$BIFROST_RUNTIME"
  printf "PHASE5_RUN_EXIT=%s\n" "$?"
' >.phase5-run.log 2>&1 </dev/null &
# Wait for PHASE5_RUN_EXIT in .phase5-run.log.
BIFROST_FORCE_PORT=1 ./debug.sh down
```

Supply the prepared runtime from
[Building the packaged runtime](../staging/README.md#building-the-packaged-runtime).
The harness prints the SHA-256 of the supplied artifact and stages the loader
modules from the checkout. On CT128, a user-local Node tarball can be used with
the Debian `libatomic1` package extracted to the task directory and its
library directory on `LD_LIBRARY_PATH`. No system install is needed.

Run `./debug.sh down` on failure as well. Compose and dotenv changes belong to
the archive. Run the suite once per fresh stack. The operator seeds a new
workflow and incarnation, and passes only the derived control key into the
Worker. The debug log and dotenv are private. Do not share them.

## Evidence

The harness prints JSON lines with the execution ID, prepared incarnation,
claim and worker IDs, sanitized wire events, DO ledger summaries, and PostgreSQL
settlement fields. It blocks each fixture after a real SDK call so the
operator can read the active claim and trigger the next fault deterministically.
The harness pauses both Linux launchers (`worker` and `scheduler`) during the
claim sequence, waits for their three-second API admission window to close, and
unpauses both on exit. PostgreSQL retains each claim for provenance.

The relay drops exactly one welcome and one ACK. API takeover uses Uvicorn's
reload supervisor. A test RPC aborts the launcher DO while preserving its
SQLite journal, and a test hook calls the real TenantHost egress gate after
stop. Cancellation uses the execution API. Once the engine has started, the
cooperative stop leaves an uncertain outcome and the API stores a failure.
Retirement marks the enrolled row
and forces a reconnect so the API can return close code 4001. These hooks are
loaded only by Miniflare in this harness.

The verified run used archive `7e1f7c846` on CT128, project
`bifrost-debug-a3acd344`. Its log is
`/opt/bifrost-tests/phase5-7e1f7c846/.phase5-run.log`. The runtime digest was
`fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5`.

## Local one-button Containers rehearsal

Use a fresh private `combined-local-*` archive on CT128 and the local
release and Compose setup in [the combined Workers rehearsal](../cloudflare-launcher/PHASE1-PILOT.md).
Use Node 26 and Buildx 0.21.3. Confirm the host is idle and has at least
8 GB available before starting the stack.

Build the Linux child from the same local API image and label it with
the source revision in that image's Workers release manifest. The pinned
outbound proxy image must be present in the local Docker daemon.

```sh
npm ci --prefix deploy/capacity/cloudflare/registry-toolchain
docker buildx build --load --pull=false --network=none \
  --build-arg "BIFROST_API_IMAGE=$IMAGE:release-1" \
  --label "org.opencontainers.image.revision=$RELEASE_SOURCE_REVISION" \
  -f deploy/capacity/cloudflare/Dockerfile.containers \
  -t "$LINUX_IMAGE" deploy/capacity/cloudflare
python3 spikes/cloudflare-pyodide/dev/e2e/combined-stack.py "$PROJECT" "$IMAGE" \
  --containers-image "$LINUX_IMAGE"
node spikes/cloudflare-pyodide/dev/e2e/combined-provider.mjs .rehearsal/config.json &
provider_pid=$!
# After .rehearsal/ready is written:
docker compose -p "$PROJECT" -f rehearsal-compose.yml -f rehearsal-overlay.yml \
  up -d --pull never api worker cloudflare-connector
# After the API and worker are healthy:
python3 spikes/cloudflare-pyodide/dev/e2e/combined-rehearsal.py --containers-only
```

This rehearsal starts with empty script, registry and application
inventories. The local fixture supplies the pilot opt-in before Activate
enqueues Enable and pins one Linux workflow through the private review
model. The release and OCI tool fixture checks the exact package inputs
and records the registry mutations locally. Pinned Miniflare runs the
uploaded registry, checks signed start and stop requests, and uses Docker
to run the paired Linux image through the exact worker ingress. Docker
placement readback is separate from the registry response.

The driver requires a successful native import, a subprocess result and an
SDK request on a Container worker. It waits for idle settlement, verifies
that Scale In retains the provisioned resources, then checks re-enablement
with the same revision and a fresh settled canary. Disconnect must remove
the scripts, namespaces, application and registry tag.

The rehearsal uses synthetic OCI and signature inputs and a local provider.
It exercises neither public registry access nor hosted provider qualification.
After the run, remove only the rehearsal's Compose project and network and
the local proxy alias `$PROJECT-containers-proxy:local`. Stop the provider
process even when the driver fails.

## Autonomous agent qualification

Use a fresh `agentrun-<sha>` archive and the same loopback debug stack as
Phase 5a. Run `phase5a.py prepare --agent` before booting the stack and
`phase5a.py run <generated-runtime.mjs> --agent` after it is ready. Keep Linux
workflow capacity available. The harness restricts the disposable Linux pool
rows so the agent and summary claims cannot race the Workers fixture.

The local stub exercises tool-less completion, ordinary workflow dispatch,
replay of a dropped terminal or tool ACK, cancellation during an open
provider stream, and uncertain settlement after a Durable Object reset.
Then the Linux consumer replays the two successful inputs against a
transport-local fixture. The comparison covers terminal output, model and
tool steps, usage, child execution status, and lineage. It excludes timings,
generated IDs and framework-specific request composition diagnostics.

## Chat qualification

Use a fresh private `/opt/bifrost-tests/chat-<sha>` archive with the same
loopback debug setup. Run `phase5a.py prepare --chat` before boot and
`phase5a.py run <generated-runtime.mjs> --chat` after readiness. This first
runs autonomous regression cases, then exercises the authenticated Chat
WebSocket subscription, a lost chunk ACK, a second turn using conversation
history, cancellation and launcher reset. The Linux writer uses the same
scripted response through a local HTTP transport fixture. The comparison
includes conversation sequences, persisted messages and model usage.

Run the outbox transaction tests separately with
`./test.sh tests/e2e/platform/test_cloudflare_chat_outbox.py`. Stop the
debug stack before starting the test stack.

## Phase 6 managed Workers capacity

Use a fresh, private `/opt/bifrost-tests/phase6-<sha>` archive with an
initial Git commit and no remotes. Use Node 26 and the same packaged Python
runtime as Phase 5a. The harness generates a throwaway TLS certificate and
limits the provider HTTPS listener to the Docker bridge. It runs the
provider and launcher as separate Workers in local workerd.

```sh
umask 077
(cd spikes/cloudflare-pyodide/dev/tenant-workers && npm install --no-audit --no-fund --package-lock=false)
python3 spikes/cloudflare-pyodide/dev/e2e/phase6.py prepare
BIFROST_FORCE_PORT=1 nohup setsid ./debug.sh up >.phase6-debug.log 2>&1 </dev/null &
# Wait for "Stack is up." in the private boot log.
nohup setsid python3 spikes/cloudflare-pyodide/dev/e2e/phase6.py run /absolute/path/generated-runtime.mjs \
  >.phase6-run.log 2>&1 </dev/null &
# After phase6-idle-release, or on failure:
BIFROST_FORCE_PORT=1 ./debug.sh down
```

The harness pauses both Linux launchers before reloading the API once with
the approvals and provider settings. It checks that the Linux-only
dispatch queues with zero managed incarnations, then submits an approved
revision and checks that the real API commits the profile and enrollment
binding before provider start. Two approved dispatches run under distinct organizations in one prepared
two-slot incarnation. Both Python children must enter their held response
before either is released. Each run has one claim and one durable log, and
its credential is revoked after settlement. The harness waits for idle
retirement and confirmed provider deletion,
then checks that the launcher does not reconnect across one alarm watchdog
window. It unpauses the Linux launchers when it exits.

Dual-provider planning is covered by the API unit test in
`test_external_capacity.py`. Configure `BIFROST_WORKER_CAPACITY_URL` and
`BIFROST_WORKER_CAPACITY_TOKEN` for the primary provider, and
`BIFROST_CLOUDFLARE_CAPACITY_URL` and `BIFROST_CLOUDFLARE_CAPACITY_TOKEN`
for the optional burst scope. In a single Cloudflare scope, use the primary
settings. Both Cloudflare modes require
`BIFROST_CLOUDFLARE_WORKER_HELLO_ENABLED=true` and reviewed workflow
approvals. Keep the TLS keys and logs inside the private archive.

Run the local provider and shared HTTP conformance suite with
`node --test spikes/cloudflare-pyodide/dev/tenant-workers/capacity-workerd.test.mjs`.
The restart hook recreates Miniflare with the same SQLite storage.

The CT128 run from archive `d9d7c1333` used project
`bifrost-debug-ebc1b3ce`. Execution `bc11af91-a0f2-41a6-bbac-3b898ca1a23d`
committed on incarnation `5a0aab8d-5d6c-4175-b935-d6b5485586ad`.
The provider confirmed deletion after idle retirement. Linux-only execution
`5ba006cc-71b4-4203-b4ee-337f7e333736` remained queued throughout.

### Installed Solution variant

Use the same Phase 6 preparation in a private `explore-sol-<sha>` archive.
Pass `--solution` to `phase6.py run`. The test deploys the same Solution and
manifest UUID into two distinct owner organizations via the ordinary deploy
job. Both installs use the same helper path with different pinned bytes. The
test checks each result, the live SDK call, concurrent claims and credential
revocation on settlement. It uses only local Miniflare and the disposable API.

### Three-scope Containers overflow

Use a new private `/opt/bifrost-tests/containers2-<sha>` Git archive on CT128.
Check that the host is free of other Bifrost test and debug stacks before
starting. Use Node 26, the local runtime fixture and the Phase 6 preparation
above. Keep the debug stack owner running while executing each check separately.

```sh
python3 spikes/cloudflare-pyodide/dev/e2e/phase6.py prepare
BIFROST_FORCE_PORT=1 ./debug.sh up
```

From the same archive, using the project printed by the owner:

```sh
node spikes/cloudflare-pyodide/dev/e2e/containers-conformance.mjs "$project"
python3 spikes/cloudflare-pyodide/dev/e2e/phase6.py run "$runtime" --containers
```

Stop the debug owner after both checks. The provider uses #1425's actual
HTTP handler, registry, ledger and container lifecycle with Docker behind
the container binding. The conformance hook replaces the in-process actors
while retaining their storage, and the Docker process is observed and
stopped independently. The three-scope check holds a run on the VM scope,
proves that queued `solution.update_check` demand launches a Container, serves
a shallow Git fetch from a local TLS fixture, and verifies the persisted
incarnation, stage, and version from the configured tag and subdirectory. It also
verifies claim ownership for an approved Workers workflow and a Linux
subprocess workflow, checks the Containers environment names and requirements
inventory, and requires canonical retirement and confirmed physical stop.
It uses the debug image for each physical launcher and caps those launchers
at 1 GiB. Remove the task-owned debug resources and image at the end.

## Phase 5b hosted launcher

Use a new mode-0700 `/opt/bifrost-tests/phase5b-<sha>` archive with a local Git
commit and no remotes. Run the same preparation and foreground debug-owner
commands with `phase5a.py prepare --hosted`. This seeds a random admin password
and platform signing key. After the stack is ready, run
`phase5a.py run /absolute/path/generated-runtime.mjs --hosted` under Node 26.
Capture its output in a private log. The runner starts a loopback relay and
writes `.phase5b-deploy.json` with mode 0600. Its enrollment window is 180 seconds.

Verify the Cloudflare account and zone, a collision-free Worker and tunnel
name, and the existing account spend policy with `spend-guard.mjs list`.
Configure a fresh named tunnel to the relay port, with anchored ingress rules
for `/api/worker-control`, `/api/sdk/tables/list`, `/api/sdk/runtime/logs`, and
`/auth/refresh`, followed by `http_status:404`. The relay accepts only POST on
the three HTTP routes. Snapshot module reads are handled in the gateway. Verify
the login and OpenAPI routes return 404, and the unauthenticated tables request
reaches the dev API and returns 401. Start cloudflared in the archive using
`--token-file` with a private file and a bounded process lifetime.

Deploy the launcher configuration with `cf deploy --secrets-file <private-json>`.
Use the four enrollment identity fields from the private relay file, set the
fixed tunnel HTTPS origin and enable launcher mode. The secret JSON contains
only `BIFROST_WORKER_CONTROL_SECRET` from the relay file. Stage the compiled
runtime and its loader files as in the staging instructions. With `cf`
`1.0.0-beta.31`, the deployment build requires local `cf` and Wrangler 4.161.0
or later. Install these in the deploy workspace with `npm install --no-save
--no-package-lock cf@1.0.0-beta.31 wrangler@4.161.0`.

If the initial enrollment window expires before the first Cron Trigger
invocation, stop the connector while the schedule propagates. Once Workers
Logs show the launcher dialing, run the same hosted command with
`--resume-enrollment` and bring the connector back up. This reopens the same
loopback port and accepts an idle incarnation. Keep the original private
enrollment file until the trial ends.

The relay records sanitized wire events and holds the first successful SDK
response for each restart case. Upload the replacement in advance with
`cf workers versions create` using the same enrollment and secret. On
`redeploy-requested`, activate the prepared version at 100 percent with
`cf workers deployments create --code-update-strategy-mode immediate`.
The relay expects recovery inventory and a single uncertain terminal for the
interrupted route. The API restart case reloads the application process and
expects live-route adoption. If the observer disconnects during activation,
`--resume-enrollment --resume-reset <workflow-route>` observes the existing
claim. `--resume-enrollment --from-reset` starts at the reset case once earlier
cases have been qualified and the previous claim has settled.
Query `cf o11y telemetry query` from outside the Worker config directory,
filtered to the trial Worker, for CPU-limit evidence and the 4001 close.

Stop the task-owned connector, remove the exact trial DNS record, tunnel and
Worker, then run `BIFROST_FORCE_PORT=1 ./debug.sh down` from the task archive.
Verify the connector is gone, `cf` reports no live trial tunnel, DNS record,
Worker or Durable Object namespaces, and Docker has no matching project
containers, volumes, networks or local images. Remove the private enrollment,
connector and secret handoff files.

### Verified hosted run, 2026-11-18

CT128 project `bifrost-debug-c4d59bff` used the compiled runtime digest from
Phase 5a. Worker and tunnel were both named `bifrost-phase5b-69f706a71` in the
Midtown Technology Group account. Each listed execution had one claim owned
by incarnation `ac6c8ddf-4b1b-49d0-9a54-b6bad6665e8f` and zero dispatches
remaining after settlement.

| Check | Execution | Evidence |
|---|---|---|
| Happy path | `2b25379a-f164-41fc-8f3c-b2d565817a54` | Hello 52, generation 2. `completed`, DB `Success`, one durable fixture log, same credential HTTP 200 → 401. |
| Allowance | `3ba66635-3765-4a9c-9802-bee3466d99d4` | Limit 4, three successful origin tables reads, then Cloudflare reported `Too many subrequests`. `completed`, DB `Failed`. |
| API restart | `d93d4e6e-c3c0-459c-ba6e-36d2468e1a5a` | New API owner, hello 52 → 54, generation 2 → 3 and live inventory. DB `Success` under the original claim, one durable fixture log. |
| DO reset | `328186f8-6ca8-4707-97bc-b3b1950c85a7` | Immediate activation of version `0099c151-9d9d-42e5-ab29-eb6c436b999d`, hello 101 → 102, generation 5 → 6. One `uncertain`/`LauncherReset` terminal in inventory and on the wire, one ACK, DB `Failed`. |
| CPU budget | `71d8aa31-1c03-42ff-b667-ce86394ae0cb` | Workers Logs classified the failed child invocation as `cpu_limit`. With the 1,000 ms budget, the launcher reported `uncertain`/`TenantHostOutcomeUnknown` and the DB recorded `Failed`. |
| Retirement | Same incarnation | API refused hello 103 with 4001. DB remained `retired` with last admitted hello 102. Workers Logs recorded `launcher_control_closed 4001 true`, and the 65-second observer saw no further hello. |

The first hosted preflight exposed a Node HTTP relay failure on a streamed
POST. The first hosted claim then exposed the missing runtime log ingress.
The initial reset used a build-and-deploy operation that ran beyond the
observer deadline, after which the original claim settled once with a retained
transport error. The measured reset above activated a pre-uploaded version
while its own SDK response was held.

Workers Logs showed 255 invocation records and 21.0 aggregate CPU-seconds
across the trial. Cloudflare readback verified zero trial Worker, namespace,
DNS and connector matches and confirmed the tunnel deletion. Docker reported
zero containers, networks, volumes and task images for both trial projects.

## Phase 7 hosted three-scope qualification

Use a private, newly initialized `<test-root>/hosted-<sha>` archive.
Set `PHASE7_ZONE` to the dedicated DNS zone in the harness and deployment
environments. Run `phase7.py prepare <trial-host>` before starting the debug
stack. Start only with at most one other Bifrost test or debug stack on the
dev guest and at least 8 GiB available in `free -g` on its host. Count the
chat reservation as one stack. Use short SSH checks a few minutes apart if
the gate is busy. Use
`BIFROST_FORCE_PORT=1 ./debug.sh up` with a detached owner and a private log.
Use Node 26, the pinned packaged runtime, and the staged runtime loader files
from Phase 5b.

Use a dedicated `bifrost-hq-<nine-hex>` prefix. Check the exact account, zone,
Worker prefix, Durable Object namespaces, container applications, registry
image, tunnel and DNS name before creating resources. Run
`node spend-guard.mjs list` with the trial account selected. Both provider
configs use one outstanding incarnation and the Workers profile declares two
workflow slots. The Containers application uses one basic instance
with a digest-pinned image built from `api/Dockerfile`. Its embedded
`BIFROST_VERSION` must equal `PHASE7_NAME`.

Copy each `phase7.config.ts` to `cloudflare.config.ts` in its own scratch
deployment directory. Deploy the managed launcher and the Workers capacity
provider separately. Deploy the Containers provider with `PHASE7_IMAGE`
set to the dedicated registry image digest. The providers each receive
their own secret file containing only `CAPACITY_TOKEN`. The managed launcher
receives the derived incarnation credential over its private DO binding.
Use `cf deploy --dry-run` before applying each configuration.

For the agent and runtime pass, deploy the private agent and summary
entrypoints with the trial HTTPS origin plus `/__fixture/v1` as their
endpoint. Run `phase7.py run <generated-runtime.mjs>` and wait for
`hosted-relay-ready`. Use the mode-0600 `.phase7-enrollment.json` to stage
the manual launcher under its own Worker name, with both private provider
bindings. Upload the replacement using `cf workers versions create` before
the reset fixture reaches its hold. At `redeploy-requested`, activate that
version immediately. The harness verifies a summary and autonomous run
against the stub, CPU settlement, DO recovery and retired hello fencing.
The harness removes its enrollment file on exit, including after retirement.

For the placement pass, use `phase7.py run <generated-runtime.mjs> --placement
<private-provider-handoff.json>`. The handoff has `workersUrl`,
`containersUrl`, `workersToken`, `containersToken`, and
`containersMode: "standard"`. The standard provider Worker name ends in
`-containers`. Use HTTPS Workers
endpoints and mode 0600. The harness seeds one approved loose workflow and
one approved Solution workflow in separate organizations and holds both
inside SDK calls on the same two-slot incarnation. It holds the sole VM
worker and sends a Linux subprocess workflow through Containers. Use the
printed execution IDs with the hosted invocation and application records
to measure scheduling and cold start. Require both the API's deletion
receipt and an independent Container instance readback before claiming
physical stop. API idle retirement and the Cloudflare inactivity timer
have separate evidence requirements.

For a natural inactivity probe, deploy the trial Containers provider with
`PHASE7_CONTAINER_EXPIRY=1` and run
`phase7.py run <generated-runtime.mjs> --container-expiry <handoff>`.
Use the same preparation as for placement, with `containersMode: "expiry"`
and the `-expiry` Worker URL in the handoff. The expiry Worker and Container
app use separate names. This mode holds
the VM primary and submits a Linux overflow workflow. The trial Container DO
sets a two-minute platform inactivity timeout and omits its exit monitor
so the DO can go inactive. Standard capacity behavior remains in the
regular provider entrypoint.

After `container-expiry-wait`, provider requests from the dev API are held
at a local relay. The worker control tunnel stays connected. Use `cf`
application and instance readback to record the hosted instance while it is
active and again after it stops. The readback must go through the account
API so it does not wake the trial DO. Create the private
`.phase7-expiry-observed` marker in the archive only after the same
instance is inactive and the harness has emitted `container-expiry-wait`.
The harness removes any stale marker before emitting that event. Before
teardown it checks that Bifrost retired the incarnation as `stopped`, with
the original successful claim and no provider deletion receipt. It records
blocked and forwarded deletes separately.

The November 20, 2026 UTC hosted run settled Linux overflow execution
`a18733f5-5b88-4ead-947b-24d2bebd14fd` while the VM hold was still running.
The account API observed the same instance transition from `running` to
`inactive` before cleanup. Bifrost retired the incarnation as `stopped`
and kept the successful execution settlement. The relay forwarded zero
deletes during the proof. See the natural inactivity qualification in
[DESIGN.md](../cloudflare-launcher/DESIGN.md#natural-inactivity-qualification).

For a Workers-only follow-up, pass `--workers-only` to `phase7.py prepare`
and `--workers-only <private-provider-handoff.json>` to `phase7.py run`.
The handoff needs only `workersUrl` and `workersToken`. Seed and approve
the loose and Solution workflows while Linux is available for the install.
The harness switches to one managed two-slot Workers incarnation, holds
both tenants in SDK calls, and then runs the CPU-limit and version-activation
fixtures. Pre-upload the replacement version and activate it only on the
`redeploy-requested` event. The Workers-only tunnel needs worker control,
tables list, runtime log writes and refresh, plus the catch-all 404.

The hosted Workers-only pass on November 20, 2026 UTC used the rebased
`a3d3cc368` harness. The loose run
`088f69c6-8010-44a3-8d15-a3f8c0cc72c0` and Solution run
`b882201c-6714-4560-aca6-b7f40a7b4545` overlapped on one two-slot launcher
and both succeeded. CPU execution `9c837430-4163-4491-867a-7128636287bc`
made a successful SDK call before settling as `CpuLimitExceeded` /
`uncertain`. The replacement reconnected, inventoried and settled
`4eecde08-c9e0-48ca-8b52-8c7188052920` as `LauncherReset` / `uncertain`
without redispatch. The Container timer probe in that pass received no
instance. See the Workers-only
follow-up in [DESIGN.md](../cloudflare-launcher/DESIGN.md) for claims,
timing, and teardown evidence.

For hosted passes, the connector uses loopback port 31847 in the archive. Use
an exact-host tunnel ingress rule with this anchored path expression, then
`http_status:404`:

```text
^(/api/worker-control|/api/sdk/tables/list|/api/sdk/runtime/logs|/auth/refresh|/__fixture/v1/chat/completions|/api/sdk/modules-resolve|/api/sdk/modules/workflows/phase6_linux_only\.py)$
```

Check 404 for login and OpenAPI, and 401 for the unauthenticated tables
POST. Query `cf o11y telemetry query` from the OS temporary directory and
filter to the trial Worker names. Keep raw logs and handoff files private.
Use the teardown order from Phase 5b, including the Containers application
and the dedicated registry image tag. Read back the exact names after
deletion. Remove the private handoffs and token files.
