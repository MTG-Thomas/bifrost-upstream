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
