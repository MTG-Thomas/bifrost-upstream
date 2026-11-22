# Cloudflare Tenant Engine Slots

The managed Workers lane runs approved workflows in Dynamic Workers behind
the private SDK gateway. Each attempt has its own run credentials. The child
sees an opaque handle and uses its TenantHost as `globalOutbound`. Provider
organization membership grants no special authority. The ordinary Cloudflare
capacity settings permission applies at Global.

## Admission and ownership

- The tenant comes from the persisted execution organization. A null
  organization maps to a single platform tenant. There are four slots per
  tenant and nine per installation by default. The final available slot
  goes to a tenant with zero occupied slots. Below that threshold, the oldest
  eligible dispatch wins, subject to per-tenant, launcher, memory, and spend
  limits. The rule reserves room for another tenant without naming hot tenants.
- PostgreSQL takes the installation row's exclusive claim lock before
  reading occupancy. The claim transaction assigns the workflow and
  occupies its ordinal together. The primary key is installation, tenant,
  and ordinal. The worker, dispatch, execution, and claim UUID bind each
  occupancy. The UUID is the fence and the suffix of the launcher route ID.
  Reconnect inventory is matched to that ticket before adopting a live route.
- A managed launcher advertises the slot protocol before the API opens its
  claim gate. Drain old managed launchers and verify cleanup before introducing
  the protocol across API replicas. An existing manually enrolled development
  launcher still uses its tenant-wide gate.
- Before sending the workflow, the API records that the exact ticket may
  have entered. A definitive refusal or completed result releases it.
  Cancellation before entry can release it immediately. Canonical workflow
  settlement, owner lease loss, and worker loss cannot release a possibly
  dispatched child. Unsent claims can be dropped as soon as ownership or the
  execution ends. A running child has a bounded observation window outside
  the launcher control queue. A confirmed stop releases the exact slot. An unconfirmed
  terminal drains its launcher. Acknowledging
  that terminal leaves the stop witness in the launcher's journal.
- The provider returns a deletion receipt only after all potentially live
  children have a durable stop witness. A stop command revokes credentials
  and retires the generation immediately. Capacity is restored after child
  completion is observed or exact managed deletion is confirmed. A lost
  child response or missing provider ledger can leave capacity held. Reset
  is insufficient evidence because the child can outlive its host. Stop
  confirmation RPCs use the launcher meter ceiling. The Bifrost controller
  persists exponential backoff up to one minute and attempts outer provider
  deletion for up to 24 hours after the first cleanup GET, including a
  failed GET. Each DELETE reserves the provider request and two Durable
  Object calls. Expiry appears in capacity status and preserves the worker
  reservation. Disconnect and revision replacement can retire
  an owned revision after its remaining incarnations are all terminal and
  stranded. They release the slots after script and namespace absence is
  verified. Missing meter receipts retain their usage reserves, and confirmed
  ledger namespace destruction closes outstanding receipt acknowledgement.
  Scale In retains the deployment. When retirement evidence is missing, its
  Action Required result identifies the worker IDs, revision, scripts and
  namespaces and gives the Disconnect recovery option.

## Engine identity and readiness

An ordinal is unique within a tenant across the installation. Each launcher
incarnation gets its own physical TenantHost name for that ordinal. That name,
the engine digest, invocation limits, and retirement generation form the
WorkerLoader key. A new claim fence leaves that key stable. A different
launcher incarnation, release code, or retirement generation gets a
different child, and the previous generation cannot borrow its successor's
gateway authority.

A child executes one attempt at a time. Concurrent attempts in one child
would share the Python interpreter and a single outbound binding. A stopped
or wedged invocation would also put every colocated attempt at risk. Each
TenantHost serializes access to just its child and gates SDK requests on the
active handle and generation.

Allocation prefers an available ordinal recently completed on that launcher.
The admission hint targets the same ticket and physical slot as dispatch. Each
host independently checks readiness and expires its linger window. Ordinary
cold boots are serialized per launcher because local workerd can fail when
Python children boot simultaneously. Execution can overlap after entry.

## Queue Pre-Warming

**Max Queue Pre-Warms Per Tenant** (`workers.queue_prewarm_per_tenant`)
defaults to zero. Operators can opt in when burst latency justifies
potentially unused boots and their estimated cost. Start with one additional
engine per tenant. A positive engine linger and a launcher that advertises
queue pre-warming are required.

When a managed launcher claims a workflow, the API checks unassigned,
Workers-eligible workflows for that tenant. It accounts for reusable engines
on that launcher, then caps new prepares by the configured maximum, open
tenant and launcher seats, and installation headroom minus the fair-admission
seat. If an older admissible dispatch for another tenant precedes any
proposed hint, staging skips the batch. This conservative check only
suppresses speculative pre-warming. Ordinary claims follow their existing
ordering.
The installation lock serializes the slot reservations with normal claims.
The launcher serializes cold boots, including queue prepares. Those boots
can overlap the queue wait and execution on other slots. Parallel Python
boots remain a hosted qualification item. Measure admitted boot latency
behind speculative work in the shared warm queue. The worst-case wait
before an admitted boot completes is about 20 seconds plus one boot.
The owning launcher can adopt a reserved slot without consuming another seat.
It gets a new workflow fence even when it takes a different queued workflow.
The independent preparation fence authorizes only a metered boot on that
same physical slot.

An unsent hint is invalid once the pinned queue entry is gone, unless the
slot has been adopted by a live workflow. Owner changes, drain and the
absolute prepare deadline close Loader authorization. Draining launchers
release their unadopted holds. A hint already in
flight can finish. An unused slot expires within the configured linger,
measured from reservation. Loader entry has at most 20 seconds within that
window. The launcher sends no idle pings or alarms. A completed workflow
uses the normal linger window.

## Evidence and limits

Run the local fixture with the pinned runtime and Node 26. Four attempts
for one tenant enter the real gateway with distinct credentials, then wait at
the same barrier while their own engine state remains isolated. A fifth
attempt reuses slot zero and its loader identity. PostgreSQL coverage races
independent claimers for the final installation slot.

```sh
CLOUDFLARE_CF_FETCH_ENABLED=false BIFROST_RUNTIME=/path/to/generated-runtime.mjs \
  node --test spikes/cloudflare-pyodide/dev/e2e/tenant-slots.test.mjs
```

Eight-way hosted concurrency, hosted memory headroom, and release-specific
billing still require qualification. The package builder's measured-bundle
gate applies to the new launcher bytes. See [usage accounting](PHASE1-USAGE.md)
and the [scale-out plan](SCALE-OUT.md).

## Combined Bundle Hosted Attempt

The disposable stack accepted approved Provider, Global and two other
organization fixtures. Enable returned `CF_WORKER_INGRESS_UNVERIFIED`
at the ingress TLS probe and kept admission closed. No workflow slots
were created. The hosted four-plus-four overlap, ninth-slot admission,
tenth-workflow wait and single-slot cancel remain unverified.

See the [hosted attempt receipt](../../results/cloudflare-scaleout-hosted-attempt.json).

## Hosted Retry Result

The retry approved fixtures for Provider, Global and two additional tenant
scopes. Enable stopped on the IPv4 HTTP ingress TLS probe before any
launcher incarnation or occupied slot was recorded. The requested four
Provider plus four Global overlap, ninth admission, tenth wait, and
single-slot cancellation are still unverified on Cloudflare.

See the [sanitized hosted retry receipt](../../results/cloudflare-scaleout-hosted-retry.json).

## Combined Hosted Qualification Slot Result

The disposable Compose stack staged scoped loose and Solution workflow
fixtures. The worker preflight and Enable's runtime canary succeeded.
Enable reported `remote_launcher`, so the run stopped at the required
execution-class gate. Provider and Global overlap, reserved ninth
admission, the queued tenth execution, and isolated cancellation were
not exercised. Teardown confirmed zero held slots.

See the [sanitized combined hosted receipt](../../results/cloudflare-scaleout-combined-hosted.json).

## Combined Hosted Re-run Slot Disposition

The disposable Compose stack staged scoped loose and Solution workflow
fixtures. The worker preflight stopped at the IPv4 TLS handshake before
Enable. There were zero launcher incarnations and zero held workflow slots.
Provider and Global overlap, ninth-slot admission, the queued tenth
execution, and isolated cancellation were not measured.

See the [sanitized re-run receipt](../../results/cloudflare-scaleout-combined-hosted-rerun.json).

## Combined Hosted Run With HTTP Connection Reuse

The 2026-11-22 UTC run passed the Existing HTTPS preflight and signed
canary as `remote_launcher`. The primary hold check then failed after
the local fixture reported Running. A subsequent teardown read found
the intended 24 MiB of free primary capacity. The run stopped at the
failed check with zero hosted workflow submissions and zero held
workflow slots at teardown. Cold and warm latency, Provider and Global
overlap, reserved-slot admission, tenth-run waiting and isolated
cancellation remain unmeasured in this run.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-connection-reuse.json).

## Observable-Gate Hosted Run

The 2026-11-22 UTC run verified the durable primary hold and completed
one cold Workers workflow on slot 0. The sent fence was released after
Success. The execution detail API returned null for the queue timestamp
and the measurement harness stopped after recording the cold workflow.
Provider and Global overlap, the ninth reserved slot, the queued tenth
run and isolated cancellation were not reached. Teardown showed zero
held workflow slots.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-observable-gates.json).

## Hosted Cold and Warm Slot Reuse

The run from `788ffc641` completed cold and warm workflows in slot 0
on one launcher. The hosted wrapper stopped when a post-reuse query
looked for the cold execution ID in the current slot row. The transport
witness confirmed one dispatch and a terminal ACK for each execution.
The burst, reserved ninth slot, waiting tenth execution and isolated
cancellation remain unqualified on hosted. Disconnect verified zero
held slots.

See the [sanitized run receipt](../../results/cloudflare-scaleout-combined-hosted-latency-reuse.json).

### Hosted Slot Qualification at `db39a3698`

The worker preflight passed. Enable stopped on `CF_OWNERSHIP_CONFLICT`
before admitting a workflow. The burst, reserved ninth slot, waiting
tenth, and isolated cancellation remain unqualified on hosted.
Disconnect verified zero held slots and live launcher incarnations.

See the [sanitized ownership-stop receipt](../../results/cloudflare-scaleout-combined-hosted-ownership-stop.json).

### Hosted Preset B Burst at `e7c59788c`

Four Provider and four Global executions held distinct slots at the
same time. All eight had entered 7.134 s after the last dispatch read.
The third tenant took the reserved ninth slot, and the tenth remained
unassigned through two eligible passes. A confirmed cancel released
one fence and left the other eight unchanged. The tenth entered after
release. The scoped Solution fixture returned its expected value.
Reconcile, active Scale In and Disconnect verified retirement.

See the [sanitized preset B receipt](../../results/cloudflare-scaleout-preset-b-hosted.json).
