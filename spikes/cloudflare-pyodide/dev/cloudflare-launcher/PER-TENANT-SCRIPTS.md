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
  persists backoff and permits up to eight outer provider deletion attempts
  per managed incarnation. Each attempt reserves the provider request and
  two Durable Object calls. Disconnect and revision replacement can retire
  an owned revision after its remaining incarnations are all terminal and
  stranded. They release the slots after script and namespace absence is
  verified. Missing meter receipts retain their usage reserves, and confirmed
  ledger namespace destruction closes outstanding receipt acknowledgement.
  Scale In retains the deployment and reports the Disconnect recovery option
  if it cannot verify retirement.

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
The warm hint targets the same ticket and physical slot as dispatch. Each
host independently checks readiness and expires its linger window. Cold boots
are serialized per launcher because local workerd can fail when Python
children boot simultaneously. Execution can overlap after entry. Queue-depth
prewarming may be evaluated separately with demand and spend evidence.

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
