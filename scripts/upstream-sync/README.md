# Personal Fork Sync Candidate 0.3.0

Status: offline source and tests prepared; no schedule or live sync activated.
Owner approval for logged-on scheduled identity/keyring use and external
production/customer-consumer confirmation is pending; no answer has been received.
Task: `f2ab3925-3645-4f2e-8a37-6fac141adb19`.

## Scope and Design

Only `MTG-Thomas/bifrost-upstream:main` may track
`gobifrost/bifrost:main`. Keep this controller on its isolated automation branch,
outside the tracked main history. **Do not merge this branch into main:** a
fork-only controller commit would break subsequent pristine fast-forwards.
There is no proposed change to the repository's default branch.

The Node standard-library implementation has no dependencies or stored secrets.
Its CLI is read-only, using the existing authenticated `gh` installation:

```powershell
node scripts/upstream-sync/sync.mjs --dry-run --state-dir C:\Users\ThomasBray\AppData\Local\MTG\bifrost-upstream-sync\state
node --test scripts/upstream-sync/sync.test.mjs
```

Every invocation, including dry-run and reconciliation, must use the same
absolute local state directory. Changing directories to evade a hold is unsafe.
The candidate writer is exported for testing and future integration with an
approved controller. It pins both 40-character commits, verifies numeric repo
and parent identities and default branches, rejects ahead/diverged histories,
checks for a concurrent fork edit, and requests a server-enforced fast-forward
with `force: false`. It never merges, resets, deletes branches, changes rules,
approves reviews, or retries writes. Before PATCH it appends and fsyncs an intent
containing the exact before/after commits and unique attempt ID. Exact readback
appends a terminal `synced` receipt; a read failure or mismatch appends an
`uncertain` receipt. A crash before either receipt leaves the intent unresolved.
The next invocation checks the journal before any API calls and refuses even a
new dry-run/plan until explicit reconciliation. Lost responses receive one
readback; anything other than the intended commit retains the durable hold.
GitHub still enforces repository protections on the write. This candidate does
not implement a review/queue adapter if future protections require one.

## Proposed Recurrence (Not Installed)

### Offline Launcher and Trigger-Manifest Gate

`launcher.mjs`, `trigger-gate.mjs`, `launcher.template.json`, and
`snapshot.template.json` prepare the proposed launcher without an activation
entrypoint or scheduler registration code. The CLI accepts only:

```powershell
node scripts/upstream-sync/launcher.mjs --check scripts/upstream-sync/launcher.template.json scripts/upstream-sync/snapshot.template.json
```

The checked-in templates deliberately refuse: approvals remain pending, consumer
confirmation is false, source pin is unset, and the snapshot is incomplete.
Tests build explicitly synthetic fixtures; setting a fixture field to approved
does not confer owner authority. The launcher accepts only `fixtureOnly:true`
data, constructs a fixed GET-only in-memory API, and never uses `gh`, a credential,
the network, or the exported writer. Its declared fixture identity is not a live
Windows-session or GitHub-actor attestation. The result labels those facts false.

The offline launcher checks the exact host/principal/session declaration; refuses
credential, Git, and Node environment overrides; pins the known absolute binary
paths; and checks the controller's clean Git worktree, exact HEAD, branch,
repository root, and fork/upstream remote URLs. Git receives literal argv arrays,
`shell:false`, disabled fsmonitor, hidden windows, ten-second command timeouts,
and bounded output. No input is expanded as shell code. Source/environment/pin
changes stop validation rather than selecting another identity or checkout.

The shared exclusive process lock covers source validation, the durable hold
check, and the plan. State stays outside the source checkout; no new plan runs
with an unresolved receipt. Operational logs retain the existing bounds.
The scheduler data is validated for InteractiveToken, least privilege, hourly
minute 17, IgnoreNew, a five-minute limit, zero retries, no wake, and no catch-up.
The offline run has a four-minute deadline, and at most six ten-second read-only
Git commands; input JSON is limited to four MiB. No scheduler is created by this
template, and no logged-on token or keyring is accessed by it.

The trigger gate requires explicit safety evidence/consumer approval, a fresh
complete snapshot, exact repo/main IDs, a commit-bound untruncated recursive Git
tree, and a complete registered-workflow inventory. It compares every blob and
mode under `.github/workflows/` and `.github/actions/` to the reviewed manifest,
including unregistered definitions. Unknown/new/removed/changed definitions,
symlinks, incomplete pages, 404 inventory, changed Actions settings/grants,
rules, or branch protection all hold. It does not parse or execute YAML. A
matching fingerprint is only a consistency check against separately qualified
safety evidence; it cannot establish that workflows are inactive. A new
non-workflow upstream commit can pass if its definitions/settings remain exactly
reviewed and the refreshed evidence is bound to that commit.

The live evidence collector, actual session/keyring qualification, exact-source
approval, and live activation adapter are still separate gated work. Do not feed
the fixture API or declared fixture identities into a future write path. See the
explicit checklist in [ACTIVATION.md](ACTIVATION.md).

Use one approved external controller, hourly at minute 17, rather than placing
fork-only workflow commits on the mirror branch. On Windows, Task Scheduler
should use a single named task, `IgnoreNew` multiple-instance policy, a five-minute
execution limit, no automatic retries, and the already-approved execution account.
Do not copy the interactive credential into a task, secret, or environment file.
The account must already have approved unattended access; interactive `gh auth
status` alone does not establish that approval or availability while logged out.

The implementation already uses an exclusive-create filesystem process lock,
with host/PID/unique ownership and no automatic stale-lock stealing. Operational
logs retain at most 30 run files, each at most 16 KiB. They contain timestamped
status events, never credentials or API response bodies. The separate append-only
receipt ledger is safety state and is never rotated with those logs. It records
attempt IDs, target scope, commits, and intent/uncertain/terminal outcomes; retain
it across upgrades. Task Scheduler's last exit result would add failure visibility
after installation. A future reviewed launcher must pin the source version;
there is no installed launcher or task today.

A failed or divergent run must not launch repair work. No-op runs perform no
write. A later hourly invocation re-plans only if no prior attempt is unresolved.
Runtime installation and scheduler acceptance remain pending the gates below.

Manual recovery: disable only the controller task; inspect live main and the
recorded intended SHA; run the dry-run command. Preserve fork-only commits on
their existing branches and obtain a reviewed resolution for any divergence.
Never reset main or force-push it to resume. If a write outcome is uncertain,
read the current reference before taking any further write action. With no active
process lock, the read-only reconciliation command is:

```powershell
node scripts/upstream-sync/sync.mjs --reconcile --state-dir C:\Users\ThomasBray\AppData\Local\MTG\bifrost-upstream-sync\state
```

This command makes only GET requests and updates local receipts. It clears a
hold only when main exactly equals the recorded intended SHA. Observing the old
SHA, another SHA, or a failed GET never establishes that the earlier request
cannot still complete, so the hold remains. There is deliberately no automatic
"abandon and retry" command.

After a process crash, `process.lock` remains. Stop/disable the controller and
confirm the recorded host and PID against native process evidence. A running or
reused PID, different host, partial lock, or uncertain process state is a stop.
Only after the exact process is confirmed dead may the operator remove that
single lock file from the fixed state directory. Do not delete the directory,
journal, or any receipts. Then reconcile as above; deleting the lock never clears
the intent hold. A torn or corrupt journal also blocks; retain it for evidence
and reviewed recovery. The tests rehearse these steps with real child processes.
Crash durability uses fsynced files on one local filesystem; network storage,
cross-host locking, power-loss/storage-controller guarantees, and adversarial
state-file changes are not qualified by these tests.

## Verified Preflight, 2026-11-02

- Fork ID `1441017913`, parent/source `gobifrost/bifrost` ID `1074603568`.
  Both default branches are `main`.
- Fork main `e611d31bbba3d7601980271c774cad5e821b1e97`;
  upstream main `3603b2d3bd172eadf30a8516cc8fa6afc14f1ad5`.
  Git reports zero fork-only commits and nine upstream-only commits. REST
  compare and the live dry-run independently confirm a fast-forward candidate.
- `gh` is logged in as MTG-Thomas through the existing keyring credential
  (repo/workflow scopes); SSH is the existing Git transport. No credential was
  minted, copied, printed, or installed for unattended use.
- Rulesets API returned `[]`; main protection returned 404 / branch not protected.
  These are fork facts, not gobifrost's merge-queue policy. No controls changed.
- Actions enabled, allowed actions `all`, default token permissions `read`,
  PR approval permission false. Actions secrets and variables lists were empty.
  No sync task appeared in the local Task Scheduler inventory.
- Registered workflows: active Dependabot auto-merge and dynamic Pages build.
  `ci.yml` and `nightly.yml` workflow API lookups returned 404, although their
  definitions exist in main. Absence of registered runs does not prove a future
  external write cannot register/run those definitions.
- Upstream `ci.yml` has main-push `Promote Dev Images` with packages-write and
  `gobifrost/bifrost-api` / `gobifrost/bifrost-client` GHCR destinations. CodeQL
  and Scorecard have main-push triggers and weekly schedules. Nightly has a
  two-hour browser-test job. Do not enable inherited workflows en masse.
- No repository webhooks returned. Recent deployments were GitHub Pages only;
  Pages uses `gh-pages`, which this controller never updates. This does not prove
  the absence of external polling consumers not represented in GitHub.
- Preserved contribution refs:
  `1177-execution-listener-cleanup` at
  `24c4556b97fe8a968dc4086f9ea8af56afaf342e` (upstream PR 1178), and
  `1176-chat-restoration` at
  `8487e192c1ddeb781386c2dcc9a99c806d7508e9` (upstream PR 1179).

## Activation Packet

See [ACTIVATION.md](ACTIVATION.md) for the exact proposed Windows identity,
host, scheduler configuration, credential handling, API contract, trigger
consequences, approval wording, and unimplemented activation gates.

No permission denial was observed: the CLI reports existing `repo` and
`workflow` OAuth scopes and repository push authority. Those observations do not
authorize scheduled reuse. Choosing an external controller is an architecture
decision to keep main pristine, not a provider refusal. No new scope or account
grant is proposed for that design. Do not create a credential, widen a workflow
grant, disable security workflows, change Actions policy, or use another
repository as a controller to work around an unsettled activation decision.

After the packet is approved and trigger safety is established: integrate the
tested writer with the proposed reviewed launcher, review the exact source, pass
its normal CI/review gates, perform one bounded update and readback, verify a
second no-op, then activate and read back the hourly schedule. Source publication,
review/merge, schedule activation, and successful synchronization are separate
acceptance items. None of the latter three is claimed here.

## Validation

Revision 0.3.0 adds fixture-only launcher/gate tests alongside the 23 durable sync
tests; all 74 targeted tests pass. Syntax checks and `git diff --check` also pass.
No broad application/CI suites were run because this changes only the isolated
controller candidate. No live API read/dry-run was repeated in this revision. The final Actions
inventory denial remains HTTP 403 / rate limit, request ID
`173D:3AFD6F:451A3C:E3E785:6AE89435`, `2026-11-02 13:46:29 UTC`.
There is no known eligible reset evidence, so no retry or identity substitution
was attempted. Source publication uses the already-established Git SSH remote;
it is not an alternate way to retrieve the denied Actions inventory.

Targeted command for 0.3.0:

```powershell
node --test scripts/upstream-sync/sync.test.mjs scripts/upstream-sync/launcher.test.mjs
```

Revision 0.2.0: all 23 targeted tests passed. Refreshed live dry-run found 11
upstream-only commits ending at `20c9db7be9fdbdd617ecd338adba11eb4657c292`;
fork main remained `e611d31bbba3d7601980271c774cad5e821b1e97`. The earlier
nine-commit snapshot above is historical, not a pinned update authorization.
The 11-commit comparison response was 1,059,719 JSON characters and initially
exceeded Node's subprocess buffer (`spawnSync gh ENOBUFS`). The adapter now asks
`gh --jq` for ancestry fields only, without transporting patches. Its regression
test and the refreshed live dry-run passed; no buffer limit was increased.
Read-only reconciliation against the local live state returned `no-hold`.
The real PATCH cases remain synthetic; no main synchronization was attempted.

`node --test scripts/upstream-sync/sync.test.mjs` covers dry-run, no-op,
fast-forward request/readback/idempotency, ahead/diverged histories, identity
mismatch, concurrent edit, lost successful response, denial, and wrong readback.
It also covers fsync/receipt failure, intent before PATCH, timeout plus failed
readback followed by a fresh process, actual child crashes before PATCH and after
simulated server acceptance, cross-process lock exclusion, safe reconciliation,
torn-journal blocking, bounded logs, and CLI rejection of writes.
The mutation cases use a fake GitHub API; they do not prove live write permission,
workflow suppression, runtime scheduling, or a successful repository sync.
`node --check scripts/upstream-sync/sync.mjs` checks syntax. The live dry-run is
read-only and found the nine-commit update above. No Bifrost application code or
contracts changed; Docker application suites, broad CI, and production testing
were not run.

GitHub reference update semantics:
https://docs.github.com/en/rest/git/refs#update-a-reference

GitHub token-trigger behavior:
https://docs.github.com/en/actions/concepts/security/github_token
