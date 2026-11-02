# Personal Fork Sync Candidate 0.1.0

Status: source and tests prepared; no schedule or live sync activated.
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
node scripts/upstream-sync/sync.mjs --dry-run
node --test scripts/upstream-sync/sync.test.mjs
```

The candidate writer is exported for testing and future integration with an
approved controller. It pins both 40-character commits, verifies numeric repo
and parent identities and default branches, rejects ahead/diverged histories,
checks for a concurrent fork edit, and requests a server-enforced fast-forward
with `force: false`. It never merges, resets, deletes branches, changes rules,
approves reviews, or retries writes. Lost responses receive one readback;
anything other than the intended commit is a stop requiring reconciliation.
GitHub still enforces repository protections on the write. This candidate does
not implement a review/queue adapter if future protections require one.

## Proposed Recurrence (Not Installed)

Use one approved external controller, hourly at minute 17, rather than placing
fork-only workflow commits on the mirror branch. On Windows, Task Scheduler
should use a single named task, `IgnoreNew` multiple-instance policy, a five-minute
execution limit, no automatic retries, and the already-approved execution account.
Do not copy the interactive credential into a task, secret, or environment file.
The account must already have approved unattended access; interactive `gh auth
status` alone does not establish that approval or availability while logged out.

Each run should retain timestamp, source version, before/after commits, outcome,
and exit code in a bounded local log (30 daily files), with Task Scheduler's last
result providing failure visibility. A failed or divergent run must not launch
repair work. No-op runs perform no write. The next hourly run re-plans from live
state; it must never blindly replay a failed PATCH. Runtime installation,
locking/logging wrapper, and scheduler acceptance remain pending the gates below.

Manual recovery: disable only the controller task; inspect live main and the
recorded intended SHA; run the dry-run command. Preserve fork-only commits on
their existing branches and obtain a reviewed resolution for any divergence.
Never reset main or force-push it to resume. If a write outcome is uncertain,
read the current reference before taking any further write action.

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

## Activation Gates and Smallest Owner Action

No permitted recurring writer was established by this preflight. The existing
interactive credential is not an unattended-access approval. An in-fork
scheduled Actions writer would need a new write grant and fork-only main
history; neither is permitted by the request. An external personal-token writer
does not suppress inherited push workflows.

The owner must designate an **already-approved external scheduler and unattended
writer for this exact fork**, and provide an approved way to establish that its
main update cannot cause registry publication, deployment, or unbounded inherited
CI. If those approvals do not already exist, the current request explicitly
requires a stop. Do not create a credential, widen a workflow grant, disable
security workflows, change Actions policy, or use another repository as a
controller to work around this gate.

After that evidence is available: integrate the tested writer with that host's
existing scheduling/locking/logging adapter, review the exact source there, pass
its normal CI/review gates, perform one bounded update and readback, verify a
second no-op, then activate and read back the hourly schedule. Source publication,
review/merge, schedule activation, and successful synchronization are separate
acceptance items. None of the latter three is claimed here.

## Validation

`node --test scripts/upstream-sync/sync.test.mjs` covers dry-run, no-op,
fast-forward request/readback/idempotency, ahead/diverged histories, identity
mismatch, concurrent edit, lost successful response, denial, and wrong readback.
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
