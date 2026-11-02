# Activation Decision Packet — Candidate 0.3.0

Prepared only. No task, credentials, permissions, workflow grants, Actions
settings, production resources, or customer resources changed. No main sync.
The CLI accepts only read-only remote dry-run and reconciliation operations.
The exported writer remains a tested candidate without an activation entrypoint.

## Concrete Proposed Execution Design

| Item | Proposed Value / Existing Evidence |
| --- | --- |
| Host | Existing workstation `LT002-H8WL8H4` |
| Windows principal | Existing `AzureAD\ThomasBray`, verified by `whoami` |
| GitHub actor | Existing GitHub CLI OAuth identity `MTG-Thomas`, user ID `87775189` |
| Credential | Existing Windows keyring entry used by `gh`, never exported or copied |
| Controller | Existing task-owned worktree `C:\Users\ThomasBray\.t3\worktrees\bifrost\upstream-sync-f2ab3925`, exact reviewed source SHA and clean status required on every run; any branch movement stops the launcher |
| State | `C:\Users\ThomasBray\AppData\Local\MTG\bifrost-upstream-sync\state`, retained across releases; one local directory for every invocation |
| Scheduler | Windows Task Scheduler task `MTG Bifrost Upstream Sync`, created only after separate approval |
| Logon | `InteractiveToken`, least privilege; only while Thomas is logged on; no password, service account, S4U, SYSTEM, or elevation |
| Cadence | Hourly at minute 17 local time; `IgnoreNew`; five-minute execution limit; no restart-on-failure, no catch-up backlog, no wake-to-run |
| Runtime | Existing `C:\Program Files\nodejs\node.exe`, `C:\Program Files\Git\cmd\git.exe`, and `C:\Program Files\GitHub CLI\gh.exe`; paths checked by the offline candidate; only Git is executed locally |
| Scheduling cost | Existing local machine; no paid scheduler or new hosted runner subscription |
| Failure visibility | Nonzero task result plus bounded local run logs and persistent receipt/hold; no new email/webhook credentials |

This is a specific architecture proposal, **not an already-approved unattended
use or a denied GitHub permission**. It trades always-on service for use of the
existing logged-on session. It will not run while this workstation is asleep,
off, or Thomas is logged out. A headless/24x7 host is a different design requiring
separate identity, credential-store, scheduler, and approval evidence.

Microsoft documents that InteractiveToken runs only in an existing interactive
session: [logon types](https://learn.microsoft.com/en-us/windows/win32/taskschd/principal-logontype).
Task bounds use the documented [scheduler settings](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-settingstype-complextype).

## Existing Access Versus Requested Authority

Current read-only evidence: `gh auth status` reports keyring-backed MTG-Thomas
with `gist`, `read:org`, `repo`, and `workflow` OAuth scopes; repository metadata
reports push/admin authority. This is not proof of an authorized unattended
schedule and not a tested live PATCH. **No new OAuth scope, token, GitHub App,
secret, workflow permission, repo setting, branch rule, or subscription is
requested for the proposed local controller.** A later provider denial must be
retained and reported, with no caller or credential substitution.

Before installation, the reviewed launcher must verify the Windows principal,
resolve the pinned binaries, reject `GH_TOKEN`/`GITHUB_TOKEN` environment overrides,
verify `gh api user` equals login/ID above, and use the existing keyring entry in
that session. It must not run `gh auth token`, put a token in arguments, task XML,
environment files, logs, or repository content, or copy credentials to a new
user/machine. Credential-store access under the selected task principal still
needs a read-only scheduled-context qualification after approval. A login or MFA
challenge is a human step, not permission to mint another credential.

The offline launcher now tests the source/env/path/ref/scheduler consistency
checks with fixtures. It does not qualify a real Windows interactive session,
GitHub actor, or keyring: it never runs `gh` or a network transport. Actual native
identity/credential checks remain activation-adapter work requiring approval.
Scope availability and unattended-use approval remain separate facts.

## Exact Remote Operation

1. Under the process lock, read the local ledger. Any unresolved intent blocks
   all new planning/writes. Lock acquisition is atomic; stale locks are never
   stolen automatically.
2. GET `repos/MTG-Thomas/bifrost-upstream` and `repos/gobifrost/bifrost`; require
   IDs `1441017913` and `1074603568`, exact parent, and default branches `main`.
3. GET each `git/ref/heads/main`, pin before and intended after SHA. Equal is a
   no-op. GET `repos/MTG-Thomas/bifrost-upstream/compare/<before>...<after>`;
   require ahead-only, zero behind, and before as both base and merge base.
4. Future activation adapter must reread current rules/protections and qualify
   the exact target tree's trigger policy (below). Required PR/review/queue
   controls must use their normal path; the present writer cannot implement
   that path and must remain inactive if one is required. No reviews as Thomas.
5. Reread the fork ref; abort if it moved. Append/fsync intent before sending:

   `PATCH /repos/MTG-Thomas/bifrost-upstream/git/refs/heads/main`

   Body: `{"sha":"<pinned-upstream-main-SHA>","force":false}`.

6. GitHub enforces fast-forward on the current server ref, including a race
   after the last GET. This endpoint has no expected-old-SHA compare-and-swap
   parameter: it may accept another compatible advance, but cannot discard
   intervening fork-only commits. A new unrelated fork commit causes refusal.
   No retries, merge commit, reset, tags, other branch writes, or rule bypass.
7. Independently GET the fork ref. Exact intended SHA appends/fsyncs terminal
   `synced`; read failure or mismatch records `uncertain`. A crash leaves intent.
   Every subsequent invocation blocks until explicit read-only reconciliation
   sees the exact intended SHA. Seeing the old SHA is not a safe retry receipt.

GitHub documents `force:false`, success `200`, conflict `409`, and validation
`422`, and Contents-write / conditional Workflows-write requirements in the
[reference API contract](https://docs.github.com/en/rest/git/refs#update-a-reference).
The observed classic OAuth scopes already include `repo` and `workflow`; no
additional scope is proposed. Any actual denial remains a stop. A response code
or lost response is never accepted in lieu of reference readback.

## Downstream Trigger Safety — Separate Unsettled Gate

An OAuth-authenticated reference update is an external push. It does **not**
receive `GITHUB_TOKEN`'s recursive-workflow suppression. There is no PATCH
parameter that means "fast-forward but suppress Actions". Do not claim that
`force:false`, lack of tag writes, empty secrets, or workflow API 404s suppress
push triggers. See GitHub's [trigger semantics](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

Current fork Actions API: enabled, actions allowed `all`, default workflow token
read-only, cannot approve PRs. Only Dependabot auto-merge and dynamic Pages are
registered. `ci.yml` and `nightly.yml` exist in the main tree but workflow lookup
returns 404. Fork workflows are disabled by default per GitHub's [fork event
documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflows-in-forked-repositories),
but the current mixed inventory is not sufficient evidence of the future
behavior after this OAuth push. No API denial or permission failure was observed.

| Surface | Known Effect / Required Acceptance |
| --- | --- |
| CI `push:main` | `Promote Dev Images`, `packages:write`, attempts to promote `gobifrost/bifrost-api` and `gobifrost/bifrost-client` GHCR tags; no repository-owner guard. Do not rely on a presumed package denial to establish safety. |
| CodeQL / Scorecard | Main push and weekly schedules; security analysis/publishing and runner work. Preserve security controls. They are not a safe target for an incidental disable. |
| Nightly | Daily scheduled two-hour browser-test job; bulk Actions enablement could activate recurring compute. |
| Pages | Legacy source `gh-pages`; no update to that ref is proposed. No main-to-gh-pages path was found in the inspected source. |
| Tags / PRs | Writer creates neither; tag-release and new-PR triggers are not directly requested by this operation. |
| New Development Worker Image | Refreshed target `20c9db7be9fdbdd617ecd338adba11eb4657c292` adds `dev-worker-image.yml`; push filter is `dev/**`, plus manual dispatch. A main-only PATCH does not match that filter. It is still new workflow content that must appear in the reviewed trigger manifest. |
| External consumers | No repository webhooks returned; GitHub metadata does not enumerate independent polling/deployment consumers. Owner confirmation is still needed for this personal fork. |

**Zero-settings-change path:** obtain authoritative evidence that inherited
publication/expensive workflows remain inactive for an OAuth main update, and
owner confirmation that this fork has no external production/customer consumer.
The offline trigger-safety gate is now implemented/tested in `trigger-gate.mjs`.
It compares the proposed commit's complete `.github/workflows/` and
`.github/actions/` path/blob-SHA/mode map
and live workflow/Actions settings against the approved evidence. Unknown/new
workflow definitions or changed settings must hold before writing. Record exact
reviewed SHAs/settings with approval; never execute proposed upstream code to
make this determination. Its snapshot includes commit/tree binding, complete
pagination, freshness, rules/protection, workflow inventory, Actions settings,
and workflow permissions. It refuses 404/incomplete inventories. The future live
adapter must collect authoritative evidence and invoke this gate while holding
the same state lock immediately before intent/PATCH; the fixture-only launcher
does not collect that evidence or expose a write path.

If inactivity cannot be established, this zero-settings-change path is blocked
on evidence, not on a denied permission. The owner would need a separately
reviewed change of architecture or controls. Exact alternatives requiring new
scope of work are: fork-only per-workflow disabling of CI/nightly (changes Actions
state; does not by itself handle security-run compute), or upstream owner-guard
changes preventing fork publication and bounding expensive schedules. Disabling
CodeQL/Scorecard or globally disabling Actions would alter security controls and
is excluded. Upstream source changes are outside this fork-only task and would
need their own approved PRs/reviews. None of these changes is authorized or
performed here; do not use them as a workaround.

An in-fork scheduled workflow is another architecture, not a provider denial:
it needs controller commits on the default branch (breaking pristine tracking)
or a changed default branch, and an explicit writer permission grant. The
read-only default alone is not a technical refusal to allow a job-level grant.
The current request excludes making such grants/settings changes; this packet
therefore proposes the local logged-on controller instead.

## Minimal Decisions and Acceptance Sequence

Proposed owner approval wording, **not yet granted**:

> Approve scheduled use of the existing MTG-Thomas GitHub CLI keyring identity
> under AzureAD\ThomasBray on LT002-H8WL8H4, logged-on session only, for an hourly
> minute-17 fast-forward-only update of MTG-Thomas/bifrost-upstream:main from
> gobifrost/bifrost:main. Use the reviewed clean controller worktree pinned to
> the exact approved commit, stable
> local receipt/lock directory, IgnoreNew, five-minute limit, no retries, no new
> credentials/scopes/permissions, and no settings or production/customer changes.
> Accept that it will not run while logged out/asleep/off. Install/activate only
> after exact-head review, normal applicable gates, the trigger-safety evidence
> and pre-write gate above, and confirmation of no external production consumer.

Approval of this wording alone does not manufacture the missing trigger-safety
proof. The next concrete evidence action is a reviewer/owner determination of
fork workflow activation behavior for the inspected definitions/settings and
external consumers, without testing it by an unsafe write. If that proof is
unavailable, the owner must select a separately scoped control/source change;
the task remains inactive.

## Separate Activation Checklist — All Unchecked

- [ ] Receive Thomas's exact logged-on-only identity/keyring scheduling approval;
  the question has been asked, and there is no answer yet. Fixture approval
  strings and agent reports cannot satisfy this item.
- [ ] Receive owner confirmation of no external production/customer polling
  consumers for this personal fork.
- [ ] Establish known eligible reset evidence for the preserved Actions HTTP 403
  rate-limit denial before any retry; preserve the same caller/credential. There
  was no retry or identity swap during offline preparation.
- [ ] Qualify inherited workflow effects independently. Capture the complete
  current settings/inventory and commit-bound workflow/local-action blob map;
  record exact safety evidence and reviewer/owner approval. A matching map,
  workflow 404, or empty secrets is insufficient proof of inactivity.
- [ ] Review the exact source commit and pass normal applicable gates. Keep the
  controller branch out of pristine main. Fill a separately approved external
  config with the exact source pin and evidence references; do not edit the
  template into a claimed approval or store credentials with it.
- [ ] Implement/review the **live** adapter after approval: native session and
  keyring/actor qualification, bounded authoritative evidence collection, a hard
  outer deadline, and invocation of the tested gate under the shared lock before
  intent/PATCH. Never reuse fixture runtime/snapshot declarations as live proof.
  Preserve direct-writer refusal if branch protections require review/queue.
- [ ] After approval, register only the reviewed logged-on-only task and qualify
  read-only credential access in that actual context; read back its principal,
  action path/source pin, state directory, minute-17 cadence, IgnoreNew,
  five-minute timeout, zero retries, no wake and no catch-up.
- [ ] Perform one bounded authorized main sync with terminal receipt and remote
  readback, followed by a separate no-op; then enable/read back hourly operation.
  Do not report registration, acknowledgement, or a fixture test as a real sync.

Today only the offline launcher, consistency gate, templates, and refusal tests
are prepared. There is no scheduler, credential reuse, main sync, or changed
Actions/security setting. Source, publication/review/merge disposition, scheduler
activation, and actual synchronization remain separate acceptance items.
