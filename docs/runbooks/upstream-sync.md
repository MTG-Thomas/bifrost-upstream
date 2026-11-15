# Upstream Sync

`MTG-Thomas/bifrost-upstream` is a fork of `gobifrost/bifrost`. The
[`Sync Upstream`](../../.github/workflows/upstream-sync.yml) workflow keeps the
fork's `main` current with `gobifrost/bifrost:main`.

## How It Works

- **Schedule:** hourly at minute 17, plus manual `workflow_dispatch`.
- **Mechanism:** the job calls GitHub's `POST /repos/{owner}/{repo}/merge-upstream`
  endpoint for `main`. This is the same operation as the repository's
  **Sync fork** button.
- **Credential:** the repository's own `GITHUB_TOKEN` (`contents: write`,
  `issues: write`). No PAT, GitHub App, or repository secret is involved.
- **History:** when the fork has no local commits the branch fast-forwards.
  Otherwise upstream `main` is merged in, preserving this fork's own commits
  (desktop packaging, floorplan, and other fork-only work). Fork-only files are
  never overwritten by the merge.
- **Workflow side effects:** events created with `GITHUB_TOKEN` do not start new
  workflow runs, so the inherited upstream main-push workflows (image promotion,
  CodeQL, Scorecard, Nightly) do not run on a sync merge. A manually pushed
  commit to `main` still follows the usual trigger rules.

## When Concurrent Work Exists

The merge waits for the current queue state; it does not force-push and does not
discard concurrent commits. If `main` moves between the merge attempt and the
next schedule, the next run reconciles it normally.

## Upstream Changes To Workflow Files

`GITHUB_TOKEN` cannot create or update `.github/workflows/*`, and `workflows`
is not a permission the workflow can request (declaring it invalidates the file,
and every run then fails with zero jobs). When upstream `main` touches a
workflow file, the hourly sync returns HTTP 422 and opens the conflict issue.
Sync that merge once with a credential that has the `workflow` scope:

```bash
gh api -X POST repos/MTG-Thomas/bifrost-upstream/merge-upstream -f branch=main
```

A user-token sync is a normal push to `main`, so the inherited main-push
workflows run for it. The next hourly run resumes normally.

## Conflicts

A non-200 response fails the run and opens (or comments on) an issue titled
`Upstream sync conflict: resolve gobifrost/bifrost main manually`. The issue body
contains the API response.

Resolve a conflict the same way as any upstream merge:

```powershell
git fetch https://github.com/gobifrost/bifrost.git main
git switch main
git merge --no-edit FETCH_HEAD
# resolve conflicts, then run the scoped checks for the changed files
git push origin main
```

Close the issue after the conflict is resolved. The next scheduled run confirms
the no-op state.

Manual one-shot sync without a local checkout:

```powershell
gh repo sync MTG-Thomas/bifrost-upstream --branch main
```

## Pausing

Disable the workflow from **Actions → Sync Upstream → Disable workflow**, or:

```powershell
gh workflow disable "Sync Upstream" --repo MTG-Thomas/bifrost-upstream
```

## Limits

- Scheduled workflows run only from the default branch and only while the fork's
  Actions are enabled.
- GitHub disables scheduled workflows in public repositories after 60 days
  without repository activity. Routine upstream merges count as activity; if the
  schedule is disabled, re-enable the workflow.
- The workflow syncs `main` only. Tags, releases, and other branches are not
  mirrored.
- Manual pushes and PR merges to `main` still follow normal trigger rules and
  start the inherited upstream main-push workflows. In this fork,
  `Promote Dev Images` fails its merge-candidate gate with
  `expected exactly one .github/workflows/ci.yml merge_group run ... found 0`
  and stops before publishing, because the fork does not use a merge queue. That
  is pre-existing fork behavior, not a sync failure; sync merges themselves do
  not trigger these workflows.
