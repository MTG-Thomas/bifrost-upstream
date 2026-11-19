# Launcher eligibility at workspace revision `37581f6`

The install-aware census and `bifrost workflows cloudflare-report` analyzed
the same Git archive before and after the changes. The inputs were 317 workflow
manifest entries, five agent artifacts, and a 1,000 ms static CPU budget.
Counts include every rule observed for an artifact before or at the first
canonical refusal. Workspace source was read offline.

| Allowance | Candidate before → after | Host review before → after | Total before → after |
| ---: | ---: | ---: | ---: |
| 50 | 47 → 70 | 10 → 11 | 57 → 81 |
| 12 | 28 → 45 | 7 → 7 | 35 → 52 |

Every baseline candidate or host-review entry kept one of those statuses.
All entries in these totals still require source-bound operator approval.
The census estimates call sites and AST complexity. Runtime allowance,
duration, and actual package and SDK behavior need execution evidence.
The older source-only census, which omits SDK gateway findings and install
policy, also reproduced its baseline totals of 124 at 50 and 78 at 12.

| Rule | 50 before → after | 12 before → after |
| --- | ---: | ---: |
| CF_AGENT | 5 → 5 | 5 → 5 |
| CF_DYNAMIC | 16 → 20 | 16 → 20 |
| CF_EXECUTABLE_TYPE | 71 → 71 | 71 → 71 |
| CF_NATIVE | 0 → 2 | 0 → 2 |
| CF_OUTBOUND | 36 → 36 | 36 → 36 |
| CF_PACKAGE | 21 → 25 | 21 → 25 |
| CF_PARSE_BUDGET | 68 → 16 | 68 → 16 |
| CF_PROCESS | 2 → 2 | 2 → 2 |
| CF_SDK_GATEWAY | 58 → 29 | 58 → 29 |
| CF_SUBREQUESTS | 18 → 42 | 49 → 73 |
| CF_TRANSPORT | 2 → 20 | 2 → 20 |
| CF_URL_REVIEW | 11 → 35 | 11 → 35 |

## Parse budget and memory

The archive contains 35 Python paths larger than 128 KiB and 18 distinct
contents among them. Twenty-four paths belong to repeated-content groups and
ten files have a generated-source marker near the top. The largest six files
are byte-identical and about 3.87 MiB each. The next largest distinct file is
about 1.21 MiB. The two distinct files contain about 665,000 and 202,000 AST
nodes respectively, both above the 100,000-node bundle limit. The largest
also exceeds the launcher's 2 MiB per-source limit. The 16 remaining
parse refusals are all per-file size refusals.
The widened cap alone leaves the candidate totals flat because it exposes
other imports, operations, and request limits.

The analyzer uses a 384 KiB file cap, a 1 MiB bundle cap, a streaming token
guard on expanded files, and explicit AST node and active scope limits. Each
imported path is visited once per bundle. The API and reapproval job use
the same analyzer and bounded reader.

Peak working set was measured in fresh CPython 3.14.4 processes on Windows.
The harness preloaded one real file and called the single-file analyzer.
The increase column is peak working set minus the working set just before
analysis. The API reader additionally avoids loading oversized files.

| Real source size (bytes) | Peak before (MiB) | Peak after (MiB) | After peak above pre-analysis (MiB) |
| ---: | ---: | ---: | ---: |
| 4,055,960 | 35.96 | 36.87 | 0.22 |
| 1,263,949 | 33.25 | 33.32 | 0.22 |
| 322,599 | 34.17 | 68.80 | 36.11 |
| 255,477 | 32.41 | 58.27 | 26.01 |
| 249,944 | 32.29 | 56.85 | 24.62 |

The two largest sources were refused before parsing on both revisions.
Synthetic dense input was refused before AST construction. A synthetic
nested-scope probe raised peak working set by 105.68 MiB before the scope
guard and 52.03 MiB with the guard. The largest increase in the measured
real-file samples was 36.11 MiB. These measurements exercise analyzer
memory within the 128 MiB headroom target. They do not measure concurrent
API requests, API baseline RSS, or the full reapproval process.

## SDK gateway

The baseline 58 affected workflows reference users (43), roles (33), and
workflow dispatch (10). The groups overlap. With the larger parse cap but
the original gateway, 75 workflows carry a gateway finding. The observed
method counts are `users.list` 32, `users.create` 25, `users.get` 18,
`users.update` 2, `roles.list` 33, `roles.list_users` 29,
`roles.assign_users` 23, and `workflows.execute` 29.

Roles and Users now use the existing streamed request bridge. It forwards
the request to the fixed API origin with the run credential, strips cookies,
preserves the JSON body and query, and returns the API's response. The API
continues to check the original user's reach and permissions and apply
configured workflow powers.

The 29 remaining affected workflows include workflow dispatch (29),
`workflows.get` (19), `workflows.cancel` (15), and `executions.list` (12).
Raw SDK access to the workflow and execution roots each occurs in four
affected workflows. These routes are outside the launcher's child control
surface. Current-run logs use the existing SDK runtime endpoint.

## Outbound-enabled Solution installs

The 36 workflows in four outbound-enabled Solutions stay Linux-only.
Linux resolves modules from the install first, then shared workspace
source. External HTTPS access is governed independently in Workers. A
tier-aware immutable snapshot and send-time source revalidation are needed
to support shared imports with the same behavior and approval guarantees.
See the [approval design](../dev/cloudflare-launcher/DESIGN.md#phase-3-workspace-approval-and-placement).

## Verification and reproduction

Local Node 26.13.1 passed 409 tests across the six requested suites.
The tenant-worker suite used the retained local runtime fixture with
SHA-256 `fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5`
and the checked-in loader modules. The new gateway test exercises an
explicit Workerd module closure and an in-process upstream authorization
fixture.

On CT128, archive `coverage2-5b49cc4e62d2` passed all 5,410 selected
unit tests. API quality reported zero Pyright errors and passed Ruff.
Archive `coverage2-fac97a036859` passed all four approval E2E tests in a
separate invocation. The initial authorization fixture inherited full
workflow powers and left an audit row. The corrected fixture explicitly
uses restricted workflow powers, owns its audit cleanup, and verifies the
real API returns 403 for both tested mutations.

Use an archive of the exact workspace revision with both the pinned
baseline and candidate analyzer. Keep the raw workspace scan local.

```sh
PYTHONPATH=api python -m bifrost.cli workflows cloudflare-report "$WORKSPACE" --allowance 50 --aggregate-only --json
PYTHONPATH=api python -m bifrost.cli workflows cloudflare-report "$WORKSPACE" --allowance 12 --aggregate-only --json
PYTHONPATH=api python spikes/cloudflare-pyodide/dev/routing/solution-census.py "$WORKSPACE"
```
