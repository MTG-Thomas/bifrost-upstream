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

## Initial parse budget and memory

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
real-file samples was 36.11 MiB. These are historical Windows 3.14 analyzer
measurements from the initial implementation. They do not establish a
memory ceiling for the Linux 3.15 API or reapproval job.

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

## Independent review follow-up

The Linux API, editor, Solution preflight, operator approval, and
reapproval job now use the isolated analyzer. The kernel caps each child's
virtual address space at 128 MiB and CPU time at 8 seconds soft and 9 seconds
hard. The parent admits one child per process, bounds the pipe protocol,
and cancels the child after 20 seconds of wall time including admission and
source reads. A wall timeout is retryable for reapproval. Deterministic
analysis refusals retain their rule IDs. The child limit covers its
interpreter, parser and analysis allocations. API baseline memory, source
buffers in the parent, and concurrent API processes are separate.

URL reconstruction caps the result at 4,096 characters and limits expression visits.
All files pass the token guard. A Python 3.11 f-string has a source-length
charge even when emitted as one token. Scope analysis measures retained
dictionary entries once and separately bounds cumulative copy work. The
bounded synthetic checks cover concatenation, f-strings, dense lists,
nested scopes, and large literals.

Measurements used CPython 3.15.2 from the API's pinned Linux base image in
a 1 GiB CT128 container from archive `coverage2-115b4687f564`. Both analyzer
revisions used the same child resource limits and bounded fixtures. The
kernel's exit receipt measured CPU time and peak RSS for each fresh child.

| Input | Bytes | Peak RSS before → after (MiB) | CPU before → after (s) |
| --- | ---: | ---: | ---: |
| Concatenation | 2,446 | 31.20 → 27.55 | 0.309 → 0.092 |
| F-string | 2,450 | 27.27 → 27.45 | 0.093 → 0.108 |
| Dense list | 40,012 | 44.34 → 44.46 | 0.182 → 0.215 |
| Dense list over token budget | 76,012 | 61.95 → 27.46 | 0.283 → 0.131 |
| Nested scopes | 1,613 | 27.39 → 27.18 | 0.088 → 0.088 |
| Large literal | 163,867 | 28.02 → 27.71 | 0.088 → 0.093 |

The highest revised wall time was 0.219 seconds. These samples measure
individual child processes and illustrate the guards. The enforced limits
come from the kernel configuration and the parent deadline.

The refreshed 50-subrequest total is 79 (69 candidates and 10 host reviews).
Two previous candidates use `PUT /api/users/{id}/role-assignments`.
The tightened gateway excludes that control route. At 12 the total is
52 (45 candidates and 7 host reviews), with zero candidate losses. The CLI
lint and install-aware census agree on the changed rule counts at both
allowances. `CF_SDK_GATEWAY` rises from 29 to 31, `CF_PARSE_BUDGET` from 16
to 20, and `CF_TRANSPORT` drops from 20 to 16. Four previously Linux-only
artifacts now reach the bounded HTTP hint budget before their transport
finding. All other counts match the initial after column.

The Roles and Users matcher now restricts both methods and path shapes.
The shared policy allows only `GET /api/platform-jobs/{id}` for durable job
polling, subject to existing API job visibility and runtime subrequest
accounting. Job listing and cancellation remain outside the child policy.

The installed CLI wheel carried the canonical JSON route table and reproduced
the 12-subrequest report. Node passed 411 tests across all six requested suites.
The Workerd test exercised the gateway against a test origin. The separately
named API E2E suite exercised the real authorization paths under restricted
and full workflow powers. All five API E2E cases passed from
`coverage2-16632bf1c3e5`. The separate unit invocation from the same archive
passed 5,509 tests. The first passes found a lost source location on
tokenizer failure and an email fixture that failed response validation in
the full-powers control. The corrected cases passed in the reruns.

The resumed `./test.sh quality api` run passed on CT128 from
`coverage2-16632bf1c3e5-quality-resume`. Pyright reported zero errors,
warnings, or information messages, and Ruff passed.
