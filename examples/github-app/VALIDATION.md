# Qualification

Package version: **0.1.0**. Runtime source was tested at commit
`964d20c497ce0705cc9ab24142653730399ae4e8`, based on upstream main
`54fbca9d8b1792083a0a82fc7c43e64264247098` plus the separate shared webhook
delivery-identity fix in [PR #1285](https://github.com/gobifrost/bifrost/pull/1285).

Observed on 2026-11-08:

- **27 package tests passed**, zero failures/errors/skips, through `./test.sh`.
  Coverage includes immutable verified bindings, destination isolation, repository
  removal, suspension, token expiry/refresh coalescing, lifecycle clock skew,
  quota cooldowns, App-JWT signature/claims, read-only mint permissions and
  lossless provider delivery IDs.
- The real-worker path installed both shipping ZIPs, rotated the private source's
  signing secret, redeployed activation while retaining the secret, and routed
  concurrent duplicate signed callbacks to only the bound tenant. Repeated tenant
  publication retained one complete receipt. A diagnostic workflow added only to
  the test activation bundle proved encrypted token write/read under the actual
  restricted operator identity, without returning the token in execution output.
- **The browser happy path passed**, with its normal authentication setup and
  zero failures, skips or flaky outcomes. It installed the operator ZIP, opened
  the real native Form, submitted Health and inspected the result through the
  production client build. No App credentials were configured in this browser test.
- Docker API pyright and Ruff passed. Client TypeScript and ESLint passed;
  ESLint reported one existing warning in `e2e/support/seed-review-pack.ts:197`.
- Independent review covered custody, immutable routing, lifecycle fences,
  refresh races, rate limits and public API attribution. Findings were fixed and
  the relevant behavior was exercised. No new GitHub review approval was issued.

Commands:

```bash
./test.sh tests/unit/services/test_github_app_solution.py \
  tests/e2e/api/test_integration_package_boundaries.py \
  tests/e2e/api/test_github_app_solution_install.py -v --durations=0
./test.sh quality api
./test.sh client e2e e2e/github-app-package.admin.spec.ts
(cd client && npm run tsc)
(cd client && npm run lint)
```

The shared fix separately passed 78 focused webhook/processor/queueing/rate-limit
tests and the existing key-custody proof in the same run. On clean main, its red
case produced two Events for one signed provider delivery. The corrected case
produced one Event, delivery and successful execution under concurrent first
acceptance; failed admission rolled back its identity. Hosted required checks,
CodeQL and Product Updates passed on shared PR head `d12c42c3cd51dcb4012e1b4544e71a9d298ebbcd`.

This is local synthetic qualification. It does not prove fresh live-GitHub
behavior, production deployment or maintainer acceptance. Full suites and the
protected merge queue remain upstream gates. The approved identity's normal
queue request for #1285 was denied by GitHub's EnablePullRequestAutoMerge
permission check; no alternative credentials or merge route were used.

Dependency order: deploy #1285's capability; install the operator and tenant
consumer packages; complete operator-owned manual credentials/HMAC setup; verify
the consumer destination and bind installations; then explicitly redeploy ingress
and scheduled reconciliation as enabled. The package refuses to operate without
the required ingress capability. Existing PAT Git sync remains untouched.
