# Stage 3 — production canary (shadow)

Status: **live**. A read-only production workflow runs every 15 minutes on the
production Linux worker and on the deployed Cloudflare per-tenant worker, and
the two results are compared. Evidence is appended to
`/opt/bifrost-canary/runs.jsonl` on `vm-mtg-bifrost-prod-01`.

Authorization: explicit ("Stage 3 canary away"). Scope kept deliberately narrow:
one read-only workflow, one identity, no customer org writes, no live traffic
routing, instant kill switch.

## What is deployed

| Piece | Value |
|---|---|
| Production | `https://bifrost.midtowntg.com` (`BIFROST_ENVIRONMENT=production`, `2.0.1-dev.1794442566`) |
| Host | `vm-mtg-bifrost-prod-01`, `/opt/bifrost-pax8` compose, pinned image digests |
| Canary worker | `bifrost-pyodide-canary-ddf316c1.midtown-technology-group.workers.dev` |
| Worker API origin | `https://bifrost.midtowntg.com` (verified: `/__gateway_probe` → `{"configured":true,"status":200}`) |
| Worker secret | its own `SPIKE_AUTH`, `0600` on the box, rotated once after a leak into a log line |
| Worker window | `SPIKE_EXPIRES_AT` = 7 days (2026-11-21T17:00Z), matched to the session's real lifetime |
| Workflow | `docs_list_organizations` (solution `44832b44-…`, module `functions/catalog.py`) — reads only, `organizations.list()` |
| Identity | device-flow session for the provider-org platform admin; refresh token at `/etc/bifrost-canary/last.json` `0600`; session expires 2026-11-21T16:32Z |
| Runner | `/opt/bifrost-canary/canary.py` `0700` (`dev/staging/prod-canary.py`), 9 unit tests |
| Schedule | `bifrost-canary.timer`, every 15 min, `Persistent=true` (catch-up after downtime) |

Nothing in the application stack was modified: no image change, no restart, no
compose edit, no migration. The only production additions are this runner file,
a systemd timer, and a device-flow session.

## Evidence shape

Each record keeps **hashes, not data** — no customer names are stored:

```json
{"ts":"2026-11-14T14:26:12Z","workflow":"docs_list_organizations",
 "module_sha256":"6d393fe8…","linux_status":"Success","cloud_status":"Success",
 "match":true,"same_result":true,"org_count":297,
 "linux_ms":2204,"cloud_ms":6567,"worker_reused":false,"cloud_http":200}
```

First results on production:

| Cycle | Linux | Cloudflare | Match | Linux ms | Cloud ms | Reused |
|---|---|---|---|---|---|---|
| 1 (pre-fix) | Success | Failed `ExecutableNotFound` | no | 2 064 | 11 746 | no |
| 2 (fixed) | Success | Success | **yes** | 2 134 | 2 396 | yes |
| 3 (timer) | Success | Success | **yes** | 2 204 | 6 567 | no |

297 organizations on both backends, identical `caller_mode: "picker"` and
canonical result hash.

## Two production-only defects found

Both are invisible in staging and neither is a Bifrost defect:

1. **Cloudflare edge `1010` on the stock Python user-agent.** The production
   host's `Python-urllib/3.13` signature is blocked at the edge (`403 error code:
   1010`); `curl`, a browser UA, or any explicit UA passes. The runner now sends
   `User-Agent: bifrost-canary/1.0`. Worth knowing for any server-side caller of
   a `workers.dev` endpoint.
2. **Solution-scoped module paths are doubled if you pass the storage path.**
   The dispatcher passes the *logical* `file_path` (`functions/catalog.py`) plus
   `solution_id`; `get_module_sync` maps it into `_solutions/<id>/…`. Passing the
   already-scoped path makes the child look for
   `_solutions/<id>/_solutions/<id>/…`, resolve nothing, and fail as
   `ExecutableNotFound` with **zero** gateway traffic — a misleading error for a
   path bug. `file_path` = logical, `requirements`/`manifest` = storage path.

## Durability across days and prod image upgrades

The canary has to keep producing evidence while nobody is watching, including
across a `docker compose up` that recreates the API. Four mechanisms:

**1. It cannot die on a bad cycle.** `main()` never raises: every outcome becomes
a record with a `phase`, and a runner crash still appends a `runner_error` line.
Phases are greppable: `ok`, `session_expired`, `api_unreachable`,
`workflow_missing`, `module_pin_missing`, `worker_gate_rejected`,
`worker_unreachable`, `cloud_execution_failed`, `mismatch`.

**2. Upgrade attribution.** Each record carries `api_version` (prod build) and
`worker_code_sha256` (canary runtime), and the solution `module_sha256`. A
transition writes one line to `events.jsonl`
(`prod_build_changed` / `worker_runtime_changed` / `module_pin_changed`). So a
mismatch after a deploy is attributable to *prod changed*, *canary runtime
changed*, or *source changed* — instead of an unexplained red line.

**3. Retries are GET-only by construction.** A deploy window makes GETs 502;
those retry with backoff. The engine POST never retries, so an attempt can never
be double-dispatched (a unit test pins this).

**4. No overlap, no lost schedule.** `flock` skips a cycle if a previous one is
still running; the unit is `Persistent=true`, so a missed run fires after the
host comes back.

### What survives an image upgrade, and what does not

- **Container roll**: survives. The session is a `human_auth_sessions` row
  (7-day expiry, rotating refresh token persisted atomically) plus stateless
  JWTs; recreating the API container does not touch it. The next cycle after the
  roll records the new `api_version` and continues.
- **Module/solution upgrade**: survives and adapts — the pin is re-read every
  cycle, so both backends immediately compare the new source. A removal shows as
  `module_pin_missing` or `workflow_missing`, not as a silent pass.
- **Auth epoch bump or session revocation by a migration**: does **not**
  self-heal. The runner records `session_expired` and stops producing comparisons
  until a human approves one device-flow code. This is the one upgrade class that
  needs a person; it is detected, not silent.

Reading state without touching anything:

```bash
cat /opt/bifrost-canary/status.json     # phase, match, run count, consecutive failures, build ids
tail -5 /opt/bifrost-canary/runs.jsonl  # per-cycle evidence
cat /opt/bifrost-canary/events.jsonl    # build/runtime/source transitions
```

Re-authenticate after `session_expired`: mint a device code with
`POST /auth/device/code`, approve it at `https://bifrost.midtowntg.com/device`,
and write the token response to `/etc/bifrost-canary/last.json` `0600`
(`dev/staging/prod-canary.py` reads `access_token` / `refresh_token` only).

## Kill switch

Immediate, in order of preference:

```bash
rm -f /opt/bifrost-canary/ENABLED          # runner exits 0 without doing anything
systemctl disable --now bifrost-canary.timer   # stop the schedule
```

And at the worker, either alone is sufficient:

```bash
WORKER_NAME=bifrost-pyodide-canary-ddf316c1 \
STAGING_API_ORIGIN=https://bifrost.midtowntg.com SPIKE_EXPIRES_AT=0 \
cf deploy --secrets-file <canary secrets>          # every request fails closed
```

Teardown at the end of the window: stop the timer, `rm -rf /opt/bifrost-canary
/etc/bifrost-canary`, disable the unit files, redeploy the worker with expiry 0
(or delete it), and stop the device-flow session (`/auth/logout` /
`/auth/revoke-all` is NOT run — only the session's own revocation is needed).

## What this does and does not prove

**Proves:** the Cloudflare runtime executes a real production workflow, resolving
a solution-scoped module through the real module transport, authenticating with a
real production token, reading real production data, and returning a result
identical to the Linux worker — repeatedly, with a warm child.

**Does not prove:** live dispatch. Production has no per-tenant execution-backend
flag; `/api/workflows/execute` always runs on Linux. Routing real traffic needs
the dispatch/claim seam (`remote_pool` + runtime profile, unmerged #1399), a
route flag, and retry-after-eviction semantics. Until that exists this is a
shadow canary, and calling it "production routing" would overstate it.

Also not covered: retries after provider eviction, concurrent tenants, sustained
load, effect comparison beyond this workflow's reads.
