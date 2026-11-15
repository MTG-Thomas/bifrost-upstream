# Real-Time Reliability: Implementation Decision Log

Companion to `docs/audits/2026-11-12-realtime-messaging-audit.md` (findings) and
`docs/plans/2026-11-12-realtime-reliability-implementation.md` (spec).
One row per discovered failure: finding → regression test → remediation → PR.
Update this log in the same PR that lands each item — future messaging changes
must check here first to avoid reintroducing retired behaviors.

| Finding | Regression test | Remediation | PR | Status |
|---|---|---|---|---|
| R-01 loopback loss | Redis-down API-local delivery | PR-1b local-first + gated suppression | — | specified |
| R-02 pre-subscribe healthy | post-`start`-pre-subscribe health | PR-1b subscribe-ack health | — | specified; reproduced host-local 2026-11-12 |
| R-01/R-02 envelope compat | wrapped/bare/self/foreign matrix | PR-1a envelope parsing, no behavior change | branch `t3/realtime-pr1a-envelope` → issue #1364, PR #1365 | **green** (10 checks pass, 0 fail/pending); not queued per instruction |
| R-03 slow-consumer stall | wedged-socket isolation | PR-2 send timeout | — | specified |
| R-04 all-channel eviction | single-channel fault containment | PR-2 failure classification | — | specified |
| R-08 retry exhaustion | outage-recovery / 4003-no-retry | PR-3 unbounded jittered reconnect | — | specified |
| R-14 unwired replay | sequence-kill resync; trim/truncation adoption | workstream B resync protocol | — | detailed design required |
| Terminal-event loss | subscribe-converges snapshot | workstream B terminal snapshot | — | detailed design required |
| Cross-instance path health | member-death ≤60s; deploy silence; degraded-on-outage | PR-5 canary + membership | — | detailed design required |

Release ordering constraint: PR-1b rollout requires PR-1a deployed to every
production replica (verified via deploy status, not assumed from merge order).
