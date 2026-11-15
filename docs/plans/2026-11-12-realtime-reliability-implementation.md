# Real-Time Reliability: Implementation Spec

- Baseline: `docs/audits/2026-11-12-realtime-messaging-audit.md` at `585b1fa`
- Date: 2026-11-12
- Rev 2 (same day): resolves review — suppression gated to PR-1b, narrowed delivery invariant, canary three-signal redesign, strengthened resync.
- Acceptance criterion (all P0 work): *following transient network, Redis, or API failures, a connected client must either recover to an authoritative state or visibly indicate that its data may be stale.*

## Ground facts (verified, not assumed)

- Publishers live in API, worker, and scheduler processes: `jobs/consumers/workflow_execution.py`, `jobs/consumers/agent_run.py`, `jobs/schedulers/execution_cleanup.py`, `jobs/schedulers/event_cleanup.py` (calls `manager.broadcast` directly). Workers never call `manager.connect()`, so no worker process runs a listener — only API replicas fan out locally.
- Origin-ID precedent exists: `scheduler/leadership.py:32` uses `f"{socket.gethostname()}:{os.getpid()}"`. Per-process IDs are unique regardless of uvicorn worker count, since each process owns its own `manager`/`listener`/`connections`.
- Chat resync needs no new endpoint: `GET .../conversations/{id}/state` already returns conversation + messages + events + `latest_sequence` (`routers/chat.py:351-359`, `services/chat_runs.py:488-504`, `contracts/agents.py:683-691`), documented "for reconnects." Only client wiring is missing.
- Repo policy forbids `xfail`/`skip` to mask failures, so each PR below carries its regression test + fix together (test written first, fails before, passes after). No standalone failing-test PRs.

## Concern 1 — Loopback suppression without an LRU

Message-ID dedup is unnecessary. Adopt **origin filtering only**, with suppression coupled to local-first in the same release:

- Every publishing process stamps an envelope `{"v": 1, "origin": "<process-uuid>", "origin_hint": "<pod-or-host>", "payload": {...}}` at `broadcast()`/`publisher.publish()` time. `origin` is a **process-start UUID** (hostname/PID are reused across container restarts and are not safe identity); the human-readable hint is for logs only.
- API `broadcast()` (from PR-1b): `_send_local(payload)` first, then `PUBLISH` the envelope.
- Listener suppression rule, active **only from PR-1b**: skip local fanout iff `v >= 1` **and** `origin == self`. PR-1a code parses the envelope but suppresses nothing — it delivers every message exactly as today. This coupling is load-bearing: stamping without local-first plus suppression would drop messages never delivered locally.
- External publishers (workers/schedulers) remain **Redis-only publishers**: they stamp their own process UUID but never local-deliver (they have no subscribers) and never match an API replica's `self`, so their messages always fan out. No process needs to know whether it has subscribers.

Rollout (suppression and local-first flip together, never separately):

- **PR-1a (no behavior change):** introduce envelope parsing; deliver everything; stamp origin on publish but take no local-first action. Tests: wrapped/bare/self/foreign matrix — all delivered. Merges and rolls out fully first.
- **PR-1b (behavior flip):** local-first + suppression activate together behind one flag; `is_healthy()` replaced by subscription-established state (subscribe-ack event from the listener loop) in the `connect()` guard. Tests: the R-02 reproduction (healthy-True pre-subscribe now False), Redis-down API-local delivery exactly-once, loopback delivered once, worker-origin message still fanned out.
- Mixed-version safety follows from the coupling: a 1a listener never suppresses (so a 1b publisher's loopback is delivered once remotely, correctly); a 1b listener only skips its own process's messages (which its own `broadcast()` already delivered locally). No cross-process combination drops or duplicates.
- **Release requirement (explicit):** independent mergeability does not imply deployment order. PR-1b must not begin rolling out until PR-1a is deployed to **every** production replica. Verify via deploy status before approving the PR-1b rollout; the merge queue gates code, not deployment sequence.

Narrowed invariant: **API-local events reach eligible local subscribers exactly once regardless of Redis availability. Cross-process events (worker/scheduler → API → browser) require a functioning Redis path and remain best-effort** — reflected in tests (Redis-down tests cover API-local only; worker-publish tests assert Redis-present delivery) and dashboard semantics (loss panels labeled by scope). `broadcast()` returns no delivery result and must not be read as one: success means "accepted for fanout," not "received."

## Concern 2 — Canary with membership awareness

No K8s API dependency. Three separate signals — Redis is transport for two of them, so an outage must read as "monitoring degraded," never as "healthy":

| Signal | What it establishes | Source of truth |
|---|---|---|
| Local listener state | subscription established, listener task running | in-process (PR-1b subscribe-ack state), exposed via `/health/messaging` |
| Membership | replica is expected to participate | Redis `SETEX bifrost:replicas:{origin-uuid} 45s`, refreshed every 15s; key expiry = unannounced death, no clock sync |
| Canary receipt | replica is actually consuming published messages | per-replica `last_seq` per origin observed; each replica publishes `{origin, seq}` every 15s on `bifrost:canary` |

- Effective expected set = live membership keys **union** last-known set retained for 10 min. If Redis is unreadable, replicas report `monitoring: degraded` (unknown) — a shrinking expected set during an outage must not resolve to "nothing to monitor."
- Precise timing: evaluation runs every 15s; a non-exempt member alerts when `now - last_seen > 45s`. Members younger than 60s (startup grace, tracked by first-seen) are exempt; members gone from live membership keep a 60s linger before absence stops alerting (covers rolling deploys). Worst-case detection: **≤60s after the last observed canary** (45s miss window + one 15s evaluation skew). Test each boundary: fire at 46s, silence at 44s; grace/linger edges; Redis-unreadable → degraded, not healthy, not alerting.
- Cardinality: process-UUID origins never become metric label values. Pod identity comes from scrape-target metadata; metrics are aggregates (`bifrost_delivery_canary_missing_members` gauge, per-receiver lag gauge without origin labels). Origin→receiver correlation lives in structured logs and `/health/messaging`, not in Prometheus.
- Scope honesty: a healthy canary proves Redis pub/sub into the receiving *process*. A replica can publish canaries while failing to consume (broken listener, live heartbeat). The listener-state signal and canary receipt are evaluated independently — alert on either. Browser receipt additionally requires the auth/delivery path (PR-2 work) — the canary does not cover that; terminal-event snapshot (workstream B) does.

**PR-4 (instrumentation):** the audit §4B counters + `/health/messaging` (listener state, fallback mode, membership/canary ages, monitoring degraded/healthy). Readiness untouched.
**PR-5 (canary):** membership heartbeats + canary publish/observe + alert rule. Tests: simulated member death → alert ≤60s; rolling-restart sequence → silence; Redis-unreadable → degraded.

## Concern 3 — Race-free chat resync

Protocol (client, `useChatStream`/`chat-runtime`):

1. (Re)subscribe to `chat:{id}` on the current socket generation; wait for the `subscribed` ack — live coverage starts here. **Bind the ack to the generation that requested it** (the existing `connectionGeneration` counter): an ack arriving on a superseded socket is discarded and never authorizes replay, otherwise a delayed ack from a dead connection can bless a stale resync on the new one.
2. `GET .../state`; merge returned `events` with `sequence > last_seen`, deduplicated by `event_id` against the existing `applied_event_ids` window (extend window if needed for the fetch overlap).
3. Live events arriving during the fetch dedup identically — no race by construction: everything before the ack is covered by state, everything after by live, the overlap by dedup.
4. Continuity validation over the whole recovered interval, not just its minimum. With `last_seen = 100`:

   | Returned | Interpretation | Action |
   |---|---|---|
   | 101, 102, 103 | continuous | merge |
   | 105, 106 | missing 101–104 | snapshot adoption (gap inside window) |
   | empty, latest = 120 | nothing recoverable incrementally | snapshot adoption |
   | 101, 103 | internal gap | snapshot adoption |
   | 95–110 | overlap | merge new, dedup rest |

   Any non-+1 step in `[last_seen, latest]` → discard incremental merge, adopt `state.messages + state.events` as authoritative, surface stale-while-refetching per the acceptance criterion.
5. Trim vs pagination: the server must distinguish them — add `earliest_sequence` (and a `truncated` flag when the 20k cap binds) to `ChatRunStateResponse`. A gap against a `truncated` response is inconclusive, never proof of trim; both cases converge on snapshot adoption, but only an untruncated response with `earliest_sequence > last_seen + 1` proves trim.
6. Verify before relying: workstream B must confirm `state.messages` is the complete authoritative transcript (note `limit()` calls in `chat_runs.py:591,623` — determine whether they bound this path) before snapshot adoption trusts it; if paginated, either remove the bound for this endpoint or carry pagination into the protocol.

**Workstream B (after P0/P1, own tests):** chat resync protocol above + terminal-event snapshot on execution/agent-run (re)subscribe (server sends current terminal state; client converges). Correctness tests: kill-Redis-at-sequence-N → exact-gap backfill, no duplicates; internal-gap and trim simulations → snapshot adoption; delayed-ack-from-dead-socket → replay refused; truncated response → snapshot adoption without trim misdiagnosis.

## `/ready` stays as-is

Do not fail K8s readiness on Redis loss (would take all replicas out simultaneously). `/health/messaging` reports degraded realtime independently; each consumer decides whether its serving contract needs Redis. Document that decision per surface (app serving: no; live-only diagnostics: yes, show stale).

## PR sequence and invariants

| PR | Invariant | Tests |
|---|---|---|
| 1a | Envelope parses and delivers everything; zero behavior change | wrapped/bare/self/foreign matrix, all delivered |
| 1b | API-local publish reaches eligible local subscribers exactly once, with or without Redis; cross-process stays best-effort | Redis-down API-local delivery; loopback-once; worker-origin fanout; R-02 health repro |
| 2 | One slow/broken socket degrades only itself; transient fault keeps all subscriptions | wedged-socket isolation; single-channel fault containment |
| 3 | Client reconnects indefinitely with jitter; permanent codes not retried; disconnected state visible | outage-recovery; 4003-no-retry; status-callback coverage |
| 4 | All §2 rate/loss questions answerable; readiness semantics unchanged | metric presence under induced outage; `/ready` still passes |
| 5 | Missing cross-instance path alerts ≤60s after last canary; deploys silent; Redis loss reads degraded, never healthy | member-death timing boundaries; rolling-restart silence; unreadable-Redis degraded |
| B1/B2 | Chat gap backfills exactly; any discontinuity adopts verified snapshot; lost terminal event converges on subscribe | sequence-kill resync; internal-gap/trim/truncation adoption; dead-socket ack refusal; terminal snapshot |

Each PR merges independently through the merge queue. No combined mega-PR.

## Explicitly not doing

Framework migration; durable bus for non-terminal events; OTel tracing before counters exist; per-message delivery receipts; failing `/ready` on Redis loss; message-ID LRU (superseded by origin filtering).
