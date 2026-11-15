# Bifrost Real-Time Messaging Audit: Observability, Reliability & Architecture

- **Revision evaluated:** `585b1fa80dba29492df2eee1e4a7bf6aab96396` (clean tree)
- **Date:** 2026-11-12
- **Scope:** WebSocket/event-delivery subsystem (API + client + Redis). Unrelated Redis uses (queues, caches, coordination, durable workflow execution) are distinguished where they intersect.
- **Method:** read-only source inspection of `api/src/core/pubsub.py`, `api/src/core/redis_reconnect.py`, `api/src/routers/websocket.py` (full), `api/src/services/websocket_auth_recovery.py` (full), `client/src/services/websocket.ts` (full), `client/src/lib/app-sdk/ws-client.ts` (subscribe path), grep-derived producer/consumer inventory, test inventory, Helm/K8s and health-endpoint review. One non-destructive host-local runtime check (listener `is_healthy` semantics, no network, no Docker). No production systems touched. Docker-backed suites were **not** executed: the shared host had another task's debug stack running with ~600 MiB free, so per host resource rules the full gate is deferred to CI/merge queue. `pytest` is not installed on the host.

## 1. Actual architecture

### 1.1 Event lifecycle

```
Producer (worker / scheduler / API route)
  │  manager.broadcast(channel, msg)          api/src/core/pubsub.py:141
  ▼
PUBLISH bifrost:{channel}  ──►  Redis pub/sub (fire-and-forget, no persistence)
  │                                    │
  │                     every API replica's ResilientPubSubListener (pattern bifrost:*)
  │                                    ▼
  │                     _send_local → per-socket _delivery_dispatcher
  │                                     ├─ table:/files: → per-message policy filter
  │                                     └─ else → per-message _delivery_allowed re-auth → send_json
  ▼ (only if PUBLISH raised)
_send_local on publishing instance only ("local-only fallback mode")
```

Chat is the exception — dual write via Lua script (`pubsub.py:58-75,440-494`):

```
publish_chat_run_event → Lua: INCR seq + XADD stream (MAXLEN ~20000) + EXPIRE 24h + PUBLISH
                              ▲ durable, replayable            ▲ ephemeral live fanout
replay_chat_run_events (xrange) ── served via GET …/conversations/{id}/state (latest_sequence)
```

Other Redis Streams in the repo are **not** part of WS delivery: `bifrost:agent_run_steps:{run}` (polled, `agent_run_step_streams.py`), `bifrost:service-logs:{attempt}` (flushed by `service_log_flush.py`); service lines reach WS only via the in-process bridge `service_channel_for_log` (`pubsub.py:40-55,158-169`), deliberately local-only to avoid N× duplication.

### 1.2 Transport relationships

- **Redis Pub/Sub** is the only cross-instance event transport. Every replica pattern-subscribes `bifrost:*`; there is no routing, partitioning, or persistence.
- **Redis Streams** exist only for chat replay (plus the two non-WS uses above). No other channel has sequence numbers or retention.
- **No message envelope standard.** Chat envelopes carry `protocol_version/event_id/sequence/conversation_id/run_id/occurred_at/kind/status` (`pubsub.py:461-472`). Every other event is an ad-hoc dict with mixed `camelCase`/`snake_case` (`executionId` vs `execution_id`, `service_id`, `agent_id`/`run_id`). No Pydantic model governs most payloads; no generated TS contract except chat chunks.
- **Two frontend transports** with different reconnect policies: `WebSocketService` (`client/src/services/websocket.ts`, ≤3 retries then silent) and the app-SDK `createReconnectingSubscription` (`client/src/lib/app-sdk/ws-client.ts`, unbounded jittered retries, 4001/4003 terminal). Same server, inconsistent client behavior.
- **Auth:** handshake via shared selector (`get_current_user_ws`); per-channel subscribe-time checks in `websocket.py:1159-1260` (table/files resolve through canonical channels + policy probes); **per-message** re-authorization on every delivery via `_delivery_dispatcher` → `_delivery_allowed` (`websocket.py:1057-1088,986-1033`) plus impersonation/epoch recheck (`1036-1055`). Revocation freshness is strong; throughput cost is the tradeoff (R-07).
- **Frontend reconnect:** `desiredChannels` retained across close, `subscribed/pending` cleared, re-subscribe on reconnect (`websocket.ts:637-664`). Only chat has replay infrastructure, and it is **unwired** (R-15): `connectToChat` just resubscribes; `latest_sequence` has zero frontend consumers.

### 1.3 Multi-instance coordination

There is none beyond shared Redis. No replica ID in logs, no per-replica connection counts, no cross-instance delivery health signal. During a Redis outage each replica degrades to local-only independently and silently (one `logger.warning` per failed publish, `pubsub.py:211`).

### 1.4 Channel inventory

| Channel | Producers | Consumers | Contract | Auth | Delivery | Ordering | Recovery | Observability |
|---|---|---|---|---|---|---|---|---|
| `execution:{id}` | `publish_execution_update/log`; workers, schedulers, executions router | WS `/connect`, `/execution/{id}`; `ExecutionLogsPanel` | ad-hoc dict | subscribe + per-message exec ACL | ephemeral pub/sub | none | REST refetch | logs only |
| `history:user:{uid}`, `history:GLOBAL` | `publish_history_update` | history pages | ad-hoc | own-user / admin | ephemeral | none | page reload | logs only |
| `agent-run:{id}`, `agent-runs` | `publish_agent_run_update/step` | run detail + list | ad-hoc | per-run ACL; list filtered per-message | ephemeral | none | REST refetch | logs only |
| `chat:{conv}` | `publish_chat_run_event` (Lua) | `ChatWindow`, tool cards | versioned envelope + stream | conversation ACL | **durable stream + ephemeral fanout** | per-conv sequence | **replay exists, unwired (R-15)** | logs only |
| `table:{uuid}` | `publish_document_change/policy_changed/table_invalidated` | app SDK table subs | ad-hoc | subscribe probe + per-message 4-way visibility filter | ephemeral | none | snapshot reload | logs + audit deny events |
| `files:{loc}:{scope}` | `publish_file_change/policy_changed` | file browsers | ad-hoc | subscribe probe + per-message prefix filter | ephemeral | none | relist | logs + audit deny events |
| `notification:{uid}`, `notification:admins` | `NotificationService`, `publish_platform_job_update` | notification center/store | Pydantic-backed (`PlatformJobPublic`) | self / admin + per-job check | ephemeral | none | REST list | logs only |
| `user:{uid}` (legacy) | `publish_user_notification` — **no in-tree callers** | WS branch still accepts | ad-hoc | self-only | ephemeral | none | — | logs only |
| `service:{id}` | bridge only, never published | service log views | raw log line | service ACL, admin surface | local-only fanout | none | refetch | logs only |
| `platform_workers` | heartbeats, lifecycle, pool events (+ Redis SETEX mirror, 60s TTL) | diagnostics (`useWorkerWebSocket`) | ad-hoc | admin | ephemeral | none | REST query | logs only |
| `app:draft:{id}`, `app:live:{id}` | draft/code/publish publishers | app editor, live viewers | ad-hoc | write ACL / read ACL | ephemeral | none | reload | logs only |
| `summary-backfill:{job}` | backfill router | progress UI, admin | ad-hoc | admin | ephemeral | none | re-query job | logs only |
| `file-activity`, `git:*`, `reindex:*`, `package:install`, `event-source:*` | misc admin machinery | admin UIs | ad-hoc | admin | ephemeral | none | re-query | logs only |
| `platform-job:{id}` | **no producer** (jobs ride `notification:`) | WS guard branch only | — | per-job | — | — | — | dead branch |

Duplications/inconsistencies: `user:` vs `notification:` dual notify paths (former apparently dead); `platform-job:{id}` guard with no producer; table `subscribed` echoes canonical UUID while files echo the requested name (`websocket.py:1551-1554` vs `1564-1567`); `ExecutionLog`/`executionId` vs `execution_id` casing across payloads.

## 2. Observability assessment

Ground rules verified: **no Prometheus/OTel instrumentation exists** (`rg` for `prometheus_client|prometheus-fastapi|opentelemetry|Counter\(|Histogram\(|Gauge\(` → only `collections.Counter` in `demand.py:31`; `requirements.lock` carries `opentelemetry-api` transitively, never imported). No `/metrics` scrape endpoint (`routers/metrics.py` is a product-dashboard REST resource), no ServiceMonitor/scrape annotations, no Grafana dashboards or alert rules in-repo. Health: `/health`, `/live` (static), `/ready` (**caches first pass**, `health.py:124-130` — a later Redis outage does not flip readiness), `/detailed` (live checks incl. Redis, uncached). K8s probes hit `/health` + `/health/ready` only.

| Question | Verdict | Evidence |
|---|---|---|
| Active WS connections (total / per replica) | **Not observable** | `manager.connections` is in-memory only (`pubsub.py:90`); connect/disconnect are `logger.debug/info` lines (`pubsub.py:130,139`; `websocket.py:1274,1627`). No counter, no per-replica export. |
| Open/close rate, durations, close-code distribution | **Not observable** | No counters. Close codes visible only by scraping log lines; durations not recorded anywhere. |
| Auth-failure frequency | **Partially** | 4001/4003 closes logged only as generic paths (`websocket.py:1149,1155`); distinguishable from text but no rate metric. |
| Repeated reconnects; expected vs failure disconnects | **Not observable** | Client `retryCount` is local-only (`websocket.ts:470`); server cannot link successive sockets from one client (no connection/session ID logged). |
| Subscription counts by channel category; reject/revoke rates | **Not observable** | `subscribed`/`error` acks sent to client only; revocations (`subscription_revoked`, `websocket.py:291-309`) not logged or counted. Denials persist audit events for table/file only. |
| Failed subscription restoration after reconnect | **Not observable** | No server-side record of what a client intended; R-08/R-12 failure modes are client-silent. |
| Orphaned/stale refs; per-connection subscription bloat | **Partially** | Detectable only via heap inspection or code reading: no cap/TTL/eviction on `connections` (`pubsub.py:90`); half-open sockets linger until next send fails (R-09). |
| Subscription churn / fanout amplification | **Not observable** | No per-channel subscriber counts or publish rates. |
| Events published / delivered; publish→delivery latency | **Not observable** | Success publishes unlogged; no IDs on non-chat events; no timestamps captured at publish vs send. |
| Failed deliveries; publisher vs subscriber fault isolation | **Partially** | Publish-to-Redis failures log one warning (`pubsub.py:211`); `_send_local` evictions are silent (`pubsub.py:183-194`). Cannot attribute a missing client update to either side without debug logging. |
| Silent drops; producer retry/buffer behavior | **Not observable** | `broadcast()` returns `None`; producers (`publish_execution_update`, etc.) cannot know delivery failed. No retry, no buffer, no DLQ (R-01, R-05, R-06). |
| Duplicates / out-of-order | **Partially** | Only chat is detectable (sequence + `applied_event_ids` 256-window dedup in `chat-runtime.ts:475-486`); no gap detection. All other channels: neither detectable nor defined. |
| Slow consumers / outbound backpressure | **Not observable** | No send-duration timing, no per-socket queue depth (there is no queue — R-03 blocks inline). |
| Client-visible receipt confirmation | **Not observable** | Server `send_json` success ≠ client render; no acks. Do not claim end-to-end delivery. |
| Redis connectivity vs functioning pub/sub path | **Partially** | `/health/detailed` pings Redis; listener reconnects log info/warning/error (`redis_reconnect.py:158-193`). But a ping says nothing about the pub/sub path, and `/ready` caching hides later outages. |
| Reconnect events; local-only fallback activation | **Partially** | Reconnects logged as text; fallback mode entered silently per-publish (only the `:211` warning). No "currently degraded" signal per replica. |
| Listener alive-but-stalled; replicas missing cross-instance events | **Not observable** | `is_healthy()` is failure-count-based, never verifies message flow (and false-positives pre-subscribe, R-02). No heartbeat/canary message; no per-replica delivery comparison. |
| Stream retention/trim/lag/replay observability; pub/sub disconnect windows | **Partially** | Chat stream bounds known in code (MAXLEN ~20000, TTL 86400, `pubsub.py:58-75`); actual length/lag/replay-gaps unmeasured. Disconnect windows unmeasured (no buffer → loss duration = outage duration). |
| Execution→events→UI correlation; trace across processes | **Partially** | `execution_id`/`run_id`/`conversation_id` propagate on most payloads (manual grep-able correlation), but no trace IDs, no OTel spans, no correlation ID on table/file/worker events. |
| Processing failure vs notification failure isolation | **Partially** | Execution terminal state is durable in DB (REST is ground truth); whether the WS update arrived is unknowable. Standard triage ("reload; if state correct, it was notification loss") works but is manual. |
| Why-didn't-user-see-update without debug logging | **Not observable** | Requires correlating client state, per-replica logs, and Redis availability by hand. |

Net: operators today can answer "is Redis pingable" and "did something log a warning." They cannot answer any rate, latency, loss, or per-replica question without code changes.

## 3. Reliability findings register

| ID | Sev | Conf | Location | Failure mode | Trigger | Impact | Detection today | Evidence | Smallest intervention |
|---|---|---|---|---|---|---|---|---|---|
| R-01 | High | Confirmed | `pubsub.py:141-156` | Publish success ≠ local delivery: instance depends on its own listener receiving its message back over Redis | Redis flap, cold start, listener restart/backoff | Local subscribers silently miss updates the publisher believes sent | None (no log on this path) | Code: fallback `_send_local` runs only when `PUBLISH` raises | Deliver locally first, then publish for cross-instance — **with origin-UUID loopback suppression gated to the same release as local-first** (listener skips `origin == self` only once local-first is active; see implementation spec). Without this gating, local-first double-delivers to local subscribers. See P0-1 |
| R-02 | High | Reproduced | `redis_reconnect.py:256-268`, `pubsub.py:123-124` | `is_healthy()` true before first subscribe; connect-guard skips init it claims to ensure | First WS connection on a fresh instance | Same silent local loss as R-01 at the exact moment the comment claims protection | None | Runtime check on this revision: `start()` → `is_healthy()==True` with `_pubsub is None` (unroutable Redis, host-local, no Docker) | Track subscription-established state (subscribe-ack event), not failure count |
| R-03 | High | Confirmed | `pubsub.py:171-194` | Sequential unbounded `await dispatcher(...)` per socket; one wedged socket stalls the whole channel | Slow/dead-but-open socket on a busy channel (exec logs) | Head-of-line blocking for all subscribers on that channel | Latency complaints only; no send-duration signal | Code: no timeout, no per-socket queue | Per-socket `wait_for` timeout + drop/evict slow socket (P0) |
| R-04 | High | Confirmed | `pubsub.py:183-194`, `websocket.py:986-1033` | Any per-message exception evicts socket from **all** channels, incl. transient DB timeouts and already-handled `WebSocketDisconnect` | DB slowdown during fanout; poison payload; single-channel send failure | Client loses every subscription silently; must reconnect (subject to R-08) | None (eviction unlogged) | Code: bare `except Exception` → `disconnect(ws)` scans all channels | Classify before acting: **transport failure** (send raises, disconnect) → drop the connection (all channels are dead anyway); **auth/policy revocation** → revoke that channel with the existing `subscription_revoked` notice; **transient app/DB error** → keep the subscription, log + metric, continue. Never evict-all on a transient single-channel fault; don't count handled disconnects as failures |
| R-05 | High | Confirmed (mechanism) / tradeoff (choice) | `redis_reconnect.py:142-226` | Reconnect re-subscribes only; pub/sub retains nothing → every non-chat message in the window is permanently lost | Any Redis restart/failover/partition longer than backoff gap | Stale execution/agent/table/notification state behind a green socket | Only by user report + REST comparison | Code + design: no buffer/replay except chat | Accept for most channels (document); add durable replay or snapshot-reconcile only for terminal events (P1) |
| R-06 | Med | Confirmed | `redis_reconnect.py:251-254` | Malformed payload skips message with log line only; no DLQ, no counter | Any publisher emitting bad JSON | Silent per-message loss, potentially recurring | Log grep | Code | Counter + alert; validate at publish boundary |
| R-07 | High | Plausible (needs load validation) | `websocket.py:986-1033`, `528-549` | Per-message, per-subscriber DB round-trips (O(subs × msgs)); table cache mitigates only tables | High-fanout channel (exec logs, `agent-runs`, `platform_workers`) | DB load amplification → transient errors → triggers R-04 (self-reinforcing) | DB metrics under load; no per-channel cost signal | Code: fresh `get_db_context()` + `refresh_caller` per delivery | Cache caller/visibility briefly per connection; batch `agent-runs`/worker fanout auth (P1, measure first) |
| R-08 | High | Confirmed | `websocket.ts:470-472,637-664` | Client retries ≤3 times with exp backoff, then permanently stops; 4001-refresh failure returns silently | Outage >~14s, expired session, laptop sleep | Dead live-updates until navigation/reload; stale UI, no indicator | User report only | Code | Unbounded jittered reconnect like app-SDK client + surfaced connection state (P0/P1) |
| R-09 | Med | Confirmed (mechanism) | `pubsub.py:90`; `websocket.py:1622,1680` | No server ping, no receive timeout, unbounded registry → half-open sockets linger | NAT timeout, mobile drops, crashed tabs | Memory/socket creep; amplifies R-03 (dead sockets iterated in fanout) | Heap/container inspection only | Grep: server only answers client pings | Server keepalive + idle reap (P1) |
| R-10 | Low | Confirmed | `websocket.ts:646`; `websocket.py:1154,1651` | 4003 (permanent) retried 3×; clean 1000 close never reconnects even with live `desiredChannels` | Access-denied loops waste; server-initiated 1000 during deploys misses reconnect | Minor waste + missed fast-reconnect on rolling deploys | Code | Code | Terminal-code set incl. 4003; reconnect on 1000 when desired non-empty |
| R-11 | Med | Plausible | `pubsub.py:123-124`, `redis_reconnect.py:263-267` | Every new WS connection during extended outage calls `_init_redis()` → stop + new listener, thrashing backoff | Redis down >~31s with steady connects | Churned subscriptions, log spam, delayed recovery | Log spam | Code | Guard re-init with in-progress flag / min-interval |
| R-12 | Med | Plausible (needs caller check) | `ws-client.ts:127`, `websocket.py:1551-1554` | Server echoes canonical `table:{uuid}` but SDK marks subscribed only on exact `table:{as-passed}` match → `hasSubscribed` never set, `onReconnect` snapshot-refresh never fires | `subscribeToTable` called with a table **name** rather than UUID | Stale table views after reconnect with no signal | None | Code pair; tests only use matching IDs (`ws-client.test.ts:85-141`) | Echo both canonical and requested names, or match on `table_id` in client |
| R-13 | Med | Plausible | `websocket.py:766-796,736-757` | File fanout does a fresh DB `is_allowed` per matching prefix per event per subscriber | Many overlapping prefix subs + file churn | Same DB-storm shape as R-07, smaller radius | None | Code | Batch prefix checks per event |
| R-14 | Med | Confirmed (dormant gap) | `pubsub.py:497-531`; `routers/chat.py:351`; `websocket.ts:1334` | Chat replay exists server-side but `connectToChat` never resyncs; `latest_sequence` has zero frontend consumers | Any disconnect during an active chat stream | Missed chat events despite durable stream + retention | User-visible gap in transcript | Grep: no consumer of `state`/`latest_sequence` outside types | Wire `after_sequence` resync on chat (re)subscribe (P1) |

Security/tenant isolation: **no issue found.** Subscribe-time + per-message authorization is consistently enforced (`websocket.py:986-1033` + table/file filters), `log_safe` redaction is used, and chat error payloads are scrubbed to public messages on both publish and replay (`pubsub.py:457-459,517-521`). Guardrail for future work: never log payload bodies or use execution/user/tenant IDs as metric labels (use trace attributes + audit events instead).

## 4. Observability improvement proposal (smallest set that matters)

No new runtime dependencies. All below is stdlib `logging` + optional `prometheus_client` (already a transitive dep family; adding it as direct dep needs the standard lockfile regen — or start with structured logs only).

**A. Structured event contracts (P2).** Add a minimal envelope (`type`, `v: 1`, `event_id` uuid7, `ts`, `channel`) at `manager.broadcast` time for non-chat events; keep domain fields as-is. Generate no TS types yet — envelope only. Do not retrofit every payload.

**B. Metrics (P1).** If `prometheus_client` is adopted, these six counters/gauges with low-cardinality labels only (`channel_prefix`, `code`, `reason` — never IDs):

- `bifrost_ws_connections_active` (gauge, labels: `replica`) — set on connect/disconnect.
- `bifrost_ws_connect_total` / `bifrost_ws_disconnect_total` (labels: `code_class`) — close-code distribution.
- `bifrost_ws_subscribed_active` (gauge, label: `channel_prefix`) + `bifrost_ws_subscribe_rejected_total` (label: `channel_prefix,reason`).
- `bifrost_ws_published_total` / `bifrost_ws_publish_failed_total` (label: `channel_prefix`) — counted in `broadcast()`. **Throughput/workload signal only**: one publish fans out to zero..N subscribers, so publish-vs-delivered comparison cannot measure loss.
- `bifrost_ws_delivered_total` / `bifrost_ws_delivery_failed_total` (label: `channel_prefix,reason`) + `bifrost_ws_send_seconds` (histogram, same labels minus reason) — counted in `_send_local`. Workload and failure-breakdown signal, not a loss rate.
- `bifrost_delivery_canary_seen_total` (labels: `origin_replica`, `receiving_replica`) — **the loss/health signal.** Each replica periodically publishes a synthetic canary (replica ID + sequence) on a dedicated channel; every replica records receipt. A missing canary proves the cross-instance path is broken regardless of fanout cardinality, and localizes which replica or direction failed. This replaces any publish-vs-delivered gap arithmetic.
- `bifrost_redis_listener_reconnects_total`, `bifrost_redis_fallback_deliveries_total` (local-only mode), `bifrost_chat_replay_gaps_total`.

Without Prometheus, the same signals as structured `logger.info` lines with fixed keys are a strict improvement over today (greppable rates), but counters are preferred.

**C. Dashboards (P1, only after B).** One page, six panels: live connections/subs; connect/disconnect rate + close-code split; publish/delivery throughput per prefix (workload, **not** loss); canary receipt matrix per replica pair (the loss signal); delivery latency p50/p99; Redis reconnects + fallback-mode windows. No per-ID drill-down (use trace/audit for that).

**D. Alerts (P1).** Four, all actionable: (1) `publish_failed` rate > 0 for 5m → Redis/pubsub path broken; (2) canary missing from any replica pair for N intervals → cross-instance delivery broken and localized; (3) listener reconnects > N/10m → flap; (4) close-code 1011/1012 spike or 4001 spike → deploy/auth incident. Explicitly **not** alerting: per-channel silence (noisy), latency p99 alone (page on burn, not spikes), publish-vs-delivered divergence (invalid as a loss metric — see §4B).

## 5. Prioritized roadmap

**P0 — correctness / material reliability (do first).** Central acceptance criterion for all P0 work:

> Following transient network, Redis, or API failures, a connected Bifrost client must either recover to an authoritative state or visibly indicate that its data may be stale.

0. *Prove R-01–R-04 with regression tests before remediating.* Each fix below lands with a failing-first test: loopback-independent local delivery under Redis outage; post-`start`-pre-subscribe health; wedged-socket isolation; single-channel fault containment. A reasoned mechanism is not a demonstrated defect — no remediation merges without its reproduction.
1. *Close R-01+R-02:* deliver locally in `broadcast()` independent of Redis loopback, **with origin-UUID suppression activating in the same PR** (never stamp-then-suppress across releases); track subscription-established state via actual subscribe-ack, not failure counts. Invariant (narrowed): API-local events reach eligible local subscribers exactly once with or without Redis; cross-process events remain best-effort over Redis. Acceptance: kill Redis mid-test → API-local subscribers still receive **exactly once**; worker-origin traffic unaffected; new instance serves first connection without loss. Tests: unit (loopback-independent delivery, suppression matrix), e2e (Redis restart during execution; replica restart mid-run).
2. *Bound R-03+R-04:* per-socket send timeout in `_send_local`; failure classification (transport → drop connection, revocation → single-channel revoke with notice, transient → keep + log + metric). Acceptance: one wedged socket degrades only its own deliveries; transient DB error costs one channel at most, never all. Tests: slow-consumer unit test, partial-failure test.
3. *Fix R-08:* unbounded jittered reconnect in `WebSocketService` (mirror app-SDK policy) + visible disconnected state; fix R-10 code handling alongside. Acceptance: 5-min outage recovers without reload; 4003 not retried. Tests: existing `websocket.test.ts` + reconnect-cap test.

**P1 — high-value observability + targeted durability.**

4. Six-metric instrumentation + four alerts + one dashboard (§4B–D). Acceptance: each §2 "Not observable" row for connection/subscription/delivery/Redis becomes answerable from metrics; alert fires on induced Redis outage.
5. Wire chat resync (R-14): `after_sequence` replay on chat (re)subscribe. Acceptance: disconnect-during-stream backfills exactly the gap, no duplicates (dedup window already exists).
6. Terminal-event reconciliation (R-05 scope-limit): on execution/agent-run (re)subscribe, server sends current terminal state snapshot so a lost terminal event self-heals. Ephemeral channels stay ephemeral — no durable bus. Acceptance: kill Redis at completion moment → client converges on subscribe.
7. Measure-then-fix R-07/R-13: add per-channel fanout cost logging under load test; cache caller/visibility per connection if confirmed. Do not optimize blind.
8. Server keepalive + idle reap (R-09); re-init guard (R-11).

**P2 — maintainability.**

9. Minimal envelope (§4A) for new/changed events only; unify `subscribed`-echo convention (canonical vs requested — resolves R-12 regardless of caller input); reconcile `user:` vs `notification:` (remove dead path or document); document `platform-job:{id}` dead branch or delete it.
10. Type the six P1 metrics into client-visible connection status where useful (already have `connectionStatusCallbacks`).

**Deferred — not worth it.**

- Framework migration (Socket.IO/SignalR): no deficiency found that a framework fixes — the failure modes are in Bifrost's delivery/auth/fallback logic, which any transport would keep. Explicitly do not migrate.
- Durable messaging for all channels: cost (retention, replay, ordering per channel) dwarfs benefit; ephemeral + snapshot-reconcile covers the real risk (terminal events).
- Full OTel tracing of every event: premature before counters exist; add trace attributes only to the P1 metric work if needed.
- Centralized "observability abstraction" layer: a small shared metrics helper is enough; a framework would add indirection without removing duplication (publishers are already one-line calls).
- Per-message delivery receipts / end-to-end acks: expensive, changes protocol; REST-as-ground-truth + reconciliation achieves the user-visible goal.

## 6. Executive recommendation

1. **Is the architecture sound?** Structurally yes. Single-publisher → Redis pub/sub → per-replica fanout with strong per-message auth and tenant isolation is appropriate for this product, and the chat stream design (Lua atomic seq + stream + ephemeral fanout) is the right pattern. The architecture is not the problem; five implementation defects in delivery, health-checking, and client retry are.
2. **Is its behavior sufficiently observable?** No. Zero counters, zero dashboards, zero alerts; `/ready` caching can hide Redis outages; every rate/latency/loss question is unanswerable without code changes. Logging hygiene (redaction, reconnect logs) is good but not a substitute.
3. **Are there concrete bugs or likely failure modes?** Yes: R-01/R-02 (silent local loss), R-03 (slow-consumer stall), R-04 (one failure kills all subscriptions), R-08 (client gives up after 3 retries), R-14 (chat replay unwired). R-07/R-09/R-11/R-12/R-13 are plausible risks with confirmed mechanisms, stated as such — not labeled bugs.
4. **Would systematic contracts/instrumentation reduce risk?** Instrumentation decisively yes (P1 item 4 is the highest leverage-per-line change). Contracts: only the minimal envelope (P2) — full schema standardization is not justified by the evidence.
5. **Would a framework migration help?** No. Demonstrated deficiencies are all above the transport layer. Do not migrate.
6. **Three highest-value changes:** (a) loopback-independent local delivery with gated origin-UUID suppression + true subscription-ready health (R-01/R-02), each with a failing-first regression test; (b) the metrics + cross-replica canary + alerts (turns every future incident from archaeology into a dashboard read, and measures loss correctly); (c) unbounded client reconnect + chat resync + terminal-event snapshot (R-08/R-14/P1-6) — together these close the "stale UI behind a green socket" class.
7. **Deliberately leave alone:** transport choice, ephemeral semantics for non-terminal events, per-message re-authorization (keep paying that cost), `user:`/`platform-job:{id}` dead branches beyond documentation, and any new abstraction layers or dependencies until the P0/P1 items land.

## Appendix — tests reviewed and validation performed

- Unit (read, not executed): `test_pubsub_connection_manager.py` (bridge no-republish covered; no loopback-loss or slow-consumer test), `core/test_redis_reconnect.py` (config/backoff math + mocked subscribe; asserts `is_healthy` false when stopped/failed but **never asserts post-`start`-pre-subscribe**, i.e. R-02 is untested), `policies/test_pubsub.py`, `chat_run_pubsub/chat_stream_serialization`, `websocket_table_messages/table_resolution/human_claims`, `websocket_auth_recovery`, notification service/access tests. Client: `websocket.test.ts` (desired/pending/subscribe reconciliation covered; no retry-exhaustion or resync test), `stable-ws-compatibility.test.ts`, `ws-client.test.ts`, `use-table.test.tsx`.
- E2E (inventoried): `api/tests/e2e/api/test_websocket.py`, `test_websocket_read_visibility.py`, `test_websocket_legacy_app_recovery.py`, notification/file-subscription suites; client `executions-realtime`, `apps-publish-notification`, `policies-app-realtime` specs. None covers Redis interruption with loss accounting, replica restart mid-execution, slow/disconnected consumers, revocation-while-subscribed, multi-replica fanout, or stream replay continuity — the P0/P1 acceptance tests above fill exactly these gaps.
- Executed: host-local `ResilientPubSubListener.start()` → `is_healthy()` returns `True` while `_pubsub is None` (R-02 reproduction, no network/Docker). Docker-backed suites not run (shared host occupied) — deferred to CI per repo merge-queue policy.
