# Property-based testing for the Bifrost TypeScript front-end

**Date:** 2026-11-17 · **Status:** adopted, narrowly — one property live, approved for expansion on evidence · **Scope:** `client/` only (fork-only work; see §9)

## TL;DR

- **Yes, but only at the reducer and pure-function layer.** The front-end already has 659 Vitest files and 139 Playwright specs; the gap is not test *count* but the absence of any statement about *all* inputs to its stateful logic.
- **One real defect, found by the first property.** `applyChatStreamEnvelopes` applied one render frame against a single shared clone, but rejected envelopes mutated that clone before deciding to reject. When a sibling envelope in the same frame succeeded, the mutations were kept — inventing phantom runs and cross-conversation writes. Fixed in `client/src/lib/chat-runtime.ts`; counterexample pinned as an example test.
- **`fast-check` 4.10.2, a dev dependency.** One property file, fixed seed in the normal suite, 100 runs, ~1.5 s of wall time. No new CI job, no change to the `@smoke` gate, no change to `maxWorkers`.
- **Bombadil is deliberately deferred.** It is pre-1.0, Chrome-only, and its findings need an owner to be dispositioned; none of the current candidate invariants need a browser.
- **Upstream eligibility is unaffected.** A test suite is not a contribution basis, and this fork is public. The defect becomes upstream-eligible only with a concrete actual-use consequence.

## 1. What the repository already had

| Measure | Count |
|---|---|
| Vitest files (`src/**/*.test.ts(x)`) | 659 |
| Playwright specs (`client/e2e/`) | 139 (15 `@smoke` tags across 12 files) |
| Components with sibling tests | 353 of 502 |
| Modules under `src/lib/` (non-test) | 92 (86 `.ts`, 6 `.tsx`) |
| Modules under `src/lib/` importing no React | 67 |
| Reducer modules under `src/stores/` | 12 |
| Property or fuzz harnesses | none (two tests use ad-hoc randomness; neither asserts an invariant) |
| Hypothesis on the Python side | not a dependency |

Gates that constrain any new test tier:

- `client/playwright.config.ts` sets `retries: 0`; `test.sh client smoke` is `--grep @smoke`.
- `.claude/skills/bifrost-testing/SKILL.md` hard rule 3: flaky is a symptom, not a disposition — no retries, no rerun-until-green.
- `.github/workflows/ci.yml` runs `npm test` for code PRs. `ci-noop.yml` stubs `test-client-unit` for docs/plugin/config-only PRs, so "every PR runs Vitest" is not accurate.

## 2. Why the unit layer, and not the browser layer

The risky logic in this codebase is *stateful* and its contracts are written in prose:

- `src/stores/executionStreamStore.ts` — reorder/dedupe a log stream by sequence, across two ingestion paths (sequenced websocket batches and unsequenced client-synthesised logs).
- `src/lib/chat-runtime.ts` — a batched projection optimiser over an event stream with duplicate, stale and out-of-order deliveries.
- `src/lib/app-sdk/use-table.ts` — a paged live window with a comparator that deliberately approximates PostgreSQL collation, plus a hand-written binary search that depends on that comparator being a total order.

Example tests can only assert the sequences someone thought of. These three modules are defined by sequences nobody thought of, and all three are pure enough to test without a DOM, a timer or a socket.

## 3. The first property, and the defect it found

The property: for any conversation-scoped projection `P` and any list of envelopes `Es`,

```
applyChatStreamEnvelopes(P, Es)  deep-equals  Es.reduce(applyChatStreamEnvelope, P)
```

That is the documented meaning of the optimiser — it must be observationally equivalent to the fold — and no example test in the repository asserted it (`chat-runtime.test.ts` checks one hand-built batch for its final content).

It failed on the first run. Mechanism, in `applyEnvelopeToProjection`:

- `projection.conversation_id` was written *before* the `!chunk` and `!runId` rejects.
- `ensureRun` inserted a run into `projection.runs` / `run_order` *before* the duplicate-`event_id` and `sequence <= last_sequence` rejects.

`applyChatStreamEnvelope` discarded those mutations by returning the original object. `applyChatStreamEnvelopes` kept them, because it shares one clone and returns it whenever any envelope in the frame succeeds. `deriveActiveRun`'s nonterminal branch then treats the phantom run as a real candidate for the active slot.

Shrunk counterexample (seed `20261117`, 12 shrinks, two envelopes):

```ts
[
  { sequence: 0, run_id: "run-2", payload: { type: "error", error: "" } },
  { sequence: null, run_id: null,
    payload: { type: "message_start", user_message_id: "user-1",
               assistant_message_id: "assistant-1" } },
]
```

The first envelope is rejected by sequencing after materialising `run-2`; the second is accepted; the batched result keeps a `run-2` with `status: null`, `last_sequence: 0` and no applied events.

**Reachability, stated honestly.** A trigger needs a *present* sequence at or below the run's last sequence on a run the projection does not know about, mixed into a frame with an accepted envelope. The server normalises a missing sequence to `0` (`api/src/core/pubsub.py:550`, `api/src/services/chat_runs.py:634`), so the shape is producible, but no end-to-end reproduction was captured. The structural asymmetry is proven; the user-visible frequency is not.

**Fix.** `applyEnvelopeToProjection` now decides against the pre-existing state and only then commits: chunk/run-id/sequence/dedupe rejection happens first, using `projection.runs[runId]` for the last-sequence lookup instead of a freshly inserted run. The accepted path is unchanged. The counterexample is pinned as an example test in `chat-runtime.test.ts` so a later generator change cannot silently drop the regression.

## 4. Seed policy

- **PR / `npm test`:** fixed seed (`SEED = 20261117`), `numRuns: 100`. Reproducible, deterministic, no flakes.
- **Nightly (future):** rotate the seed, print it on failure. A rotating-seed failure is a real counterexample and gets a disposition under the testing skill — never a retry.
- **Every counterexample** is pinned as an explicit example test at the narrowest layer.

## 5. What not to do

- Do not put randomised runs in the `@smoke` gate or the merge queue.
- Do not property-test presentational components, and do not fuzz the login form.
- Do not assert a property that restates the implementation; use an independent oracle (a fold, a naive sort, a server-side comparison) or an algebraic law.
- Do not let a property test touch the DOM, timers, `Math.random` or the network.
- Do not expand to component-level PBT until the unit tier has produced a defect per unit of effort.

## 6. Approved next slices

1. **`executionStreamStore.appendLogs`** — permutations of positive sequences with identical retransmissions against a contiguous-prefix oracle; include the completion flush path. The current contract is narrower than it looks: production callers pass one-element arrays, and a batch that mixes sequenced with unsequenced logs silently drops the unsequenced ones.
2. **`applyPagedEvent`** — from a sorted, unique window within `pageSize`, events must preserve size, uniqueness and order, and must follow the documented page-edge rules. `sortedIndex` is private, so drive it through the hook's reducer.
3. **`compileFilterToExpr`** — never emits `{and: [...]}` with fewer than two operands (the server validator rejects it), empty filter maps to `null`, unsupported operators always throw.
4. **`parseApiError`** — total over `unknown`: never throws, always yields a non-empty message.
5. **`compareRows` vs PostgreSQL** — differential, live-stack, e2e only: the module comment already admits an intentional collation approximation; the property bounds the divergence instead of leaving it anecdotal.

## 7. Bombadil

Worth a spike, not a schedule. If it is ever scheduled it must come with a replayable trace, a named owner, and a disposition path — otherwise it violates the testing skill's first hard rule, and `continue-on-error` just hides the violation. Auth is not the blocker (`--header`/`--cookie` plus custom actions can drive a login form); duplicated fixture setup and pre-1.0 churn are. Best first target if it happens: the live execution log surface, where convergence after reconnect is the property that matters.

## 8. Acceptance criteria met

- Property file runs inside the existing suite; no new CI job, no gate change.
- 100 runs in ~1.5 s wall; three-file run (property + runtime + store) 4.1 s total, dominated by happy-dom environment creation.
- `npm test` determinism preserved: fixed seed, no retries, no timers.
- Fix verified against both the property and the pre-existing `chat-runtime.test.ts` and `chatStore.test.ts` suites.

## 9. Upstream position

`CONTRIBUTING.md` is explicit that a reproduction, benchmark or test suite does not by itself make a submission eligible, and that unsolicited non-security audit findings are out of scope. This fork is public. Therefore:

- The harness and this note stay fork-only.
- The `chat-runtime` fix is a candidate only with a concrete actual-use basis: the deployment or task, the version, and the practical consequence (for example a stuck active-run indicator after a reconnect).
- Anything that turns out to be a path-filter or authorization divergence goes through `SECURITY.md` as a private draft, never a fork PR.
