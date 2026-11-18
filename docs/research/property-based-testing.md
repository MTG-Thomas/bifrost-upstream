# Property-based testing for the Bifrost TypeScript front-end

**Date:** 2026-11-17 · **Status:** adopted, narrowly — two properties live; expansion gated on defect yield · **Scope:** `client/` only (fork-only work; see §10)

## TL;DR

- **Yes, but only at the reducer and pure-function layer.** The front-end already has 659 Vitest files and 139 Playwright specs; the gap is not test *count* but the absence of any statement about *all* inputs to its stateful logic.
- **Two real defects, both found by properties.** The first: `applyChatStreamEnvelopes` applied one render frame against a single shared clone, but rejected envelopes mutated that clone before deciding to reject — inventing phantom runs, and letting a rejected envelope bind an unscoped projection to its conversation. The second: the execution log buffer expected 1-based sequences while the server publishes from 0, so the first line of every streamed execution was dropped, and retransmitted buffered logs flushed twice. Both fixed; both counterexamples pinned as example tests.
- **`fast-check` 4.10.2, a dev dependency.** Two property files, fixed seed in the normal suite, 100 runs each. No new CI job, no change to the `@smoke` gate, no change to `maxWorkers`.
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

**Reachability, stated honestly.** Two things are proven: the structural asymmetry, and that a rejected envelope can bind an *unscoped* projection to its conversation (a second property seeds `conversation_id: null` and catches exactly that). The guard at `chat-runtime.ts:278-287` rejects any envelope whose conversation differs from an already-scoped projection, so a write that crosses conversations is **not** possible once the projection is bound; an earlier draft of this note overstated the defect that way. The phantom run needs a *present* sequence at or below the run's last sequence on a run the projection does not know about: the chat replay path defaults a missing sequence to `0` (`api/src/core/pubsub.py:550`, `api/src/services/chat_runs.py:634`), but the live chat publisher assigns a Redis `INCR` (`api/src/core/pubsub.py:81`), so the live path is unlikely to emit it. No end-to-end reproduction was captured. The structural asymmetry is proven; the user-visible frequency is not.

**Fix.** `applyEnvelopeToProjection` now decides against the pre-existing state and only then commits: chunk/run-id/sequence/dedupe rejection happens first, using `projection.runs[runId]` for the last-sequence lookup instead of a freshly inserted run. The accepted path is unchanged. Both counterexamples are pinned as example tests in `chat-runtime.test.ts` — the phantom run and the premature conversation binding — so a later generator change cannot silently drop a regression.

## 4. Second property: the execution log reorder buffer

`src/stores/executionStreamStore.ts` buffers out-of-order websocket logs against `expectedSequence`. It had four example tests, none of which touched sequencing, so the reorder buffer was untested.

**Oracle.** Given any delivery order and any batching, the store must display exactly the contiguous run of sequences starting at the server's first sequence — ascending, deduplicated — and completion must flush every delivered log exactly once, in order. The oracle is computed from the delivered set, not from the reducer.

**Counterexample 1 (seed `20261118`): `[0]` displayed nothing.** The server's counter starts at 0 (`api/bifrost/_logging.py:267`, and persistence allocates from `coalesce(max(sequence), -1) + 1` at lines 627-657), but `expectedSequence` was initialised to 1. Sequence 0 therefore matched neither `=== nextExpected` nor `> nextExpected` and fell through the "duplicates" branch. The first log line of every streamed execution was dropped from the live view until the persisted logs were fetched.

**Counterexample 2: `[10, 10]` flushed as `[10, 10]`.** Future logs buffered while a gap existed were queued once per delivery, and `completeExecution` flushes `pendingLogs` verbatim. A retransmitted future log became a visible duplicate on completion.

**Fixes.** Expect sequence 0; queue at most one buffered log per sequence within a pass (previously buffered logs are already part of the pending + incoming set, so the dedupe set is built inside the pass rather than pre-seeded). Both are internal to the store; no API or contract change.

**Known and deliberately unchanged.** A batch that mixes sequenced and unsequenced logs drops the unsequenced ones, because the sequenced loop admits only `=== nextExpected` or `> nextExpected`. Production batches are uniform and single-element (`useExecutionStream.ts:156`, `useWorkflowMutation.ts:309`), so the path is unreachable today; adding a branch for it would be a fallback nobody asked for. The trigger is recorded here so a future mixed-source caller sees it.

## 5. Seed policy

- **PR / `npm test`:** fixed seed per file (`20261117` for the chat projection, `20261118` for the log buffer), `numRuns: 100`. Reproducible, deterministic, no flakes.
- **Nightly (future):** rotate the seed, print it on failure. A rotating-seed failure is a real counterexample and gets a disposition under the testing skill — never a retry.
- **Every counterexample** is pinned as an explicit example test at the narrowest layer.

## 6. What not to do

- Do not put randomised runs in the `@smoke` gate or the merge queue.
- Do not property-test presentational components, and do not fuzz the login form.
- Do not assert a property that restates the implementation; use an independent oracle (a fold, a naive sort, a server-side comparison) or an algebraic law.
- Do not let a property test touch the DOM, timers, `Math.random` or the network.
- Do not expand to component-level PBT until the unit tier has produced a defect per unit of effort.

## 7. Next slices under consideration

1. **`applyPagedEvent`** — from a sorted, unique window within `pageSize`, events must preserve size, uniqueness and order, and must follow the documented page-edge rules. `sortedIndex` is private, so drive it through the hook's reducer.
2. **`compileFilterToExpr`** — never emits `{and: [...]}` with fewer than two operands (the server validator rejects it), empty filter maps to `null`, unsupported operators always throw.
3. **`parseApiError`** — total over `unknown`: never throws, always yields a non-empty message.
4. **`compareRows` vs PostgreSQL** — differential, live-stack, e2e only: the module comment already admits an intentional collation approximation; the property bounds the divergence instead of leaving it anecdotal.
5. **Server/client sequence agreement** — the two defects above were both off-by-one or origin mismatches found by reading `api/bifrost/_logging.py`. A single cross-boundary test that asserts the published origin matches the client's expectation would make the next such mismatch fail loudly.

## 8. Bombadil

Not evaluated. Deferred for three reasons — pre-1.0 and Chrome-only, it needs a named owner for its findings, and none of the current candidate invariants require a browser. A future evaluation should not be scheduled as a required check until it can produce a replayable trace that someone dispositions.

## 9. Acceptance criteria met

- Property files run inside the existing suite; no new CI job, no gate change.
- 100 generated cases per property execute in under 2 s of test time; total run time is dominated by happy-dom environment creation, which happens per test file.
- `npm test` determinism preserved: fixed seed, no retries, no timers. The cost of a fixed seed is that the same 100 cases repeat forever; a rotating seed would need its own scheduled job, which does not exist.
- Every fix verified three ways: the property red before and green after, the pinned example red against the pre-fix source and green after, and the pre-existing suites still green.

## 10. Upstream position

`CONTRIBUTING.md` is explicit that a reproduction, benchmark or test suite does not by itself make a submission eligible, and that unsolicited non-security audit findings are out of scope. This fork is public. Therefore:

- The harness and this note stay fork-only.
- The `chat-runtime` fix is a candidate only with a concrete actual-use basis: the deployment or task, the version, and the practical consequence (for example a stuck active-run indicator after a reconnect).
- The log-buffer fixes have a clearer user-visible claim — the first line of a streamed execution's live log was missing until a refetch — but that still needs a real deployment observation, not a deduction from source.
- Anything that turns out to be a path-filter or authorization divergence goes through `SECURITY.md` as a private draft, never a fork PR.
