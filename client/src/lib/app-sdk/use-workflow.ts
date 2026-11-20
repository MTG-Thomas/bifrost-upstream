/**
 * `useWorkflow` — the v2 SDK's workflow-execution hook.
 *
 * A standalone_v2 app calls `import { useWorkflow } from "bifrost"` and runs a
 * workflow through the authed transport its `<BifrostProvider>` established —
 * NOT the v1 `globalThis.__bifrost_platform` path (which reaches into platform
 * stores that a standalone app doesn't have). This mirrors how `useTable` reads
 * the provider context: auth/baseUrl/org come from `useBifrostContext()`, so the
 * same code runs in `npm run dev` (cross-origin, bearer token) and deployed.
 *
 * Two shapes, matching the v1 surface:
 *   - `useWorkflow(workflowRef)` → a query-style result you trigger with `run()`.
 *   - `run(input)` POSTs `/api/workflows/execute` (no `sync` flag — the server
 *     always starts the run asynchronously and returns `{ execution_id, status }`
 *     immediately).
 *
 * Streaming transport: after starting the run, `run()` subscribes to the
 * execution over the SDK websocket (`subscribeToExecution`) for live `status`
 * and `log` events. Terminal websocket frames do NOT carry the result (see
 * execution-stream.ts) — on `isTerminal`, `run()` unsubscribes and fetches
 * `GET /api/executions/{id}` once to get the actual `result`/`error_message`.
 * A fast run can finish before the socket even opens, so `run()` also does one
 * immediate `GET` right after subscribing. If the socket goes down
 * (`onSocketDown`), `run()` degrades to polling that same GET every 2s until
 * terminal — no client-side deadline; the server enforces the workflow's own
 * timeout and will eventually flip the execution terminal.
 *
 * Two exceptions bypass the stream entirely and settle inline from the POST
 * response: `is_transient` runs (data-provider-style executions) and runs
 * that are already terminal by the time the POST responds.
 */
import { useCallback, useRef, useState } from "react";

import { useBifrostContext } from "./provider";
import { runWorkflow, type WorkflowLogEntry } from "./workflow-run";

export type { WorkflowLogEntry } from "./workflow-run";

export interface UseWorkflowState<T> {
  /** Last successful result, or null before the first run. */
  data: T | null;
  /** True while a run is in flight. */
  loading: boolean;
  /** Last error, or null. */
  error: Error | null;
  /** Execute the workflow with `input_data`; resolves to the result. */
  run: (input?: Record<string, unknown>) => Promise<T>;
  /** Live log stream for the latest run. Reset to `[]` at the start of each run. */
  logs: WorkflowLogEntry[];
  /** Latest run's status (e.g. "Pending", "Running", "Success"), or null before the first run. */
  status: string | null;
  /** Latest run's execution id, or null before the first run. */
  executionId: string | null;
}

/**
 * Run a Bifrost workflow by UUID, portable `path::function` ref, or workflow
 * name from a v2 app. All three resolve identically: the server scopes the
 * ref to THIS install (via the X-Bifrost-App transport), so a name or path
 * ref reaches the install's own workflow first. Prefer `path::function` —
 * it's the ref shape that also runs locally under `bifrost solution start`
 * (name/UUID refs proxy to the deployed copy there). Must be called within a
 * `<BifrostProvider>` (throws otherwise — same contract as `useBifrostContext`).
 */
export function useWorkflow<T = unknown>(workflowRef: string): UseWorkflowState<T> {
  const { authedFetch, appId, orgScope } = useBifrostContext();
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [logs, setLogs] = useState<WorkflowLogEntry[]>([]);
  const [status, setStatus] = useState<string | null>(null);
  const [executionId, setExecutionId] = useState<string | null>(null);
  // Monotonic run counter: overlapping runs each capture their own seq, and
  // only the LATEST run (seq === seqRef.current) may write hook state. A slow
  // stale run can't overwrite a newer run's data or flip `loading` while the
  // newer run is still in flight. Each caller's promise still settles with its
  // own result/rejection.
  const seqRef = useRef(0);

  const run = useCallback(
    async (input: Record<string, unknown> = {}): Promise<T> => {
      const seq = ++seqRef.current;
      setLoading(true);
      setError(null);
      setLogs([]);
      setStatus("Pending");
      setExecutionId(null);

      try {
        const isLatest = () => seq === seqRef.current;
        const result = await runWorkflow<T>(
          authedFetch,
          { workflowRef, input, appId, orgScope },
          {
            onExecutionId: (id) => {
              if (isLatest()) setExecutionId(id);
            },
            onStatus: (next) => {
              if (isLatest()) setStatus(next);
            },
            onLog: (log) => {
              if (isLatest()) setLogs((prev) => [...prev, log]);
            },
          },
        );
        if (seq === seqRef.current) setData(result);
        return result;
      } catch (e) {
        const err = e instanceof Error ? e : new Error(String(e));
        if (seq === seqRef.current) setError(err);
        throw err;
      } finally {
        if (seq === seqRef.current) setLoading(false);
      }
    },
		[authedFetch, workflowRef, appId, orgScope],
  );

  return { data, loading, error, run, logs, status, executionId };
}
