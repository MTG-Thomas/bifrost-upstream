/**
 * Run one workflow to completion through an authenticated `fetch`.
 *
 * `useWorkflow` and the framework-neutral `bifrost/core` client share this, so
 * both get the same execution semantics:
 *
 *   - `POST /api/workflows/execute` starts the run (the server always starts
 *     it asynchronously and returns `{ execution_id, status }`).
 *   - Transient and already-terminal responses settle inline from the POST.
 *   - Otherwise the run subscribes to the execution websocket for live
 *     `status` and `log` events. Terminal frames do not carry the result, so a
 *     terminal event (or the subscription's `ready` ack) triggers one
 *     `GET /api/executions/{id}`. A fast run can finish before the socket
 *     opens, so one GET also runs right after subscribing.
 *   - If the socket goes down, or the post-terminal GET fails transiently,
 *     the run degrades to polling that GET every 2s. There is no client-side
 *     deadline: the server enforces the workflow's own timeout.
 */
import type { components } from "@/lib/v1";

import { subscribeToExecution, type ExecutionStreamEvent } from "./execution-stream";

export interface WorkflowLogEntry {
	level: string;
	message: string;
	timestamp: string;
	sequence?: number;
}

export interface WorkflowRunRequest {
	/** Workflow UUID, portable `path::function` ref, or workflow name. */
	workflowRef: string;
	input?: Record<string, unknown>;
	/** This App's id, so a `path::function` ref resolves to its own install. */
	appId?: string | null;
	orgScope?: string | null;
}

export interface WorkflowRunCallbacks {
	onExecutionId?: (executionId: string) => void;
	onStatus?: (status: string) => void;
	onLog?: (log: WorkflowLogEntry) => void;
	/** Abort stops following the run; it does not cancel the server execution. */
	signal?: AbortSignal;
}

// The generated contract type: `status` is the PascalCase `ExecutionStatus`
// literal union ("Success" | "Failed" | ...), so the compiler enforces the
// exact wire casing in the failed-status check below.
type ExecuteResponse = components["schemas"]["WorkflowExecutionResponse"];

export const TERMINAL_STATUSES: ReadonlySet<string> = new Set([
	"Success",
	"Failed",
	"CompletedWithErrors",
	"Timeout",
	"Cancelled",
]);

const POLL_INTERVAL_MS = 2000;

class ExecutionFetchError extends Error {
	readonly retryable: boolean;

	constructor(status: number, statusText: string) {
		super(`failed to fetch execution: ${status}${statusText ? ` ${statusText}` : ""}`);
		this.name = "ExecutionFetchError";
		// A just-created execution can briefly 404, and transient overload/server
		// failures can recover. Other 4xx responses (notably an expired/forbidden
		// token) will not improve by polling forever.
		this.retryable = status === 404 || status === 408 || status === 429 || status >= 500;
	}
}

interface ExecutionSnapshot {
	status?: string;
	result?: unknown;
	error_message?: string | null;
}

function settleFromExecution<T>(exec: ExecutionSnapshot): T {
	if (exec.status === "Success" || exec.status === "CompletedWithErrors") {
		return exec.result as T;
	}
	throw new Error(
		exec.error_message ?? `Workflow failed (status: ${exec.status ?? "unknown"})`,
	);
}

function abortError(signal: AbortSignal): unknown {
	return signal.reason ?? new DOMException("The workflow run was aborted.", "AbortError");
}

/** Start a workflow and resolve with its result, or reject with its error. */
export async function runWorkflow<T = unknown>(
	authedFetch: typeof fetch,
	{ workflowRef, input = {}, appId, orgScope }: WorkflowRunRequest,
	{ onExecutionId, onStatus, onLog, signal }: WorkflowRunCallbacks = {},
): Promise<T> {
	if (signal?.aborted) throw abortError(signal);

	const fetchExecution = async (id: string): Promise<ExecutionSnapshot> => {
		const r = await authedFetch(`/api/executions/${id}`);
		if (!r.ok) throw new ExecutionFetchError(r.status, r.statusText);
		return (await r.json()) as ExecutionSnapshot;
	};

	const resp = await authedFetch("/api/workflows/execute", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({
			workflow_id: workflowRef,
			input_data: input,
			// Scope a path::function ref to THIS install's own workflow (so it
			// can't resolve a sibling install's workflow sharing the path).
			...(appId ? { app_id: appId } : {}),
			...(orgScope ? { org_id: orgScope } : {}),
		}),
		...(signal ? { signal } : {}),
	});
	if (!resp.ok) {
		throw new Error(`workflow execution failed: ${resp.status} ${resp.statusText}`);
	}
	const body = (await resp.json()) as ExecuteResponse & { is_transient?: boolean };
	const execId = body.execution_id;
	if (execId) onExecutionId?.(execId);

	// Transient (data-provider-style) and already-terminal responses
	// settle inline — never touch the websocket.
	if (body.is_transient || TERMINAL_STATUSES.has(body.status)) {
		onStatus?.(body.status);
		return settleFromExecution<T>({
			status: body.status,
			result: body.result,
			error_message: body.error,
		});
	}

	return new Promise<T>((resolve, reject) => {
		if (signal?.aborted) {
			reject(abortError(signal));
			return;
		}
		let settled = false;
		let pollTimer: ReturnType<typeof setInterval> | null = null;
		let unsubscribe: () => void = () => {};
		// Set once a terminal ws frame has been seen. There's no client
		// deadline, so if the one post-terminal fetch fails transiently we
		// must not just give up — arm the poll fallback so the run still
		// settles instead of hanging forever.
		let sawTerminal = false;
		const onAbort = () => settle(() => reject(abortError(signal as AbortSignal)));
		const settle = (fn: () => void) => {
			if (settled) return;
			settled = true;
			if (pollTimer) clearInterval(pollTimer);
			signal?.removeEventListener("abort", onAbort);
			unsubscribe();
			fn();
		};
		const checkOnce = async () => {
			if (settled) return;
			try {
				const exec = await fetchExecution(execId);
				// Another concurrent check (ready ack, terminal frame, or poll)
				// may have settled while this request was in flight. Never let
				// its older snapshot regress terminal state after resolution.
				if (settled) return;
				if (exec.status) onStatus?.(exec.status);
				if (exec.status && TERMINAL_STATUSES.has(exec.status)) {
					settle(() => {
						try {
							resolve(settleFromExecution<T>(exec));
						} catch (e) {
							reject(e);
						}
					});
				}
			} catch (fetchError) {
				if (settled) return;
				if (fetchError instanceof ExecutionFetchError && !fetchError.retryable) {
					settle(() => reject(fetchError));
					return;
				}
				// Transient fetch failure — the stream/poll keeps driving.
				// If this was the post-terminal-event fetch, there's no further
				// status frame coming and the socket may stay open (so
				// onSocketDown never fires) — arm the poll fallback so the run
				// still settles instead of hanging forever.
				if (sawTerminal && !pollTimer) {
					pollTimer = setInterval(checkOnce, POLL_INTERVAL_MS);
				}
			}
		};
		signal?.addEventListener("abort", onAbort, { once: true });
		unsubscribe = subscribeToExecution(
			execId,
			(evt: ExecutionStreamEvent) => {
				if (evt.type === "ready") {
					// The server has installed the subscription. Re-check now so a
					// terminal transition between the initial GET and this ack
					// cannot be lost forever.
					void checkOnce();
				}
				if (evt.type === "log" && evt.log) onLog?.(evt.log);
				if (evt.type === "status") {
					if (evt.status) onStatus?.(evt.status);
					if (evt.isTerminal) {
						sawTerminal = true;
						void checkOnce();
					}
				}
			},
			() => {
				// Socket died — degrade to polling. No deadline: the server
				// enforces the workflow's own timeout and will flip it terminal.
				if (!settled && !pollTimer) pollTimer = setInterval(checkOnce, POLL_INTERVAL_MS);
			},
		);
		void checkOnce(); // fast runs can finish before the socket opens
	});
}
