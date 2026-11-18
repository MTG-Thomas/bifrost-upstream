import { useEffect, useId, useMemo, useRef } from "react";
import { Schema } from "effect";
import { Runtime } from "foldkit";
import { toast } from "sonner";
import type {
	ExecutionStatus,
	LogEntry,
} from "@/components/execution/ExecutionLogsPanel";

import { HostInputs, makeElement } from "./main";

/** Same inputs ExecutionDetails passes to the React ExecutionLogsPanel. */
export interface FoldkitExecutionLogsPanelProps {
	logs?: LogEntry[];
	status?: ExecutionStatus;
	isConnected?: boolean;
	isLoading?: boolean;
	canReadDiagnostics?: boolean;
	maxHeight?: string;
}

type EncodedHostInputs = typeof HostInputs.Encoded;
type EncodedLogLine = EncodedHostInputs["logs"][number];

const DEVTOOLS_OPT_IN_KEY = "bifrost.foldkit.devtools";

const isDevToolsOptedIn = (): boolean => {
	if (!import.meta.env.DEV) return false;
	try {
		return window.localStorage.getItem(DEVTOOLS_OPT_IN_KEY) === "1";
	} catch {
		return false;
	}
};

const lineKey = (log: LogEntry, index: number): string => {
	if (log.sequence !== undefined) return `seq-${log.sequence}`;
	if (log.id !== undefined) return `id-${log.id}`;
	return `index-${index}`;
};

/** Converts a merged API/stream log entry into the port's wire shape. */
export function toLogLine(log: LogEntry, index: number): EncodedLogLine {
	const data = "data" in log ? log.data : undefined;
	return {
		key: lineKey(log, index),
		level: log.level ?? "info",
		message: log.message ?? "",
		timestamp: log.timestamp ?? null,
		maybeDataJson:
			data && Object.keys(data).length > 0 ? JSON.stringify(data) : null,
	};
}

const decodeHostInputs = Schema.decodeUnknownSync(HostInputs);

type EmbedHandle = Runtime.EmbedHandle<ReturnType<typeof makeElement>["ports"]>;

/**
 * React host for the Foldkit execution-logs program. React keeps ownership
 * of server state, the execution stream, authorization, and toasts; the
 * Foldkit runtime owns only the panel's interaction state. Data crosses the
 * boundary through Schema-validated ports: host inputs in, notifications out.
 */
export function FoldkitExecutionLogsPanel({
	logs,
	status,
	isConnected,
	isLoading = false,
	canReadDiagnostics = false,
	maxHeight = "600px",
}: FoldkitExecutionLogsPanelProps) {
	const hostRef = useRef<HTMLDivElement>(null);
	const elementId = `execution-logs-${useId().replace(/[^a-zA-Z0-9_-]/g, "")}`;

	const logLines = useMemo(() => (logs ?? []).map(toLogLine), [logs]);
	const inputs = useMemo<EncodedHostInputs>(
		() => ({
			logs: logLines,
			maybeStatus: status ?? null,
			connection:
				isConnected === undefined
					? "Unknown"
					: isConnected
						? "Connected"
						: "Disconnected",
			isLoading,
			canReadDiagnostics,
		}),
		[logLines, status, isConnected, isLoading, canReadDiagnostics],
	);

	const latestInputs = useRef(inputs);
	const handleRef = useRef<EmbedHandle | null>(null);

	// NOTE: Foldkit swaps its rendered root in for the container node and only
	// swaps the container back from an asynchronous scope finalizer. React
	// must therefore never own the container itself: if it did, unmounting
	// this component while its parent stays mounted would make React remove
	// a node that is no longer in the DOM. React owns `hostRef`; the runtime
	// gets a node created here, which is removed once disposal completes.
	useEffect(() => {
		const host = hostRef.current;
		if (!host) return undefined;
		const container = document.createElement("div");
		container.id = `${elementId}-root`;
		host.appendChild(container);
		let maybeHandle: EmbedHandle | null = null;
		let unsubscribe = () => {};
		let isDisposed = false;
		const start = () => {
			if (isDisposed) return;
			const isDevToolsEnabled = isDevToolsOptedIn();
			const handle = Runtime.embed(
				makeElement(
					container,
					{
						elementId,
						maxHeight,
						inputs: decodeHostInputs(latestInputs.current),
					},
					{ isDevToolsEnabled },
				),
			);
			unsubscribe = handle.ports.notificationRequested.subscribe(
				({ text }) => toast.error(text),
			);
			maybeHandle = handle;
			handleRef.current = handle;
		};
		// The DevTools overlay registers itself with the Foldkit runtime, so it
		// must load before the program boots. foldkitScoped() serves it on
		// demand instead of injecting it into every page. The DEV check is
		// static, so production builds drop the import and its chunk.
		if (import.meta.env.DEV && isDevToolsOptedIn()) {
			void import("virtual:foldkit-devtools-overlay").then(start);
		} else {
			start();
		}
		return () => {
			isDisposed = true;
			unsubscribe();
			handleRef.current = null;
			const handle = maybeHandle;
			if (handle) {
				void handle.disposeAsync().then(() => container.remove());
			} else {
				container.remove();
			}
		};
	}, [elementId, maxHeight]);

	useEffect(() => {
		if (latestInputs.current === inputs) return;
		latestInputs.current = inputs;
		const exit = handleRef.current?.ports.hostInputsChanged.send(inputs);
		if (exit && exit._tag === "Failure") {
			console.error(
				"[FoldkitExecutionLogsPanel] rejected host inputs",
				exit,
			);
		}
	}, [inputs]);

	return (
		<div
			ref={hostRef}
			data-renderer="foldkit"
			className="min-w-0 xl:flex xl:min-h-0 xl:flex-1 xl:flex-col"
		/>
	);
}
