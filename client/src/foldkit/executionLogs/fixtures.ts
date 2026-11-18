import { Option } from "effect";

import { CopyFeedback, type HostInputs, type Model } from "./main";
import type { LogLine } from "./logs";

export const ELEMENT_ID = "execution-logs-test";

export const logLine = (
	overrides: Partial<LogLine> & Pick<LogLine, "key" | "message">,
): LogLine => ({
	level: "INFO",
	timestamp: Option.some("2026-11-18T10:00:00Z"),
	maybeDataJson: Option.none(),
	...overrides,
});

export const SAMPLE_LOGS: ReadonlyArray<LogLine> = [
	logLine({ key: "seq-1", message: "Fetching tickets" }),
	logLine({
		key: "seq-2",
		level: "WARNING",
		message: "Rate limited, retrying",
	}),
	logLine({
		key: "seq-3",
		level: "ERROR",
		message: "Ticket sync failed",
		maybeDataJson: Option.some('{"ticket":42}'),
	}),
	logLine({
		key: "seq-4",
		level: "TRACEBACK",
		message: "Traceback (most recent call last):",
	}),
	logLine({
		key: "seq-5",
		level: "TRACEBACK",
		message: '  File "sync.py", line 9',
	}),
];

export const hostInputs = (
	overrides: Partial<HostInputs> = {},
): HostInputs => ({
	logs: SAMPLE_LOGS,
	maybeStatus: Option.some("Running"),
	connection: "Connected",
	isLoading: false,
	canReadDiagnostics: false,
	...overrides,
});

export const model = (overrides: Partial<Model> = {}): Model => ({
	elementId: ELEMENT_ID,
	maxHeight: "600px",
	inputs: hostInputs(),
	query: "",
	levelFilter: "all",
	isFollowing: true,
	copyFeedback: CopyFeedback.Idle(),
	...overrides,
});
