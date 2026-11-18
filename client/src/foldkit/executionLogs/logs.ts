import { Array, Option, Schema, pipe } from "effect";

// SCHEMA

/** One execution log line as it crosses the React → Foldkit port boundary. */
export const LogLine = Schema.Struct({
	key: Schema.String,
	level: Schema.String,
	message: Schema.String,
	timestamp: Schema.OptionFromNullOr(Schema.String),
	maybeDataJson: Schema.OptionFromNullOr(Schema.String),
});
export type LogLine = typeof LogLine.Type;

/** Execution statuses the backend can report, plus the client-side
 *  transitional statuses the execution stream store synthesizes. */
export const ExecutionStatus = Schema.Literals([
	"Scheduled",
	"Pending",
	"Running",
	"Success",
	"Failed",
	"Timeout",
	"Stuck",
	"CompletedWithErrors",
	"Cancelling",
	"Cancelled",
]);
export type ExecutionStatus = typeof ExecutionStatus.Type;

export const LevelFilter = Schema.Literals([
	"all",
	"info",
	"warning",
	"error",
	"traceback",
	"debug",
]);
export type LevelFilter = typeof LevelFilter.Type;

// STATUS

const RUNNING_STATUSES: ReadonlyArray<ExecutionStatus> = [
	"Running",
	"Pending",
	"Cancelling",
];
const COMPLETE_STATUSES: ReadonlyArray<ExecutionStatus> = [
	"Success",
	"Failed",
	"CompletedWithErrors",
	"Timeout",
	"Cancelled",
];

export const isRunningStatus = (maybeStatus: Option.Option<ExecutionStatus>) =>
	Option.exists(maybeStatus, (status) => RUNNING_STATUSES.includes(status));

export const isCompleteStatus = (maybeStatus: Option.Option<ExecutionStatus>) =>
	Option.exists(maybeStatus, (status) => COMPLETE_STATUSES.includes(status));

// IDENTITY

const isSameLine = (left: LogLine, right: LogLine): boolean =>
	left.key === right.key &&
	left.level === right.level &&
	left.message === right.message &&
	Option.getOrNull(left.timestamp) === Option.getOrNull(right.timestamp) &&
	Option.getOrNull(left.maybeDataJson) ===
		Option.getOrNull(right.maybeDataJson);

/** Port values are Schema-decoded into fresh objects, so reference identity
 *  never survives the host boundary. Reuses the previous line object wherever
 *  the content is unchanged, and the previous array when nothing changed, so
 *  reference-based view memoization keeps working across host updates. */
export const reconcileLogs = (
	previous: ReadonlyArray<LogLine>,
	next: ReadonlyArray<LogLine>,
): ReadonlyArray<LogLine> => {
	const reconciled = next.map((line, index) => {
		const previousLine = previous[index];
		return previousLine !== undefined && isSameLine(previousLine, line)
			? previousLine
			: line;
	});
	return reconciled.length === previous.length &&
		reconciled.every((line, index) => line === previous[index])
		? previous
		: reconciled;
};

// FILTERING

const normalizedLevel = (line: LogLine): string =>
	(line.level || "info").toLowerCase();

const matchesQuery = (line: LogLine, normalizedQuery: string): boolean =>
	normalizedQuery === "" ||
	`${line.message} ${normalizedLevel(line)} ${Option.getOrElse(line.maybeDataJson, () => "")}`
		.toLowerCase()
		.includes(normalizedQuery);

/** Applies the search query and level filter exactly as the React panel does:
 *  case-insensitive substring over message, level, and serialized data. */
export const filterLogs = (
	logs: ReadonlyArray<LogLine>,
	query: string,
	levelFilter: LevelFilter,
): ReadonlyArray<LogLine> => {
	const normalizedQuery = query.trim().toLowerCase();
	return Array.filter(
		logs,
		(line) =>
			(levelFilter === "all" || normalizedLevel(line) === levelFilter) &&
			matchesQuery(line, normalizedQuery),
	);
};

// RENDER ITEMS

/** A renderable unit: a single log line, or consecutive TRACEBACK lines
 *  coalesced into one block (a Python traceback arrives as N entries). */
export type RenderItem =
	| Readonly<{ kind: "Line"; line: LogLine }>
	| Readonly<{
			kind: "Traceback";
			timestamp: Option.Option<string>;
			lines: ReadonlyArray<string>;
	  }>;

const isTraceback = (line: LogLine) => normalizedLevel(line) === "traceback";

// NOTE: grouping runs on every render of a streaming log, so it accumulates
// into local arrays to stay linear. Effect's immutable Array.append would make
// it quadratic in the number of lines.
export const coalesceTracebacks = (
	logs: ReadonlyArray<LogLine>,
): ReadonlyArray<RenderItem> => {
	const items: Array<RenderItem> = [];
	const tracebackLines: Array<string> = [];
	let tracebackTimestamp: Option.Option<string> = Option.none();
	const flushTraceback = () => {
		if (tracebackLines.length > 0) {
			items.push({
				kind: "Traceback",
				timestamp: tracebackTimestamp,
				lines: tracebackLines.splice(0),
			});
		}
	};
	logs.forEach((line) => {
		if (isTraceback(line)) {
			if (tracebackLines.length === 0) {
				tracebackTimestamp = line.timestamp;
			}
			tracebackLines.push(line.message);
			return;
		}
		flushTraceback();
		items.push({ kind: "Line", line });
	});
	flushTraceback();
	return items;
};

// FORMATTING

export const formatLogTime = (maybeTimestamp: Option.Option<string>): string =>
	pipe(
		maybeTimestamp,
		Option.map((timestamp) => new Date(timestamp).toLocaleTimeString()),
		Option.getOrElse(() => ""),
	);

const formatLine = (line: LogLine): string =>
	`${formatLogTime(line.timestamp)}  ${(line.level || "INFO").toUpperCase()}  ${line.message}`;

/** Clipboard text: the currently visible lines, in display order. */
export const toClipboardText = (logs: ReadonlyArray<LogLine>): string =>
	Array.join(Array.map(logs, formatLine), "\n");

/** Download text: every line, with structured data appended. */
export const toDownloadText = (logs: ReadonlyArray<LogLine>): string =>
	Array.join(
		Array.map(
			logs,
			(line) =>
				`${formatLine(line)}${Option.match(line.maybeDataJson, {
					onNone: () => "",
					onSome: (json) => ` ${json}`,
				})}`,
		),
		"\n",
	);
