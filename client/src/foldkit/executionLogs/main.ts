import { copyToClipboard } from "@/lib/clipboard";
import clsx from "clsx";
import { Effect, Option, Queue, Schema, Stream } from "effect";
import {
	Command,
	Mount,
	Port,
	Render,
	Runtime,
	Subscription,
	type Update,
} from "foldkit";
import {
	createKeyedLazy,
	createLazy,
	type Html,
	type HtmlBuilder,
} from "foldkit/html";
import { defineMessageUnion } from "foldkit/message";
import { defineTaggedUnion } from "foldkit/schema";
import { modifyFields } from "foldkit/struct";
import { Select } from "@foldkit/ui";
import { ArrowDown, Check, Copy, Download, Search, X } from "lucide";

import * as BifrostBadge from "@/foldkit/ui/badge";
import * as BifrostButton from "@/foldkit/ui/button";
import * as BifrostInput from "@/foldkit/ui/input";

import { icon } from "@/foldkit/ui/icons";
import {
	ExecutionStatus,
	LevelFilter,
	LogLine,
	type RenderItem,
	coalesceTracebacks,
	filterLogs,
	formatLogTime,
	isCompleteStatus,
	isRunningStatus,
	reconcileLogs,
	toClipboardText,
	toDownloadText,
} from "./logs";

// MODEL

/** Live-socket state as the host knows it. `Unknown` mirrors the React
 *  panel's `isConnected === undefined`, which shows neither the Live badge
 *  nor the connection warning. */
export const Connection = Schema.Literals([
	"Connected",
	"Disconnected",
	"Unknown",
]);
export type Connection = typeof Connection.Type;

export const CopyFeedback = defineTaggedUnion({
	Idle: {},
	Copied: {},
});

/** Everything the React host owns and pushes in. The host remains the owner
 *  of server state (React Query) and the execution stream (Zustand). */
export const HostInputs = Schema.Struct({
	logs: Schema.Array(LogLine),
	maybeStatus: Schema.OptionFromNullOr(ExecutionStatus),
	connection: Connection,
	isLoading: Schema.Boolean,
	canReadDiagnostics: Schema.Boolean,
});
export type HostInputs = typeof HostInputs.Type;

export const Model = Schema.Struct({
	elementId: Schema.String,
	maxHeight: Schema.String,
	inputs: HostInputs,
	query: Schema.String,
	levelFilter: LevelFilter,
	isFollowing: Schema.Boolean,
	copyFeedback: CopyFeedback,
});
export type Model = typeof Model.Type;

// MESSAGE

export const Message = defineMessageUnion({
	UpdatedHostInputs: { inputs: HostInputs },
	UpdatedQuery: { query: Schema.String },
	ClickedClearQuery: {},
	SelectedLevelFilter: { value: Schema.String },
	ClickedClearFilters: {},
	ClickedCopyLogs: {},
	SucceededCopyLogs: {},
	FailedCopyLogs: {},
	CompletedWaitBeforeResetCopied: {},
	CompletedNotifyCopyFailed: {},
	ClickedDownloadLogs: {},
	CompletedDownloadLogs: {},
	ObservedFollowEdge: { isAtFollowEdge: Schema.Boolean },
	ClickedJumpToLatest: {},
	CompletedScrollToLatest: {},
});
export type Message = typeof Message.Type;

// PORT

export const Notification = Schema.Struct({
	tone: Schema.Literals(["Error"]),
	text: Schema.String,
});

export const ports = {
	inbound: { hostInputsChanged: Port.inbound(HostInputs) },
	outbound: { notificationRequested: Port.outbound(Notification) },
};

// INIT

export const Flags = Schema.Struct({
	elementId: Schema.String,
	maxHeight: Schema.String,
	inputs: HostInputs,
});
export type Flags = typeof Flags.Type;

export const init: Runtime.ElementInit<Model, Message, Flags> = (flags) => ({
	model: {
		elementId: flags.elementId,
		maxHeight: flags.maxHeight,
		inputs: flags.inputs,
		query: "",
		levelFilter: "all",
		isFollowing: true,
		copyFeedback: CopyFeedback.Idle(),
	},
	commands: [ScrollToLatest({ elementId: flags.elementId })],
});

// COMMAND

const COPIED_FEEDBACK_DURATION = "2 seconds";
const FOLLOW_EDGE_THRESHOLD_PX = 50;

export const logListId = (elementId: string) => `${elementId}-messages`;

export const CopyLogs = Command.define("CopyLogs", {
	args: { text: Schema.String },
	messages: [Message.SucceededCopyLogs, Message.FailedCopyLogs],
	execute: ({ text }) =>
		Effect.promise(() => copyToClipboard(text)).pipe(
			Effect.map((isCopied) =>
				isCopied
					? Message.SucceededCopyLogs()
					: Message.FailedCopyLogs(),
			),
		),
});

export const WaitBeforeResetCopied = Command.define("WaitBeforeResetCopied", {
	messages: [Message.CompletedWaitBeforeResetCopied],
	execute: Effect.sleep(COPIED_FEEDBACK_DURATION).pipe(
		Effect.as(Message.CompletedWaitBeforeResetCopied()),
	),
});

export const NotifyCopyFailed = Command.define("NotifyCopyFailed", {
	messages: [Message.CompletedNotifyCopyFailed],
	execute: Port.emit(ports.outbound.notificationRequested, {
		tone: "Error",
		text: "Failed to copy logs",
	}).pipe(Effect.as(Message.CompletedNotifyCopyFailed())),
});

export const DownloadLogs = Command.define("DownloadLogs", {
	args: { text: Schema.String },
	messages: [Message.CompletedDownloadLogs],
	execute: ({ text }) =>
		Effect.sync(() => {
			const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
			const url = URL.createObjectURL(blob);
			const link = document.createElement("a");
			link.href = url;
			link.download = "execution-logs.txt";
			link.click();
			URL.revokeObjectURL(url);
			return Message.CompletedDownloadLogs();
		}),
});

/** Scrolls the log list to its newest line once the pending render commits.
 *  A missing list (empty, loading, or filtered-out states) is not an error. */
export const ScrollToLatest = Command.define("ScrollToLatest", {
	args: { elementId: Schema.String },
	messages: [Message.CompletedScrollToLatest],
	execute: ({ elementId }) =>
		Render.afterCommit.pipe(
			Effect.andThen(
				Effect.sync(() => {
					const list = document.getElementById(logListId(elementId));
					if (list !== null) {
						list.scrollTop = list.scrollHeight;
					}
					return Message.CompletedScrollToLatest();
				}),
			),
		),
});

// MOUNT

const isAtFollowEdge = (element: Element) =>
	element.scrollHeight - element.scrollTop - element.clientHeight <
	FOLLOW_EDGE_THRESHOLD_PX;

/** Watches the log list's scroll position for as long as the list is in the
 *  DOM. The runtime removes the listener when the element unmounts. */
export const ObserveFollowEdge = Mount.defineStream("ObserveFollowEdge", {
	messages: [Message.ObservedFollowEdge],
	execute: ({ element }) =>
		Stream.callback<typeof Message.ObservedFollowEdge.Type>((queue) =>
			Effect.acquireRelease(
				Effect.sync(() => {
					const onScroll = () =>
						Queue.offerUnsafe(
							queue,
							Message.ObservedFollowEdge({
								isAtFollowEdge: isAtFollowEdge(element),
							}),
						);
					element.addEventListener("scroll", onScroll, {
						passive: true,
					});
					return onScroll;
				}),
				(onScroll) =>
					Effect.sync(() =>
						element.removeEventListener("scroll", onScroll),
					),
			),
		).pipe(Stream.changes),
});

// UPDATE

type UpdateReturn = Update.Return<Model, Message>;

const hasVisibleLogs = (model: Model) =>
	filterLogs(model.inputs.logs, model.query, model.levelFilter).length > 0;

const followLatest = (model: Model): UpdateReturn =>
	model.isFollowing && hasVisibleLogs(model)
		? { model, commands: [ScrollToLatest({ elementId: model.elementId })] }
		: { model };

export const update = (model: Model, message: Message): UpdateReturn =>
	Message.match<UpdateReturn>(message, {
		UpdatedHostInputs: ({ inputs }) => {
			const logs = reconcileLogs(model.inputs.logs, inputs.logs);
			const nextModel = modifyFields(model, {
				inputs: () => modifyFields(inputs, { logs: () => logs }),
			});
			return logs === model.inputs.logs
				? { model: nextModel }
				: followLatest(nextModel);
		},
		UpdatedQuery: ({ query }) =>
			followLatest(modifyFields(model, { query: () => query })),
		ClickedClearQuery: () =>
			followLatest(modifyFields(model, { query: () => "" })),
		SelectedLevelFilter: ({ value }) =>
			Schema.is(LevelFilter)(value)
				? followLatest(
						modifyFields(model, { levelFilter: () => value }),
					)
				: { model },
		ClickedClearFilters: () =>
			followLatest(
				modifyFields(model, {
					query: () => "",
					levelFilter: () => "all" as const,
				}),
			),
		ClickedCopyLogs: () => ({
			model,
			commands: [
				CopyLogs({
					text: toClipboardText(
						filterLogs(
							model.inputs.logs,
							model.query,
							model.levelFilter,
						),
					),
				}),
			],
		}),
		SucceededCopyLogs: () => ({
			model: modifyFields(model, {
				copyFeedback: () => CopyFeedback.Copied(),
			}),
			commands: [WaitBeforeResetCopied()],
		}),
		FailedCopyLogs: () => ({ model, commands: [NotifyCopyFailed()] }),
		CompletedWaitBeforeResetCopied: () => ({
			model: modifyFields(model, {
				copyFeedback: () => CopyFeedback.Idle(),
			}),
		}),
		CompletedNotifyCopyFailed: () => ({ model }),
		ClickedDownloadLogs: () => ({
			model,
			commands: [
				DownloadLogs({ text: toDownloadText(model.inputs.logs) }),
			],
		}),
		CompletedDownloadLogs: () => ({ model }),
		ObservedFollowEdge: ({ isAtFollowEdge }) => ({
			model: modifyFields(model, { isFollowing: () => isAtFollowEdge }),
		}),
		ClickedJumpToLatest: () => ({
			model: modifyFields(model, { isFollowing: () => true }),
			commands: [ScrollToLatest({ elementId: model.elementId })],
		}),
		CompletedScrollToLatest: () => ({ model }),
	});

// SUBSCRIPTION

export const subscriptions = Subscription.make<Model, Message>()(() => ({
	hostInputs: Port.subscriptionEntry(
		ports.inbound.hostInputsChanged,
		(inputs) => Message.UpdatedHostInputs({ inputs }),
	),
}));

// VIEW

const LEVEL_COLORS: Record<string, string> = {
	debug: "text-muted-foreground",
	info: "text-[var(--bf-info)]",
	warning: "text-[var(--bf-warning)]",
	error: "text-[var(--bf-danger)]",
	traceback: "text-[var(--bf-danger)]",
};

const ROW_CLASS =
	"grid min-w-0 grid-cols-[auto_1fr] gap-x-3 gap-y-1 border-b border-border/50 px-4 py-3 font-mono text-xs last:border-0 @3xl:grid-cols-[auto_78px_minmax(0,1fr)] @3xl:py-2 hover:bg-muted/40";

const SELECT_CLASS =
	"h-10 w-full rounded-[var(--bf-radius-control)] border border-border/70 bg-background px-3 py-2 text-sm outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/25";

const SKELETON_CLASS =
	"animate-pulse rounded-[var(--bf-radius-surface)] bg-muted motion-reduce:animate-none h-3";

const LIVE_DOT_CLASS =
	"h-2 w-2 rounded-full bg-[image:var(--bf-activity-gradient)] motion-safe:animate-pulse motion-reduce:animate-none";

/** Mirrors `logSeverity` in LogEntryRow.tsx: the inline-start severity rule. */
const severity = (
	level: string,
): Readonly<{ label: string; color: string }> => {
	const normalized = level.toLowerCase();
	if (["error", "critical", "fatal", "traceback"].includes(normalized)) {
		return { label: "error", color: "var(--bf-danger)" };
	}
	if (["warn", "warning"].includes(normalized)) {
		return { label: "warning", color: "var(--bf-warning)" };
	}
	if (["debug", "trace"].includes(normalized)) {
		return { label: "debug", color: "var(--muted-foreground)" };
	}
	return { label: "info", color: "var(--bf-info)" };
};

const severityAttributes = (level: string, h: HtmlBuilder<Message>) => {
	const { label, color } = severity(level);
	return [
		h.DataAttribute("severity", label),
		h.Style({ borderInlineStart: `3px solid ${color}` }),
	];
};

const prettyJson = (json: string): string =>
	JSON.stringify(JSON.parse(json), null, 2);

const tracebackView = (
	item: Extract<RenderItem, { kind: "Traceback" }>,
	key: string,
	h: HtmlBuilder<Message>,
): Html =>
	h.keyed("div")(
		key,
		[
			h.Class(ROW_CLASS),
			h.DataAttribute("testid", "log-traceback-block"),
			...severityAttributes("traceback", h),
		],
		[
			h.span(
				[
					h.Class(
						"whitespace-nowrap tabular-nums text-muted-foreground",
					),
				],
				[formatLogTime(item.timestamp)],
			),
			h.span(
				[
					h.Class(
						`min-w-0 font-semibold uppercase ${LEVEL_COLORS.traceback}`,
					),
				],
				["traceback"],
			),
			h.pre(
				[
					h.Class(
						"col-span-full min-w-0 whitespace-pre-wrap text-sm leading-relaxed [overflow-wrap:anywhere] @3xl:col-span-1",
					),
				],
				[item.lines.join("\n")],
			),
		],
	);

const lineView = (line: LogLine, h: HtmlBuilder<Message>): Html => {
	const levelColor =
		LEVEL_COLORS[line.level.toLowerCase()] ?? "text-muted-foreground";
	return h.keyed("div")(
		line.key,
		[h.Class(ROW_CLASS), ...severityAttributes(line.level, h)],
		[
			h.span(
				[
					h.Class(
						"whitespace-nowrap tabular-nums text-muted-foreground",
					),
				],
				[formatLogTime(line.timestamp)],
			),
			h.span(
				[h.Class(`min-w-0 font-semibold uppercase ${levelColor}`)],
				[line.level],
			),
			h.span(
				[
					h.DataAttribute("testid", "log-message"),
					h.Class(
						"col-span-full min-w-0 whitespace-pre-wrap text-sm leading-relaxed [overflow-wrap:anywhere] @3xl:col-span-1",
					),
				],
				[line.message],
			),
			Option.match(line.maybeDataJson, {
				onNone: () => h.empty,
				onSome: (json) =>
					h.details(
						[
							h.Class(
								"col-span-full min-w-0 text-xs @3xl:col-start-3",
							),
						],
						[
							h.summary(
								[
									h.Class(
										"min-h-11 cursor-pointer content-center text-muted-foreground",
									),
								],
								["data"],
							),
							h.pre(
								[
									h.Class(
										"mt-1 min-w-0 whitespace-pre-wrap rounded-[var(--bf-radius-control)] bg-muted p-2 [overflow-wrap:anywhere]",
									),
								],
								[prettyJson(json)],
							),
						],
					),
			}),
		],
	);
};

/** Per-row memo: a row is rebuilt only when its line object changes, which
 *  `reconcileLogs` limits to lines whose content actually changed. */
const lazyLineView = createKeyedLazy();

const logListView = (
	logs: ReadonlyArray<LogLine>,
	query: string,
	levelFilter: LevelFilter,
	elementId: string,
	maxHeight: string,
	h: HtmlBuilder<Message>,
): Html =>
	h.keyed("div")(
		"list",
		[
			h.Id(logListId(elementId)),
			h.Style({
				maxHeight: `var(--execution-log-max-height, ${maxHeight})`,
			}),
			h.Class(
				"min-h-0 min-w-0 overflow-y-auto focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring",
			),
			h.Role("region"),
			h.AriaLabel("Execution log messages"),
			h.Tabindex(0),
			h.OnMount(ObserveFollowEdge()),
		],
		coalesceTracebacks(filterLogs(logs, query, levelFilter)).map(
			(item, index) =>
				item.kind === "Line"
					? lazyLineView(item.line.key, lineView, [item.line, h])
					: tracebackView(item, `traceback-${index}`, h),
		),
	);

const lazyLogList = createLazy();

const iconButton = (
	config: Readonly<{
		label: string;
		title: string;
		onClick: Message;
		content: Html;
	}>,
	h: HtmlBuilder<Message>,
): Html =>
	BifrostButton.view(
		{
			variant: "ghost",
			size: "icon-lg",
			onClick: config.onClick,
			attributes: [h.Title(config.title), h.AriaLabel(config.label)],
			content: [config.content],
		},
		h,
	);

const headerView = (
	model: Model,
	visibleCount: number,
	h: HtmlBuilder<Message>,
): Html => {
	const { logs, canReadDiagnostics, connection, maybeStatus } = model.inputs;
	const lineCount = logs.length;
	const isCopied = model.copyFeedback._tag === "Copied";
	const countLabel = [
		`${lineCount} line${lineCount !== 1 ? "s" : ""}`,
		visibleCount !== lineCount ? ` · ${visibleCount} visible` : "",
		canReadDiagnostics ? "" : " · INFO and above",
	].join("");
	return h.div(
		[
			h.Class(
				"shrink-0 space-y-3 border-b border-border/50 bg-muted px-3 py-3",
			),
		],
		[
			h.div(
				[h.Class("flex items-center justify-between gap-3")],
				[
					h.div(
						[h.Class("flex min-w-0 flex-wrap items-center gap-2")],
						[
							h.span(
								[h.Class("text-sm font-semibold")],
								["Logs"],
							),
							lineCount > 0
								? h.span(
										[
											h.Class(
												"text-xs text-muted-foreground",
											),
										],
										[countLabel],
									)
								: h.empty,
							connection === "Connected" &&
							isRunningStatus(maybeStatus)
								? BifrostBadge.view(
										{
											variant: "secondary",
											className: "gap-1 text-xs",
											content: [
												h.span([
													h.DataAttribute(
														"testid",
														"execution-live-indicator",
													),
													h.AriaHidden(true),
													h.Class(LIVE_DOT_CLASS),
												]),
												"Live",
											],
										},
										h,
									)
								: h.empty,
						],
					),
					lineCount > 0
						? h.div(
								[h.Class("flex shrink-0 items-center gap-1")],
								[
									iconButton(
										{
											label: isCopied
												? "Logs copied"
												: "Copy logs",
											title: "Copy logs",
											onClick: Message.ClickedCopyLogs(),
											content: isCopied
												? icon(
														Check,
														"h-3.5 w-3.5 text-[var(--bf-success)]",
														h,
													)
												: icon(Copy, "h-3.5 w-3.5", h),
										},
										h,
									),
									iconButton(
										{
											label: "Download all logs",
											title: "Download all logs",
											onClick:
												Message.ClickedDownloadLogs(),
											content: icon(
												Download,
												"h-3.5 w-3.5",
												h,
											),
										},
										h,
									),
								],
							)
						: h.empty,
				],
			),
			lineCount > 0 ? filtersView(model, h) : h.empty,
		],
	);
};

const LEVEL_OPTIONS: ReadonlyArray<
	Readonly<{ value: LevelFilter; label: string }>
> = [
	{ value: "all", label: "All levels" },
	{ value: "info", label: "Info" },
	{ value: "warning", label: "Warning" },
	{ value: "error", label: "Error" },
	{ value: "traceback", label: "Traceback" },
];

const filtersView = (model: Model, h: HtmlBuilder<Message>): Html =>
	h.div(
		[h.Class("grid gap-2 @lg:grid-cols-[minmax(0,1fr)_9rem]")],
		[
			BifrostInput.view(
				{
					id: `${model.elementId}-search`,
					label: "Search logs",
					isLabelVisible: false,
					value: model.query,
					placeholder: "Search logs...",
					onInput: (query) => Message.UpdatedQuery({ query }),
					className: "pl-9 pr-11",
					leading: icon(
						Search,
						"pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground",
						h,
					),
					trailing:
						model.query === ""
							? undefined
							: h.span(
									[
										h.Class(
											"absolute inset-y-0 right-1 z-20 flex items-center",
										),
									],
									[
										BifrostButton.view(
											{
												variant: "ghost",
												size: "icon",
												className: "size-8",
												onClick:
													Message.ClickedClearQuery(),
												attributes: [
													h.AriaLabel(
														"Clear log search",
													),
												],
												content: [icon(X, "size-4", h)],
											},
											h,
										),
									],
								),
				},
				h,
			),
			Select.view(
				{
					id: `${model.elementId}-level`,
					value: model.levelFilter,
					onChange: (value) => Message.SelectedLevelFilter({ value }),
					toView: (attributes) =>
						h.div(
							[h.Class("min-w-0")],
							[
								h.label(
									[...attributes.label, h.Class("sr-only")],
									["Log level"],
								),
								h.select(
									[
										...attributes.select,
										h.Class(SELECT_CLASS),
									],
									[
										...LEVEL_OPTIONS,
										...(model.inputs.canReadDiagnostics
											? [
													{
														value: "debug" as const,
														label: "Debug",
													},
												]
											: []),
									].map(({ value, label }) =>
										h.option(
											[
												h.Value(value),
												h.Selected(
													value === model.levelFilter,
												),
											],
											[label],
										),
									),
								),
							],
						),
				},
				h,
			),
		],
	);

const contentView = (
	model: Model,
	visibleCount: number,
	h: HtmlBuilder<Message>,
): Html => {
	const { logs, isLoading, maybeStatus } = model.inputs;
	if (logs.length === 0 && !isLoading) {
		return h.keyed("div")(
			"empty",
			[h.Class("py-8 text-center text-sm text-muted-foreground")],
			[
				isRunningStatus(maybeStatus)
					? "Waiting for logs..."
					: isCompleteStatus(maybeStatus)
						? "No logs captured"
						: "No execution in progress",
			],
		);
	}
	if (logs.length === 0) {
		return h.keyed("div")(
			"loading",
			[
				h.Role("status"),
				h.AriaLabel("Loading execution logs"),
				h.Class("space-y-2 p-4"),
			],
			[
				h.div([h.Class(clsx(SKELETON_CLASS, "w-full"))]),
				h.div([h.Class(clsx(SKELETON_CLASS, "w-5/6"))]),
				h.div([h.Class(clsx(SKELETON_CLASS, "w-4/5"))]),
			],
		);
	}
	if (visibleCount === 0) {
		return h.keyed("div")(
			"no-matches",
			[
				h.Class(
					"space-y-3 px-4 py-8 text-center text-sm text-muted-foreground",
				),
			],
			[
				h.p([], ["No logs match the current filters"]),
				BifrostButton.view(
					{
						variant: "outline",
						className: "min-h-11",
						onClick: Message.ClickedClearFilters(),
						content: ["Clear filters"],
					},
					h,
				),
			],
		);
	}
	return lazyLogList(logListView, [
		model.inputs.logs,
		model.query,
		model.levelFilter,
		model.elementId,
		model.maxHeight,
		h,
	]);
};

export const view = (model: Model, h: HtmlBuilder<Message>): Html => {
	const visibleCount = filterLogs(
		model.inputs.logs,
		model.query,
		model.levelFilter,
	).length;
	const { connection, maybeStatus } = model.inputs;
	return h.div(
		[
			h.Class(
				"@container relative min-w-0 overflow-hidden rounded-[var(--bf-radius-surface)] border border-border bg-background xl:flex xl:min-h-0 xl:flex-1 xl:flex-col xl:[--execution-log-max-height:none]",
			),
		],
		[
			headerView(model, visibleCount, h),
			isRunningStatus(maybeStatus) && connection === "Disconnected"
				? h.p(
						[
							h.Role("status"),
							h.Class(
								"border-b border-border/50 bg-[var(--bf-warning-soft)] px-3 py-3 text-sm text-foreground",
							),
						],
						[
							"Live connection unavailable. New log messages may be delayed.",
						],
					)
				: h.empty,
			contentView(model, visibleCount, h),
			!model.isFollowing && visibleCount > 0
				? h.div(
						[
							h.Class(
								"absolute inset-x-0 bottom-0 z-10 flex flex-wrap items-center justify-between gap-2 border-t border-border bg-muted px-3 py-2",
							),
						],
						[
							h.span(
								[
									h.Role("status"),
									h.Class("text-xs text-muted-foreground"),
								],
								["Following paused"],
							),
							BifrostButton.view(
								{
									variant: "outline",
									className: "min-h-11",
									onClick: Message.ClickedJumpToLatest(),
									content: [
										icon(ArrowDown, "h-4 w-4", h),
										"Jump to latest",
									],
								},
								h,
							),
						],
					)
				: h.empty,
		],
	);
};

// PROGRAM

const HIGH_FREQUENCY_MESSAGES = ["ObservedFollowEdge"];

/** Builds the embeddable program. DevTools records every Model, which here
 *  includes raw execution log text, so it is enabled only for an explicit
 *  local opt-in and never in a production build. */
export const makeElement = (
	container: HTMLElement,
	flags: Flags,
	options: Readonly<{ isDevToolsEnabled: boolean }>,
) =>
	Runtime.makeElement({
		Model,
		Flags,
		flags: Effect.succeed(flags),
		init,
		update,
		view,
		subscriptions,
		ports,
		container,
		devTools: options.isDevToolsEnabled
			? {
					Message,
					excludeFromHistory: HIGH_FREQUENCY_MESSAGES,
					mode: "Inspect",
				}
			: false,
	});
