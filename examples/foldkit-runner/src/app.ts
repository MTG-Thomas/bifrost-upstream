import { Array, Effect, Option, Schedule, Schema, Stream } from "effect";
import { Command, Runtime, Subscription, type Update } from "foldkit";
import type { Html, HtmlBuilder } from "foldkit/html";
import { defineMessageUnion } from "foldkit/message";
import { defineTaggedUnion } from "foldkit/schema";
import { modifyFields } from "foldkit/struct";
import { Button, Input } from "@foldkit/ui";
import clsx from "clsx";

import {
	Connection,
	CurrentUser,
	Execution,
	TERMINAL_STATUSES,
	fetchCurrentUser,
	fetchExecution,
	startWorkflow,
} from "./bifrost";

const WORKFLOW_REF = "workflows/spike_log_stream.py::spike_log_stream";
const POLL_INTERVAL = "500 millis";

// MODEL

const Run = defineTaggedUnion({
	Idle: {},
	Starting: {},
	InProgress: { executionId: Schema.String, status: Schema.String },
	Finished: { execution: Execution },
	Failed: { error: Schema.String },
});
type Run = typeof Run.Type;

const Model = Schema.Struct({
	connection: Connection,
	maybeUser: Schema.Option(CurrentUser),
	lines: Schema.String,
	isFailRequested: Schema.Boolean,
	run: Run,
	history: Schema.Array(Execution),
});
type Model = typeof Model.Type;

// MESSAGE

const Message = defineMessageUnion({
	SucceededFetchCurrentUser: { user: CurrentUser },
	FailedFetchCurrentUser: { error: Schema.String },
	UpdatedLines: { value: Schema.String },
	ToggledFail: { isRequested: Schema.Boolean },
	ClickedRunWorkflow: {},
	SucceededStartWorkflow: { execution: Execution },
	FailedStartWorkflow: { error: Schema.String },
	PolledExecution: { execution: Execution },
	FailedPollExecution: { error: Schema.String },
});
type Message = typeof Message.Type;

// COMMAND

const FetchCurrentUser = Command.define("FetchCurrentUser", {
	args: { connection: Connection },
	messages: [Message.SucceededFetchCurrentUser, Message.FailedFetchCurrentUser],
	execute: ({ connection }) =>
		fetchCurrentUser(connection).pipe(
			Effect.match({
				onSuccess: (user) => Message.SucceededFetchCurrentUser({ user }),
				onFailure: (error) =>
					Message.FailedFetchCurrentUser({ error: error.detail }),
			}),
		),
});

const StartWorkflow = Command.define("StartWorkflow", {
	args: {
		connection: Connection,
		lines: Schema.Number,
		isFailRequested: Schema.Boolean,
	},
	messages: [Message.SucceededStartWorkflow, Message.FailedStartWorkflow],
	execute: ({ connection, lines, isFailRequested }) =>
		startWorkflow(connection, WORKFLOW_REF, {
			lines,
			delay_ms: 50,
			fail: isFailRequested,
		}).pipe(
			Effect.match({
				onSuccess: (execution) => Message.SucceededStartWorkflow({ execution }),
				onFailure: (error) =>
					Message.FailedStartWorkflow({
						error: `${error.status} ${error.detail}`,
					}),
			}),
		),
});

// UPDATE

const isTerminal = (execution: Execution) =>
	TERMINAL_STATUSES.includes(execution.status);

const settle = (model: Model, execution: Execution): Model =>
	modifyFields(model, {
		run: () =>
			isTerminal(execution)
				? Run.Finished({ execution })
				: Run.InProgress({
						executionId: execution.execution_id,
						status: execution.status,
					}),
		history: (history) =>
			isTerminal(execution) ? Array.prepend(history, execution) : history,
	});

const update = (model: Model, message: Message): Update.Return<Model, Message> =>
	Message.match<Update.Return<Model, Message>>(message, {
		SucceededFetchCurrentUser: ({ user }) => ({
			model: modifyFields(model, { maybeUser: () => Option.some(user) }),
		}),
		FailedFetchCurrentUser: () => ({ model }),
		UpdatedLines: ({ value }) => ({
			model: modifyFields(model, { lines: () => value }),
		}),
		ToggledFail: ({ isRequested }) => ({
			model: modifyFields(model, { isFailRequested: () => isRequested }),
		}),
		ClickedRunWorkflow: () => {
			const lines = Number.parseInt(model.lines, 10);
			if (!Number.isInteger(lines) || lines < 1 || lines > 500) {
				return {
					model: modifyFields(model, {
						run: () => Run.Failed({ error: "Lines must be between 1 and 500." }),
					}),
				};
			}
			return {
				model: modifyFields(model, { run: () => Run.Starting() }),
				commands: [
					StartWorkflow({
						connection: model.connection,
						lines,
						isFailRequested: model.isFailRequested,
					}),
				],
			};
		},
		SucceededStartWorkflow: ({ execution }) => ({ model: settle(model, execution) }),
		FailedStartWorkflow: ({ error }) => ({
			model: modifyFields(model, { run: () => Run.Failed({ error }) }),
		}),
		PolledExecution: ({ execution }) =>
			model.run._tag === "InProgress" &&
			model.run.executionId === execution.execution_id
				? { model: settle(model, execution) }
				: { model },
		FailedPollExecution: ({ error }) => ({
			model: modifyFields(model, { run: () => Run.Failed({ error }) }),
		}),
	});

// SUBSCRIPTION

/** Polls the execution only while one is in progress; the runtime stops the
 *  stream when the run leaves `InProgress`, so there is no timer to clear. */
const subscriptions = Subscription.make<Model, Message>()((entry) => ({
	execution: entry(
		{ maybeExecutionId: Schema.Option(Schema.String), connection: Connection },
		{
			modelToDependencies: (model) => ({
				maybeExecutionId:
					model.run._tag === "InProgress"
						? Option.some(model.run.executionId)
						: Option.none(),
				connection: model.connection,
			}),
			dependenciesToStream: ({ maybeExecutionId, connection }) =>
				Option.match(maybeExecutionId, {
					onNone: () => Stream.empty,
					onSome: (executionId) =>
						Stream.fromEffectSchedule(
							fetchExecution(connection, executionId).pipe(
								Effect.match({
									onSuccess: (execution) =>
										Message.PolledExecution({ execution }),
									onFailure: (error) =>
										Message.FailedPollExecution({ error: error.detail }),
								}),
							),
							Schedule.spaced(POLL_INTERVAL),
						),
				}),
		},
	),
}));

// VIEW

const STATUS_TONE: Record<string, string> = {
	Success: "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400",
	Failed: "bg-red-500/15 text-red-600 dark:text-red-400",
	Running: "bg-sky-500/15 text-sky-600 dark:text-sky-400",
	Pending: "bg-muted text-muted-foreground",
};

const statusBadge = (status: string, h: HtmlBuilder<Message>): Html =>
	h.span(
		[
			h.DataAttribute("testid", "run-status"),
			h.Class(
				clsx(
					"inline-flex h-6 items-center rounded-md px-2 text-xs font-medium",
					STATUS_TONE[status] ?? "bg-muted text-muted-foreground",
				),
			),
		],
		[status],
	);

const runView = (run: Run, h: HtmlBuilder<Message>): Html =>
	Run.match(run, {
		Idle: () => h.p([h.Class("text-sm text-muted-foreground")], ["No run yet."]),
		Starting: () => statusBadge("Starting", h),
		InProgress: ({ status }) => statusBadge(status, h),
		Finished: ({ execution }) =>
			h.div(
				[h.Class("grid gap-2")],
				[
					h.div(
						[h.Class("flex items-center gap-2")],
						[
							statusBadge(execution.status, h),
							h.span(
								[h.Class("text-xs text-muted-foreground")],
								[
									execution.duration_ms === null || execution.duration_ms === undefined
										? ""
										: `${execution.duration_ms} ms`,
								],
							),
						],
					),
					h.pre(
						[
							h.DataAttribute("testid", "run-result"),
							h.Class("overflow-x-auto rounded-md bg-muted p-3 text-xs"),
						],
						[
							execution.error
								? execution.error
								: JSON.stringify(execution.result ?? null, null, 2),
						],
					),
				],
			),
		Failed: ({ error }) =>
			h.p([h.Role("alert"), h.Class("text-sm text-red-600")], [error]),
	});

const view = (model: Model, h: HtmlBuilder<Message>): Html => {
	const isBusy = model.run._tag === "Starting" || model.run._tag === "InProgress";
	return h.main(
		[h.Class("mx-auto grid max-w-2xl gap-6 p-6 text-foreground")],
		[
			h.header(
				[h.Class("grid gap-1")],
				[
					h.h1([h.Class("text-xl font-semibold")], ["Foldkit Runner"]),
					h.p(
						[h.Class("text-sm text-muted-foreground")],
						[
							Option.match(model.maybeUser, {
								onNone: () => "A Bifrost App written in Foldkit.",
								onSome: (user) => `Signed in as ${user.email}`,
							}),
						],
					),
				],
			),
			h.section(
				[h.Class("grid gap-4 rounded-lg border border-border p-4"), h.AriaLabel("Run Workflow")],
				[
					Input.view(
						{
							id: "lines",
							value: model.lines,
							type: "number",
							onInput: (value) => Message.UpdatedLines({ value }),
							toView: (attributes) =>
								h.div(
									[h.Class("grid gap-1")],
									[
										h.label([...attributes.label, h.Class("text-sm font-medium")], ["Lines"]),
										h.input([
											...attributes.input,
											h.Class("h-9 rounded-md border border-border bg-background px-3 text-sm"),
										]),
									],
								),
						},
						h,
					),
					h.label(
						[h.Class("flex items-center gap-2 text-sm")],
						[
							h.input([
								h.Type("checkbox"),
								h.Checked(model.isFailRequested),
								h.OnChange(() => Message.ToggledFail({ isRequested: !model.isFailRequested })),
							]),
							"Fail at the end",
						],
					),
					Button.view(
						{
							onClick: Message.ClickedRunWorkflow(),
							isDisabled: isBusy,
							toView: (attributes) =>
								h.button(
									[
										...attributes.button,
										...(isBusy ? [h.Disabled(true)] : []),
										h.Class(
											"inline-flex h-9 w-fit items-center rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground disabled:opacity-50",
										),
									],
									[isBusy ? "Running…" : "Run Workflow"],
								),
						},
						h,
					),
					h.div([h.AriaLive("polite")], [runView(model.run, h)]),
				],
			),
			h.section(
				[h.Class("grid gap-2"), h.AriaLabel("Recent Runs")],
				[
					h.h2([h.Class("text-sm font-semibold")], ["Recent Runs"]),
					Array.match(model.history, {
						onEmpty: () => h.p([h.Class("text-sm text-muted-foreground")], ["None yet."]),
						onNonEmpty: (history) =>
							h.ul(
								[h.Class("grid gap-1 text-sm")],
								Array.map(history, (execution) =>
									h.keyed("li")(
										execution.execution_id,
										[h.Class("flex items-center gap-2")],
										[
											statusBadge(execution.status, h),
											h.code([h.Class("text-xs text-muted-foreground")], [execution.execution_id]),
										],
									),
								),
							),
					}),
				],
			),
		],
	);
};

// PROGRAM

export const makeRunner = (container: HTMLElement, connection: Connection) =>
	Runtime.makeElement({
		Model,
		init: () => ({
			model: {
				connection,
				maybeUser: Option.none(),
				lines: "20",
				isFailRequested: false,
				run: Run.Idle(),
				history: [],
			},
			commands: [FetchCurrentUser({ connection })],
		}),
		update,
		view,
		subscriptions,
		container,
		devTools: import.meta.env.DEV ? { Message } : false,
	});
