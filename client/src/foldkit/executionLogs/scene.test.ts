import { Option } from "effect";
import {
	Command,
	Mount,
	change,
	click,
	expect,
	given,
	label,
	role,
	scene,
	testId,
	text,
	type,
} from "foldkit/scene";
import { describe, test } from "vitest";

import {
	CopyLogs,
	Message,
	ObserveFollowEdge,
	ScrollToLatest,
	WaitBeforeResetCopied,
	update,
	view,
} from "./main";
import { hostInputs, model } from "./fixtures";

const followingEdge = Message.ObservedFollowEdge({ isAtFollowEdge: true });
const awayFromEdge = Message.ObservedFollowEdge({ isAtFollowEdge: false });

describe("execution logs view", () => {
	test("renders lines, coalesced tracebacks, the Live badge, and the diagnostics hint", () => {
		scene(
			{ update, view },
			given(model()),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			expect(text("5 lines · INFO and above")).toExist(),
			expect(testId("execution-live-indicator")).toExist(),
			expect(text("Fetching tickets")).toExist(),
			expect(testId("log-traceback-block")).toContainText(
				'File "sync.py", line 9',
			),
			expect(
				role("region", { name: "Execution log messages" }),
			).toExist(),
		);
	});

	test("searching narrows the list and clearing filters restores it", () => {
		scene(
			{ update, view },
			given(model()),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			type(label("Search logs"), "no such line"),
			expect(text("No logs match the current filters")).toExist(),
			Mount.expectEnded(ObserveFollowEdge),
			click(role("button", { name: "Clear filters" })),
			Command.resolve(ScrollToLatest, Message.CompletedScrollToLatest()),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			expect(text("Fetching tickets")).toExist(),
			Command.resolveAll([
				ScrollToLatest,
				Message.CompletedScrollToLatest(),
			]),
		);
	});

	test("the level filter offers Debug only to diagnostics readers", () => {
		scene(
			{ update, view },
			given(model({ inputs: hostInputs({ canReadDiagnostics: true }) })),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			expect(role("option", { name: "Debug" })).toExist(),
			change(label("Log level"), "error"),
			Command.resolve(ScrollToLatest, Message.CompletedScrollToLatest()),
			expect(text("5 lines · 1 visible")).toExist(),
		);
	});

	test("scrolling away pauses following and Jump to latest resumes it", () => {
		scene(
			{ update, view },
			given(model()),
			Mount.resolve(ObserveFollowEdge, awayFromEdge),
			expect(text("Following paused")).toExist(),
			click(role("button", { name: "Jump to latest" })),
			Command.resolve(ScrollToLatest, Message.CompletedScrollToLatest()),
			expect(text("Following paused")).toBeAbsent(),
		);
	});

	test("copy shows confirmation, then reverts", () => {
		scene(
			{ update, view },
			given(model()),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			click(role("button", { name: "Copy logs" })),
			Command.resolve(CopyLogs, Message.SucceededCopyLogs()),
			expect(role("button", { name: "Logs copied" })).toExist(),
			Command.resolve(
				WaitBeforeResetCopied,
				Message.CompletedWaitBeforeResetCopied(),
			),
			expect(role("button", { name: "Copy logs" })).toExist(),
		);
	});

	test("empty, loading, and disconnected states match the React panel copy", () => {
		scene(
			{ update, view },
			given(model({ inputs: hostInputs({ logs: [] }) })),
			expect(text("Waiting for logs...")).toExist(),
		);
		scene(
			{ update, view },
			given(
				model({
					inputs: hostInputs({
						logs: [],
						maybeStatus: Option.some("Success"),
					}),
				}),
			),
			expect(text("No logs captured")).toExist(),
		);
		scene(
			{ update, view },
			given(model({ inputs: hostInputs({ logs: [], isLoading: true }) })),
			expect(
				role("status", { name: "Loading execution logs" }),
			).toExist(),
		);
		scene(
			{ update, view },
			given(
				model({ inputs: hostInputs({ connection: "Disconnected" }) }),
			),
			Mount.resolve(ObserveFollowEdge, followingEdge),
			expect(
				text(
					"Live connection unavailable. New log messages may be delayed.",
				),
			).toExist(),
			expect(testId("execution-live-indicator")).toBeAbsent(),
		);
	});
});
