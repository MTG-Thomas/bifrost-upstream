import { Command, given, message, model, story } from "foldkit/story";
import { describe, expect, test } from "vitest";

import {
	CopyLogs,
	DownloadLogs,
	Message,
	NotifyCopyFailed,
	ScrollToLatest,
	WaitBeforeResetCopied,
	update,
} from "./main";
import {
	ELEMENT_ID,
	SAMPLE_LOGS,
	hostInputs,
	logLine,
	model as makeModel,
} from "./fixtures";

const scrollToLatest = ScrollToLatest({ elementId: ELEMENT_ID });

describe("execution logs update", () => {
	describe("following the live edge", () => {
		test("new host logs scroll to the latest line while following", () => {
			story(
				update,
				given(makeModel()),
				message(
					Message.UpdatedHostInputs({
						inputs: hostInputs({
							logs: [
								...SAMPLE_LOGS,
								logLine({ key: "seq-6", message: "Done" }),
							],
						}),
					}),
				),
				model((next) => expect(next.inputs.logs).toHaveLength(6)),
				Command.expectExact(scrollToLatest),
				Command.resolve(
					ScrollToLatest,
					Message.CompletedScrollToLatest(),
				),
			);
		});

		test("new host logs do not move a reader who scrolled away", () => {
			story(
				update,
				given(makeModel()),
				message(Message.ObservedFollowEdge({ isAtFollowEdge: false })),
				message(
					Message.UpdatedHostInputs({
						inputs: hostInputs({
							logs: [
								...SAMPLE_LOGS,
								logLine({ key: "seq-6", message: "Done" }),
							],
						}),
					}),
				),
				model((next) => expect(next.isFollowing).toBe(false)),
				Command.expectNone(),
			);
		});

		test("status-only updates keep the same logs and never scroll", () => {
			const initial = makeModel();
			story(
				update,
				given(initial),
				message(
					Message.UpdatedHostInputs({
						inputs: {
							...initial.inputs,
							connection: "Disconnected",
						},
					}),
				),
				model((next) =>
					expect(next.inputs.connection).toBe("Disconnected"),
				),
				Command.expectNone(),
			);
		});

		test("jumping to latest resumes following and scrolls", () => {
			story(
				update,
				given(makeModel({ isFollowing: false })),
				message(Message.ClickedJumpToLatest()),
				model((next) => expect(next.isFollowing).toBe(true)),
				Command.expectExact(scrollToLatest),
				Command.resolve(
					ScrollToLatest,
					Message.CompletedScrollToLatest(),
				),
			);
		});
	});

	describe("filters", () => {
		test("an unknown level from the select is ignored", () => {
			story(
				update,
				given(makeModel()),
				message(Message.SelectedLevelFilter({ value: "verbose" })),
				model((next) => expect(next.levelFilter).toBe("all")),
				Command.expectNone(),
			);
		});

		test("clearing filters resets both the query and the level", () => {
			story(
				update,
				given(makeModel({ query: "nothing", levelFilter: "error" })),
				message(Message.ClickedClearFilters()),
				model((next) => {
					expect(next.query).toBe("");
					expect(next.levelFilter).toBe("all");
				}),
				Command.resolve(
					ScrollToLatest,
					Message.CompletedScrollToLatest(),
				),
			);
		});
	});

	describe("copy and download", () => {
		test("copy sends only the visible lines", () => {
			const copy = update(
				makeModel({ levelFilter: "warning" }),
				Message.ClickedCopyLogs(),
			);
			const [command] = copy.commands ?? [];
			expect(command?.name).toBe("CopyLogs");
			expect(command?.args).toEqual({
				text: expect.stringMatching(
					/^\S.*WARNING {2}Rate limited, retrying$/,
				),
			});
		});

		test("copy feedback shows until the reset timer fires", () => {
			story(
				update,
				given(makeModel()),
				message(Message.ClickedCopyLogs()),
				Command.expectExact(CopyLogs),
				Command.resolve(CopyLogs, Message.SucceededCopyLogs()),
				model((next) => expect(next.copyFeedback._tag).toBe("Copied")),
				Command.resolve(
					WaitBeforeResetCopied,
					Message.CompletedWaitBeforeResetCopied(),
				),
				model((next) => expect(next.copyFeedback._tag).toBe("Idle")),
			);
		});

		test("a failed copy asks the host for a notification and shows no feedback", () => {
			story(
				update,
				given(makeModel()),
				message(Message.ClickedCopyLogs()),
				Command.resolve(CopyLogs, Message.FailedCopyLogs()),
				Command.expectExact(NotifyCopyFailed),
				Command.resolve(
					NotifyCopyFailed,
					Message.CompletedNotifyCopyFailed(),
				),
				model((next) => expect(next.copyFeedback._tag).toBe("Idle")),
			);
		});

		test("download includes every line regardless of filters, with data", () => {
			story(
				update,
				given(makeModel({ query: "fetching" })),
				message(Message.ClickedDownloadLogs()),
				Command.expectExact(DownloadLogs),
				Command.resolve(DownloadLogs, Message.CompletedDownloadLogs()),
			);
			const download = update(
				makeModel({ query: "fetching" }),
				Message.ClickedDownloadLogs(),
			);
			const [command] = download.commands ?? [];
			expect(command?.args).toEqual({
				text: expect.stringContaining(
					'Ticket sync failed {"ticket":42}',
				),
			});
		});
	});
});
