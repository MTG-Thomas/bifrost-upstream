import { Option } from "effect";
import { describe, expect, it } from "vitest";

import {
	coalesceTracebacks,
	filterLogs,
	reconcileLogs,
	toDownloadText,
} from "./logs";
import { SAMPLE_LOGS, logLine } from "./fixtures";

describe("filterLogs", () => {
	it("matches message, level, and serialized data case-insensitively", () => {
		expect(filterLogs(SAMPLE_LOGS, "  RATE ", "all")).toHaveLength(1);
		expect(filterLogs(SAMPLE_LOGS, "warning", "all")).toHaveLength(1);
		expect(filterLogs(SAMPLE_LOGS, '"ticket":42', "all")).toHaveLength(1);
	});

	it("applies the level filter against the normalized level", () => {
		expect(filterLogs(SAMPLE_LOGS, "", "traceback")).toHaveLength(2);
		expect(filterLogs(SAMPLE_LOGS, "", "debug")).toHaveLength(0);
	});
});

describe("coalesceTracebacks", () => {
	it("folds consecutive traceback lines into one block and keeps the first timestamp", () => {
		const items = coalesceTracebacks(SAMPLE_LOGS);
		expect(items.map((item) => item.kind)).toEqual([
			"Line",
			"Line",
			"Line",
			"Traceback",
		]);
		const traceback = items[3];
		expect(traceback?.kind === "Traceback" && traceback.lines).toEqual([
			"Traceback (most recent call last):",
			'  File "sync.py", line 9',
		]);
	});

	it("starts a new block after an interrupting line", () => {
		const items = coalesceTracebacks([
			logLine({ key: "a", level: "TRACEBACK", message: "one" }),
			logLine({ key: "b", message: "between" }),
			logLine({ key: "c", level: "TRACEBACK", message: "two" }),
		]);
		expect(items.map((item) => item.kind)).toEqual([
			"Traceback",
			"Line",
			"Traceback",
		]);
	});
});

describe("reconcileLogs", () => {
	const decodedCopy = () => SAMPLE_LOGS.map((line) => ({ ...line }));

	it("returns the previous array when a decoded snapshot has identical content", () => {
		expect(reconcileLogs(SAMPLE_LOGS, decodedCopy())).toBe(SAMPLE_LOGS);
	});

	it("shares unchanged lines and replaces changed or appended ones", () => {
		const next = [
			...decodedCopy().slice(0, 4),
			{ ...SAMPLE_LOGS[4]!, maybeDataJson: Option.some("{}") },
			logLine({ key: "seq-6", message: "Done" }),
		];
		const reconciled = reconcileLogs(SAMPLE_LOGS, next);
		expect(reconciled).not.toBe(SAMPLE_LOGS);
		expect(
			reconciled
				.slice(0, 4)
				.every((line, index) => line === SAMPLE_LOGS[index]),
		).toBe(true);
		expect(reconciled[4]).toBe(next[4]);
		expect(reconciled[5]).toBe(next[5]);
	});
});

describe("toDownloadText", () => {
	it("appends compact data JSON to lines that carry it", () => {
		expect(toDownloadText(SAMPLE_LOGS).split("\n")[2]).toMatch(
			/ERROR {2}Ticket sync failed \{"ticket":42\}$/,
		);
	});
});
