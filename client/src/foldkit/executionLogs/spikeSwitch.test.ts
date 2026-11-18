import { afterEach, describe, expect, it } from "vitest";

import { LOGS_RENDERER_STORAGE_KEY, readFoldkitLogsOptIn } from "./spikeSwitch";

describe("readFoldkitLogsOptIn", () => {
	afterEach(() => window.localStorage.removeItem(LOGS_RENDERER_STORAGE_KEY));

	it("defaults to the React panel", () => {
		expect(readFoldkitLogsOptIn("")).toBe(false);
		expect(readFoldkitLogsOptIn("?logsRenderer=react")).toBe(false);
	});

	it("opts in through the query string or localStorage", () => {
		expect(readFoldkitLogsOptIn("?tab=logs&logsRenderer=foldkit")).toBe(
			true,
		);
		window.localStorage.setItem(LOGS_RENDERER_STORAGE_KEY, "foldkit");
		expect(readFoldkitLogsOptIn("")).toBe(true);
	});
});
