/**
 * Renderer parity and host-boundary tests for the Foldkit logs spike.
 *
 * The parity block runs the same user-level scenarios against the shipping
 * React panel and the embedded Foldkit panel, so behavioural drift between
 * the two implementations fails here rather than in review. The host block
 * covers what only the embed boundary can get wrong: port updates, outbound
 * notifications, and React-driven unmounting.
 */
import {
	describe,
	expect,
	it,
	vi,
	beforeEach,
	type MockInstance,
} from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState, type ComponentType } from "react";
import { toast } from "sonner";
import * as clipboard from "@/lib/clipboard";
import {
	ExecutionLogsPanel,
	type LogEntry,
} from "@/components/execution/ExecutionLogsPanel";

import {
	FoldkitExecutionLogsPanel,
	type FoldkitExecutionLogsPanelProps,
	toLogLine,
} from "./FoldkitExecutionLogsPanel";

vi.mock("sonner", () => ({ toast: { error: vi.fn(), success: vi.fn() } }));

const LOGS: LogEntry[] = [
	{
		id: 1,
		sequence: 1,
		level: "INFO",
		message: "Fetching tickets",
		timestamp: "2026-11-18T10:00:00Z",
	},
	{
		id: 2,
		sequence: 2,
		level: "WARNING",
		message: "Rate limited, retrying",
		timestamp: "2026-11-18T10:00:01Z",
	},
	{
		id: 3,
		sequence: 3,
		level: "ERROR",
		message: "Ticket sync failed",
		timestamp: "2026-11-18T10:00:02Z",
		data: { ticket: 42 },
	},
	{
		id: 4,
		sequence: 4,
		level: "TRACEBACK",
		message: "Traceback (most recent call last):",
		timestamp: "2026-11-18T10:00:03Z",
	},
	{
		id: 5,
		sequence: 5,
		level: "TRACEBACK",
		message: '  File "sync.py", line 9',
		timestamp: "2026-11-18T10:00:03Z",
	},
];

const RENDERERS: ReadonlyArray<
	[string, ComponentType<FoldkitExecutionLogsPanelProps>]
> = [
	["React", ExecutionLogsPanel],
	["Foldkit", FoldkitExecutionLogsPanel],
];

let copySpy: MockInstance;

beforeEach(() => {
	vi.mocked(toast.error).mockClear();
	copySpy = vi.spyOn(clipboard, "copyToClipboard").mockResolvedValue(true);
});

describe.each(RENDERERS)("%s logs panel parity", (_name, Panel) => {
	it("renders lines, a coalesced traceback, the count, and the Live badge", async () => {
		render(<Panel logs={LOGS} status="Running" isConnected />);
		expect(await screen.findByText("Fetching tickets")).toBeInTheDocument();
		expect(
			screen.getByText(/5 lines · INFO and above/),
		).toBeInTheDocument();
		expect(screen.getAllByTestId("log-traceback-block")).toHaveLength(1);
		expect(
			screen.getByTestId("execution-live-indicator"),
		).toBeInTheDocument();
	});

	it("search narrows the list and Clear filters restores it", async () => {
		const user = userEvent.setup();
		render(<Panel logs={LOGS} status="Success" />);
		await screen.findByText("Fetching tickets");
		await user.type(screen.getByLabelText("Search logs"), "rate limited");
		await waitFor(() =>
			expect(
				screen.queryByText("Fetching tickets"),
			).not.toBeInTheDocument(),
		);
		expect(screen.getByText(/5 lines · 1 visible/)).toBeInTheDocument();
		await user.clear(screen.getByLabelText("Search logs"));
		await user.type(screen.getByLabelText("Search logs"), "zzz");
		await user.click(
			await screen.findByRole("button", { name: "Clear filters" }),
		);
		expect(await screen.findByText("Fetching tickets")).toBeInTheDocument();
	});

	it("streams appended lines in order", async () => {
		const { rerender } = render(
			<Panel logs={LOGS.slice(0, 2)} status="Running" isConnected />,
		);
		await screen.findByText("Rate limited, retrying");
		rerender(<Panel logs={LOGS} status="Running" isConnected />);
		await screen.findByText("Ticket sync failed");
		const messages = screen
			.getAllByTestId("log-message")
			.map((element) => element.textContent);
		expect(messages).toEqual([
			"Fetching tickets",
			"Rate limited, retrying",
			"Ticket sync failed",
		]);
	});

	it("copies the visible lines and reports a failed copy through a toast", async () => {
		const user = userEvent.setup();
		render(<Panel logs={LOGS} status="Success" />);
		await user.click(
			await screen.findByRole("button", { name: "Copy logs" }),
		);
		await waitFor(() => expect(copySpy).toHaveBeenCalledTimes(1));
		expect(copySpy.mock.calls[0]?.[0]).toContain(
			"WARNING  Rate limited, retrying",
		);
		expect(
			await screen.findByRole("button", { name: "Logs copied" }),
		).toBeInTheDocument();

		copySpy.mockResolvedValue(false);
		await user.click(screen.getByRole("button", { name: "Logs copied" }));
		await waitFor(() =>
			expect(toast.error).toHaveBeenCalledWith("Failed to copy logs"),
		);
	});

	it("warns when a running execution loses its live connection", async () => {
		render(<Panel logs={LOGS} status="Running" isConnected={false} />);
		expect(
			await screen.findByText(
				"Live connection unavailable. New log messages may be delayed.",
			),
		).toBeInTheDocument();
		expect(
			screen.queryByTestId("execution-live-indicator"),
		).not.toBeInTheDocument();
	});

	it("expands structured data on demand", async () => {
		render(<Panel logs={LOGS} status="Success" />);
		const row = (await screen.findByText("Ticket sync failed"))
			.parentElement!;
		expect(within(row).getByText("data")).toBeInTheDocument();
		expect(within(row).getByText(/"ticket": 42/)).toBeInTheDocument();
	});
});

describe("Foldkit host boundary", () => {
	function Toggle({ initiallyShown }: { initiallyShown: boolean }) {
		const [isShown, setIsShown] = useState(initiallyShown);
		return (
			<section>
				<button
					type="button"
					onClick={() => setIsShown((shown) => !shown)}
				>
					Toggle Panel
				</button>
				{isShown && (
					<FoldkitExecutionLogsPanel logs={LOGS} status="Success" />
				)}
			</section>
		);
	}

	it("unmounts cleanly while its React parent stays mounted, and remounts", async () => {
		const user = userEvent.setup();
		render(<Toggle initiallyShown />);
		await screen.findByText("Fetching tickets");
		await user.click(screen.getByRole("button", { name: "Toggle Panel" }));
		await waitFor(() =>
			expect(
				screen.queryByText("Fetching tickets"),
			).not.toBeInTheDocument(),
		);
		await user.click(screen.getByRole("button", { name: "Toggle Panel" }));
		expect(await screen.findByText("Fetching tickets")).toBeInTheDocument();
		expect(
			document.querySelectorAll("[data-renderer=foldkit] > *"),
		).toHaveLength(1);
	});

	it("keeps local filter state when the host pushes new inputs", async () => {
		const user = userEvent.setup();
		const { rerender } = render(
			<FoldkitExecutionLogsPanel
				logs={LOGS}
				status="Running"
				isConnected
			/>,
		);
		await user.type(await screen.findByLabelText("Search logs"), "ticket");
		rerender(
			<FoldkitExecutionLogsPanel
				logs={LOGS}
				status="Success"
				isConnected={false}
			/>,
		);
		await waitFor(() =>
			expect(
				screen.queryByTestId("execution-live-indicator"),
			).not.toBeInTheDocument(),
		);
		expect(screen.getByLabelText("Search logs")).toHaveValue("ticket");
		expect(screen.getByText(/5 lines · 2 visible/)).toBeInTheDocument();
	});

	it("maps API and streaming entries onto the port wire shape", () => {
		expect(toLogLine(LOGS[2]!, 2)).toEqual({
			key: "seq-3",
			level: "ERROR",
			message: "Ticket sync failed",
			timestamp: "2026-11-18T10:00:02Z",
			maybeDataJson: '{"ticket":42}',
		});
		expect(
			toLogLine({ level: "INFO", message: "x", timestamp: "t" }, 7),
		).toEqual({
			key: "index-7",
			level: "INFO",
			message: "x",
			timestamp: "t",
			maybeDataJson: null,
		});
	});
});
