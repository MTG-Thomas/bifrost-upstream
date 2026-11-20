import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
	get: vi.fn(),
	start: vi.fn(),
	rotate: vi.fn(),
	observe: vi.fn(),
	subscribe: vi.fn(),
}));
vi.mock("@/services/cloudflareCapacity", () => ({
	getCloudflareCapacity: mocks.get,
	startCloudflareAction: mocks.start,
	rotateCloudflareAccountToken: mocks.rotate,
}));
vi.mock("@/services/authorization", () => ({
	useAuthorization: () => ({ canAt: () => true, isLoading: false }),
}));
vi.mock("@/services/platformJobs", () => ({
	observePlatformJob: mocks.observe,
}));
vi.mock("@/services/websocket", () => ({
	webSocketService: { onPlatformJobUpdate: mocks.subscribe },
}));

import { CloudflareCapacity } from "./CloudflareCapacity";

it("drains using the displayed installation revision and the shared job observer", async () => {
	const installation = {
		installation_id: "installation-id",
		desired_revision: 3,
		desired_state: "enabled",
		admission_open: true,
		worker_ingress_origin: "https://workers.example.test",
		policy: {
			monthly_limit_usd: "40.00",
			workers: { launcher_cap: 2, slots_per_launcher: 2 },
		},
		fleet_envelope_usd: "10.50",
	};
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockImplementation(async () => {
		mocks.get.mockResolvedValue({
			installation: {
				...installation,
				desired_revision: 4,
				desired_state: "scaled_in",
				admission_open: false,
			},
		});
		return { job_id: "job-id", status: "queued" };
	});
	mocks.subscribe.mockReturnValue(vi.fn());
	mocks.observe.mockImplementation((_id, update) => ({
		promise: Promise.resolve().then(() =>
			update({
				id: "job-id",
				status: "succeeded",
				revision: 2,
				progress: { phase: "Completed" },
			}),
		),
		cancel: vi.fn(),
	}));
	render(
		<QueryClientProvider client={new QueryClient()}>
			<CloudflareCapacity />
		</QueryClientProvider>,
	);
	fireEvent.click(await screen.findByRole("button", { name: "Scale In" }));
	expect(await screen.findByText("Completed")).toBeVisible();
	expect(screen.getByText("Scaled In")).toBeVisible();
	expect(mocks.start).toHaveBeenCalledWith("scale-in", {
		installation_id: "installation-id",
		expected_revision: 3,
	});
	expect(mocks.subscribe).toHaveBeenCalledWith(
		"job-id",
		expect.any(Function),
	);
});

it("recovers Action Required from the last visible job on page load", async () => {
	mocks.get.mockResolvedValue({ installation: null, latest_job_id: "previous-job" });
	mocks.observe.mockImplementation((_id, update) => ({
		promise: Promise.resolve().then(() => update({
			id: "previous-job",
			status: "requires_action",
			revision: 4,
			progress: { phase: "Reconnect Cloudflare" },
		})),
		cancel: vi.fn(),
	}));
	render(
		<QueryClientProvider client={new QueryClient()}>
			<CloudflareCapacity />
		</QueryClientProvider>,
	);
	expect(await screen.findByText("Reconnect Cloudflare")).toBeVisible();
	expect(mocks.subscribe).toHaveBeenCalledWith("previous-job", expect.any(Function));
});
