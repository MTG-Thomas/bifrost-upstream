import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
	get: vi.fn(),
	configure: vi.fn(),
	start: vi.fn(),
	rotate: vi.fn(),
	observe: vi.fn(),
	subscribe: vi.fn(),
}));
vi.mock("@/services/cloudflareCapacity", () => ({
	configureCloudflareCapacity: mocks.configure,
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
	webSocketService: { onPlatformJobUpdate: mocks.subscribe, onAnyPlatformJobUpdate: () => vi.fn() },
}));

import { CloudflareCapacity } from "./CloudflareCapacity";

it.each([
	["Scale In", "scale-in", "scaled_in", "Scaled In"],
	["Reconcile Cloudflare", "reconcile", "enabled", "Admission Closed"],
	["Upgrade Cloudflare", "upgrade", "enabled", "Upgrade Pending"],
])("queues %s using the displayed revision and shared job observer", async (label, action, desiredState, status) => {
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
		spend: { state: "ready", estimated_usd: "0.12", workers_paid_subscription_usd: "5.00",
			coverage: ["Current reservations", "Earlier rate coverage", "External traffic is outside the estimate."] },
	};
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockImplementation(async () => {
		mocks.get.mockResolvedValue({
			installation: {
				...installation,
				desired_revision: 4,
				desired_state: desiredState,
				admission_open: false,
				upgrade_pending: action === "upgrade",
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
	const button = await screen.findByRole("button", { name: label });
	expect(screen.getByText("Estimated Bifrost Usage This UTC Month")).toBeVisible();
	expect(screen.getByText("0.12 USD")).toBeVisible();
	expect(screen.getByText("Workers Paid Subscription (Billed by Cloudflare)")).toBeVisible();
	expect(screen.getByText("5.00 USD per Month")).toBeVisible();
	expect(screen.getByText("Usage Estimate Coverage")).toBeVisible();
	expect(screen.getByText("Current reservations")).toBeVisible();
	expect(screen.getByText("Earlier rate coverage")).toBeVisible();
	expect(screen.getByText("External traffic is outside the estimate.")).toBeVisible();
	fireEvent.click(button);
	expect(await screen.findByText("Completed")).toBeVisible();
	expect(screen.getByText(status)).toBeVisible();
	expect(mocks.start).toHaveBeenCalledWith(action, {
		installation_id: "installation-id",
		expected_revision: 3,
	});
	expect(mocks.subscribe).toHaveBeenCalledWith(
		"job-id",
		expect.any(Function),
	);
});

it("submits the current consent version and clears the write-only token", async () => {
	mocks.get.mockResolvedValue({
		installation: null,
		consent_text_version: "existing-https-v1",
		consent_text: "Current operator consent",
	});
	mocks.configure.mockResolvedValue({});
	render(
		<QueryClientProvider client={new QueryClient()}>
			<CloudflareCapacity />
		</QueryClientProvider>,
	);
	fireEvent.change(await screen.findByLabelText("Cloudflare Account ID"), { target: { value: "a".repeat(32) } });
	fireEvent.change(screen.getByLabelText("Existing HTTPS Endpoint"), { target: { value: "https://workers.example.test" } });
	const linger = screen.getByLabelText("Tenant Engine Linger in Seconds");
	expect(linger).toHaveValue(90);
	fireEvent.change(linger, { target: { value: "0" } });
	const tenantSlots = screen.getByLabelText("Slots Per Tenant");
	const installationSlots = screen.getByLabelText("Installation Slot Limit");
	expect(tenantSlots).toHaveValue(4);
	expect(installationSlots).toHaveValue(9);
	fireEvent.change(tenantSlots, { target: { value: "3" } });
	fireEvent.change(installationSlots, { target: { value: "8" } });
	const token = screen.getByLabelText("Cloudflare API Token");
	expect(token).toHaveAttribute("type", "password");
	fireEvent.change(token, { target: { value: "synthetic-management-token" } });
	expect(screen.getByText("Current operator consent")).toBeVisible();
	fireEvent.click(screen.getByRole("checkbox", { name: "I Accept the Cloudflare Terms Above" }));
	await act(async () => {
		fireEvent.submit((screen.getByRole("button", { name: "Connect Cloudflare" }) as HTMLButtonElement).form!);
	});
	expect(mocks.configure).toHaveBeenCalledWith(expect.objectContaining({
		action: "connect",
		account_id: "a".repeat(32),
		worker_ingress_origin: "https://workers.example.test",
		consent: true,
		consent_version: "existing-https-v1",
		api_token: "synthetic-management-token",
		policy: expect.objectContaining({
			workers: expect.objectContaining({ linger_seconds: 0, slots_per_tenant: 3, installation_slots: 8 }),
		}),
	}));
	expect(token).toHaveValue("");
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

it("confirms deployment deletion before rotating the scope bearer", async () => {
	mocks.start.mockClear();
	mocks.get.mockResolvedValue({
		installation: {
			installation_id: "installation-id",
			desired_revision: 4,
			desired_state: "scaled_in",
			worker_ingress_origin: "https://workers.example.test",
			fleet_envelope_usd: "10.50",
			policy: { monthly_limit_usd: "30.00", workers: { launcher_cap: 1, slots_per_launcher: 2 } },
		},
	});
	mocks.start.mockResolvedValue({ job_id: "job-id", status: "queued" });
	render(
		<QueryClientProvider client={new QueryClient()}>
			<CloudflareCapacity />
		</QueryClientProvider>,
	);
	fireEvent.click(await screen.findByRole("button", { name: "Rotate Scope Bearer" }));
	const dialog = within(screen.getByRole("alertdialog", { name: "Rotate Cloudflare Scope Bearer" }));
	expect(dialog.getByText(/delete the previous Workers deployment/)).toBeVisible();
	expect(mocks.start).not.toHaveBeenCalled();
	await act(async () => {
		fireEvent.click(dialog.getByRole("button", { name: "Rotate Scope Bearer" }));
	});
	expect(mocks.start).toHaveBeenCalledWith("rotate-bearer", {
		installation_id: "installation-id",
		expected_revision: 4,
	});
});
