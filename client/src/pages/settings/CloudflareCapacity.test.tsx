// @vitest-environment jsdom
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, expect, it, vi } from "vitest";

import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { ApiError } from "@/lib/api-error";

const mocks = vi.hoisted(() => ({
	get: vi.fn(),
	configure: vi.fn(),
	preflight: vi.fn(),
	activate: vi.fn(),
	start: vi.fn(),
	rotate: vi.fn(),
	observe: vi.fn(),
	subscribe: vi.fn(),
	connection: vi.fn(),
	canAt: vi.fn(),
}));
vi.mock("@/services/cloudflareCapacity", () => ({
	configureCloudflareCapacity: mocks.configure,
	preflightCloudflareCapacity: mocks.preflight,
	activateCloudflareCapacity: mocks.activate,
	getCloudflareCapacity: mocks.get,
	startCloudflareAction: mocks.start,
	rotateCloudflareAccountToken: mocks.rotate,
}));
vi.mock("@/services/authorization", () => ({
	useAuthorization: () => ({ canAt: mocks.canAt, isLoading: false }),
}));
vi.mock("@/services/platformJobs", () => ({
	observePlatformJob: mocks.observe,
}));
vi.mock("@/services/websocket", () => ({
	webSocketService: { onPlatformJobUpdate: mocks.subscribe, onAnyPlatformJobUpdate: () => vi.fn(), onConnectionStatusChange: mocks.connection },
}));
// The approvals section has its own tests; keep this page test focused on capacity behaviour.
vi.mock("@/components/workflows/CloudflareWorkflowApprovals", () => ({
	CloudflareWorkflowApprovals: () => null,
}));

import { CloudflareCapacity } from "./CloudflareCapacity";

beforeEach(() => {
	vi.resetAllMocks();
	mocks.canAt.mockReturnValue(true);
	mocks.subscribe.mockReturnValue(vi.fn());
	mocks.connection.mockReturnValue(vi.fn());
	mocks.observe.mockReturnValue({ promise: Promise.resolve(), cancel: vi.fn() });
});

function renderCapacity() {
	return render(
		<MemoryRouter>
			<QueryClientProvider client={new QueryClient()}>
				<CloudflareCapacity />
			</QueryClientProvider>
		</MemoryRouter>,
	);
}

const installation = {
	installation_id: "installation-id",
	desired_revision: 3,
	desired_state: "scaled_in",
	admission_open: false,
	worker_ingress_origin: "https://workers.example.test",
	policy: {
		monthly_limit_usd: "40.00",
		workers: {
			launcher_cap: 2, slots_per_launcher: 2, max_lifetime_seconds: 120,
			installation_slots: 8, work_kinds: ["workflow"],
		},
	},
	fleet_envelope_usd: "10.50",
};

function snapshot(id: string, status: string, phase: string, result?: Record<string, unknown>) {
	return { id, status, revision: status === "running" ? 1 : 2, progress: { phase }, result };
}

it("shows the combined floor and allows editing the Container hours budget", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation: {
		...installation,
		policy: { ...installation.policy, container_instance_hours: 100 },
		fleet_envelope_usd: "44.40", workers_fleet_envelope_usd: "32.13", containers_fleet_envelope_usd: "12.27",
		containers_remaining_instance_hours: "25", containers_instance_budget_resets_at: "2026-12-01T00:00:00Z",
	} });
	renderCapacity();
	expect(await screen.findByText("Remaining Container Instance-Hours")).toBeVisible();
	expect(screen.getByText("25 of 100")).toBeVisible();
	expect(screen.getByText("Resets 2026-12-01 UTC")).toBeVisible();
	expect(screen.getByText("32.13 USD Workers · 12.27 USD Containers")).toBeVisible();
	await user.click(screen.getByRole("button", { name: "Edit Policy" }));
	const field = screen.getByLabelText("Monthly Container Instance-Hours");
	expect(field).toHaveValue(100);
	await user.clear(field);
	await user.type(field, "90");
	expect(field).toHaveValue(90);
});

it("displays scoped overflow, ledger and recovery evidence from status", async () => {
	mocks.get.mockResolvedValue({
		installation: {
			...installation,
			spend: {
				state: "ready", settled_usd: "1.25", reserved_usd: "0.75",
				remaining_headroom_usd: "38.00", period_end: "2026-12-01T00:00:00Z",
			},
		},
		approved_workflow_count: 2,
		lanes: [{
			lane: "workers", eligible_queued: 3, claimed: 1, running: 2,
			primary_fit: 0, primary_saturated: true, oldest_eligible_queue_age_seconds: 1, launch_gate_seconds: 2,
			reasons: [{
				code: "waiting_for_launch_gate", label: "Waiting for Launch Gate", remedy: "Wait for the queue age threshold.",
				next_eligible_at: "2026-11-22T00:00:02Z",
			}],
		}],
		recovery: {
			code: "CF_DEPLOYMENT_UNVERIFIED", remedy: "Reconcile to verify the deployed revision.",
			check: "capabilities_shape", observed_category: "invalid", expected_category: "valid_profile",
		},
	});
	renderCapacity();

	await screen.findByRole("heading", { name: "Overflow Placement" });
	expect(screen.getByText("2 approved workflows.")).toBeVisible();
	expect(screen.getByText("Waiting for Launch Gate")).toBeVisible();
	expect(screen.getByText("1.00 s / 2 s")).toBeVisible();
	expect(screen.getByText("0.75 USD")).toBeVisible();
	expect(within(screen.getByRole("region", { name: "Cloudflare Recovery" })).getByText("capabilities_shape")).toBeVisible();
});

it("acknowledges a completed disconnect when opening the page again", async () => {
	mocks.get.mockResolvedValue({ installation: null, disconnected_at: "2026-11-22T00:00:00Z" });
	renderCapacity();

	expect(await screen.findByText(/^Cloudflare was disconnected/)).toHaveAttribute("role", "status");
	expect(mocks.observe).not.toHaveBeenCalled();
});


it.each([
	["Scale In", "scale-in", "scaled_in", "Scaled In"],
	["Reconcile Cloudflare", "reconcile", "enabled", "Admission Closed"],
	["Upgrade Cloudflare", "upgrade", "enabled", "Upgrade Pending"],
])("queues %s using the displayed revision and shared job observer", async (label, action, desiredState, status) => {
	const installation = {
		installation_id: "installation-id",
		desired_revision: 3,
		deployed_revision: 3,
		desired_state: "enabled",
		admission_open: true,
		upgrade_available: action === "upgrade",
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
	renderCapacity();
	const button = await screen.findByRole("button", { name: label });
	if (action !== "upgrade") {
		expect(screen.getByRole("button", { name: "Upgrade Cloudflare" })).toBeDisabled();
	}
	expect(screen.getByText("Estimated Bifrost Usage This UTC Month")).toBeVisible();
	expect(screen.getByText("0.12 USD")).toBeVisible();
	expect(screen.getByText("10.50 USD/month")).toBeVisible();
	expect(screen.getByText("Workers Paid Subscription (Billed by Cloudflare)")).toBeVisible();
	expect(screen.getByText("5.00 USD/month")).toBeVisible();
	expect(screen.getByText("Usage Estimate Coverage")).toBeVisible();
	expect(screen.getByText("Current reservations")).toBeVisible();
	expect(screen.getByText("Current reservations").closest("ul")).not.toHaveAttribute("tabindex");
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

const suggestedPolicy = {
	monthly_limit_usd: "75.00",
	workers: {
		launcher_cap: 3, slots_per_launcher: 4, max_lifetime_seconds: 3600,
		installation_slots: 9, work_kinds: ["workflow"],
	},
};

it("reviews a discovered account and route, then queues exactly one activation", async () => {
	const user = userEvent.setup();
	const token = "synthetic-management-token";
	mocks.get.mockResolvedValue({ installation: null, suggested_policy: suggestedPolicy });
	mocks.preflight.mockResolvedValue({
		account_id: "a".repeat(32), accounts: [{ id: "a".repeat(32), name: "Example Account" }],
		zone_id: "b".repeat(32), zone_name: "example.test", zones: [{ id: "b".repeat(32), name: "example.test" }],
		workers_paid: "verified", connector_ready: true, connector_channel: "compose", primary: "static",
		reachability_mode: "managed_tunnel", worker_ingress_origin: "https://workers.example.test",
		preview: {
			policy: suggestedPolicy, fleet_envelope_usd: "32.13",
			estimated_usage_min_usd: "2", estimated_usage_max_usd: "7", workers_paid_subscription_usd: "5",
		},
		consent_version: "existing-https-v1", consent_text: "Current operator consent", preflight_receipt: "receipt",
	});
	mocks.activate.mockImplementation(async () => {
		mocks.get.mockResolvedValue({ installation });
		return { job_id: "activation-job", status: "queued" };
	});
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Set Up Cloudflare" }));
	const secret = screen.getByLabelText("Cloudflare API Token");
	expect(secret).toHaveFocus();
	expect(secret).toHaveAttribute("type", "password");
	await user.type(secret, token);
	await user.click(screen.getByRole("button", { name: "Run Preflight" }));
	await screen.findByText(/Cloudflare Account: Example Account/);
	expect(screen.getByText(/Cloudflare DNS Zone: example.test/)).toBeVisible();
	expect(screen.getByText(/Dedicated Hostname: https:\/\/workers.example.test/)).toBeVisible();
	expect(screen.getByText("32.13 USD/month")).toBeVisible();
	expect(screen.getByText("About $2–$7/month")).toBeVisible();
	expect(screen.getByLabelText("Monthly Estimated Usage Limit")).toHaveValue(75);
	expect(screen.getByLabelText("Monthly Estimated Usage Limit")).toHaveAttribute("min", "32.13");
	expect(screen.getByRole("button", { name: "Scale Out with Cloudflare" })).toBeVisible();
	await user.click(screen.getByRole("button", { name: "Scale Out with Cloudflare" }));
	expect(mocks.activate).not.toHaveBeenCalled();
	const required = screen.getByRole("region", { name: "Cloudflare Settings Error" });
	expect(required).toHaveFocus();
	expect(screen.getByRole("checkbox", { name: "Accept Cloudflare Terms" })).toHaveAttribute("aria-invalid", "true");

	await user.click(screen.getByRole("checkbox", { name: "Confirm Monthly Usage Limit" }));
	await user.click(screen.getByRole("checkbox", { name: "Accept Cloudflare Terms" }));
	await user.click(screen.getByRole("button", { name: "Scale Out with Cloudflare" }));
	await waitFor(() => expect(mocks.subscribe).toHaveBeenCalledWith("activation-job", expect.any(Function)));
	expect(mocks.activate).toHaveBeenCalledExactlyOnceWith({
		api_token: token, monthly_limit_usd: "75.00", consent: true,
		consent_version: "existing-https-v1", preflight_receipt: "receipt",
	});
	expect(mocks.configure).not.toHaveBeenCalled();
	expect(mocks.start).not.toHaveBeenCalled();
	expect(secret).toHaveValue("");
});

it("opts into Containers during preflight and accepts the combined fleet limit", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation: null, suggested_policy: suggestedPolicy });
	mocks.preflight.mockResolvedValue({
		accounts: [{ id: "a".repeat(32), name: "Example Account" }], account_id: "a".repeat(32),
		reachability_mode: "existing_https_endpoint", workers_paid: "verified",
		worker_ingress_origin: "https://workers.example.test",
		preview: {
			policy: { ...suggestedPolicy, container_instance_hours: 100 },
			fleet_envelope_usd: "44.40", containers_fleet_envelope_usd: "12.27",
			containers_remaining_instance_hours: "100",
			workers_paid_subscription_usd: "5",
			containers_policy: { installation_instances: 2, instance_type: "standard-1", tenant_instances: 1,
				workflow_slots: 1, idle_seconds: 120, monthly_instance_hours: 100 },
		},
		consent_version: "cloudflare-containers-pilot-v1",
		consent_text: "I authorize Bifrost to provision paid Cloudflare Containers for approved Linux workflows.",
		preflight_receipt: "containers-receipt",
	});
	renderCapacity();
	await user.click(await screen.findByRole("button", { name: "Set Up Cloudflare" }));
	const optIn = screen.getByRole("checkbox", { name: "Include Paid Containers Pilot" });
	expect(optIn).not.toBeChecked();
	await user.click(optIn);
	await user.type(screen.getByLabelText("Cloudflare API Token"), "synthetic-management-token");
	await user.click(screen.getByRole("button", { name: "Run Preflight" }));
	expect(await screen.findByRole("heading", { name: "Containers Capacity Preview" })).toBeVisible();
	expect(mocks.preflight).toHaveBeenCalledWith(expect.objectContaining({ containers: true }));
	expect(screen.getByLabelText("Monthly Estimated Usage Limit")).toHaveValue(75);
	expect(screen.getByLabelText("Monthly Estimated Usage Limit")).toHaveAttribute("min", "44.4");
	expect(screen.getByText(/provision paid Cloudflare Containers/)).toBeVisible();
	const limit = screen.getByLabelText("Monthly Estimated Usage Limit");
	await user.clear(limit);
	await user.type(limit, "70");
	await user.type(screen.getByLabelText("Monthly Container Instance-Hours"), "80");
	await user.click(screen.getByRole("button", { name: "Check Cloudflare Setup" }));
	await waitFor(() => expect(mocks.preflight).toHaveBeenLastCalledWith(
		expect.objectContaining({ containers: true, container_instance_hours: 80 }),
	));
	expect(limit).toHaveValue(70);
});

it("routes activation limit refusals and refreshes status when the review becomes stale", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation: null, suggested_policy: suggestedPolicy });
	mocks.preflight.mockResolvedValue({
		accounts: [{ id: "a".repeat(32), name: "Example Account" }], account_id: "a".repeat(32),
		zone_id: "b".repeat(32), zone_name: "example.test", zones: [{ id: "b".repeat(32), name: "example.test" }],
		workers_paid: "verified", connector_ready: true, primary: "static", reachability_mode: "managed_tunnel",
		worker_ingress_origin: "https://workers.example.test", preflight_receipt: "review",
		consent_version: "v1", consent_text: "Review the account.",
		preview: { policy: suggestedPolicy, fleet_envelope_usd: "32.13", workers_paid_subscription_usd: "5" },
	});
	mocks.activate
		.mockRejectedValueOnce(new ApiError({
			error: "cloudflare_budget_refused", message: "Raise the limit.",
			details: { fields: { monthly_limit_usd: "Enter at least 32.13 USD." } },
		}, 422))
		.mockRejectedValueOnce(new ApiError({
			error: "cloudflare_configuration_refused", message: "Cloudflare preflight expired.",
			details: { code: "CF_REVIEW_REQUIRED", remedy: "Run preflight again." },
		}, 409));
	renderCapacity();
	await user.click(await screen.findByRole("button", { name: "Set Up Cloudflare" }));
	await user.type(screen.getByLabelText("Cloudflare API Token"), "synthetic-management-token");
	await user.click(screen.getByRole("button", { name: "Run Preflight" }));
	await user.click(await screen.findByRole("checkbox", { name: "Confirm Monthly Usage Limit" }));
	await user.click(screen.getByRole("checkbox", { name: "Accept Cloudflare Terms" }));
	await user.click(screen.getByRole("button", { name: "Scale Out with Cloudflare" }));
	const summary = await screen.findByRole("region", { name: "Cloudflare Settings Error" });
	const limit = screen.getByLabelText("Monthly Estimated Usage Limit");
	expect(summary).toHaveFocus();
	expect(limit).toHaveAccessibleDescription("Enter at least 32.13 USD.");
	await user.click(within(summary).getByRole("button", { name: /Monthly Estimated Usage Limit:/ }));
	expect(limit).toHaveFocus();
	await user.click(screen.getByRole("button", { name: "Scale Out with Cloudflare" }));
	expect(await screen.findByRole("button", { name: "Refresh and Review" })).toBeVisible();
	expect(mocks.get).toHaveBeenCalledTimes(2);
	expect(screen.queryByRole("button", { name: "Scale Out with Cloudflare" })).toBeNull();
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
	renderCapacity();
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
	renderCapacity();
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


it("lets a read-only operator find the required permission and workflow review", async () => {
	mocks.canAt.mockImplementation((permission) => permission === "platform.read");
	mocks.get.mockResolvedValue({ installation: null });
	renderCapacity();

	expect(await screen.findByRole("heading", { name: "Cloudflare Connection" })).toBeVisible();
	expect(screen.getByText(/Ask a Platform Admin/)).toBeVisible();
	expect(screen.getByRole("link", { name: "Review Workflows" })).toHaveAttribute("href", "/workflows");
	expect(screen.queryByRole("button", { name: "Set Up Cloudflare" })).toBeNull();
});

it("recovers a failed capacity load from the retry button", async () => {
	const user = userEvent.setup();
	mocks.get.mockRejectedValueOnce(new Error("Capacity is unavailable.")).mockResolvedValue({ installation: null, suggested_policy: suggestedPolicy });
	renderCapacity();

	expect(await screen.findByRole("alert")).toHaveTextContent("Capacity is unavailable.");
	await user.click(screen.getByRole("button", { name: "Retry Loading" }));
	expect(await screen.findByRole("button", { name: "Set Up Cloudflare" })).toBeEnabled();
	expect(screen.queryByRole("alert")).toBeNull();
});

it("focuses the error summary and links a rejected field to its inline message", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.configure.mockRejectedValue(new ApiError({
		error: "validation_error",
		message: "policy.policy.workers.slots_per_tenant: Choose a lower slot limit.; policy.policy: Review the worker policy as a whole.",
		details: {
			code: "CAPACITY_POLICY",
			reason: "The submitted slot limit is too high.",
			remedy: "Enter a lower slot limit.",
		},
	}, 422));
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Edit Policy" }));
	await user.click(screen.getByRole("button", { name: "Save Policy" }));
	const summary = await screen.findByRole("region", { name: "Cloudflare Settings Error" });
	expect(summary).toHaveFocus();
	expect(summary).toHaveTextContent("CAPACITY_POLICY");
	expect(within(summary).getByText("Review the worker policy as a whole.").tagName).toBe("LI");
	expect(summary).toHaveTextContent("The submitted slot limit is too high.");
	expect(summary).toHaveTextContent("Enter a lower slot limit.");
	const slots = screen.getByRole("spinbutton", { name: "Slots per Tenant" });
	expect(slots).toHaveAttribute("aria-invalid", "true");
	expect(slots).toHaveAccessibleDescription("Choose a lower slot limit.");
	await user.click(within(summary).getByRole("button", { name: "Slots per Tenant: Choose a lower slot limit." }));
	expect(slots).toHaveFocus();
});

it("explains a rejected spending envelope beside the editable limit", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.configure.mockRejectedValue(new ApiError({
		error: "cloudflare_budget_refused",
		message: "The fleet envelope exceeds the submitted limit.",
		details: {
			code: "CF_BUDGET_REFUSED",
			fields: { "policy.monthly_limit_usd": "The fleet envelope is 41 USD/month. The submitted limit was 40 USD/month." },
			remedy: "Raise the limit or reduce the fleet size or lifetime.",
		},
	}, 422));
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Edit Policy" }));
	expect(screen.getByLabelText("Workers Launcher Limit")).toHaveValue(2);
	expect(screen.getByLabelText("Slots per Launcher")).toHaveValue(2);
	expect(screen.getByLabelText("Maximum Lifetime in Seconds")).toHaveValue(120);
	expect(screen.getByLabelText("Installation Slot Limit")).toHaveValue(8);
	await user.click(screen.getByRole("button", { name: "Save Policy" }));
	expect(await screen.findByRole("region", { name: "Cloudflare Settings Error" })).toHaveFocus();
	const limit = screen.getByRole("spinbutton", { name: "Monthly Estimated Usage Limit" });
	expect(limit).toHaveValue(40);
	expect(limit).toHaveAccessibleDescription(/fleet envelope is 41 USD.*submitted limit was 40/);
	expect(screen.getByText("Raise the limit or reduce the fleet size or lifetime.")).toBeVisible();
});

it("reads back a lost job snapshot on reconnect and focuses the refusal", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockResolvedValue({ job_id: "enable-job" });
	mocks.observe
		.mockImplementationOnce(() => ({ promise: Promise.reject(new Error("Unavailable")), cancel: vi.fn() }))
		.mockImplementationOnce((_id, update) => ({
			promise: Promise.resolve().then(() => update(snapshot("enable-job", "requires_action", "Check the Workers release.", {
				code: "CF_POLICY_RELEASE_CHANGED",
				reason: "The provisioned release changed.",
				instructions: ["Install the matching release."],
			}))),
			cancel: vi.fn(),
		}));
	renderCapacity();
	await user.click(await screen.findByRole("button", { name: "Enable" }));
	expect(await screen.findByText("Could not read the Cloudflare job status.")).toBeVisible();
	expect(screen.getByRole("button", { name: "Enable" })).toBeDisabled();

	act(() => mocks.connection.mock.lastCall?.[0](true));
	expect(await screen.findByText("Install the matching release.")).toBeVisible();
	expect(screen.getByText("CF_POLICY_RELEASE_CHANGED")).toBeVisible();
	expect(screen.getByText("The provisioned release changed.")).toBeVisible();
	expect(screen.getByRole("region", { name: "Cloudflare Job Status" })).toHaveFocus();
	expect(screen.getByRole("button", { name: "Enable" })).toBeEnabled();
	expect(mocks.observe).toHaveBeenCalledTimes(2);
});

it("observes a newer job when the locally accepted job finishes", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockResolvedValue({ job_id: "enable-job" });
	mocks.observe.mockImplementation((id, update) => ({
		promise: Promise.resolve().then(() => update(snapshot(id, "running", id === "next-job" ? "Reconciling the new revision." : "Provisioning capacity."))),
		cancel: vi.fn(),
	}));
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Enable" }));
	expect(await screen.findByText("Provisioning capacity.")).toBeVisible();
	const [, update] = mocks.subscribe.mock.lastCall!;
	mocks.get.mockResolvedValue({ installation, latest_job_id: "next-job" });
	act(() => update(snapshot("enable-job", "succeeded", "Complete.")));
	expect(await screen.findByText("Reconciling the new revision.")).toBeVisible();
	expect(screen.queryByText("Provisioning capacity.")).toBeNull();
	expect(mocks.observe).toHaveBeenCalledWith("next-job", expect.any(Function));
	expect(screen.getByRole("button", { name: "Enable" })).toBeDisabled();
});

it("recovers a missed notification on demand and announces verified disconnect", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockResolvedValue({ job_id: "disconnect-job" });
	mocks.observe.mockImplementation((_id, update) => ({
		promise: Promise.resolve().then(() => update(snapshot("disconnect-job", "running", "Removing resources."))),
		cancel: vi.fn(),
	}));
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Disconnect" }));
	const dialog = screen.getByRole("alertdialog");
	await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
	expect(screen.getByRole("button", { name: "Disconnect" })).toHaveFocus();

	await user.click(screen.getByRole("button", { name: "Disconnect" }));
	await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "Disconnect" }));
	expect(await screen.findByText("Removing resources.")).toBeVisible();
	mocks.get.mockResolvedValue({ installation: null, suggested_policy: suggestedPolicy });
	mocks.observe.mockImplementation((_id, update) => ({
		promise: Promise.resolve().then(() => update(snapshot("disconnect-job", "succeeded", "Complete."))),
		cancel: vi.fn(),
	}));
	await user.click(screen.getByRole("button", { name: "Check Job Status" }));
	await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Cloudflare was disconnected."));
	expect(screen.getByRole("button", { name: "Set Up Cloudflare" })).toHaveFocus();
});

it("focuses the job status after token rotation is accepted and again on refusal", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.rotate.mockResolvedValue({ job_id: "rotation-job", status: "queued" });
	mocks.observe.mockImplementation((id, update) => ({
		promise: Promise.resolve().then(() => update(snapshot(id, "queued", "Preparing rotation."))),
		cancel: vi.fn(),
	}));
	renderCapacity();

	await user.click(await screen.findByRole("button", { name: "Rotate API Token" }));
	const dialog = within(screen.getByRole("dialog", { name: "Rotate Cloudflare API Token" }));
	await user.type(dialog.getByLabelText("Replacement API Token"), "synthetic-token");
	await user.click(dialog.getByRole("button", { name: "Rotate API Token" }));
	const status = await screen.findByRole("region", { name: "Cloudflare Job Status" });
	await waitFor(() => expect(status).toHaveFocus());
	expect(within(status).getByRole("status")).toHaveTextContent("The Cloudflare job is queued.");
	expect(within(status).getByText("Preparing rotation.")).toBeVisible();

	screen.getByRole("link", { name: "Review Workflows" }).focus();
	const [, update] = mocks.subscribe.mock.lastCall!;
	act(() => update({ ...snapshot("rotation-job", "requires_action", "Replace the token."), revision: 3 }));
	expect(await screen.findByText("Replace the token.")).toBeVisible();
	expect(status).toHaveFocus();
	expect(within(status).getByRole("status")).toHaveTextContent("The Cloudflare job needs your attention.");
});

it("shows visual progress without putting phases into the job live region", async () => {
	mocks.get.mockResolvedValue({ installation, latest_job_id: "job-id" });
	mocks.observe.mockImplementation((id, update) => ({
		promise: Promise.resolve().then(() => update(snapshot(id, "running", "Provisioning capacity."))),
		cancel: vi.fn(),
	}));
	renderCapacity();

	expect(await screen.findByText("Provisioning capacity.")).toBeVisible();
	expect(screen.getByRole("status")).toBeEmptyDOMElement();
	const [, update] = mocks.subscribe.mock.lastCall!;
	act(() => update({ ...snapshot("job-id", "running", "Verifying capacity."), revision: 2 }));
	expect(await screen.findByText("Verifying capacity.")).toBeVisible();
	expect(screen.getByRole("status")).toBeEmptyDOMElement();
});

it("shows Scaled In while an upgrade prevents enabling", async () => {
	mocks.get.mockResolvedValue({ installation: { ...installation, upgrade_pending: true } });
	renderCapacity();
	expect(await screen.findByText("Scaled In")).toBeVisible();
	expect(screen.queryByText("Ready to Enable")).toBeNull();
	expect(screen.getByRole("button", { name: "Enable" })).toBeDisabled();
});

it("dismisses the focused action error when refreshing settings", async () => {
	const user = userEvent.setup();
	mocks.get.mockResolvedValue({ installation });
	mocks.start.mockRejectedValue(new ApiError("Cloudflare installation changed.", 409));
	renderCapacity();
	await user.click(await screen.findByRole("button", { name: "Enable" }));
	const error = await screen.findByRole("region", { name: "Cloudflare Action Error" });
	expect(error).toHaveFocus();
	await user.click(within(error).getByRole("button", { name: "Refresh and Review Settings" }));
	expect(screen.queryByRole("region", { name: "Cloudflare Action Error" })).toBeNull();
	expect(mocks.get).toHaveBeenCalledTimes(2);
});
