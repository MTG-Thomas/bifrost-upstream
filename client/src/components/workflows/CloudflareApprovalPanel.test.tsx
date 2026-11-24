import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { ApiError } from "@/lib/api-client";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	ApprovalReviewError,
	acceptCloudflareApproval,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
	type ApprovalState,
} from "@/services/cloudflare-approval";
import { CloudflareApprovalPanel } from "./CloudflareApprovalPanel";

vi.mock("@/services/cloudflare-approval", async (importOriginal) => ({
	...await importOriginal<typeof import("@/services/cloudflare-approval")>(),
	acceptCloudflareApproval: vi.fn(),
	getCloudflareApproval: vi.fn(),
	proposeCloudflareApproval: vi.fn(),
	revokeCloudflareApproval: vi.fn(),
}));

const digest = "f".repeat(64);
const pending: ApprovalState = { status: "pending_review", can_manage: true };
const proposal: NonNullable<ApprovalState["proposal"]> = {
	policy_version: "v2", workflow_id: "workflow", organization_id: null, previous_revision: null,
	inactive_history_count: 0, review_sha256: digest, wildcard_https_hosts: ["*.example.com"],
	entry_path: "test.py", function_name: "run", entry_sha256: digest, module_sha256: {},
	manifest_sha256: digest, packages: ["httpx"], https_hosts: ["*.example.com"], required_https_hosts: [],
	unresolved_url_calls: 1, estimated_subrequests: 1, subrequest_allowance: 10, estimated_cpu_ms: 10, cpu_budget_ms: 1000,
};

function open() {
	const queryClient = new QueryClient();
	render(
		<QueryClientProvider client={queryClient}>
			<CloudflareApprovalPanel workflowId="workflow" />
		</QueryClientProvider>,
	);
	return userEvent.setup();
}

describe("CloudflareApprovalPanel", () => {
	beforeEach(() => vi.resetAllMocks());

	it("accepts the reviewed digest, then confirms revocation", async () => {
		vi.mocked(getCloudflareApproval).mockResolvedValue(pending);
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({ ...pending, proposal });
		vi.mocked(acceptCloudflareApproval).mockResolvedValue({ ...pending, status: "approved", current_revision: digest });
		vi.mocked(revokeCloudflareApproval).mockResolvedValue(pending);
		const user = open();
		await user.type(await screen.findByLabelText("Additional HTTPS Hosts"), "*.example.com");
		await user.click(screen.getByRole("button", { name: "Propose Approval" }));
		expect(within(await screen.findByRole("alert")).getByText("Wildcard HTTPS Hosts")).toBeVisible();
		await user.click(screen.getByRole("button", { name: "Accept Reviewed Digest" }));
		expect(await screen.findByText("Approved")).toBeVisible();
		expect(acceptCloudflareApproval).toHaveBeenCalledWith("workflow", {
			cpu_budget_ms: 1000, subrequest_allowance: 10, https_hosts: ["*.example.com"], review_sha256: digest,
		});
		await user.click(screen.getByRole("button", { name: "Revoke Approval" }));
		await user.click(screen.getByRole("button", { name: "Confirm Revocation" }));
		expect(await screen.findByText("Pending Review")).toBeVisible();
	});

	it("reviews a saved proposal and full source on open", async () => {
		vi.mocked(getCloudflareApproval).mockResolvedValue({
			...pending, proposal, proposal_sources: { "test.py": "def run(): return 7" },
		});
		const user = open();
		await user.click(await screen.findByText("Review Full Source"));
		expect(screen.getByText("def run(): return 7")).toBeVisible();
		expect(screen.getByText("Approving this Global workflow allows it to run on Cloudflare for every tenant.")).toBeVisible();
		expect(screen.getByRole("button", { name: "Accept Reviewed Digest" })).toBeVisible();
		expect(proposeCloudflareApproval).not.toHaveBeenCalled();
	});

	it("proposes and accepts a Containers review using the reviewed runtime", async () => {
		const container: NonNullable<ApprovalState["current_container"]> = {
			findings: ["CF_NATIVE"], release_digest: digest, installation_id: "installation",
			deployment_revision: 1, consent_version: "v1", worker_ingress_origin: "https://workers.example.test",
			egress: "worker-ingress-only-v1", path: "test.py", function_name: "run", source_pins: { "test.py": digest },
			wheels: {}, memory_bytes: 96 * 2 ** 20, timeout_seconds: 30, ingress_bytes: 250000, outbound_bytes: 250000,
			previous_revision: null, inactive_history_count: 0,
		};
		vi.mocked(getCloudflareApproval).mockResolvedValue(pending);
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({
			...pending, proposal: { ...proposal, container, https_hosts: [], wildcard_https_hosts: [] },
		});
		vi.mocked(acceptCloudflareApproval).mockResolvedValue({
			...pending, status: "approved", current_revision: digest, current_container: container,
		});
		vi.mocked(revokeCloudflareApproval).mockResolvedValue(pending);
		const user = open();
		await user.click(await screen.findByRole("combobox", { name: "Runtime" }));
		await user.click(await screen.findByRole("option", { name: "Containers (Linux)" }));
		await user.click(screen.getByRole("button", { name: "Propose Approval" }));
		expect(await screen.findByText("Containers (Linux) · Worker Ingress Only")).toBeVisible();
		expect(proposeCloudflareApproval).toHaveBeenCalledWith("workflow", { runtime: "containers", https_hosts: [] });
		await user.click(screen.getByRole("button", { name: "Accept Reviewed Digest" }));
		expect(await screen.findByText("Current Containers (Linux) Approval")).toBeVisible();
		expect(acceptCloudflareApproval).toHaveBeenCalledWith("workflow", expect.objectContaining({
			runtime: "containers", review_sha256: digest,
		}));
		await user.click(screen.getByRole("button", { name: "Revoke Approval" }));
		await user.click(screen.getByRole("button", { name: "Confirm Revocation" }));
		expect(await screen.findByText("Pending Review")).toBeVisible();
	});

	it("lets the operator supply a larger reviewed allowance after a budget refusal", async () => {
		vi.mocked(getCloudflareApproval).mockResolvedValue({
			...pending,
			diagnostics: [{ rule_id: "CF_SUBREQUESTS", severity: "warning", message: "Estimated subrequests exceed the allowance", hint: "Review the allowance." }],
		});
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({ ...pending, proposal });
		const user = open();
		expect(await screen.findByText("Needs Larger Budget")).toBeVisible();
		await user.type(screen.getByLabelText("Subrequest Allowance"), "20");
		await user.click(screen.getByRole("button", { name: "Propose Approval" }));
		expect(proposeCloudflareApproval).toHaveBeenCalledWith("workflow", { subrequest_allowance: 20, https_hosts: [] });
	});

	it("shows status diagnostics without exposing analysis controls to readers", async () => {
		vi.mocked(getCloudflareApproval).mockResolvedValue({
			...pending, can_manage: false, status: "ineligible", diagnostics: [{
				rule_id: "CF_FAST_PATH", severity: "warning", message: "Fast Path", hint: "Use the Normal lane.",
			}],
		});
		open();
		expect(await screen.findByText("Ineligible")).toBeVisible();
		expect(screen.getByText("Use the Normal lane.")).toBeVisible();
		expect(screen.queryByRole("button", { name: "Propose Approval" })).toBeNull();
	});

	it("clears the reviewed digest when the form changes", async () => {
		vi.mocked(getCloudflareApproval).mockResolvedValue(pending);
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({ ...pending, proposal });
		const user = open();
		await user.click(await screen.findByRole("button", { name: "Propose Approval" }));
		expect(await screen.findByRole("button", { name: "Accept Reviewed Digest" })).toBeVisible();
		await user.type(screen.getByLabelText("Additional HTTPS Hosts"), "new.example.com");
		expect(screen.queryByRole("button", { name: "Accept Reviewed Digest" })).toBeNull();
		expect(screen.queryByRole("heading", { name: "Approval Proposal" })).toBeNull();
	});

	it.each([
		["revision_changed", "Revision Changed", "CF_SOURCE"],
		["temporarily_unavailable", "Temporarily Unavailable", "CF_ANALYSIS_UNAVAILABLE"],
	] as const)("renders the %s refusal and its remedy", async (status, label, rule) => {
		vi.mocked(getCloudflareApproval).mockResolvedValue(pending);
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({ ...pending, proposal });
		vi.mocked(acceptCloudflareApproval).mockRejectedValue(new ApprovalReviewError("Approval refused.", {
			...pending, status, diagnostics: [{
				rule_id: rule, severity: "warning", message: "Review refused", hint: "Generate a new proposal.",
			}],
		}));
		const user = open();
		await user.click(await screen.findByRole("button", { name: "Propose Approval" }));
		await user.click(await screen.findByRole("button", { name: "Accept Reviewed Digest" }));
		expect(await screen.findByText(label)).toBeVisible();
		expect(screen.getByText(rule)).toBeVisible();
		expect(screen.getByText("Generate a new proposal.")).toBeVisible();
		expect(screen.queryByRole("button", { name: "Accept Reviewed Digest" })).toBeNull();
	});

	it.each([403, 404])("distinguishes HTTP %s on status reads", async (status) => {
		vi.mocked(getCloudflareApproval).mockRejectedValue(new ApiError("Workflow approval is inaccessible.", status));
		open();
		expect(await screen.findByRole("alert")).toHaveTextContent("Workflow approval is inaccessible.");
		expect(screen.getByRole("status")).toHaveTextContent(/^Unavailable$/);
		expect(screen.queryByRole("button", { name: "Propose Approval" })).toBeNull();
	});
});
