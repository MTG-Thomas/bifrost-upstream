import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	acceptCloudflareApproval,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
	type ApprovalState,
} from "@/services/cloudflare-approval";
import { CloudflareApprovalPanel } from "./CloudflareApprovalPanel";

vi.mock("@/services/cloudflare-approval", () => ({
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
	estimated_subrequests: 1, subrequest_allowance: 10, estimated_cpu_ms: 10, cpu_budget_ms: 1000,
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
		expect(await screen.findByText("*.example.com")).toBeVisible();
		await user.click(screen.getByRole("button", { name: "Accept Reviewed Digest" }));
		expect(await screen.findByText("Approved")).toBeVisible();
		expect(acceptCloudflareApproval).toHaveBeenCalledWith("workflow", {
			cpu_budget_ms: 1000, subrequest_allowance: 10, https_hosts: ["*.example.com"], review_sha256: digest,
		});
		await user.click(screen.getByRole("button", { name: "Revoke Approval" }));
		await user.click(screen.getByRole("button", { name: "Confirm Revocation" }));
		expect(await screen.findByText("Pending Review")).toBeVisible();
	});

	it("shows diagnostics without exposing operator controls to readers", async () => {
		const readOnly = { ...pending, can_manage: false };
		vi.mocked(getCloudflareApproval).mockResolvedValue(readOnly);
		vi.mocked(proposeCloudflareApproval).mockResolvedValue({
			...readOnly, proposal, diagnostics: [{
				rule_id: "CF_URL_REVIEW", severity: "info", message: "Host review required.", hint: "Ask a Global operator.",
			}],
		});
		const user = open();
		await user.click(await screen.findByRole("button", { name: "Propose Approval" }));
		expect(await screen.findByText("CF_URL_REVIEW")).toBeVisible();
		expect(screen.queryByRole("button", { name: "Accept Reviewed Digest" })).toBeNull();
		expect(screen.queryByLabelText("Additional HTTPS Hosts")).toBeNull();
	});
});
