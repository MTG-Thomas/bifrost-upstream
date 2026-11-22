import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { getCloudflareApproval, listCloudflareApprovals, proposeCloudflareApproval } from "@/services/cloudflare-approval";
import { CloudflareWorkflowApprovals } from "./CloudflareWorkflowApprovals";

vi.mock("@/services/cloudflare-approval", async (importOriginal) => ({
	...await importOriginal<typeof import("@/services/cloudflare-approval")>(),
	getCloudflareApproval: vi.fn(),
	listCloudflareApprovals: vi.fn(),
	proposeCloudflareApproval: vi.fn(),
}));

it("lists scoped counts and generates a proposal with the default budgets", async () => {
	const state = { status: "pending_review", can_manage: true } as const;
	vi.mocked(listCloudflareApprovals).mockResolvedValue({
		counts: { pending_review: 1, approved: 0, ineligible: 0, revision_changed: 0, temporarily_unavailable: 0 },
		workflows: [{ workflow_id: "workflow", organization_id: null, scope: "global", name: "Global Workflow", status: "pending_review", has_proposal: false }],
		can_propose: true,
	});
	vi.mocked(proposeCloudflareApproval).mockResolvedValue(state);
	vi.mocked(getCloudflareApproval).mockResolvedValue(state);
	render(
		<QueryClientProvider client={new QueryClient()}>
			<CloudflareWorkflowApprovals />
		</QueryClientProvider>,
	);
	const user = userEvent.setup();
	expect(await screen.findByText("Temporarily Unavailable")).toBeVisible();
	await user.click(screen.getByRole("button", { name: "Review Eligible Workflows" }));
	await user.click(screen.getByRole("button", { name: "Generate Proposal for Global Workflow" }));
	expect(proposeCloudflareApproval).toHaveBeenCalledWith("workflow", { subrequest_allowance: 10, cpu_budget_ms: 1000 });
	expect(await screen.findByRole("dialog", { name: "Global Workflow" })).toBeVisible();
});
