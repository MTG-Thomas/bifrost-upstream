import { beforeEach, describe, expect, it, vi } from "vitest";
import { apiClient } from "@/lib/api-client";
import {
	acceptCloudflareApproval,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
} from "./cloudflare-approval";

vi.mock("@/lib/api-client", () => ({
	apiClient: { GET: vi.fn(), POST: vi.fn(), DELETE: vi.fn() },
}));

describe("Cloudflare workflow approval API", () => {
	const path = "/api/workflows/{workflow_id}/cloudflare-approval";
	const params = { path: { workflow_id: "workflow" } };
	const state = { status: "pending_review" as const, can_manage: true };
	const options = { subrequest_allowance: 10, cpu_budget_ms: 1000, https_hosts: ["*.example.com"] };

	beforeEach(() => {
		vi.resetAllMocks();
		for (const method of [apiClient.GET, apiClient.POST, apiClient.DELETE]) {
			vi.mocked(method).mockResolvedValue({ data: state, response: new Response() });
		}
	});

	it("uses the workflow-scoped read and revoke endpoints", async () => {
		await expect(getCloudflareApproval("workflow")).resolves.toEqual(state);
		await expect(revokeCloudflareApproval("workflow")).resolves.toEqual(state);
		expect(apiClient.GET).toHaveBeenCalledWith(path, { params });
		expect(apiClient.DELETE).toHaveBeenCalledWith(path, { params });
	});

	it("sends the reviewed options and digest", async () => {
		const accepted = { ...options, review_sha256: "a".repeat(64) };
		await proposeCloudflareApproval("workflow", options);
		await acceptCloudflareApproval("workflow", accepted);
		expect(apiClient.POST).toHaveBeenNthCalledWith(1, `${path}/propose`, { params, body: options });
		expect(apiClient.POST).toHaveBeenNthCalledWith(2, `${path}/accept`, { params, body: accepted });
	});
});
