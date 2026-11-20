import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	ApprovalReviewError,
	acceptCloudflareApproval,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
} from "./cloudflare-approval";

const { get, post, remove, handleApiError } = vi.hoisted(() => ({
	get: vi.fn(),
	post: vi.fn(),
	remove: vi.fn(),
	handleApiError: vi.fn(),
}));
vi.mock("@/lib/api-client", async (importOriginal) => ({
	...await importOriginal<typeof import("@/lib/api-client")>(),
	handleApiError,
	apiClient: { GET: get, POST: post, DELETE: remove },
}));

describe("Cloudflare workflow approval API", () => {
	const path = "/api/workflows/{workflow_id}/cloudflare-approval";
	const params = { path: { workflow_id: "workflow" } };
	const state = { status: "pending_review" as const, can_manage: true };
	const options = { subrequest_allowance: 10, cpu_budget_ms: 1000, https_hosts: ["*.example.com"] };

	beforeEach(() => {
		vi.resetAllMocks();
		for (const method of [get, post, remove]) {
			method.mockResolvedValue({ data: state, response: new Response() });
		}
	});

	it("uses the workflow-scoped read and revoke endpoints", async () => {
		await expect(getCloudflareApproval("workflow")).resolves.toEqual(state);
		await expect(revokeCloudflareApproval("workflow")).resolves.toEqual(state);
		expect(get).toHaveBeenCalledWith(path, { params });
		expect(remove).toHaveBeenCalledWith(path, { params });
	});

	it.each([
		[409, "revision_changed"],
		[503, "temporarily_unavailable"],
	] as const)("retains the approval state and message for HTTP %s", async (status, approvalStatus) => {
		const refused = { ...state, status: approvalStatus };
		post.mockResolvedValue({
			error: { detail: { message: "Review refused.", state: refused } }, response: new Response(null, { status }),
		});
		await expect(acceptCloudflareApproval("workflow", {
			...options, review_sha256: "a".repeat(64),
		})).rejects.toEqual(new ApprovalReviewError("Review refused.", refused));
		expect(handleApiError).not.toHaveBeenCalled();
	});

	it("delegates other errors to the shared handler", async () => {
		const error = { detail: "Invalid request" };
		get.mockResolvedValue({ error, response: new Response(null, { status: 422 }) });
		handleApiError.mockImplementation(() => { throw new Error("Invalid request"); });
		await expect(getCloudflareApproval("workflow")).rejects.toThrow("Invalid request");
		expect(handleApiError).toHaveBeenCalledExactlyOnceWith(error);
	});

	it("sends the reviewed options and digest", async () => {
		const accepted = { ...options, review_sha256: "a".repeat(64) };
		await proposeCloudflareApproval("workflow", options);
		await acceptCloudflareApproval("workflow", accepted);
		expect(post).toHaveBeenNthCalledWith(1, `${path}/propose`, { params, body: options });
		expect(post).toHaveBeenNthCalledWith(2, `${path}/accept`, { params, body: accepted });
	});
});
