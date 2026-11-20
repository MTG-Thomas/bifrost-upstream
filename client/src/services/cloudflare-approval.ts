import { ApiError, apiClient, handleApiError } from "@/lib/api-client";
import type { components } from "@/lib/v1";

export type ApprovalState = components["schemas"]["CloudflareApprovalState"];
export type ApprovalOptions = components["schemas"]["CloudflareApprovalOptions"];
export type ApprovalAccept = components["schemas"]["CloudflareApprovalAccept"];

const path = "/api/workflows/{workflow_id}/cloudflare-approval";

export class ApprovalReviewError extends Error {
	constructor(message: string, readonly state: ApprovalState) {
		super(message);
	}
}

function isReviewRefusal(detail: unknown): detail is { message: string; state: ApprovalState } {
	if (typeof detail !== "object" || detail === null || !("message" in detail) || !("state" in detail)) {
		return false;
	}
	const state = detail.state;
	return typeof detail.message === "string" && typeof state === "object" && state !== null
		&& "status" in state && "can_manage" in state;
}

function result({ data, error, response }: { data?: ApprovalState; error?: unknown; response: Response }): ApprovalState {
	if (data) return data;
	if (typeof error === "object" && error !== null && "detail" in error && isReviewRefusal(error.detail)) {
		throw new ApprovalReviewError(error.detail.message, error.detail.state);
	}
	if (response.status === 403 || response.status === 404) {
		throw new ApiError(
			response.status === 403 ? "You do not have access to this approval." : "This workflow could not be found.",
			response.status,
		);
	}
	handleApiError(error);
}

export async function getCloudflareApproval(workflowId: string) {
	return result(await apiClient.GET(path, { params: { path: { workflow_id: workflowId } } }));
}

export async function proposeCloudflareApproval(workflowId: string, body: ApprovalOptions) {
	return result(await apiClient.POST(`${path}/propose`, { params: { path: { workflow_id: workflowId } }, body }));
}

export async function acceptCloudflareApproval(workflowId: string, body: ApprovalAccept) {
	return result(await apiClient.POST(`${path}/accept`, { params: { path: { workflow_id: workflowId } }, body }));
}

export async function revokeCloudflareApproval(workflowId: string) {
	return result(await apiClient.DELETE(path, { params: { path: { workflow_id: workflowId } } }));
}
