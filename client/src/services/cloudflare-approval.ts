import { apiClient } from "@/lib/api-client";
import type { components } from "@/lib/v1";

export type ApprovalState = components["schemas"]["CloudflareApprovalState"];
export type ApprovalOptions = components["schemas"]["CloudflareApprovalOptions"];
export type ApprovalAccept = components["schemas"]["CloudflareApprovalAccept"];

const path = "/api/workflows/{workflow_id}/cloudflare-approval";

function result(response: { data?: ApprovalState; error?: unknown }): ApprovalState {
	if (!response.data) {
		throw new Error("The approval request failed. Refresh the status before retrying.");
	}
	return response.data;
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
