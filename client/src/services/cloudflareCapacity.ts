import { apiClient } from "@/lib/api-client";
import { getErrorMessage } from "@/lib/api-error";
import type { components } from "@/lib/v1";

export type CloudflareCapacityStatus =
	components["schemas"]["CloudflareCapacityStatus"];
export type CloudflareCapacityAction =
	components["schemas"]["CloudflareCapacityAction"];
export type CloudflareAccountTokenRotation =
	components["schemas"]["CloudflareAccountTokenRotation"];
export type CloudflareAction = "scale-in" | "disconnect";

const actionPaths = {
	"scale-in": "/api/capacity/cloudflare/scale-in",
	disconnect: "/api/capacity/cloudflare/disconnect",
} as const;

export async function getCloudflareCapacity(): Promise<CloudflareCapacityStatus> {
	const { data, error } = await apiClient.GET("/api/capacity/cloudflare");
	if (error) {
		throw new Error(
			getErrorMessage(error, "Could not load Cloudflare capacity."),
		);
	}
	return data;
}

export async function startCloudflareAction(
	action: CloudflareAction,
	body: CloudflareCapacityAction,
): Promise<components["schemas"]["PlatformJobAccepted"]> {
	const { data, error } = await apiClient.POST(actionPaths[action], { body });
	if (error) {
		throw new Error(
			getErrorMessage(error, "Could not start the Cloudflare job."),
		);
	}
	return data;
}

export async function rotateCloudflareAccountToken(
	body: CloudflareAccountTokenRotation,
): Promise<components["schemas"]["PlatformJobAccepted"]> {
	const { data, error } = await apiClient.POST("/api/capacity/cloudflare/rotate", { body });
	if (error) {
		throw new Error(getErrorMessage(error, "Could not queue Cloudflare API token rotation."));
	}
	return data;
}
