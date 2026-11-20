import { apiClient } from "@/lib/api-client";
import { getErrorMessage } from "@/lib/api-error";
import type { components } from "@/lib/v1";

export type CloudflareCapacityStatus =
	components["schemas"]["CloudflareCapacityStatus"];
export type CloudflareCapacityAction =
	components["schemas"]["CloudflareCapacityAction"];
export type CloudflareAccountTokenRotation =
	components["schemas"]["CloudflareAccountTokenRotation"];
export type CloudflareConfiguration =
	| components["schemas"]["CloudflareConnectRequest"]
	| components["schemas"]["CloudflarePolicyUpdate"];
export type CloudflareAction = "enable" | "reconcile" | "scale-in" | "disconnect" | "rotate-root" | "rotate-bearer";

const actionPaths = {
	enable: "/api/capacity/cloudflare/enable",
	reconcile: "/api/capacity/cloudflare/reconcile",
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

export async function configureCloudflareCapacity(body: CloudflareConfiguration): Promise<CloudflareCapacityStatus> {
	const { data, error } = await apiClient.PATCH("/api/capacity/cloudflare", { body });
	if (error) {
		throw new Error(getErrorMessage(error, "Could not save Cloudflare capacity."));
	}
	return data;
}

export async function startCloudflareAction(
	action: CloudflareAction,
	body: CloudflareCapacityAction,
): Promise<components["schemas"]["PlatformJobAccepted"]> {
	const { data, error } = action === "rotate-root" || action === "rotate-bearer"
		? await apiClient.POST("/api/capacity/cloudflare/rotate", { body: {
			...body, credential: action === "rotate-root" ? "control_root" : "scope_bearer",
		} })
		: await apiClient.POST(actionPaths[action], { body });
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
