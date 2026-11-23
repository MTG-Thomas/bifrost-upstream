import { apiClient } from "@/lib/api-client";
import { parseApiError } from "@/lib/api-error";
import type { components } from "@/lib/v1";

export type CloudflareCapacityStatus =
	components["schemas"]["CloudflareCapacityStatus"];
export type CloudflareCapacityAction =
	components["schemas"]["CloudflareCapacityAction"];
export type CloudflareAccountTokenRotation =
	components["schemas"]["CloudflareAccountTokenRotation"];
export type CloudflareConfiguration = components["schemas"]["CloudflarePolicyUpdate"];
export type CloudflareAction = "enable" | "reconcile" | "upgrade" | "scale-in" | "disconnect" | "rotate-root" | "rotate-bearer" | "rotate-connector";
export type CloudflarePreflight = components["schemas"]["CloudflarePreflight"];
export type CloudflarePreflightRequest = components["schemas"]["CloudflarePreflightRequest"];
export type CloudflareActivateRequest = components["schemas"]["CloudflareActivateRequest"];
export type CloudflareJobAccepted = components["schemas"]["PlatformJobAccepted"];

const actionPaths = {
	enable: "/api/capacity/cloudflare/enable",
	reconcile: "/api/capacity/cloudflare/reconcile",
	upgrade: "/api/capacity/cloudflare/upgrade",
	"scale-in": "/api/capacity/cloudflare/scale-in",
	disconnect: "/api/capacity/cloudflare/disconnect",
} as const;

const credentials = {
	"rotate-root": "control_root",
	"rotate-bearer": "scope_bearer",
	"rotate-connector": "connector_token",
} as const;

export async function getCloudflareCapacity(): Promise<CloudflareCapacityStatus> {
	const { data, error, response } = await apiClient.GET("/api/capacity/cloudflare");
	if (data) return data;
	throw parseApiError(error, response.status);
}

export async function configureCloudflareCapacity(body: CloudflareConfiguration): Promise<CloudflareCapacityStatus> {
	const { data, error, response } = await apiClient.PATCH("/api/capacity/cloudflare", { body });
	if (error) {
		throw parseApiError(error, response.status);
	}
	return data;
}

export async function preflightCloudflareCapacity(body: CloudflarePreflightRequest): Promise<CloudflarePreflight> {
	const { data, error, response } = await apiClient.POST("/api/capacity/cloudflare/preflight", { body });
	if (error) throw parseApiError(error, response.status);
	return data;
}

export async function activateCloudflareCapacity(body: CloudflareActivateRequest): Promise<CloudflareJobAccepted> {
	const { data, error, response } = await apiClient.POST("/api/capacity/cloudflare/activate", { body });
	if (error) throw parseApiError(error, response.status);
	return data;
}

export async function startCloudflareAction(
	action: CloudflareAction,
	body: CloudflareCapacityAction,
): Promise<components["schemas"]["PlatformJobAccepted"]> {
	const { data, error, response } = action === "rotate-root" || action === "rotate-bearer" || action === "rotate-connector"
		? await apiClient.POST("/api/capacity/cloudflare/rotate", { body: {
			...body, credential: credentials[action],
		} })
		: await apiClient.POST(actionPaths[action], { body });
	if (error) {
		throw parseApiError(error, response.status);
	}
	return data;
}

export async function rotateCloudflareAccountToken(
	body: CloudflareAccountTokenRotation,
): Promise<components["schemas"]["PlatformJobAccepted"]> {
	const { data, error, response } = await apiClient.POST("/api/capacity/cloudflare/rotate", { body });
	if (error) {
		throw parseApiError(error, response.status);
	}
	return data;
}
