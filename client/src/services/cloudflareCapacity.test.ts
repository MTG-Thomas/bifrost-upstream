import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), patch: vi.fn() }));
vi.mock("@/lib/api-client", () => ({
	apiClient: { GET: mocks.get, POST: mocks.post, PATCH: mocks.patch },
}));

import {
	activateCloudflareCapacity,
	configureCloudflareCapacity,
	getCloudflareCapacity,
	preflightCloudflareCapacity,
	rotateCloudflareAccountToken,
	startCloudflareAction,
} from "./cloudflareCapacity";

describe("Cloudflare capacity service", () => {
	beforeEach(() => {
		mocks.get.mockReset();
		mocks.post.mockReset();
		mocks.patch.mockReset();
	});

	it("reads Cloudflare capacity status", async () => {
		const status = {
			consent_text_version: "existing-https-v1",
			installation: null,
		};
		mocks.get.mockResolvedValue({ data: status, response: { ok: true, status: 200 } });
		await expect(getCloudflareCapacity()).resolves.toBe(status);
		expect(mocks.get).toHaveBeenCalledWith("/api/capacity/cloudflare");
	});

	it("posts preflight and activation through their write-only contracts", async () => {
		const body = { api_token: "synthetic-management-token" };
		const preflight = { preflight_receipt: "opaque-receipt", consent_version: "cloudflare-consent" };
		const activation = {
			...body, ...preflight, consent: true as const, monthly_limit_usd: "75.00",
		};
		const accepted = { job_id: "job-id", status: "queued" };
		mocks.post.mockResolvedValueOnce({ data: preflight }).mockResolvedValueOnce({ data: accepted });
		await expect(preflightCloudflareCapacity(body)).resolves.toBe(preflight);
		await expect(activateCloudflareCapacity(activation)).resolves.toBe(accepted);
		expect(mocks.post.mock.calls).toEqual([
			["/api/capacity/cloudflare/preflight", { body }],
			["/api/capacity/cloudflare/activate", { body: activation }],
		]);
	});

	it("preserves the response status on a failed capacity read", async () => {
		mocks.get.mockResolvedValue({
			error: { detail: "Platform access is required." },
			response: { ok: false, status: 403 },
		});
		await expect(getCloudflareCapacity()).rejects.toMatchObject({
			message: "Platform access is required.",
			statusCode: 403,
		});
	});

	it.each(["enable", "reconcile", "upgrade", "scale-in", "disconnect"] as const)(
		"queues %s with the observed revision",
		async (action) => {
			const body = {
				installation_id: "installation-id",
				expected_revision: 3,
			};
			const accepted = { job_id: "job-id", status: "queued" };
			mocks.post.mockResolvedValue({ data: accepted });
			await expect(startCloudflareAction(action, body)).resolves.toBe(
				accepted,
			);
			expect(mocks.post).toHaveBeenCalledWith(
				"/api/capacity/cloudflare/" + action,
				{ body },
			);
		},
	);

	it("reports a changed installation rather than retrying an action", async () => {
		mocks.post.mockResolvedValue({
			error: { detail: "Cloudflare installation changed." },
			response: { status: 409 },
		});
		await expect(
			startCloudflareAction("scale-in", {
				installation_id: "installation-id",
				expected_revision: 1,
			}),
		).rejects.toMatchObject({ message: "Cloudflare installation changed.", statusCode: 409 });
		expect(mocks.post).toHaveBeenCalledTimes(1);
	});

	it("posts the replacement to the write-only rotation action", async () => {
		const body = {
			installation_id: "installation-id",
			expected_revision: 3,
			api_token: "synthetic-replacement",
		};
		const accepted = { job_id: "job-id", status: "queued" };
		mocks.post.mockResolvedValue({ data: accepted });
		await expect(rotateCloudflareAccountToken(body)).resolves.toBe(accepted);
		expect(mocks.post).toHaveBeenCalledWith("/api/capacity/cloudflare/rotate", { body });
	});

	it.each([["rotate-root", "control_root"], ["rotate-bearer", "scope_bearer"], ["rotate-connector", "connector_token"]] as const)(
		"queues %s using the server-generated credential path",
		async (action, credential) => {
			const body = { installation_id: "installation-id", expected_revision: 3 };
			mocks.post.mockResolvedValue({ data: { job_id: "job-id", status: "queued" } });
			await startCloudflareAction(action, body);
			expect(mocks.post).toHaveBeenCalledWith("/api/capacity/cloudflare/rotate", {
				body: { ...body, credential },
			});
		},
	);

	it("preserves structured validation details without using the submitted input as the message", async () => {
		const issue = { loc: ["body", "api_token"], msg: "Invalid token format.", input: "synthetic-secret", type: "value_error" };
		mocks.post.mockResolvedValue({ error: { detail: [issue] }, response: { status: 422 } });
		await expect(preflightCloudflareCapacity({ api_token: "synthetic-secret" })).rejects.toMatchObject({
			errorCode: "ValidationError",
			statusCode: 422,
			message: "api_token: Invalid token format.",
			details: { validation_errors: [issue] },
		});
	});

	it("patches the observed revision, policy version, and policy", async () => {
		const body = {
			action: "policy" as const,
			installation_id: "installation-id",
			expected_revision: 3,
			expected_policy_version: 7,
			policy: {
				version: 1 as const,
				workers: {
					launcher_cap: 1, slots_per_launcher: 2, slots_per_tenant: 4, installation_slots: 9,
					max_lifetime_seconds: 3600,
					linger_seconds: 45, queue_prewarm_per_tenant: 0, work_kinds: ["workflow" as const],
				},
				monthly_limit_usd: "10.00",
			},
		};
		const data = { installation: null, consent_text_version: "existing-https-v1", consent_text: "Current consent" };
		mocks.patch.mockResolvedValue({ data });
		await expect(configureCloudflareCapacity(body)).resolves.toBe(data);
		expect(mocks.patch).toHaveBeenCalledWith("/api/capacity/cloudflare", { body });
	});
});
