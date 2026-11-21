import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn(), patch: vi.fn() }));
vi.mock("@/lib/api-client", () => ({
	apiClient: { GET: mocks.get, POST: mocks.post, PATCH: mocks.patch },
}));

import {
	configureCloudflareCapacity,
	getCloudflareCapacity,
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
		mocks.get.mockResolvedValue({ data: status });
		await expect(getCloudflareCapacity()).resolves.toBe(status);
		expect(mocks.get).toHaveBeenCalledWith("/api/capacity/cloudflare");
	});

	it.each(["enable", "reconcile", "scale-in", "disconnect"] as const)(
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
		});
		await expect(
			startCloudflareAction("scale-in", {
				installation_id: "installation-id",
				expected_revision: 1,
			}),
		).rejects.toThrow("Cloudflare installation changed.");
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

	it("patches the observed revision and policy", async () => {
		const body = {
			action: "policy" as const,
			installation_id: "installation-id",
			expected_revision: 3,
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
