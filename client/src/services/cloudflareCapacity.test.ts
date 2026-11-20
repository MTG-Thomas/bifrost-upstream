import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));
vi.mock("@/lib/api-client", () => ({
	apiClient: { GET: mocks.get, POST: mocks.post },
}));

import {
	getCloudflareCapacity,
	startCloudflareAction,
} from "./cloudflareCapacity";

describe("Cloudflare capacity service", () => {
	beforeEach(() => {
		mocks.get.mockReset();
		mocks.post.mockReset();
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

	it.each(["scale-in", "disconnect"] as const)(
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
});
