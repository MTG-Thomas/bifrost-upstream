import { expect, test } from "./fixtures/api-fixture";

test("an operator scales in Cloudflare capacity from Settings", async ({
	page,
}) => {
	const installation = {
		installation_id: "365a5b2b-7b12-40c4-b5ff-06322da4a3a9",
		worker_ingress_origin: "https://worker-ingress.example.test",
		desired_revision: 1,
		desired_state: "enabled",
		admission_open: true,
		fleet_envelope_usd: "10.00",
		policy: {
			monthly_limit_usd: "30.00",
			workers: { launcher_cap: 1, slots_per_launcher: 2 },
		},
	};
	const jobId = "f8081ebd-1432-4a2f-b51f-1f0e9f8c05d4";
	await page.route("**/api/capacity/cloudflare", (route) =>
		route.fulfill({
			json: { consent_text_version: "existing-https-v1", installation },
		}),
	);
	await page.route("**/api/capacity/cloudflare/scale-in", async (route) => {
		expect(route.request().postDataJSON()).toEqual({
			installation_id: installation.installation_id,
			expected_revision: 1,
		});
		installation.desired_revision = 2;
		installation.desired_state = "scaled_in";
		installation.admission_open = false;
		await route.fulfill({
			status: 202,
			json: { job_id: jobId, status: "queued" },
		});
	});
	await page.route("**/api/platform-jobs/" + jobId, (route) =>
		route.fulfill({
			json: {
				id: jobId,
				status: "succeeded",
				revision: 1,
				progress: { phase: "Scale In Complete" },
			},
		}),
	);

	await page.goto("/settings/ai");
	await page.getByRole("button", { name: "Capacity", exact: true }).click();
	await page
		.getByRole("button", { name: "Scale Out With Cloudflare", exact: true })
		.click();
	await expect(
		page.getByText("https://worker-ingress.example.test"),
	).toBeVisible();
	await page.getByRole("button", { name: "Scale In", exact: true }).click();
	await expect(
		page.getByText("Scale In Complete", { exact: true }),
	).toBeVisible();
	await expect(page.getByText("Scaled In", { exact: true })).toBeVisible();
});
