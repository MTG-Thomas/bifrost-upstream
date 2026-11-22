import { expect, test } from "./fixtures/api-fixture";

test("a platform operator activates and reconciles Cloudflare capacity", async ({ page }) => {
	const origin = "https://workers.browser-fixture.test";
	const token = "browser-fixture-cloudflare-token";
	await page.goto("/settings/ai");
	await page.getByRole("button", { name: "Capacity", exact: true }).click();
	await page.getByRole("button", { name: "Scale Out with Cloudflare", exact: true }).click();
	await expect(page.getByRole("heading", { name: "Scale Out with Cloudflare" })).toBeVisible();
	const setup = page.getByRole("button", { name: "Set Up Cloudflare" });
	await setup.click();
	const form = page.getByRole("form", { name: "Cloudflare Setup" });
	const tokenField = form.getByLabel("Cloudflare API Token");
	await expect(tokenField).toBeFocused();
	await tokenField.fill(token);
	await form.getByRole("button", { name: "Run Preflight" }).click();
	await expect(form.getByText("Cloudflare Account: Browser Fixture Account")).toBeVisible();
	await form.getByLabel("Existing HTTPS Endpoint").fill(origin);
	const preflight = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/preflight"));
	await form.getByRole("button", { name: "Check Cloudflare Setup" }).click();
	const reviewed = await preflight;
	expect(reviewed.status()).toBe(200);
	expect(await reviewed.text()).not.toContain(token);
	await expect(form.getByText("About $2–$7/month")).toBeVisible();
	await expect(form.getByText("32.13 USD/month")).toBeVisible();
	await expect(form.getByLabel("Monthly Estimated Usage Limit")).toHaveValue("75.00");
	await form.getByRole("checkbox", { name: "Confirm Monthly Usage Limit" }).check();
	await form.getByRole("checkbox", { name: "Accept Cloudflare Terms" }).check();

	const activate = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/activate"));
	const admitted = page.waitForResponse(async (response) =>
		response.url().endsWith("/api/capacity/cloudflare") &&
		response.request().method() === "GET" &&
		response.ok() &&
		(await response.json()).installation?.admission_open === true,
	);
	await form.getByRole("button", { name: "Scale Out with Cloudflare" }).click();
	const queued = await activate;
	expect(queued.status()).toBe(202);
	expect(queued.headers().location).toBe(`/api/platform-jobs/${(await queued.json()).job_id}`);
	await expect(page.getByRole("region", { name: "Cloudflare Job Status" })).toBeVisible();
	await admitted;
	await expect(page.getByText("Enabled", { exact: true })).toBeVisible();
	await expect(page.getByText("Observed Deployment Revision", { exact: true })).toBeVisible();
	await expect(page.getByText("Workflow Review Required", { exact: true })).toBeVisible();

	await expect(page.getByRole("button", { name: "Upgrade Cloudflare" })).toBeDisabled();
	const reconcile = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/reconcile"));
	await page.getByRole("button", { name: "Reconcile Cloudflare" }).click();
	const accepted = await reconcile;
	expect(accepted.status()).toBe(202);
	expect(accepted.headers().location).toBe(`/api/platform-jobs/${(await accepted.json()).job_id}`);

	const retired = page.waitForResponse(async (response) =>
		response.url().endsWith("/api/capacity/cloudflare") &&
		response.request().method() === "GET" &&
		response.ok() &&
		(await response.json()).installation === null,
	);
	await page.getByRole("button", { name: "Disconnect", exact: true }).click();
	await page.getByRole("alertdialog").getByRole("button", { name: "Disconnect", exact: true }).click();
	await retired;
	await expect(page.getByRole("status").filter({ hasText: "Cloudflare was disconnected." })).toBeVisible();
	await expect(setup).toBeFocused();
	await page.goto("/settings/ai");
	await page.getByRole("button", { name: "Capacity", exact: true }).click();
	await page.getByRole("button", { name: "Scale Out with Cloudflare", exact: true }).click();
	await expect(page.getByRole("status").filter({ hasText: "Cloudflare was disconnected." })).toBeVisible();
});
