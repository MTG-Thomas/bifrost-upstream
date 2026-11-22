import { expect, test } from "./fixtures/api-fixture";

test("a platform operator enables and reconciles Cloudflare capacity", async ({ page }) => {
	const origin = "https://workers.browser-fixture.test";
	const token = "browser-fixture-cloudflare-token";
	await page.goto("/settings/ai");
	await page.getByRole("button", { name: "Capacity", exact: true }).click();
	await page.getByRole("button", { name: "Scale Out with Cloudflare", exact: true }).click();
	await expect(page.getByRole("heading", { name: "Scale Out with Cloudflare" })).toBeVisible();
	const setup = page.getByRole("button", { name: "Set Up Cloudflare" });
	await setup.click();
	const account = page.getByRole("textbox", { name: "Cloudflare Account ID" });
	await expect(account).toBeFocused();
	await expect(page.getByRole("group", { name: "Connection" })).toBeVisible();
	await expect(page.getByRole("group", { name: "Spending" })).toBeVisible();
	await expect(page.getByRole("group", { name: "Advanced Policy" })).toBeVisible();
	await expect(page.getByRole("spinbutton", { name: "Monthly Estimated Usage Limit" })).toHaveAccessibleDescription("USD/month");
	await page.getByRole("button", { name: "Connect Cloudflare" }).click();
	const summary = page.getByRole("region", { name: "Cloudflare Settings Error" });
	await expect(summary).toBeFocused();
	await expect(summary.getByRole("button", { name: /^Cloudflare Account ID:/ })).toBeVisible();
	await expect(summary.getByRole("button", { name: /^Existing HTTPS Endpoint:/ })).toBeVisible();
	await expect(summary.getByRole("button", { name: /^Cloudflare API Token:/ })).toBeVisible();
	await expect(account).toHaveAttribute("aria-invalid", "true");
	await account.fill("f".repeat(32));
	await page.getByLabel("Existing HTTPS Endpoint").fill(origin);
	const tokenField = page.getByLabel("Cloudflare API Token");
	await expect(tokenField).toHaveAttribute("type", "password");
	await tokenField.fill(token);
	await page.getByRole("checkbox", { name: "Accept Cloudflare Terms" }).check();

	const connect = page.waitForResponse((response) =>
		response.url().endsWith("/api/capacity/cloudflare") && response.request().method() === "PATCH",
	);
	await page.getByRole("button", { name: "Connect Cloudflare" }).click();
	const connected = await connect;
	expect(connected.status()).toBe(200);
	expect(await connected.text()).not.toContain(token);
	await expect(page.getByText(origin, { exact: true })).toBeVisible();
	await expect(page.getByText("Ready to Enable", { exact: true })).toBeVisible();
	await expect(page.getByRole("button", { name: "Enable", exact: true })).toBeFocused();
	await expect(page.getByText("Estimated Fleet Usage")).toBeVisible();
	await expect(page.getByText("Estimated Bifrost Usage This UTC Month")).toBeVisible();
	await expect(page.getByText("Workers Paid Subscription (Billed by Cloudflare)")).toBeVisible();

	await page.getByRole("button", { name: "Rotate API Token", exact: true }).click();
	const rotation = page.getByRole("dialog", { name: "Rotate Cloudflare API Token" });
	await rotation.getByLabel("Replacement API Token").fill("browser-fixture-cloudflare-refused-token");
	const rotated = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/rotate"));
	await rotation.getByRole("button", { name: "Rotate API Token", exact: true }).click();
	expect((await rotated).status()).toBe(202);
	const jobStatus = page.getByRole("region", { name: "Cloudflare Job Status" });
	await expect(jobStatus).toBeFocused();
	await expect(jobStatus.getByText("CF_MANAGEMENT_CREDENTIAL")).toBeVisible();
	await expect(jobStatus).toBeFocused();

	const enable = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/enable"));
	const admitted = page.waitForResponse(async (response) =>
		response.url().endsWith("/api/capacity/cloudflare") &&
		response.request().method() === "GET" &&
		response.ok() &&
		(await response.json()).installation?.admission_open === true,
	);
	await page.getByRole("button", { name: "Enable", exact: true }).click();
	expect((await enable).status()).toBe(202);
	await admitted;
	await expect(page.getByText("Enabled", { exact: true })).toBeVisible();
	await expect(page.getByText("Deployed Revision", { exact: true })).toBeVisible();

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
});
