import { expect, test } from "./fixtures/api-fixture";

test("a platform operator connects and enables Cloudflare capacity", async ({ page }) => {
	const origin = "https://workers.browser-fixture.test";
	const token = "browser-fixture-cloudflare-token";
	await page.goto("/settings/ai");
	await page.getByRole("button", { name: "Capacity", exact: true }).click();
	await page.getByRole("button", { name: "Scale Out With Cloudflare", exact: true }).click();
	await page.getByLabel("Cloudflare Account ID").fill("f".repeat(32));
	await page.getByLabel("Existing HTTPS Endpoint").fill(origin);
	const tokenField = page.getByLabel("Cloudflare API Token");
	await expect(tokenField).toHaveAttribute("type", "password");
	await tokenField.fill(token);
	await page.getByRole("checkbox", { name: "I Accept the Cloudflare Terms Above" }).check();

	const connect = page.waitForResponse((response) =>
		response.url().endsWith("/api/capacity/cloudflare") && response.request().method() === "PATCH",
	);
	await page.getByRole("button", { name: "Connect Cloudflare" }).click();
	const connected = await connect;
	expect(connected.status()).toBe(200);
	expect(await connected.text()).not.toContain(token);
	await expect(page.getByText(origin, { exact: true })).toBeVisible();

	const enable = page.waitForResponse((response) => response.url().endsWith("/api/capacity/cloudflare/enable"));
	await page.getByRole("button", { name: "Enable", exact: true }).click();
	expect((await enable).status()).toBe(202);
	await expect(page.getByText("Enabled", { exact: true })).toBeVisible();

	await page.getByRole("button", { name: "Disconnect", exact: true }).click();
	await page.getByRole("alertdialog").getByRole("button", { name: "Disconnect", exact: true }).click();
	await expect(page.getByRole("button", { name: "Connect Cloudflare" })).toBeVisible();
});
