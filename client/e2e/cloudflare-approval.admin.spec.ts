import { randomUUID } from "node:crypto";
import { test, expect } from "./fixtures/api-fixture";

test("reviews, accepts and revokes a workflow approval", async ({ page, api }) => {
	const name = `approval_${randomUUID().replaceAll("-", "")}`;
	const path = `${name}.py`;
	const write = await api.put("/api/files/editor/content", {
		data: {
			path,
			encoding: "utf-8",
			content: `from bifrost import workflow

@workflow(name="${name}")
async def ${name}():
    return {"ok": True}
`,
		},
	});
	expect(write.ok(), await write.text()).toBe(true);
	try {
		const registered = await api.post("/api/workflows/register", {
			data: { path, function_name: name },
		});
		expect(registered.ok(), await registered.text()).toBe(true);
		await page.goto("/workflows");
		await page.getByPlaceholder("Search by name, description, or category...").fill(name);
		await page.getByRole("button", { name: `${name} actions`, exact: true }).click();
		await page.getByRole("menuitem", { name: "Review Cloudflare Approval" }).click();

		const panel = page.getByRole("region", { name: "Cloudflare Approval" });
		await expect(panel.getByRole("status")).toHaveText("Pending Review");
		await panel.getByRole("button", { name: "Propose Approval" }).click();
		await expect(panel.getByRole("heading", { name: "Approval Proposal" })).toBeVisible();
		await panel.getByRole("button", { name: "Accept Reviewed Digest" }).click();
		await expect(panel.getByRole("status")).toHaveText("Approved");
		await panel.getByRole("button", { name: "Revoke Approval" }).click();
		await panel.getByRole("button", { name: "Confirm Revocation" }).click();
		await expect(panel.getByRole("status")).toHaveText("Pending Review");
	} finally {
		const removed = await api.delete(`/api/files/editor?path=${encodeURIComponent(path)}`);
		expect(removed.ok(), await removed.text()).toBe(true);
	}
});
