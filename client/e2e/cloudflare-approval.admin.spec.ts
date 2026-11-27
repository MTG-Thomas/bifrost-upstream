import { randomUUID } from "node:crypto";
import { test, expect } from "./fixtures/api-fixture";

test("generates and reopens a workflow proposal from Capacity", async ({ page, api }) => {
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
		await page.goto("/settings/ai");
		await page.getByRole("button", { name: "Capacity", exact: true }).click();
		await page.getByRole("button", { name: "Scale Out with Cloudflare", exact: true }).click();
		await page.getByRole("button", { name: "Review Eligible Workflows" }).click();
		await page.getByRole("button", { name: `Generate Proposal for ${name}`, exact: true }).click();

		const panel = page.getByRole("region", { name: "Cloudflare Approval" });
		await expect(panel.getByRole("status")).toHaveText("Pending Review");
		await expect(panel.getByRole("heading", { name: "Approval Proposal" })).toBeVisible();
		await page.keyboard.press("Escape");
		await expect(panel).toHaveCount(0);
		// Reload invalidates browser state, so review must come from the worktree API.
		await page.reload();
		await page.getByRole("button", { name: "Review Eligible Workflows" }).click();
		await page.getByRole("button", { name: `Review Approval for ${name}`, exact: true }).click();
		await panel.getByText("Review Full Source").click();
		await expect(panel.getByText(/return {"ok": True}/)).toBeVisible();
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
