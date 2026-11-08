import { Buffer } from "node:buffer";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { test, expect } from "./fixtures/api-fixture";

type ZipEntry = { path: string; content: Buffer };

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
	let c = n;
	for (let k = 0; k < 8; k += 1) {
		c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
	}
	return c >>> 0;
});

function crc32(input: Buffer): number {
	let crc = 0xffffffff;
	for (const byte of input) {
		crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
	}
	return (crc ^ 0xffffffff) >>> 0;
}

function buildStoreOnlyZip(entries: ZipEntry[]): Buffer {
	const localParts: Buffer[] = [];
	const centralParts: Buffer[] = [];
	let offset = 0;

	for (const entry of entries) {
		const name = Buffer.from(entry.path);
		const checksum = crc32(entry.content);
		const local = Buffer.alloc(30);
		local.writeUInt32LE(0x04034b50, 0);
		local.writeUInt16LE(20, 4);
		local.writeUInt16LE(0, 6);
		local.writeUInt16LE(0, 8);
		local.writeUInt32LE(checksum, 14);
		local.writeUInt32LE(entry.content.length, 18);
		local.writeUInt32LE(entry.content.length, 22);
		local.writeUInt16LE(name.length, 26);
		local.writeUInt16LE(0, 28);
		localParts.push(local, name, entry.content);

		const central = Buffer.alloc(46);
		central.writeUInt32LE(0x02014b50, 0);
		central.writeUInt16LE(20, 4);
		central.writeUInt16LE(20, 6);
		central.writeUInt16LE(0, 8);
		central.writeUInt16LE(0, 10);
		central.writeUInt32LE(checksum, 16);
		central.writeUInt32LE(entry.content.length, 20);
		central.writeUInt32LE(entry.content.length, 24);
		central.writeUInt16LE(name.length, 28);
		central.writeUInt16LE(0, 30);
		central.writeUInt16LE(0, 32);
		central.writeUInt32LE(offset, 42);
		centralParts.push(central, name);
		offset += local.length + name.length + entry.content.length;
	}

	const centralDirectory = Buffer.concat(centralParts);
	const end = Buffer.alloc(22);
	end.writeUInt32LE(0x06054b50, 0);
	end.writeUInt16LE(entries.length, 8);
	end.writeUInt16LE(entries.length, 10);
	end.writeUInt32LE(centralDirectory.length, 12);
	end.writeUInt32LE(offset, 16);
	return Buffer.concat([...localParts, centralDirectory, end]);
}

test("installed GitHub App form reports cold setup without credentials", async ({
	page,
	api,
}) => {
	// One install and one workflow execution prove the real package's Form UI.
	const packageRoot = join(
		process.cwd(),
		"examples",
		"github-app",
		"operator",
	);
	const paths = [
		"bifrost.solution.yaml",
		".bifrost/workflows.yaml",
		".bifrost/tables.yaml",
		".bifrost/events.yaml",
		".bifrost/connections.yaml",
		".bifrost/forms.yaml",
		"modules/github_app.py",
		"workflows/operator.py",
	];
	const entries = await Promise.all(
		paths.map(async (path) => ({
			path,
			content: await readFile(join(packageRoot, path)),
		})),
	);
	let solutionId: string | undefined;
	try {
		const installed = await api.post("/api/solutions/install", {
			multipart: {
				organization_id: "00000000-0000-0000-0000-000000000002",
				file: {
					name: "github-app-operator.zip",
					mimeType: "application/zip",
					buffer: buildStoreOnlyZip(entries),
				},
			},
		});
		expect(installed.status(), await installed.text()).toBe(202);
		const accepted = (await installed.json()) as { deploy_job_id: string };
		await expect
			.poll(
				async () => {
					const response = await api.get(
						`/api/solutions/deploy-jobs/${accepted.deploy_job_id}`,
					);
					expect(response.ok(), await response.text()).toBe(true);
					const job = await response.json();
					if (job.status === "failed") throw new Error(job.error);
					if (job.status === "succeeded")
						solutionId = job.result.solution_id;
					return job.status;
				},
				{ timeout: 60_000 },
			)
			.toBe("succeeded");
		const entities = await api.get(`/api/solutions/${solutionId}/entities`);
		expect(entities.ok(), await entities.text()).toBe(true);
		const form = (await entities.json()).forms.find(
			(item: { name: string }) =>
				item.name === "GitHub App Setup And Health",
		);
		expect(form).toBeTruthy();
		await page.goto(`/execute/${form.id}`);
		await expect(
			page.getByRole("heading", { name: "GitHub App Setup And Health" }),
		).toBeVisible();
		await expect(
			page.getByRole("combobox", { name: "Action *" }),
		).toContainText("Health");
		await expect(
			page.getByLabel("Installation ID", { exact: true }),
		).toBeVisible();
		const submitted = page.waitForResponse(
			(response) =>
				response.url().includes(`/api/forms/${form.id}/submissions`) &&
				response.request().method() === "POST",
		);
		await page.getByRole("button", { name: "Submit", exact: true }).click();
		const submission = await submitted;
		expect(submission.ok(), await submission.text()).toBe(true);
		const execution = (await submission.json()) as { execution_id: string };
		await expect(page).toHaveURL(
			new RegExp(`/history/${execution.execution_id}$`),
		);
		await page.getByRole("tab", { name: "Result", exact: true }).click();
		const panel = page.getByRole("tabpanel", { name: "Result" });
		await expect(
			panel.getByText("retained Event only", { exact: false }).first(),
		).toBeVisible({ timeout: 30_000 });
		const result = await api.get(
			`/api/executions/${execution.execution_id}`,
		);
		expect(result.ok(), await result.text()).toBe(true);
		expect((await result.json()).result).toMatchObject({
			connection_configured: false,
			external_writes: false,
		});
	} finally {
		if (solutionId) {
			const removed = await api.delete(`/api/solutions/${solutionId}`, {
				params: { confirm: "github-app-operator" },
			});
			expect(removed.ok(), await removed.text()).toBe(true);
		}
	}
});
