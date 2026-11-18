import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";

import { connect } from "./core";
import { getBifrostTransport } from "./transport";

vi.mock("./execution-stream", () => ({ subscribeToExecution: vi.fn() }));

import { subscribeToExecution, type ExecutionStreamEvent } from "./execution-stream";

type Call = { url: string; init?: RequestInit; headers: Headers };

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
	return new Response(JSON.stringify(body), {
		status: 200,
		headers: { "content-type": "application/json" },
		...init,
	});
}

function recordingFetch(respond: (url: string, init?: RequestInit) => Response) {
	const calls: Call[] = [];
	const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = String(input);
		calls.push({ url, init, headers: new Headers(init?.headers) });
		return respond(url, init);
	}) as unknown as typeof fetch;
	return { fetchImpl, calls };
}

type PlatformAuthGlobal = typeof globalThis & {
	__BIFROST_PLATFORM_AUTH_V1__?: unknown;
};

beforeEach(() => {
	(subscribeToExecution as Mock).mockReset();
});

afterEach(() => {
	delete (globalThis as PlatformAuthGlobal).__BIFROST_PLATFORM_AUTH_V1__;
});

describe("connect", () => {
	it("sends the bearer token and identity headers under the base URL", async () => {
		const { fetchImpl, calls } = recordingFetch(() => jsonResponse({}));
		const client = connect({
			baseUrl: "https://bifrost.example/",
			token: "tok-1",
			orgScope: "org-1",
			appId: "app-1",
			fetchImpl,
		});

		await client.fetch("/api/anything");
		client.disconnect();

		expect(calls[0].url).toBe("https://bifrost.example/api/anything");
		expect(calls[0].headers.get("Authorization")).toBe("Bearer tok-1");
		expect(calls[0].headers.get("X-Bifrost-Org")).toBe("org-1");
		expect(calls[0].headers.get("X-Bifrost-App")).toBe("app-1");
	});

	it("installs the tables and files transport until disconnect", () => {
		const before = getBifrostTransport();
		const client = connect({ baseUrl: "https://bifrost.example", token: "tok-1", appId: "app-1" });

		expect(getBifrostTransport()).toMatchObject({
			baseUrl: "https://bifrost.example",
			headers: { "X-Bifrost-App": "app-1" },
		});

		client.disconnect();
		client.disconnect();
		expect(getBifrostTransport()).toBe(before);
	});

	it("uses the host's rotated token and replays after a refresh-required challenge", async () => {
		let hostToken = "stale";
		(globalThis as PlatformAuthGlobal).__BIFROST_PLATFORM_AUTH_V1__ = {
			getAccessToken: () => hostToken,
			canRefreshAccessToken: () => true,
			refreshAccessToken: async () => {
				hostToken = "fresh";
				return true;
			},
			handleAuthenticationFailure: vi.fn(),
		};
		const { fetchImpl, calls } = recordingFetch((_url, init) =>
			new Headers(init?.headers).get("Authorization") === "Bearer fresh"
				? jsonResponse({ ok: true })
				: jsonResponse(
						{},
						{
							status: 401,
							headers: {
								"WWW-Authenticate":
									'Bearer error="invalid_token", error_description="refresh_required"',
							},
						},
					),
		);
		const client = connect({ baseUrl: "https://bifrost.example", token: "bootstrap", fetchImpl });

		const response = await client.fetch("/api/anything");
		client.disconnect();

		expect(response.status).toBe(200);
		expect(calls.map((call) => call.headers.get("Authorization"))).toEqual([
			"Bearer stale",
			"Bearer fresh",
		]);
	});
});

describe("currentUser", () => {
	it("reads the signed-in user", async () => {
		const { fetchImpl, calls } = recordingFetch(() =>
			jsonResponse({
				id: "u-1",
				email: "ada@example.com",
				name: "Ada",
				is_superuser: false,
				organization_id: null,
				roles: ["Viewer"],
			}),
		);
		const client = connect({ baseUrl: "https://bifrost.example", token: "tok-1", fetchImpl });

		const user = await client.currentUser();
		client.disconnect();

		expect(calls[0].url).toBe("https://bifrost.example/api/auth/me");
		expect(user).toEqual({
			id: "u-1",
			email: "ada@example.com",
			name: "Ada",
			roles: ["Viewer"],
			organizationId: "",
			isPlatformAdmin: false,
		});
	});

	it("rejects when the user cannot be read", async () => {
		const { fetchImpl } = recordingFetch(() => jsonResponse({}, { status: 403 }));
		const client = connect({ baseUrl: "https://bifrost.example", token: "tok-1", fetchImpl });

		await expect(client.currentUser()).rejects.toThrow("Could not read the signed-in user (403)");
		client.disconnect();
	});
});

describe("runWorkflow", () => {
	it("starts the run in this App's install and follows the stream to its result", async () => {
		let emit: (event: ExecutionStreamEvent) => void = () => {};
		const unsubscribe = vi.fn();
		(subscribeToExecution as Mock).mockImplementation((_id, onEvent) => {
			emit = onEvent;
			return unsubscribe;
		});
		let isFinished = false;
		const { fetchImpl, calls } = recordingFetch((url) => {
			if (url.endsWith("/api/workflows/execute")) {
				return jsonResponse({ execution_id: "e-1", status: "Pending" });
			}
			return jsonResponse(
				isFinished ? { status: "Success", result: { count: 2 } } : { status: "Running" },
			);
		});
		const client = connect({
			baseUrl: "https://bifrost.example",
			token: "tok-1",
			orgScope: "org-1",
			appId: "app-1",
			fetchImpl,
		});
		const statuses: string[] = [];
		const logs: string[] = [];

		const run = client.runWorkflow("workflows/count.py::count", { n: 2 }, {
			onStatus: (status) => statuses.push(status),
			onLog: (log) => logs.push(log.message),
		});
		await vi.waitFor(() => expect(subscribeToExecution).toHaveBeenCalled());
		emit({ type: "log", log: { level: "INFO", message: "counting", timestamp: "t" } });
		isFinished = true;
		emit({ type: "status", status: "Success", isTerminal: true });

		await expect(run).resolves.toEqual({ count: 2 });
		client.disconnect();
		expect(JSON.parse(String(calls[0].init?.body))).toEqual({
			workflow_id: "workflows/count.py::count",
			input_data: { n: 2 },
			app_id: "app-1",
			org_id: "org-1",
		});
		expect(logs).toEqual(["counting"]);
		expect(statuses).toContain("Success");
		expect(unsubscribe).toHaveBeenCalled();
	});

	it("stops following the run when aborted", async () => {
		const unsubscribe = vi.fn();
		(subscribeToExecution as Mock).mockReturnValue(unsubscribe);
		const { fetchImpl } = recordingFetch((url) =>
			url.endsWith("/api/workflows/execute")
				? jsonResponse({ execution_id: "e-2", status: "Pending" })
				: jsonResponse({ status: "Running" }),
		);
		const client = connect({ baseUrl: "https://bifrost.example", token: "tok-1", fetchImpl });
		const controller = new AbortController();

		const run = client.runWorkflow("wf", {}, { signal: controller.signal });
		await vi.waitFor(() => expect(subscribeToExecution).toHaveBeenCalled());
		controller.abort();

		await expect(run).rejects.toMatchObject({ name: "AbortError" });
		client.disconnect();
		expect(unsubscribe).toHaveBeenCalled();
	});
});
