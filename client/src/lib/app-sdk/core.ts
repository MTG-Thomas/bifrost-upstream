/**
 * `bifrost/core` — the framework-neutral App client. **Experimental.**
 *
 * For V2 Apps that do not use React (for example Foldkit Apps). It reuses the
 * same modules as the React SDK: the authenticated fetch, `tables`, `files`,
 * and the workflow run with its execution stream. It adds no new wire surface.
 *
 * Unlike the default `bifrost` entry, this module is compiled into the App
 * rather than loaded from the instance, so an SDK update reaches an App that
 * uses it on the App's next build.
 *
 * `connect()` installs the module-wide transport that `tables` and `files`
 * use, as `<BifrostProvider>` does for React Apps. Call it once per mount and
 * `disconnect()` on teardown.
 */
import { createAuthenticatedFetch } from "./authenticated-fetch";
import { setDefaultFileScope } from "./files";
import { setBifrostTransport, setDefaultAppScope } from "./tables";
import { getPlatformAuth } from "./transport";
import { runWorkflow, type WorkflowRunCallbacks } from "./workflow-run";

export { tables, TableAccessDeniedError, TableNotFoundError } from "./tables";
export type { TableChangeEvent } from "./tables";
export {
	files,
	FileAccessDeniedError,
	FileNotFoundError,
	FilePolicyError,
} from "./files";
export type {
	FileListMetadataItem,
	FileListOptions,
	FileListResult,
	FileMode,
	FileOptions,
	SignedUrlMethod,
	SignedUrlOptions,
	SignedUrlResult,
} from "./files";
export { TERMINAL_STATUSES } from "./workflow-run";
export type { WorkflowLogEntry, WorkflowRunCallbacks } from "./workflow-run";

/** The values the platform passes to an App's `mount(el, bootstrap)`. */
export interface BifrostConnection {
	baseUrl: string;
	token: string;
	orgScope?: string | null;
	appId?: string | null;
	/** Override `fetch` (tests / non-browser). Defaults to global `fetch`. */
	fetchImpl?: typeof fetch;
}

/** The signed-in user. Display only: workflows and table policies enforce access. */
export interface BifrostCurrentUser {
	id: string;
	email: string;
	name: string;
	roles: string[];
	/** The user's organization id, or "" for a user with no organization. */
	organizationId: string;
	isPlatformAdmin: boolean;
}

export interface BifrostClient {
	/** `fetch` with the App's base URL, current token, and identity headers. */
	readonly fetch: typeof fetch;
	currentUser(options?: { signal?: AbortSignal }): Promise<BifrostCurrentUser>;
	/** Start a workflow and resolve with its result, or reject with its error. */
	runWorkflow<T = unknown>(
		workflowRef: string,
		input?: Record<string, unknown>,
		callbacks?: WorkflowRunCallbacks,
	): Promise<T>;
	/** Restore the transport that was installed before `connect()`. Idempotent. */
	disconnect(): void;
}

interface MeResponse {
	id: string;
	email: string;
	name: string;
	is_superuser: boolean;
	organization_id: string | null;
	roles: string[];
}

/** Connect to the Bifrost API with the App's mount bootstrap. */
export function connect({
	baseUrl,
	token,
	orgScope = null,
	appId = null,
	fetchImpl,
}: BifrostConnection): BifrostClient {
	const normalizedBaseUrl = baseUrl.replace(/\/$/, "");
	const getToken = () => {
		const auth = getPlatformAuth();
		return auth ? auth.getAccessToken() : token;
	};
	const authedFetch = createAuthenticatedFetch({
		baseUrl: normalizedBaseUrl,
		getToken,
		orgScope,
		appId,
		fetchImpl,
	});

	const restoreTransport = setBifrostTransport({
		baseUrl: normalizedBaseUrl,
		token,
		getToken: () => getToken() ?? undefined,
		fetchImpl: authedFetch,
		headers: appId ? { "X-Bifrost-App": appId } : {},
	});
	const restoreScope = setDefaultAppScope(orgScope);
	const restoreFileScope = setDefaultFileScope(orgScope);
	let connected = true;

	return {
		fetch: authedFetch,
		async currentUser({ signal } = {}) {
			const response = await authedFetch("/api/auth/me", signal ? { signal } : undefined);
			if (!response.ok) {
				throw new Error(`Could not read the signed-in user (${response.status})`);
			}
			const me = (await response.json()) as MeResponse;
			return {
				id: me.id,
				email: me.email,
				name: me.name,
				roles: me.roles,
				organizationId: me.organization_id ?? "",
				isPlatformAdmin: me.is_superuser,
			};
		},
		runWorkflow: <T = unknown>(
			workflowRef: string,
			input: Record<string, unknown> = {},
			callbacks: WorkflowRunCallbacks = {},
		) => runWorkflow<T>(authedFetch, { workflowRef, input, appId, orgScope }, callbacks),
		disconnect() {
			if (!connected) return;
			connected = false;
			restoreFileScope();
			restoreScope();
			restoreTransport();
		},
	};
}
