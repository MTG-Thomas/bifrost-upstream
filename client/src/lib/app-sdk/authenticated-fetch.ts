/**
 * The authenticated `fetch` every V2 App request goes through.
 *
 * `<BifrostProvider>` and the framework-neutral `bifrost/core` client both
 * build their fetch here, so token rotation, the refresh-required replay
 * budget, and the org/App identity headers have one implementation.
 */
import {
	getPlatformAuth,
	isRefreshRequiredChallenge,
	REQUEST_REPLAY_BUDGET,
} from "./transport";

export interface AuthenticatedFetchOptions {
	/** Absolute base URL of the Bifrost API. */
	baseUrl: string;
	/** Read the current access token at request time. */
	getToken: () => string | null;
	/** Active organization scope, sent as `X-Bifrost-Org`. */
	orgScope?: string | null;
	/** This App's id, sent as `X-Bifrost-App` so refs resolve to its install. */
	appId?: string | null;
	/** Override `fetch` (tests / non-browser). Defaults to global `fetch`. */
	fetchImpl?: typeof fetch;
}

/** Join a root-relative API path to `baseUrl`; absolute URLs and Requests pass through. */
export function joinUrl(baseUrl: string, input: RequestInfo | URL): RequestInfo | URL {
	if (typeof input !== "string") return input;
	if (/^https?:\/\//i.test(input)) return input;
	const base = baseUrl.replace(/\/$/, "");
	const path = input.startsWith("/") ? input : `/${input}`;
	return `${base}${path}`;
}

/**
 * Build a `fetch` that joins `baseUrl`, sends the current bearer token and
 * identity headers, and replays only after the server's refresh-required
 * challenge, using the host's single-flight refresh.
 */
export function createAuthenticatedFetch({
	baseUrl,
	getToken,
	orgScope,
	appId,
	fetchImpl,
}: AuthenticatedFetchOptions): typeof fetch {
	const baseFetch = fetchImpl ?? globalThis.fetch;
	return async (input, init) => {
		const callerHeaders = new Headers(init?.headers);
		const managesAuthorization = !callerHeaders.has("Authorization");
		const requestWithToken = (accessToken: string | null) => {
			const headers = new Headers(callerHeaders);
			if (managesAuthorization && accessToken) {
				headers.set("Authorization", `Bearer ${accessToken}`);
			}
			if (orgScope && !headers.has("X-Bifrost-Org")) {
				headers.set("X-Bifrost-Org", orgScope);
			}
			// Same context signal the tables/files transport sends: the server's
			// auth layer resolves it to the install scope (ctx.solution_id), so a
			// workflow path::fn ref scopes to THIS install without body plumbing.
			if (appId && !headers.has("X-Bifrost-App")) {
				headers.set("X-Bifrost-App", appId);
			}
			return baseFetch(joinUrl(baseUrl, input), { ...init, headers });
		};

		let attemptedToken = getToken();
		let response = await requestWithToken(attemptedToken);
		const auth = getPlatformAuth();
		if (
			!isRefreshRequiredChallenge(response) ||
			!managesAuthorization ||
			!auth?.canRefreshAccessToken()
		) {
			return response;
		}

		let remaining = REQUEST_REPLAY_BUDGET;
		while (remaining > 0 && isRefreshRequiredChallenge(response)) {
			remaining -= 1;
			// The host may already have rotated the token while this request was
			// in flight. Prefer that value before asking it to rotate again.
			let freshToken = auth.getAccessToken();
			if (!freshToken || freshToken === attemptedToken) {
				const refreshed = await auth.refreshAccessToken();
				freshToken = refreshed ? auth.getAccessToken() : null;
			}

			if (!freshToken) {
				auth.handleAuthenticationFailure();
				return response;
			}

			attemptedToken = freshToken;
			response = await requestWithToken(freshToken);
		}
		if (isRefreshRequiredChallenge(response)) auth.handleAuthenticationFailure();
		return response;
	};
}
