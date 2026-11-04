/**
 * Pure server-URL helpers for the hosted-only desktop shell.
 *
 * No Electron imports here so `node --test` can cover this module directly.
 * The shell loads the configured hosted Bifrost server with `loadURL`, which
 * keeps `/api`, `/ws`, cookies, and Web Locks same-origin with zero client
 * changes — so the only URL policy lives here.
 */

/**
 * Normalize a user-entered server URL to its origin, or return null.
 * `https://` only, except `http://localhost` for local dev.
 */
export function validateServerUrl(raw: string): string | null {
	let parsed: URL;
	try {
		parsed = new URL(raw.trim());
	} catch {
		return null;
	}
	if (parsed.protocol === "https:") return parsed.origin;
	if (parsed.protocol === "http:" && parsed.hostname === "localhost")
		return parsed.origin;
	return null;
}

/** Stable session partition per server origin — no cookie leaks on switch. */
export function partitionFor(origin: string): string {
	return `persist:bifrost-${origin.replace(/[^a-zA-Z0-9]/g, "-")}`;
}
