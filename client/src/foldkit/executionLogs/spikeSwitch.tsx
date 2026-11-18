import { lazy, useState } from "react";

/**
 * Evaluation-only switch for the Foldkit execution-logs spike. React stays
 * the default; the Foldkit panel renders only when a developer opts in with
 * `?logsRenderer=foldkit` (sticky for the tab) or the localStorage key below.
 * This module must not import Foldkit or Effect statically: the lazy import
 * keeps both out of the bundle users download by default.
 */
export const LOGS_RENDERER_STORAGE_KEY = "bifrost.spike.logsRenderer";

export function readFoldkitLogsOptIn(search: string): boolean {
	if (new URLSearchParams(search).get("logsRenderer") === "foldkit") {
		return true;
	}
	try {
		return (
			window.localStorage.getItem(LOGS_RENDERER_STORAGE_KEY) === "foldkit"
		);
	} catch {
		return false;
	}
}

export function useFoldkitLogsOptIn(): boolean {
	const [isOptedIn] = useState(() =>
		readFoldkitLogsOptIn(window.location.search),
	);
	return isOptedIn;
}

export const LazyFoldkitExecutionLogsPanel = lazy(() =>
	import("./FoldkitExecutionLogsPanel").then((module) => ({
		default: module.FoldkitExecutionLogsPanel,
	})),
);
