import { Runtime } from "foldkit";
import { Option } from "effect";

import { makeRunner } from "./app";
import "./index.css";

export interface BifrostAppBootstrap {
	basename: string;
	baseUrl: string;
	token: string;
	orgScope: string | null;
	appId: string | null;
	/** Owning Solution supplied by the platform host. */
	solutionId?: string | null;
	onLogout: () => void;
	theme: "light" | "dark";
}

interface BifrostAppModule {
	mount: (mountEl: HTMLElement, bootstrap: BifrostAppBootstrap) => () => void;
}

declare global {
	interface Window {
		__BIFROST_APP_MODULES__?: Map<string, BifrostAppModule>;
	}
}

/** The mount-v1 lifecycle with Foldkit in place of React: the host hands
 *  over a DOM node and the bootstrap, and gets back a teardown. */
export function mount(mountEl: HTMLElement, bootstrap: BifrostAppBootstrap) {
	const container = document.createElement("div");
	container.id = "foldkit-runner";
	mountEl.appendChild(container);
	const handle = Runtime.embed(
		makeRunner(container, {
			baseUrl: bootstrap.baseUrl,
			token: bootstrap.token,
			maybeOrgScope: Option.fromNullishOr(bootstrap.orgScope),
			maybeAppId: Option.fromNullishOr(bootstrap.appId),
		}),
	);
	return () => {
		void handle.disposeAsync().then(() => container.remove());
	};
}

// Register the reusable factory under this immutable entry's canonical URL.
(window.__BIFROST_APP_MODULES__ ??= new Map()).set(import.meta.url, { mount });

// Vite dev serves this entry directly; production mounting is owned by the host.
if (import.meta.env.DEV) {
	const mountEl = document.getElementById("root");
	if (!mountEl) throw new Error("Missing #root mount element");
	mount(mountEl, {
		basename: "/",
		baseUrl: import.meta.env.VITE_BIFROST_API_URL ?? window.location.origin,
		token: import.meta.env.VITE_BIFROST_TOKEN ?? "",
		orgScope: import.meta.env.VITE_BIFROST_ORG_ID ?? null,
		appId: import.meta.env.VITE_BIFROST_APP_ID ?? null,
		onLogout: () => window.location.assign("/login"),
		theme: document.documentElement.classList.contains("dark") ? "dark" : "light",
	});
}
