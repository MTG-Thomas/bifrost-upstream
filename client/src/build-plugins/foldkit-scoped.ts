import path from "node:path";
import { foldkit, type FoldkitPluginOptions } from "@foldkit/vite-plugin";
import type { Plugin } from "vite";

/**
 * `@foldkit/vite-plugin`, confined to the Foldkit part of a React app: the
 * TypeScript modules under `directory`. JSX/TSX files there are React host
 * adapters, so they stay with React Fast Refresh and are never branded.
 *
 * The upstream plugin assumes it owns the whole application, which breaks a
 * React host in three ways. This wrapper scopes each one to `directory` and
 * leaves the rest of the plugin (dependency resolution, build identity, the
 * DevTools MCP relay) unchanged:
 *
 * - View identity: upstream brands the return value of every function in
 *   every application module, React components included. Here only modules
 *   under `directory` are branded.
 * - Live reload: upstream answers every hot update with a full page reload,
 *   which disables React Fast Refresh everywhere. Here only edits under
 *   `directory` reload (preserving the Foldkit Model); every other edit falls
 *   through to Vite's normal HMR.
 * - DevTools overlay: upstream injects the overlay into the page's HTML, so
 *   every page load pulls in `@foldkit/devtools`. Here nothing is injected;
 *   a Foldkit program imports `virtual:foldkit-devtools-overlay` itself, only
 *   when its DevTools are enabled.
 */
export function foldkitScoped(
	options: FoldkitPluginOptions & { directory: string },
): Plugin[] {
	const { directory, ...foldkitOptions } = options;
	const root = path.resolve(directory) + path.sep;
	const isScoped = (id: string) => {
		const file = path.resolve(id.split("?")[0] ?? id);
		return file.startsWith(root) && !/\.[jt]sx$/.test(file);
	};

	return [foldkit(foldkitOptions)].flat().map((plugin): Plugin => {
		switch (plugin.name) {
			case "foldkit:view-identity":
				return scopeTransform(plugin, isScoped);
			case "foldkit":
				return scopeHotUpdate(plugin, isScoped);
			case "foldkit:devtools-overlay": {
				const { transformIndexHtml: _injected, ...onDemand } = plugin;
				return onDemand;
			}
			default:
				return plugin;
		}
	});
}

type TransformHook = Exclude<
	NonNullable<Plugin["transform"]>,
	{ handler: unknown }
>;
type HotUpdateHook = Exclude<
	NonNullable<Plugin["handleHotUpdate"]>,
	{ handler: unknown }
>;

const scopeTransform = (
	plugin: Plugin,
	isScoped: (id: string) => boolean,
): Plugin => {
	const transform = plugin.transform;
	if (typeof transform !== "function") {
		throw new Error(`${plugin.name}: expected a function transform hook`);
	}
	const scoped: TransformHook = function (code, id, transformOptions) {
		return isScoped(id)
			? transform.call(this, code, id, transformOptions)
			: null;
	};
	return { ...plugin, transform: scoped };
};

const scopeHotUpdate = (
	plugin: Plugin,
	isScoped: (id: string) => boolean,
): Plugin => {
	const handleHotUpdate = plugin.handleHotUpdate;
	if (typeof handleHotUpdate !== "function") {
		throw new Error(`${plugin.name}: expected a function hot-update hook`);
	}
	const scoped: HotUpdateHook = function (context) {
		return isScoped(context.file)
			? handleHotUpdate.call(this, context)
			: undefined;
	};
	return { ...plugin, handleHotUpdate: scoped };
};
