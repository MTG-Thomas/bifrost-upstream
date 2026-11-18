// Production-mode harness that renders the React and Foldkit logs panels
// with identical inputs. See ../../foldkit-execution-logs.md#reproducing.
import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const here = import.meta.dirname;

export default defineConfig({
	root: here,
	plugins: [react(), tailwindcss()],
	resolve: {
		alias: { "@": path.resolve(here, "../../../../client/src") },
	},
	build: { outDir: path.resolve(here, "bench-dist"), emptyOutDir: true },
});
