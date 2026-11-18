// Production build of the parity gallery. See ../../foldkit-execution-logs.md.
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
  build: { outDir: path.resolve(here, "parity-dist"), emptyOutDir: true },
});
