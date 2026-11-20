// Assemble REST-uploadable Workers from a trusted, already qualified runtime.
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { isDeepStrictEqual } from "node:util";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const spike = resolve(root, "spikes/cloudflare-pyodide");
const require = createRequire(new URL("../../../api/src/services/app_bundler/package.json", import.meta.url));
const runtimePin = "fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5";
const uploadLimit = 64 * 1024 * 1024;
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const durableObject = (name, class_name, script) => ({
  name, type: "durable_object_namespace", class_name, ...(script ? { script } : {}),
});
const input = (name, source, type = "plain_text") => ({ name, type, source });

const workerDefinitions = {
  launcher: {
    entry: "dev/tenant-workers/launcher-entry.mjs",
    exports: ["CloudflareLauncher", "TenantHost"],
    bindings: [
      { name: "LOADER", type: "worker_loader" },
      input("API_ORIGIN", "worker_ingress_origin"),
      { name: "BIFROST_CLOUDFLARE_MANAGED", type: "plain_text", text: "true" },
      { name: "SDK_SCOPE", type: "plain_text", text: "sdk" },
      { name: "RUNTIME_SHA256", type: "plain_text", text: runtimePin },
    ],
    cpu_ms: 30_000,
    public_endpoint: false,
  },
  capacity: {
    entry: "dev/cloudflare-launcher/capacity.mjs",
    exports: ["ScopeRegistry", "WorkerLedger"],
    bindings: [
      durableObject("SCOPE_REGISTRY", "ScopeRegistry"),
      durableObject("WORKER_LEDGER", "WorkerLedger"),
      durableObject("CLOUDFLARE_LAUNCHER", "CloudflareLauncher", "launcher"),
      input("CAPACITY_TOKEN", "scope_bearer", "secret_text"),
      input("MAX_WORKERS", "launcher_cap"),
      input("MAX_LIFETIME_SECONDS", "max_lifetime_seconds"),
      input("WORKFLOW_SLOTS", "slots_per_launcher"),
      input("WORKER_VERSION", "worker_version"),
      { name: "SCOPE", type: "plain_text", text: "workers" },
    ],
    cpu_ms: 30_000,
    public_endpoint: true,
  },
};

export async function buildWorkers(runtimePath, outputPath, sourceRevision, meterBounds) {
  if (!/^[0-9a-f]{40}$/.test(sourceRevision)) throw new Error("An exact source commit is required");
  const runtime = await readFile(runtimePath);
  if (sha256(runtime) !== runtimePin) throw new Error("The qualified Workers runtime digest does not match");
  const pinnedBounds = JSON.parse(await readFile(
    resolve(root, "api/src/services/worker_pool/cloudflare_meter_bounds.json"), "utf8",
  ));
  if (!isDeepStrictEqual(meterBounds, pinnedBounds)) {
    throw new Error("Supply the pinned Workers meter bounds from cloudflare_meter_bounds.json");
  }
  const { build } = require("esbuild");
  const output = resolve(outputPath);
  const release = {
    manifest_version: 1,
    source_revision: sourceRevision,
    runtime_sha256: runtimePin,
    pyodide_version: "314.0.6",
    compatibility_date: "2026-10-30",
    compatibility_flags: [],
    capacity_protocol: 2,
    credential_protocol: 2,
    runtime_profile: "cloudflare-python-v1",
    capabilities: [["workflow.execute", 1]],
    max_slots_per_launcher: 4,
    meter_bounds: pinnedBounds,
    workers: {},
  };
  const artifacts = [];
  for (const [role, definition] of Object.entries(workerDefinitions)) {
    const { entry, exports, ...metadata } = definition;
    const filename = role + ".mjs";
    const result = await build({
      absWorkingDir: root, entryPoints: [resolve(spike, entry)],
      bundle: true, format: "esm", platform: "neutral", write: false,
      outfile: filename, minify: true, legalComments: "inline", external: ["cloudflare:workers"],
      plugins: [{
        name: "qualified-workers-runtime",
        setup(builder) {
          builder.onResolve({ filter: /^\.\.\/runtime\/runtime-code\.mjs$/ }, () => ({
            path: resolve(spike, "backend/runtime-code.mjs"),
          }));
          builder.onResolve({ filter: /^\.\/generated-runtime\.mjs$/ }, ({ importer }) => {
            if (dirname(importer) === resolve(spike, "backend")) {
              return { path: "generated-runtime.mjs", namespace: "bifrost-runtime" };
            }
          });
          builder.onLoad({ filter: /.*/, namespace: "bifrost-runtime" }, () => ({
            contents: runtime, loader: "js",
          }));
        },
      }],
    });
    const bytes = result.outputFiles[0].contents;
    const compressedBytes = gzipSync(bytes, { level: 9 }).byteLength;
    if (bytes.byteLength > uploadLimit) throw new Error(role + " exceeds the Workers upload limit");
    artifacts.push([filename, bytes]);
    release.workers[role] = {
      main_module: filename,
      modules: [{
        name: filename, sha256: sha256(bytes), bytes: bytes.byteLength,
        gzip_bytes: compressedBytes, mime_type: "application/javascript+module",
      }],
      exports: Object.fromEntries(exports.map((name) => [name, { type: "durable-object", storage: "sqlite" }])),
      ...metadata,
      preview_urls: false,
    };
  }
  await mkdir(output);
  for (const [name, bytes] of artifacts) await writeFile(resolve(output, name), bytes, { flag: "wx" });
  await writeFile(resolve(output, "manifest.json"), JSON.stringify(release, null, 2) + "\n", { flag: "wx" });
  return release;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [runtime, output, revision, ...extra] = process.argv.slice(2);
  if (!runtime || !output || !revision || extra.length) {
    throw new Error("Usage: node build-workers.mjs <qualified-runtime.mjs> <new-output-directory> <source-sha>");
  }
  const bounds = process.env.BIFROST_RELEASE_WORKERS_METER_BOUNDS;
  await buildWorkers(runtime, output, revision, bounds ? JSON.parse(bounds) : undefined);
}
