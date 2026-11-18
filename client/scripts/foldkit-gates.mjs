// Bundle gates for the Foldkit evaluation. Run after `vite build`:
//
//   node scripts/foldkit-gates.mjs dist
//
// Fails when Effect or Foldkit leaks into a chunk that users download without
// opting in, or when a Foldkit chunk outgrows its budget. Thresholds and the
// reasoning behind them live in docs/spikes/foldkit-adoption-gates.md.
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

const BUDGETS = {
	/** Gzip bytes for the lazily loaded Foldkit logs panel chunk. */
	foldkitChunkGzip: 115_000,
	/** Gzip bytes for the entry chunk every user downloads. */
	entryChunkGzip: 113_000,
};

/** String literals that survive minification and only exist in Effect's
 *  Schema/runtime or Foldkit's runtime. */
const LEAK_MARKERS = [
	"~effect/",
	"foldkit-dialog-initial-focus",
	"foldkitMount",
];

/** Chunks that may contain Effect/Foldkit: the lazy Foldkit chunks only. */
const isFoldkitChunk = (file) => /^Foldkit[A-Za-z]*-/.test(file);

const dist = process.argv[2] ?? "dist";
const assets = path.join(dist, "assets");
const indexHtml = fs.readFileSync(path.join(dist, "index.html"), "utf8");
const entryFile = indexHtml.match(/src="\/assets\/(index-[^"]+\.js)"/)?.[1];
if (!entryFile) throw new Error("Entry chunk not found in index.html");

const gzipSize = (file) =>
	zlib.gzipSync(fs.readFileSync(path.join(assets, file)), { level: 9 })
		.length;

const failures = [];
const report = [];

const jsFiles = fs.readdirSync(assets).filter((file) => file.endsWith(".js"));
for (const file of jsFiles) {
	if (isFoldkitChunk(file)) continue;
	const source = fs.readFileSync(path.join(assets, file), "utf8");
	const leaked = LEAK_MARKERS.filter((marker) => source.includes(marker));
	if (leaked.length > 0) {
		failures.push(
			`${file} contains Effect/Foldkit code (${leaked.join(", ")})`,
		);
	}
}
report.push(
	`non-Foldkit chunks checked for leaks: ${jsFiles.filter((file) => !isFoldkitChunk(file)).length}`,
);

const entryGzip = gzipSize(entryFile);
report.push(
	`entry ${entryFile}: ${entryGzip} B gzip (budget ${BUDGETS.entryChunkGzip})`,
);
if (entryGzip > BUDGETS.entryChunkGzip)
	failures.push(`entry chunk over budget`);

const foldkitChunks = jsFiles.filter(isFoldkitChunk);
if (foldkitChunks.length === 0) failures.push("no Foldkit chunk found");
for (const file of foldkitChunks) {
	const size = gzipSize(file);
	report.push(`${file}: ${size} B gzip (budget ${BUDGETS.foldkitChunkGzip})`);
	if (size > BUDGETS.foldkitChunkGzip) failures.push(`${file} over budget`);
}

console.log(report.join("\n"));
if (failures.length > 0) {
	console.error(`\nFoldkit bundle gates failed:\n- ${failures.join("\n- ")}`);
	process.exit(1);
}
console.log("\nFoldkit bundle gates passed");
