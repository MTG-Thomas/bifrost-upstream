import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import * as yaml from "js-yaml";

const decideCapturesScriptPath = join(import.meta.dirname, "decide-captures.mjs");

function temporaryDirectory(t, prefix) {
  const root = mkdtempSync(join(tmpdir(), prefix));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

function git(repo, args) {
  execFileSync("git", args, { cwd: repo, stdio: "ignore" });
}

function initRepo(repo) {
  mkdirSync(repo, { recursive: true });
  git(repo, ["init", "--quiet"]);
  git(repo, ["config", "user.email", "docs-test@gobifrost.com"]);
  git(repo, ["config", "user.name", "Docs Test"]);
}

function commitFiles(repo, files, message) {
  git(repo, ["add", "--", ...files]);
  git(repo, ["commit", "--quiet", "-m", message]);
  return execFileSync("git", ["rev-parse", "HEAD"], { cwd: repo, encoding: "utf8" }).trim();
}

function writeManifest(docsRepo, entries) {
  mkdirSync(docsRepo, { recursive: true });
  writeFileSync(join(docsRepo, "screenshots.yaml"), yaml.dump({ entries }));
}

function selectCaptures(docsRepo, bifrostRepo) {
  return execFileSync(
    process.execPath,
    [decideCapturesScriptPath, "--docs-repo", docsRepo, "--bifrost-repo", bifrostRepo],
    { encoding: "utf8" },
  );
}

test("a manifest SHA cannot run a shell command", (t) => {
  const root = temporaryDirectory(t, "bifrost-docs-decide-sha-");
  const docsRepo = join(root, "docs");
  const bifrostRepo = join(root, "bifrost");
  const marker = join(bifrostRepo, "sha-injected");
  initRepo(bifrostRepo);
  writeFileSync(join(bifrostRepo, "marker"), "capture source\n");
  commitFiles(bifrostRepo, ["marker"], "capture source");
  writeManifest(docsRepo, [
    {
      id: "example",
      captured_at: { bifrost_sha: "HEAD;touch sha-injected" },
      source_globs: ["marker"],
    },
  ]);

  const output = selectCaptures(docsRepo, bifrostRepo);

  assert.equal(existsSync(marker), false);
  assert.equal(output, "example");
});

test("a manifest pathspec cannot run a shell command", (t) => {
  const root = temporaryDirectory(t, "bifrost-docs-decide-glob-");
  const docsRepo = join(root, "docs");
  const bifrostRepo = join(root, "bifrost");
  const marker = join(bifrostRepo, "glob-injected");
  initRepo(bifrostRepo);
  writeFileSync(join(bifrostRepo, "marker"), "capture source\n");
  const sha = commitFiles(bifrostRepo, ["marker"], "capture source");
  writeManifest(docsRepo, [
    {
      id: "example",
      captured_at: { bifrost_sha: sha },
      source_globs: ["marker';touch glob-injected;#"],
    },
  ]);

  const output = selectCaptures(docsRepo, bifrostRepo);

  assert.equal(existsSync(marker), false);
  assert.equal(output, "");
});

test("a changed source path is selected for recapture", (t) => {
  const root = temporaryDirectory(t, "bifrost-docs-decide-changed-");
  const docsRepo = join(root, "docs");
  const bifrostRepo = join(root, "bifrost");
  initRepo(bifrostRepo);
  writeFileSync(join(bifrostRepo, "marker"), "capture source\n");
  const sha = commitFiles(bifrostRepo, ["marker"], "capture source");
  writeFileSync(join(bifrostRepo, "marker"), "changed\n");
  commitFiles(bifrostRepo, ["marker"], "change source");
  writeManifest(docsRepo, [
    {
      id: "example",
      captured_at: { bifrost_sha: sha },
      source_globs: ["marker"],
    },
  ]);

  assert.equal(selectCaptures(docsRepo, bifrostRepo), "example");
});

test("an unchanged path containing a quote is not selected", (t) => {
  const root = temporaryDirectory(t, "bifrost-docs-decide-quote-");
  const docsRepo = join(root, "docs");
  const bifrostRepo = join(root, "bifrost");
  const filename = "quote'name.txt";
  initRepo(bifrostRepo);
  writeFileSync(join(bifrostRepo, filename), "capture source\n");
  const sha = commitFiles(bifrostRepo, [filename], "capture source");
  writeManifest(docsRepo, [
    {
      id: "example",
      captured_at: { bifrost_sha: sha },
      source_globs: [filename],
    },
  ]);

  assert.equal(selectCaptures(docsRepo, bifrostRepo), "");
});

test("missing or unknown capture history is selected for recapture", (t) => {
  const root = temporaryDirectory(t, "bifrost-docs-decide-history-");
  const docsRepo = join(root, "docs");
  const bifrostRepo = join(root, "bifrost");
  initRepo(bifrostRepo);
  writeFileSync(join(bifrostRepo, "marker"), "capture source\n");
  const sha = commitFiles(bifrostRepo, ["marker"], "capture source");
  writeManifest(docsRepo, [
    { id: "no-sha", source_globs: ["marker"] },
    { id: "empty-globs", captured_at: { bifrost_sha: sha }, source_globs: [] },
    {
      id: "unknown-sha",
      captured_at: { bifrost_sha: "0".repeat(40) },
      source_globs: ["marker"],
    },
  ]);

  assert.equal(selectCaptures(docsRepo, bifrostRepo), "no-sha,empty-globs,unknown-sha");
});
