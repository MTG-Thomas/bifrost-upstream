#!/usr/bin/env node
/**
 * Floorplan map wrapper for Bifrost.
 *
 * The authored part of the map - what the parts of the platform are, plus a name
 * and one-line description for every file and request - lives in
 * docs/architecture/floorplan/ and is tracked in git. The rendered map
 * (index.html, graph.json) is generated into floorplan/ and stays out of git.
 *
 * The two are separate because the Floorplan plugin owns floorplan/.gitignore and
 * rewrites it to "ignore everything" on every scan or build, so tracked inputs
 * cannot live there. This wrapper copies the tracked inputs into the generated
 * folder, runs the plugin, and copies any metadata changes back.
 *
 * Always use this wrapper rather than running the plugin scripts against
 * floorplan/ directly, otherwise you are editing files that are ignored by git.
 * `build`, `refresh` and `describe` work on floorplan/ when it already exists (so
 * worksheet edits survive) and start from the tracked copy when it does not.
 *
 * Usage:
 *   node scripts/floorplan.mjs load      copy tracked inputs into floorplan/ (forces the tracked copy)
 *   node scripts/floorplan.mjs save      copy metadata changes back into docs/
 *   node scripts/floorplan.mjs build     scan, build the map, save
 *   node scripts/floorplan.mjs refresh   mechanically refresh, save
 *   node scripts/floorplan.mjs describe  write worksheets and skeletons, save
 *   node scripts/floorplan.mjs check     validate the tracked copy and fail on drift (CI)
 *   node scripts/floorplan.mjs open      serve the built map and print its URL
 *
 * Environment:
 *   FLOORPLAN_SKILL_DIR       use an existing Floorplan plugin folder (skips the clone)
 *   FLOORPLAN_PLUGIN_REF      plugin commit to use (default: the pinned commit below)
 *   FLOORPLAN_MAX_UNASSIGNED_PCT  fail check when more than this share of files has no home (default 10)
 *   FLOORPLAN_STRICT_REVIEW   set to 1 to make changed exports/routes a check failure
 */

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOURCE = path.join(ROOT, 'docs', 'architecture', 'floorplan');
const MAP = path.join(ROOT, 'floorplan');
const SKILL_CACHE = path.join(ROOT, '.cache', 'floorplan-skill');

// Pinned on purpose: CI runs this third-party code, so a moving branch would be a
// supply-chain hole. Bump this commit deliberately after reviewing the plugin diff.
const PLUGIN_REPO =
  process.env.FLOORPLAN_PLUGIN_REPO || 'https://github.com/haydenschmitty/floorplan.git';
const PLUGIN_REF =
  process.env.FLOORPLAN_PLUGIN_REF || 'cec107540da3910c6d7a5fa6400fc9851bf4c15e';

const AUTHORED = ['architecture.json', 'recent-work.json'];
const DESCRIPTION_DIR = 'descriptions';
const MAX_UNASSIGNED_PCT = Number(process.env.FLOORPLAN_MAX_UNASSIGNED_PCT || 10);
const STRICT_REVIEW = process.env.FLOORPLAN_STRICT_REVIEW === '1';

const say = (line = '') => process.stdout.write(`${line}\n`);

function skillDir() {
  if (process.env.FLOORPLAN_SKILL_DIR) {
    const dir = path.resolve(process.env.FLOORPLAN_SKILL_DIR);
    if (!fs.existsSync(path.join(dir, 'scripts', 'build.mjs'))) {
      throw new Error(`FLOORPLAN_SKILL_DIR does not look like the plugin: ${dir}`);
    }
    return dir;
  }
  const stamp = path.join(SKILL_CACHE, '.ref');
  const installed = path.join(SKILL_CACHE, 'skills', 'floorplan');
  if (
    fs.existsSync(path.join(installed, 'scripts', 'build.mjs')) &&
    fs.existsSync(stamp) &&
    fs.readFileSync(stamp, 'utf8').trim() === PLUGIN_REF
  ) {
    return installed;
  }
  fs.rmSync(SKILL_CACHE, { recursive: true, force: true });
  fs.mkdirSync(SKILL_CACHE, { recursive: true });
  say(`Fetching Floorplan ${PLUGIN_REF.slice(0, 12)} into .cache/floorplan-skill ...`);
  execFileSync('git', ['clone', '--quiet', '--no-checkout', PLUGIN_REPO, SKILL_CACHE], {
    stdio: 'inherit',
  });
  execFileSync('git', ['-C', SKILL_CACHE, 'fetch', '--quiet', '--depth', '1', 'origin', PLUGIN_REF], {
    stdio: 'inherit',
  });
  execFileSync('git', ['-C', SKILL_CACHE, 'checkout', '--quiet', 'FETCH_HEAD'], { stdio: 'inherit' });
  fs.writeFileSync(stamp, `${PLUGIN_REF}\n`);
  return installed;
}

function runScript(name, extra = []) {
  const file = path.join(skillDir(), 'scripts', name);
  return execFileSync(process.execPath, [file, ROOT, ...extra], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
  });
}

const copy = (from, to) => {
  fs.mkdirSync(path.dirname(to), { recursive: true });
  fs.copyFileSync(from, to);
};

const descriptionFiles = (dir) =>
  fs.existsSync(dir) ? fs.readdirSync(dir).filter((name) => name.endsWith('.json')) : [];

/** Copy the tracked inputs into the generated map folder. Never deletes there. */
function load() {
  if (!fs.existsSync(path.join(SOURCE, 'architecture.json'))) {
    throw new Error(`No map source found at ${path.relative(ROOT, SOURCE)}`);
  }
  fs.mkdirSync(path.join(MAP, DESCRIPTION_DIR), { recursive: true });
  for (const name of AUTHORED) {
    const from = path.join(SOURCE, name);
    if (fs.existsSync(from)) copy(from, path.join(MAP, name));
  }
  for (const name of descriptionFiles(path.join(SOURCE, DESCRIPTION_DIR))) {
    copy(path.join(SOURCE, DESCRIPTION_DIR, name), path.join(MAP, DESCRIPTION_DIR, name));
  }
}

/** Copy metadata the plugin wrote back into the tracked source. */
function save() {
  fs.mkdirSync(path.join(SOURCE, DESCRIPTION_DIR), { recursive: true });
  for (const name of AUTHORED) {
    const from = path.join(MAP, name);
    if (fs.existsSync(from)) copy(from, path.join(SOURCE, name));
  }
  const live = new Set(descriptionFiles(path.join(MAP, DESCRIPTION_DIR)));
  for (const name of live) {
    copy(path.join(MAP, DESCRIPTION_DIR, name), path.join(SOURCE, DESCRIPTION_DIR, name));
  }
  for (const name of descriptionFiles(path.join(SOURCE, DESCRIPTION_DIR))) {
    if (!live.has(name)) fs.rmSync(path.join(SOURCE, DESCRIPTION_DIR, name));
  }
}

function build() {
  if (!fs.existsSync(path.join(MAP, 'architecture.json'))) load();
  process.stdout.write(runScript('scan.mjs'));
  process.stdout.write(runScript('build.mjs'));
  save();
}

function refresh() {
  if (!fs.existsSync(path.join(MAP, 'graph.json'))) load();
  process.stdout.write(runScript('refresh.mjs'));
  save();
}

function describe() {
  if (!fs.existsSync(path.join(MAP, 'architecture.json'))) load();
  process.stdout.write(runScript('describe.mjs'));
  save();
}

/**
 * CI gate. Re-reads the code the way the plugin does and fails when the tracked
 * map has fallen behind it. Mechanical only - nothing here needs an AI.
 */
function check() {
  load();
  process.stdout.write(runScript('refresh.mjs'));

  let buildOut = '';
  let buildOk = true;
  try {
    buildOut = runScript('build.mjs', ['--check']);
  } catch (error) {
    buildOut = `${error.stdout || ''}${error.stderr || ''}`;
    buildOk = false;
  }
  process.stdout.write(buildOut);

  const graphFile = path.join(MAP, 'graph.json');
  if (!fs.existsSync(graphFile)) {
    say('\nFLOORPLAN CHECK FAILED: the build did not produce graph.json.');
    process.exit(1);
  }
  const graph = JSON.parse(fs.readFileSync(graphFile, 'utf8'));
  const files = graph.files || [];
  const routes = graph.routes || [];
  const missingFiles = files.filter((file) => file.auto).length;
  const missingRoutes = routes.filter((route) => route.auto).length;
  const review = [...files, ...routes].filter((entry) => entry.review).length;
  const counts = graph.meta?.counts || {};
  const unassignedPct = counts.files ? (counts.unassigned / counts.files) * 100 : 0;
  const globMisses = [...buildOut.matchAll(/path "([^"]+)" matches no included file/g)].map(
    (match) => match[1],
  );
  const stale = /described (?:file|route)\(s\) no longer (?:exist|match)/.test(buildOut);

  const failures = [];
  if (!buildOk) failures.push('the map build reported errors (see above)');
  if (missingFiles) failures.push(`${missingFiles} file(s) have no description yet`);
  if (missingRoutes) failures.push(`${missingRoutes} request(s) have no description yet`);
  if (stale) failures.push('descriptions point at files or requests that no longer exist');
  if (globMisses.length) {
    failures.push(
      `${globMisses.length} architecture glob(s) match nothing, so a folder was renamed or removed: ${globMisses
        .slice(0, 5)
        .join(', ')}`,
    );
  }
  if (unassignedPct > MAX_UNASSIGNED_PCT) {
    failures.push(
      `${unassignedPct.toFixed(1)}% of files have no home on the map (limit ${MAX_UNASSIGNED_PCT}%)`,
    );
  }
  if (STRICT_REVIEW && review) {
    failures.push(
      `${review} description(s) need review because their exports or requests changed`,
    );
  }

  const summary = process.env.GITHUB_STEP_SUMMARY;
  if (summary) {
    fs.appendFileSync(
      summary,
      [
        '## Floorplan map',
        '',
        `- ${files.length} files, ${routes.length} requests`,
        `- ${missingFiles} file(s) without a description, ${missingRoutes} request(s) without one`,
        `- ${review} description(s) need review${STRICT_REVIEW ? '' : ' (advisory)'}`,
        `- ${unassignedPct.toFixed(1)}% of files unassigned`,
        '',
        failures.length ? `**Failed:** ${failures.join('; ')}` : '**Passed.**',
        '',
      ].join('\n'),
    );
  }

  if (failures.length) {
    say('\nFLOORPLAN CHECK FAILED');
    for (const failure of failures) say(`  - ${failure}`);
    say(
      '\nRun `node scripts/floorplan.mjs refresh` and describe what changed on the new or flagged cards, then commit docs/architecture/floorplan/.',
    );
    process.exit(1);
  }

  say(
    `\nFLOORPLAN CHECK PASSED  ${files.length} files, ${routes.length} requests, ${review} advisory review note(s)`,
  );
}

const command = process.argv[2];
try {
  if (command === 'load') load();
  else if (command === 'save') save();
  else if (command === 'build') build();
  else if (command === 'refresh') refresh();
  else if (command === 'describe') describe();
  else if (command === 'check') check();
  else if (command === 'open') process.stdout.write(runScript('open.mjs', process.argv.slice(3)));
  else if (command === 'skill') say(skillDir());
  else {
    say('usage: node scripts/floorplan.mjs <load|save|build|refresh|describe|check|open|skill>');
    process.exit(command ? 1 : 0);
  }
} catch (error) {
  say(`\nFloorplan failed: ${error.message}`);
  process.exit(1);
}
