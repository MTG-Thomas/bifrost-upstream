# Bifrost architecture map

An interactive, plain-English map of the Bifrost platform: every subsystem, file and
web request with a name and a one-line description, connected by the imports and HTTP
calls between them. It is built with [Floorplan](https://github.com/haydenschmitty/floorplan)
(v0.6.0, MIT), which runs locally and needs no API keys.

**Proposal status.** This folder and the two workflows beside it are a proposal for
upstream review, not an established convention. The questions at the end are the ones
worth settling before this becomes a required check.

**View it:** <https://mtg-thomas.github.io/bifrost-upstream/> is the rendered map,
published from the fork's `gh-pages` branch. It is a static export (no Ask bar, no
health panel, no local paths), so it opens on a phone. Regenerate it with
`node floorplan/serve.mjs --export floorplan/bifrost-map.html` and push that file as
`index.html` on `gh-pages`.

## What is tracked, and what is not

| Path | Tracked | Why |
|---|---|---|
| `docs/architecture/floorplan/architecture.json` | yes | What the parts of the platform are, and how they connect |
| `docs/architecture/floorplan/descriptions/*.json` | yes | A name and one-line description per file and request |
| `docs/architecture/floorplan/recent-work.json` | yes | The "what changed and why" notes shown on the map |
| `floorplan/` | no | The rendered map (`index.html`, `graph.json`), rebuilt on demand |

The authored part is tracked; the rendered part is generated. They are in different
folders because Floorplan owns `floorplan/.gitignore` and rewrites it to "ignore
everything in this folder" on every scan or build. Committed inputs inside `floorplan/`
would be silently hidden from `git status` on the next run, so the source of truth lives
in `docs/` and `scripts/floorplan.mjs` copies it in and out.

`floorplan/` remains the plugin's standard location, so the plugin's own commands
(`open.mjs`, `refresh.mjs`, the Ask bar) keep working unchanged.

## Everyday use

```bash
node scripts/floorplan.mjs build     # rebuild the map from the tracked metadata
node scripts/floorplan.mjs refresh   # re-read only what changed since the last map
node scripts/floorplan.mjs describe  # worksheets for new or changed files and requests
node scripts/floorplan.mjs open      # serve the map and print its URL
node scripts/floorplan.mjs check     # what CI runs: fail if the map has fallen behind
```

`build`, `refresh` and `describe` copy metadata changes back into
`docs/architecture/floorplan/`, so commit that folder after running them.

The plugin is fetched into `.cache/floorplan-skill/` at a pinned commit
(`PLUGIN_REF` in `scripts/floorplan.mjs`). Set `FLOORPLAN_SKILL_DIR` to use a local
checkout while developing, and bump `PLUGIN_REF` deliberately after reviewing the
plugin's diff.

## What still needs an AI

The scripts are mechanical: they scan, match requests to handlers, carry descriptions
across renames and flag changed exports. Deciding what a new part *is* and writing its
sentence is the part only a reader can do. The loop is:

1. `node scripts/floorplan.mjs refresh` — drops deleted entries, carries moved ones,
   and marks entries whose exports or requests changed with `"review"`.
2. `node scripts/floorplan.mjs describe` — writes worksheets for just those entries.
3. Ask Claude Code or Codex to fill the worksheets in (the Floorplan skill does this),
   then rebuild and commit.

`check` treats missing descriptions as a failure and `review` notes as advisory, because
only a person or an agent can clear a review note. Set `FLOORPLAN_STRICT_REVIEW=1` to
make review notes blocking once the workflow has bedded in.

## The two workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `.github/workflows/floorplan.yml` | pull requests touching `api/`, `client/`, the map source, or the wrapper | Re-reads the code and fails when the tracked map has fallen behind it. Mechanical only, no model calls, no secrets. |
| `.github/workflows/floorplan-refresh.yml` | Mondays, manual dispatch, and pushes to `main` | Runs the mechanical refresh and opens a pull request when descriptions changed, with the refresh report in the body. |

Both jobs reuse the pinned action SHAs already in `ci.yml` and need no repository
secrets. The refresh job uses `GITHUB_TOKEN` unless a `FLOORPLAN_BOT_TOKEN` secret is
set; a `GITHUB_TOKEN` push does not trigger CI on the pull request it opens, so a
follow-up run or an App token is needed before that PR can merge.

## Scope and limits

- Backend tests, client end-to-end tests, generated migrations and `v1.d.ts` are
  excluded from the map on purpose: they describe the test suite, not the product.
- Connections come from static analysis (imports and matched request URLs). A few
  links that only exist at runtime are recorded as explicit edges in
  `architecture.json`.
- The health panel is a quick automatic check, not a security audit.
- The map is a local artifact: nothing is sent anywhere, and no source code leaves the
  machine beyond what Claude Code or Codex already read.

## Questions for upstream

1. Should the authored map live in the repository at all, or stay a per-developer
   artifact with only the CI check upstream?
2. Should `floorplan.yml` be a required check, and should `review` notes block once
   there is a documented way to clear them?
3. Would Floorplan accept a first-class "tracked inputs, generated output" mode, so the
   copy in and out in `scripts/floorplan.mjs` could go away?
