# Foldkit Spike: Design-System Port

This is a follow-up to [the execution-logs spike](foldkit-execution-logs.md). It answers two questions: whether Bifrost's design system can be ported to Foldkit, and what that costs when measured rather than estimated. Dated 2026-11-18; versions are Foldkit `0.171.0`, `@foldkit/ui 0.171.0`, `effect 4.1.4`, `lucide 1.72.0`, and `@foldkit/oxlint-plugin 0.15.4`.

**Short answer:** yes. The port was smaller and more faithful than the first assessment predicted.

- The ported Dialog and DropdownMenu match Radix on 43 of 48 scripted keyboard, focus, and pointer observations in Chromium.
- With a menu open, the Foldkit version has fewer axe violations than the Radix version.
- The style layer (tokens, `cva` variants, icons) is now shared rather than copied.

The remaining costs:

- **Consumer code.** Screens using stateful components need about 1.8× the code because of Submodel wiring.
- **Missing capability.** There are no sub-menus, which Bifrost uses once.
- **One accessibility defect.** Pressing Tab in an open menu loses focus.
- **Test coupling.** Scene tests must name library-internal Commands.
- **Unchanged costs.** The Effect runtime bundle and the tooling conflicts from the first spike remain.

## What Was Ported

| Bifrost component | Foldkit port | Built on |
|---|---|---|
| `button.tsx` | `client/src/foldkit/ui/button.ts` | `@foldkit/ui` Button + shared `buttonVariants` |
| `badge.tsx` | `client/src/foldkit/ui/badge.ts` | shared `badgeVariants` |
| `input.tsx` + `label.tsx` | `client/src/foldkit/ui/input.ts` | `@foldkit/ui` Input |
| `dialog.tsx` | `client/src/foldkit/ui/dialog.ts` | `@foldkit/ui` Dialog (native `<dialog>`) |
| `dropdown-menu.tsx` | `client/src/foldkit/ui/dropdownMenu.ts` | `@foldkit/ui` Menu |
| `lucide-react` icons | `client/src/foldkit/ui/icons.ts` | vanilla `lucide` (same set and version) |

The style layer is shared, not copied:

- `buttonVariants` and `badgeVariants` moved out of the React files into framework-free `button-variants.ts` and `badge-variants.ts`. The React components re-export them, so no existing import changed, and all 169 component tests under `src/components/ui` and `src/foldkit` pass.
- Tokens are CSS variables and need no port.
- The `lucide` package exports every icon as plain data. One 42-line renderer gives Foldkit views all of them, replacing the first spike's hand-copied SVG paths.

The logs panel now uses the ported Button, Badge, and Input, which cut its view from 601 to 528 lines.

## Behavioural Parity (Chromium)

`docs/spikes/foldkit/parity/` contains two galleries with identical content: React built from the shipping shadcn components, and Foldkit built from the port. Each has buttons, badges, a labelled input, a delete-confirmation dialog with a form field, and an actions menu with a disabled item, a labelled "Danger Zone" group, and a destructive item.

`run-parity.mjs` drives both through the same 48 observations in a production build. Every step waits two animation frames because Foldkit renders on the next frame. axe-core 4.14 scans each page closed, with the dialog open, and with the menu open.

**Identical (43 of 48):**

- tab order, including skipping the disabled button
- input label association
- **Dialog:**
  - accessible name and description
  - initial focus on the first field
  - Tab and Shift+Tab containment
  - typing inside the dialog
  - closing by Escape, Cancel, backdrop, the close button, and confirm, with focus returning to the trigger after each
  - field reset on reopen
- **Menu:**
  - `aria-haspopup` and `aria-expanded`
  - opening with Enter, ArrowDown, and click
  - first-item highlight
  - Home, End, and ArrowUp
  - skipping the disabled item
  - typeahead ("du" → Duplicate)
  - group label and separator
  - `aria-disabled`
  - selecting with Enter or click, with focus returning to the trigger
  - Escape closing it, with focus returning to the trigger
  - ignored clicks on disabled items
  - closing on an outside click
- no page errors in either renderer

**Different (5):**

| Observation | Radix (React) | Foldkit | Assessment |
|---|---|---|---|
| Trigger in accessibility tree while menu open | Removed (modal menu hides siblings) | Kept | Design difference |
| Focus model | DOM focus moves to each item | Focus stays on `role=menu`; items via `aria-activedescendant` | Both are valid ARIA patterns |
| axe with menu open | `aria-hidden-focus`, `landmark-one-main`, `page-has-heading-one`, `region` (+ harness `html-has-lang`) | `region` (+ harness `html-has-lang`) | **Foldkit has fewer violations.** Radix's modal hiding causes three of them. |
| ArrowDown at last item | Stops | Wraps to first | Radix offers `loop`; Foldkit always wraps |
| Tab while menu open | Menu stays open, focus stays on the item | Menu closes, **focus falls to `<body>`** | **Foldkit defect.** The blur handler closes without restoring focus, and the item list is portaled to the end of `<body>`. This is a candidate upstream issue for review. |

Close animations: React closes in 290–380 ms; Foldkit takes 310–565 ms on every path. Foldkit waits for the transition to settle and then makes a Command round trip.

Two faults surfaced during the port, both fixed:

- **Disabled buttons.** `@foldkit/ui` disables a Button with `aria-disabled` only, so it stays focusable. Bifrost's `cva` styles key off `:disabled`, so a disabled Foldkit button would also not *look* disabled. The port adds the native `disabled` attribute, matching DESIGN.md's behaviour-preservation rule.
- **Harness timing.** My own harness read state synchronously. Before it waited for frames, Foldkit appeared to fail arrow-key and close scenarios it actually passes. Any parity suite has to account for the frame-scheduled renderer.

## Measured Cost

### Code

| Component | React (shadcn) | Foldkit port | Note |
|---|---|---|---|
| Button | 64 | 52 | |
| Badge | 52 | 28 | |
| Input + Label | 35 | 82 | The port bundles the label and leading/trailing slots, which React composes at call sites |
| Dialog | 149 | 158 | |
| DropdownMenu | 251 | 107 | React includes checkbox, radio, sub-menu, and shortcut parts that were not ported |
| Icons | `lucide-react` | 42 | Covers all lucide icons |

Non-blank, non-comment lines.

**Consumer code is the real cost.** The same gallery screen is 129 lines in React and 236 in Foldkit (1.8×). A Foldkit screen that uses a Dialog or Menu owns the child's Model, wraps its Messages (`GotDeleteDialogMessage`), and folds its update and OutMessages (`Update.foldChild`, `foldChildStep`).

In React, Radix keeps that state internally. In Foldkit, it is explicit and inspectable, which is the point, but it is code someone writes and reviews. Because the code is written with a uniform recipe, agents can produce it mechanically.

### Bundle

- **Logs chunk.** Switching the logs panel to the ported Button, Input, and Badge plus vanilla `lucide` took its chunk from 324.18 kB / 107.18 kB gzip to **326.26 kB / 107.94 kB gzip**. Lucide tree-shakes to the icons used.
- **CSS.** +1.4 kB raw (+0.17 kB gzip) for the new `data-[active]` and `data-[closed]` variants.
- **Main chunk.** +0.4 kB raw from the variant extraction.
- **Dialog and Menu.** Adding `@foldkit/ui` Dialog and Menu to a Foldkit chunk costs about **+20 kB gzip** (esbuild delta). For comparison, Radix Dialog and DropdownMenu are about 36 kB gzip standalone, but Bifrost already ships those in the main bundle, so their marginal cost there is zero.

### Feature Coverage Against Bifrost's Usage

These are counts of usage sites in `client/src` outside `components/ui`.

| Radix feature | Uses in Bifrost | Foldkit Menu/Dialog |
|---|---|---|
| `DropdownMenuItem` | 41 files | ✅ |
| `DropdownMenuSeparator` / `Label` | 9 / 4 | ✅ via groups |
| `DropdownMenuItem asChild` (links) | 8 | ⚠️ Items carry arbitrary content; navigation becomes a Command on selection |
| `DropdownMenuSub` | 1 | ❌ No sub-menus |
| Checkbox / radio / shortcut items | 0 | Not needed |
| Dialogs containing forms | 30 files | ✅ Inputs and Tab containment verified |
| `DialogTrigger asChild` | 5 | ✅ Any button dispatching an open Message |

### Testing

Scene tests for the port live in `client/src/foldkit/ui/ui.scene.test.ts` and cover classes, `data-slot` hooks, native `disabled`, label association, menu ARIA, typed selection, and dialog slots.

A Scene test of any screen containing a `@foldkit/ui` Menu must resolve that component's internal Commands and Mounts **by name**: `FocusItems`, `AnchorMenu`, `PortalMenuBackdrop`, and `FocusButton`. Scene's strictness is valuable for code you own. For library components, it couples consumer tests to library internals that may be renamed in any minor release. RTL tests of Radix components never require this.

## Linting: Yes, Two Layers

**`@foldkit/oxlint-plugin` 0.15.4** has 37 architecture rules. Examples:

- `no-spread-in-modify-fields`
- `keyed-required-for-mapped-rows`
- `no-array-index-view-keys`
- `lazy-view-stable-references`
- `no-empty-commands-array`
- `no-impure-call-at-decision-time`
- `prefer-option-over-nullable-in-model`
- `require-rel-for-external-link`

It runs on oxlint (Rust) as a JS plugin, beside ESLint rather than inside it. On the first spike it found 7 issues, all the cosmetic `no-empty-children-array`. On the current code it reports none.

**Effect diagnostics via `@effect/tsgo` 0.70** add type-aware rules, for example:

- `lazy-promise-in-effect-sync`
- `catch-unfailable-effect`
- `leaking-requirements`
- `return-effect-in-gen`
- `try-catch-in-effect-gen`
- `schema-sync-in-effect`

They report nothing on the spike code. A deliberately planted `Effect.sync(() => promise)` was caught.

Setup friction for Bifrost (resolved in step 2 below):

- The type-aware layer runs on TypeScript-Go (TS 7). It **rejects Bifrost's `tsconfig.app.json`** because TS 7 removed `baseUrl`. It also needs a separately installed and patched `oxlint-tsgolint` binary.
- To run it here, I copied the code into a scratch project with a TS 7-compatible `tsconfig`.
- Adopting it means migrating the client `tsconfig` off `baseUrl` (paths without `baseUrl` work in TS 5.9 too) and adding an oxlint CI step.

Neither layer enforces Bifrost's DESIGN.md rules. Those live in the shadcn ESLint rules, which only read JSX `className`. The port goes some way to compensating: Foldkit views now get classes from the shared `cva` functions and from wrappers that own the class strings, instead of inline `h.Class("…")`, which is the same discipline the shadcn rules enforce.

## Agent-Assisted Development

The port and the parity harness were written by an agent in one session. Effort observations:

- **API discovery was the main cost.** `@foldkit/ui` Submodel wiring (fold helpers, OutMessage shapes such as `Selected { value }` rather than `{ item }`, which Dialog update types are exported) had to be read from source and the showcase example. Foldkit's own guidance is to vendor the Foldkit repo as a subtree pinned to the installed version, so agents pattern-match against canonical code. That would have removed most of this cost and should be part of any adoption.
- **The type checker and Scene caught real mistakes quickly:**
  - 4 type errors: the OutMessage field, an unexported type alias, and lucide tag typing (twice)
  - 4 Scene iterations, each one naming the exact unresolved Command or Mount
- **The parity harness found what tests alone did not:** the disabled-button semantics and the Tab focus loss.
- **The explicit-state payoff showed in the first spike.** A Story test exposed the port-decode identity bug before any browser run.

## What Changes in the Assessment

The design system is **not** the blocker the first assessment suggested. The style layer is shared, the primitives Bifrost uses most are covered with high behavioural parity, and wrappers are about the same size as their React counterparts.

The open costs are now concentrated in:

1. **Platform.** About 107 kB gzip of Effect and Foldkit for the first route that uses it.
2. **Boundary.** Foldkit islands inside React pay the port, identity, and frame costs from the first spike. Converting whole routes avoids them, but a whole-route change is a much bigger one.
3. **Tooling.** There is no scoped Vite plugin, so no DevTools or MCP inside Bifrost's dev server; there are matcher collisions; and type-aware linting needs TS 7.
4. **Gaps and upstream dependencies.** Sub-menus, Tab focus restoration, disabled semantics, and Scene coupling to library internals, all owned by a pre-1.0 project with one primary maintainer.
5. **Apps/Solutions stay React.** Two paradigms remain permanently.

If Bifrost adopts Foldkit as a direction, here is a staged path that keeps every step reversible:

1. **Keep the shared style layer.** Extract the remaining `cva` variants (alert, tabs, toggle, input-group) the same way. This is useful with or without Foldkit.
2. **Remove the tooling blockers first.**
   - Move `tsconfig` off `baseUrl`.
   - Ask Foldkit for a scoped Vite plugin (include/exclude) and test-matcher namespacing.
   - Raise the Tab-focus and disabled-semantics findings upstream.
3. **Pilot a whole route, not an island**, with no Radix-only dependencies. Execution history detail is a candidate once its dependencies are ported. Vendor the Foldkit subtree and run both lint layers in CI.
4. **Gate expansion** on the pilot's measured cost: bundle, review time for Submodel wiring, defect rate, and agent iteration counts. Keep the parity harness running against each newly ported primitive.

## Progress on the Staged Path

Steps 1, 2, and 4 are done; step 3 (a whole-route pilot) is not started.

**1. Shared style layer.**

- These now live in framework-free `client/src/components/ui/*-variants.ts` modules that the React components consume:
  - `cva` variants for button, badge, alert, tabs, toggle, and input-group
  - class strings for input, label, and skeleton
  - structural classes for dialog and dropdown-menu
- The Foldkit port imports the same modules. Dialog and dropdown share structure only, because Radix and Foldkit use different animation and highlight hooks.
- Rendering 94 React component and variant combinations before and after produced identical class sets (92 byte-identical, 2 reordered).
- Sharing removed three drifts the hand copies had accumulated: dialog header padding, separator opacity, and label line height.

**2. Tooling blockers.**

- **`baseUrl` removed** from `client/tsconfig*.json`; `tsc -b` passes.
- **Lint.** `npm run lint:foldkit` runs both lint layers on `src/foldkit` against Bifrost's real `tsconfig.app.json`, in about 3.5 s. A seeded defect fails each layer.
- **Scoped Vite plugin.** `client/src/build-plugins/foldkit-scoped.ts` wraps `@foldkit/vite-plugin`:
  - view identity applies only to `.ts` modules under `src/foldkit`
  - the full-page reload fires only for those files
  - the DevTools overlay loads on demand, when a program enables DevTools
- **Verified on Bifrost's own Vite dev server:**
  - Editing `src/pages/Login.tsx` hot-updated with no page reload, and the embedded runtime stayed mounted.
  - Editing `src/foldkit/executionLogs/logs.ts` triggered one Model-preserving reload.
  - The DevTools MCP discovered the runtime inside the Bifrost page, read the Model, and dispatched `UpdatedQuery`; the live DOM updated.
- **Bundle effect.** The production entry chunk and `ExecutionDetails` are byte-identical with and without the plugin. The Foldkit chunk grows about 0.7 kB gzip for view identity.
- **Tests.** The plugin is in the Vitest config too; the full client suite passes (666 files, 4,473 tests).
- **Dependencies.** The plugin brings `@effect/platform-node`, which pulls a Redis client and `ws` into dev dependencies, and bumps `ws` 8.21.0 → 8.23.0.
- **Upstream.** Seven upstream asks are drafted in [Foldkit Upstream Drafts](foldkit-upstream-drafts.md) and tracked as fork issues #58–#64. Nothing has been filed with Foldkit.

**4. Gates.** [Foldkit Adoption Gates](foldkit-adoption-gates.md) defines four automated gates (lint, tests, bundle, parity) and the review thresholds a pilot must meet:

- `client/scripts/foldkit-gates.mjs` fails on Effect or Foldkit code outside Foldkit chunks, or on budget overruns.
- The parity harness now fails on any unaccepted or resolved difference (`accepted-differences.json`).

Wiring the gates up surfaced a harness bug: Tailwind generated almost no CSS for the parity and benchmark pages. After the fix, parity is unchanged at 43 of 48, and the execution-logs benchmark was re-run. A real-browser check with `prefers-reduced-motion` confirmed dialogs close and reopen correctly in both renderers.

## Verification

Commands run, all from `client/` unless noted:

- `npx vitest run src/foldkit src/components/ui src/components/execution src/pages/ExecutionDetails.test.tsx`: 52 files, 414 tests, all passing
- `npx tsc -p tsconfig.app.json --noEmit` is clean, and the parity gallery typechecks against the same config (`docs/spikes/foldkit/parity/tsconfig.json`)
- `npm run lint`: 0 errors, 3,976 warnings, unchanged from before this follow-up. `src/foldkit/ui/**` shares the design-system exemption for token-based arbitrary values with `src/components/ui/**`.
- `@foldkit/oxlint-plugin` (recommended set) reports 0 findings, and the Effect type-aware rules report 0 findings
- `npx vite build`, plus the Chromium parity run with axe-core 4.14

Not run: full Vitest, Playwright, and backend suites; screen-reader, Firefox, and Safari testing.

## Reproducing

```bash
cd docs/spikes/foldkit/parity
ln -s ../../../../client/node_modules node_modules
./node_modules/.bin/vite build --config vite.config.ts
node run-parity.mjs parity-dist /path/to/axe-core/axe.min.js   # axe path optional
rm node_modules && rm -rf parity-dist
```

The tests are in `client/src/foldkit/ui/ui.scene.test.ts`. Lint setup: install `oxlint @foldkit/oxlint-plugin` (and `@effect/tsgo oxlint-tsgolint` for the Effect layer) and use the `.oxlintrc*.json` files from `create-foldkit-app`'s base template.
