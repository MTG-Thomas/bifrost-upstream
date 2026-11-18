# Foldkit Adoption Gates

These are the checks and thresholds that must hold before Foldkit use in Bifrost expands beyond the current spike. They are step 4 of the staged path in [Design-System Port](foldkit-design-system-port.md#what-changes-in-the-assessment). Automated gates fail a command; review gates are recorded per pilot and decided by a human.

## Automated Gates

| Gate | Command (from `client/`) | Fails when |
|---|---|---|
| Foldkit and Effect lint | `npm run lint:foldkit` | Any `@foldkit/oxlint-plugin` rule (37, recommended set) or Effect type-aware rule fires in `src/foldkit` (warnings are denied) |
| Unit and Scene tests | `npx vitest run src/foldkit` | Any Story, Scene, parity, or host-boundary test fails |
| Bundle | `npx vite build && node scripts/foldkit-gates.mjs dist` | Effect or Foldkit code appears in any non-Foldkit chunk; the entry chunk exceeds 113 kB gzip; a Foldkit chunk exceeds 115 kB gzip |
| Behavioural parity | `node docs/spikes/foldkit/parity/run-parity.mjs …` (see that folder) | React and Foldkit differ on any observation not in `accepted-differences.json`, or an accepted difference no longer occurs |

Notes on each gate:

- **Leak markers.** The bundle gate looks for string literals that survive minification (`~effect/`, `foldkitMount`, `foldkit-dialog-initial-focus`). A negative test with a marker planted in the entry chunk fails the gate.
- **Bundle budgets.** They sit about 2% above the current measurements: entry 110,956 B, logs chunk 107,454 B. Raise them only with a recorded reason.
- **Accepted differences.** Each entry records both renderers' values and a reason. The parity gate fails when an accepted difference *resolves*, so the list cannot outlive the behaviour it describes. When Foldkit fixes Tab focus in Menu, the gate will fail until the entry is removed.
- **New primitives.** Every newly ported primitive adds scenarios to `gallery.ts`, `ReactGallery.tsx`, and `run-parity.mjs` before any production screen uses it.

None of these runs in CI yet. Wiring them in is a CI-policy decision for the fork.

## Review Gates for a Pilot Route

These are recorded in the pilot's PR and compared against the same route's React implementation:

| Measure | How | Expansion threshold |
|---|---|---|
| Consumer code | Non-blank, non-comment lines for the route's views and state, excluding the shared design system | ≤ 1.5× React. The parity gallery measured 1.8× for Submodel-heavy screens, so the pilot must show the ratio falling as `foldkit/ui` matures. |
| Review time | Reviewer-reported minutes per 100 changed lines, React vs Foldkit PRs in the same period | No more than 25% higher |
| Defects | Bugs attributed to the route in its first 30 days, with severity | No accessibility or data-loss defects; total no higher than the React baseline |
| Agent iterations | For agent-authored changes: typecheck, test, and lint failures before green, from the session record | Within 1.5× of a comparable React change once Foldkit sources are vendored |
| Performance | The `docs/spikes/foldkit/bench` method, adapted to the route | No regression in p95 interaction latency at realistic data sizes |
| Platform | Route-level bundle via the bundle gate; first-load cost for a user who never visits the route must be zero | Zero |

## What Each Gate Already Caught

The gates have already found these real defects:

- **Parity harness:**
  - `@foldkit/ui` Button disabled semantics (fixed in the port)
  - Menu Tab focus loss (drafted upstream)
  - the harness's own frame-timing bug
- **Shared class modules:** they exposed three drifts in the hand-copied port: dialog header padding, separator opacity, and label line height.
- **CSS check while wiring the parity gate:** it found that the parity and benchmark harnesses generated almost no Tailwind CSS. Their Vite root sits outside `client/`, so Tailwind never scanned the design-system sources. Both now import `styles.css` with an explicit `@source`. The parity result was unchanged; the benchmark was re-run (see the execution-logs spike).
- **Bundle gate, first run:** it caught the on-demand DevTools overlay import being emitted in production. That created a DevTools chunk and grew the logs chunk from 107.5 to 113.5 kB gzip. The import is now behind a static `import.meta.env.DEV` check.
- **Lint:** the Foldkit layer found 7 issues in the first spike's code, all fixed; the Effect layer caught a planted `Effect.sync(() => promise)`.
