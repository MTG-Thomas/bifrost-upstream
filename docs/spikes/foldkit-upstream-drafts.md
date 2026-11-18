# Foldkit Upstream Drafts

These are draft issues for [foldkit/foldkit](https://github.com/foldkit/foldkit), prepared from the Bifrost Foldkit spike. **None has been filed.** Each one is for Thomas to review, edit, and post if he chooses.

Versions: `foldkit@0.171.0`, `@foldkit/ui@0.171.0`, `@foldkit/vite-plugin@0.29.1`, `effect@4.1.4`, React 19.3, Vite 8.3, Chromium (Playwright 1.65).

Foldkit has no issue templates. Its contribution guide asks for Conventional Commits, and its commit hook rejects `Co-Authored-By` lines naming Claude, which is relevant to any follow-up PR.

---

## 1. vite-plugin: scope the plugin to part of an application

**Title:** `vite-plugin: support include/exclude so Foldkit can be adopted inside an existing app`

`foldkit()` assumes it owns the whole Vite application. Embedding a Foldkit program in an existing React app (`Runtime.embed`, as the embedding guide suggests) runs into three problems:

1. **`foldkit:view-identity` brands every application module.** It wraps the return value of every function in every non-`node_modules` script, React components and hooks included (`isEligibleModuleId` has no include/exclude).
2. **The `foldkit` plugin's `handleHotUpdate` answers every update with `full-reload`.** That disables the host framework's HMR (React Fast Refresh) for the whole app, not just the Foldkit modules.
3. **`foldkit:devtools-overlay` injects the overlay into the page HTML.** Every page load pulls in `@foldkit/devtools`, even on routes with no Foldkit runtime.

We worked around this in a wrapper (Bifrost: `client/src/build-plugins/foldkit-scoped.ts`):

- It applies the view-identity transform only under a directory.
- It delegates `handleHotUpdate` only for files under that directory, returning `undefined` otherwise so Vite's normal HMR proceeds.
- It drops `transformIndexHtml`, so a program imports `virtual:foldkit-devtools-overlay` itself when its DevTools are enabled.

With the wrapper:

- Editing a React file hot-updates with no page reload and leaves the embedded runtime mounted.
- Editing a Foldkit file triggers the Model-preserving reload.
- The DevTools MCP relay works against the Foldkit runtime inside the host page: discovery, `get_model`, and `dispatch_message` all verified.
- The host's production entry chunk is byte-identical with and without the plugin.

**Proposal:** an option such as `foldkit({ include: ["src/foldkit/**"], devToolsOverlay: "on-demand" })`. Its semantics:

- `include` limits view identity and the full-reload hot update.
- Out-of-scope updates fall through to Vite.
- The overlay is importable on demand instead of injected.

Wrapping internals by plugin name is fragile across releases.

---

## 2. Menu: Tab with the menu open drops focus to `<body>`

**Title:** `ui/Menu: pressing Tab while open loses focus (portaled items)`

**Steps:**

1. Render a `Menu` with the default anchor, so the items are portaled.
2. Focus the menu button and press Enter to open it.
3. Press Tab.

**Expected** (WAI-ARIA menu button pattern): the menu closes, and focus moves to the next tabbable element after the button, or returns to the button.

**Actual:** the menu closes and `document.activeElement` is `<body>`.

**Cause, from reading the source:**

- `BlurredItems` closes with `closeWithoutFocusCommands`.
- The browser then moves focus from the items container, which is portaled to the end of `<body>`, so there is no next tabbable element.

For comparison, Radix's DropdownMenu keeps focus in the open menu on Tab. Found with a Chromium harness that runs identical scenarios against Radix and `@foldkit/ui`. The other 43 of 48 observations matched, including Escape and selection focus restoration.

---

## 3. Button: `isDisabled` is `aria-disabled` only

**Title:** `ui/Button: isDisabled keeps the button focusable and unmatched by :disabled`

`Button.view({ isDisabled: true })` emits `aria-disabled="true"` and `data-disabled`, but not the native `disabled` attribute. As a result:

- The button stays in the tab order. This may be intentional, as a "focusable disabled" pattern.
- Styling written against `:disabled` (Tailwind `disabled:` variants, shadcn-style design systems) does not apply, so the button looks enabled.

We add `disabled` ourselves when porting a design system that is contractually native-disabled.

**Question or proposal:** is focusable-disabled the intended default? If so, could the docs say so? Alternatively, an option (`disabledMode: "native" | "focusable"`) would make the trade-off explicit.

---

## 4. Scene: consumer tests must resolve library-internal Commands by name

**Title:** `test/scene: resolving @foldkit/ui internals in consumer scene tests`

A Scene test of any screen containing a `@foldkit/ui` Menu has to resolve that component's internal Commands and Mounts by name:

- `FocusItems`
- `AnchorMenu`
- `PortalMenuBackdrop`
- `FocusButton`
- `Mount.expectEnded(AnchorMenu, PortalMenuBackdrop)`

The strictness is valuable for code the application owns. For library components, though, it couples application tests to internals that can be renamed in a minor release.

**Proposal:** each stateful component could export a resolver set, e.g. `Menu.sceneDefaults`. Alternatively, Scene could offer an opt-in step to auto-resolve Commands and Mounts originating inside a named Submodel with their success Messages.

---

## 5. test/vitest: `setup()` overrides jest-dom matchers

**Title:** `test/vitest: setup() registers matchers that collide with @testing-library/jest-dom`

`foldkit/test/vitest`'s `setup()` registers these global matchers:

- `toHaveClass`
- `toHaveValue`
- `toBeVisible`
- `toBeDisabled`
- `toBeEnabled`
- `toBeChecked`
- `toBeEmpty`
- `toHaveStyle`

All of them share names with jest-dom's matchers. In a codebase that already uses jest-dom, calling `setup()` silently changes the meaning of existing assertions.

We avoided `setup()` and use Scene's own `expect`, which works.

**Proposal:** namespaced matchers, or `setup({ matchers: [...] })`, plus a note in the testing docs.

---

## 6. Runtime.embed: a container without `id` fails asynchronously

**Title:** `Runtime.embed: missing container id logs an error and renders nothing instead of throwing`

`Runtime.embed(makeElement({ container }))` with a container that has no `id` does not throw. It logs `Runtime container must have an id for Model preservation across reloads` from the runtime fiber and renders nothing.

**Proposal:** throw synchronously from `embed` (or `makeElement`), or generate an id when absent.

---

## 7. Docs: React embedding pattern and unmounting

**Title:** `docs(embedding): ref'd container pattern breaks when the React component unmounts on its own`

The embedding guide's React pattern is:

- `useRef` container
- `Runtime.embed` in `useEffect`
- `dispose` in cleanup

It throws when the component unmounts while its React parent stays mounted:

```
DOMException: Failed to execute 'removeChild' on 'Node':
The node to be removed is not a child of this node.
```

**Cause:**

- The renderer swaps its root in for the container node.
- The container is restored only in an asynchronous scope finalizer.
- React's commit phase removes its host node before passive-effect cleanups run.

A reproduction is available. The robust pattern is for React to own a wrapper element while the effect creates the container imperatively:

```tsx
useEffect(() => {
  const container = document.createElement("div")
  container.id = id
  wrapperRef.current!.appendChild(container)
  const handle = Runtime.embed(makeElement(container, flags))
  return () => void handle.disposeAsync().then(() => container.remove())
}, [])
```

**Proposal:** document this pattern, or render into the container rather than replacing it.

---

## Not Proposed

These are findings recorded in the spike that are not bugs, so there is no issue for them:

- **Port values lose referential identity.** Inbound port values are Schema-decoded into fresh objects, so `createLazy` keyed on a port-fed array never hits unless `update` reconciles against the previous Model. This is inherent to validated ports. A sentence in the embedding or performance docs might save others the profiling.
- **Dialog timing.** The Dialog takes about 100 ms longer to close than Radix, because it waits for transitions to settle and then makes a Command round trip.
