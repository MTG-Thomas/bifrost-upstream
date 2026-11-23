# Foldkit Apps (Experimental)

An independent V2 App can use [Foldkit](https://foldkit.dev) instead of React. Foldkit is The Elm Architecture on [Effect](https://effect.website): one Schema-typed Model, Messages that describe what happened, a pure `update`, and side effects only as Commands, Subscriptions, and Mounts.

Foldkit support is **experimental**. Use it only when the user asks for Foldkit. Default to React for every other App, and for every Solution App: `bifrost solution scaffold-app` has no Foldkit option.

The platform contract is the same as for React Apps (`apps-v2.md`): `index.html` declares `mount-v1`, and `src/main.ts` exports `mount(el, bootstrap)` and registers it in `window.__BIFROST_APP_MODULES__`. Read `apps-independent.md` for create, bind, start, and deploy, and `app-quality.md` for UI quality. Ignore the React-specific parts of `web-sdk-v2.md`.

## Scaffold

```bash
bifrost app create operations --name "Operations" --framework foldkit
cd operations && npm install
bifrost app start
```

```text
operations/
├── package.json        # foldkit, effect; no react, no bifrost (the platform supplies it)
├── vite.config.ts      # same local-dev wiring as React, with the Foldkit plugin
├── tsconfig.json
├── index.html          # mount-v1 marker, loads src/main.ts
└── src/
    ├── main.ts         # mount(el, bootstrap) → Runtime.embed
    ├── bifrost.ts      # Bifrost as an Effect service + currentUser/runWorkflow/followWorkflow
    ├── app.ts          # the program: Model, Message, Commands, update, view
    └── index.css       # Bifrost theme tokens (Tailwind v4)
```

Keep `src/main.ts` as generated. Commit `package-lock.json`.

## Talking to Bifrost

Apps import the framework-neutral client from `bifrost/core`, never from `bifrost` (that entry is the React SDK). The scaffold's `src/bifrost.ts` wraps it:

| Need | Use |
|---|---|
| Signed-in user | `currentUser` (Effect) in a Command |
| Run a workflow, use only the outcome | `runWorkflow(ref, input)` (Effect) in a Command |
| Run a workflow with live status and logs | `followWorkflow(ref, input)` (Stream) in a Subscription |
| Tables | `tables` from `bifrost/core`, wrapped in `Effect.tryPromise` |
| Files | `files` from `bifrost/core`, wrapped in `Effect.tryPromise` |
| Any other Bifrost endpoint | `Bifrost.use((client) => Effect.tryPromise((signal) => client.fetch(path, { signal })))` |

The `Bifrost` service is provided by the program's `resources` Layer. It connects once per mount and disconnects when the runtime is disposed. Never put the client in the Model.

Use `path::function` workflow refs. They resolve to this App's install, and they also run locally under `bifrost app start`.

`tables` and `files` behave exactly as in `tables.md` and `files.md`, including org scoping and errors (`TableNotFoundError`, `FileAccessDeniedError`, and the rest, exported from `bifrost/core`).

### Following a workflow run

Drive the run from the Model: the Subscription runs while the Model says a run is in progress, and the runtime interrupts it when that changes. Interrupting stops following the run; the server execution continues.

```ts
const subscriptions = Subscription.make<Model, Message, Bifrost>()((entry) => ({
  run: entry(
    { maybeRun: Schema.Option(Schema.Struct({ runId: Schema.String, lines: Schema.Number })) },
    {
      modelToDependencies: (model) => ({
        maybeRun: model.run._tag === "Requested"
          ? Option.some({ runId: model.run.runId, lines: model.run.lines })
          : Option.none(),
      }),
      dependenciesToStream: ({ maybeRun }) =>
        Option.match(maybeRun, {
          onNone: () => Stream.empty,
          onSome: ({ runId, lines }) =>
            followWorkflow("workflows/report.py::build_report", { lines }).pipe(
              Stream.map((event) => Message.GotWorkflowEvent({ runId, event })),
              Stream.catch((error) => Stream.make(Message.FailedWorkflow({ runId, error: error.message }))),
            ),
        }),
    },
  ),
}));
```

Key the run with an id stored in the Model so a repeated run with the same input starts a fresh stream.

## Conventions

- Model and Message fields are Schemas. Name Messages for what happened (`ClickedRunReport`, `SucceededFetchUser`), not for what to do.
- Every side effect is a Command, Subscription, or Mount. `update` stays pure.
- Pass the Commands' service requirement through `update`'s return type: `Update.Return<Model, Message, Bifrost>`.
- Use Tailwind classes against the theme tokens in `src/index.css`, as in React Apps.

## Inspecting a running App

In `bifrost app start`, the Foldkit Vite plugin serves DevTools. With `@foldkit/devtools-mcp` configured, an agent can read the current Model, the Message history, and dispatch Messages. Check behavior there before asking the user to click through it.

## Limits

- No `BifrostHeader`, `useBranding`, or `useOrganizations` equivalent. Build the header in the program.
- `bifrost/core` is compiled into the App. An SDK change reaches the App on its next build, not on reload.
- The host still preloads the hosted React SDK when it opens the App. This costs one cached download; it does not affect behavior.
