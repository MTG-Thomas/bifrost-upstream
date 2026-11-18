"""Experimental Foldkit scaffold for independent V2 Apps.

``bifrost app create --framework foldkit`` writes these files instead of the
React skeleton. The App keeps the same platform contract: ``index.html``
declares ``mount-v1`` and ``src/main.ts`` exports ``mount(el, bootstrap)``,
registered in ``window.__BIFROST_APP_MODULES__``. It talks to Bifrost through
the framework-neutral ``bifrost/core`` entry of the instance's SDK package.

The local-development wiring (``vite.config.ts``, ``.env.example``) and the
theme tokens (``src/index.css``) are taken from the React scaffold so both
frameworks stay on one implementation of them.
"""
from __future__ import annotations

import json

FOLDKIT_VERSION = "0.171.0"
EFFECT_VERSION = "4.1.4"
FOLDKIT_VITE_PLUGIN_VERSION = "0.29.1"

_REACT_PLUGIN_IMPORT = 'import react from "@vitejs/plugin-react";\n'
_FOLDKIT_PLUGIN_IMPORT = 'import { foldkit } from "@foldkit/vite-plugin";\n'
_REACT_PLUGINS = "plugins: [react(), tailwindcss()],"
_FOLDKIT_PLUGINS = "plugins: [foldkit(), tailwindcss()],"

_MAIN_TS = """\
import { Runtime } from "foldkit";

import { makeApp } from "./app";
import "./index.css";

export interface BifrostAppBootstrap {
  basename: string;
  baseUrl: string;
  token: string;
  orgScope: string | null;
  appId: string | null;
  /** Owning Solution supplied by the platform host. */
  solutionId?: string | null;
  onLogout: () => void;
  theme: "light" | "dark";
}

interface BifrostAppModule {
  mount: (mountEl: HTMLElement, bootstrap: BifrostAppBootstrap) => () => void;
}

declare global {
  interface Window {
    __BIFROST_APP_MODULES__?: Map<string, BifrostAppModule>;
  }
}

// The host hands over a DOM node and the bootstrap, and gets back a teardown.
// Foldkit renders into a container this function owns, so the host's node is
// never replaced. Disposing the runtime also disconnects the Bifrost client.
export function mount(mountEl: HTMLElement, bootstrap: BifrostAppBootstrap) {
  const container = document.createElement("div");
  container.id = "app";
  container.className = "h-full";
  mountEl.appendChild(container);
  const handle = Runtime.embed(
    makeApp(container, {
      baseUrl: bootstrap.baseUrl,
      token: bootstrap.token,
      orgScope: bootstrap.orgScope,
      appId: bootstrap.appId,
    }),
  );
  return () => {
    void handle.disposeAsync().then(() => container.remove());
  };
}

// Register the reusable factory under this immutable entry's canonical URL.
// Child chunks import this exact URL, so the browser evaluates the graph once.
(window.__BIFROST_APP_MODULES__ ??= new Map()).set(import.meta.url, { mount });

// Vite dev serves this entry directly; production mounting is owned by the host.
if (import.meta.env.DEV) {
  const mountEl = document.getElementById("root");
  if (!mountEl) throw new Error("Missing #root mount element");
  mount(mountEl, {
    basename: "/",
    baseUrl: import.meta.env.VITE_BIFROST_API_URL ?? window.location.origin,
    token: import.meta.env.VITE_BIFROST_TOKEN ?? "",
    orgScope: import.meta.env.VITE_BIFROST_ORG_ID ?? null,
    appId: import.meta.env.VITE_BIFROST_APP_ID ?? null,
    onLogout: () => window.location.assign("/login"),
    theme: document.documentElement.classList.contains("dark") ? "dark" : "light",
  });
}
"""

_BIFROST_TS = """\
import { Cause, Context, Data, Effect, Layer, Stream } from "effect";
import * as Queue from "effect/Queue";
import {
  connect,
  type BifrostClient,
  type BifrostConnection,
  type BifrostCurrentUser,
  type WorkflowLogEntry,
} from "bifrost/core";

/**
 * Bifrost as an Effect service. The program's `resources` Layer connects once
 * per mount and disconnects when the runtime is disposed. Commands and
 * Subscriptions reach the API through the functions below, so the client
 * never enters the Model.
 */
export class Bifrost extends Context.Service<Bifrost, BifrostClient>()("app/Bifrost") {
  static readonly layer = (connection: BifrostConnection) =>
    Layer.effect(
      Bifrost,
      Effect.acquireRelease(
        Effect.sync(() => connect(connection)),
        (client) => Effect.sync(() => client.disconnect()),
      ),
    );
}

export class BifrostError extends Data.TaggedError("BifrostError")<{
  readonly message: string;
}> {}

const toBifrostError = (cause: unknown) =>
  new BifrostError({ message: cause instanceof Error ? cause.message : String(cause) });

/** The signed-in user. Display only: workflows and table policies enforce access. */
export const currentUser: Effect.Effect<BifrostCurrentUser, BifrostError, Bifrost> = Bifrost.use(
  (client) =>
    Effect.tryPromise({
      try: (signal) => client.currentUser({ signal }),
      catch: toBifrostError,
    }),
);

/**
 * Run a workflow and succeed with its result. Use it in a Command when only
 * the outcome matters. Prefer a `path::function` ref: it resolves to this
 * App's install and also runs locally under `bifrost app start`.
 */
export const runWorkflow = (workflowRef: string, input: Record<string, unknown> = {}) =>
  Bifrost.use((client) =>
    Effect.tryPromise({
      try: (signal) => client.runWorkflow(workflowRef, input, { signal }),
      catch: toBifrostError,
    }),
  );

export type WorkflowEvent =
  | { readonly _tag: "Started"; readonly executionId: string }
  | { readonly _tag: "Status"; readonly status: string }
  | { readonly _tag: "Log"; readonly log: WorkflowLogEntry }
  | { readonly _tag: "Finished"; readonly result: unknown };

/**
 * Run a workflow as a Stream of its execution id, status changes, logs, and
 * result. Use it from a Subscription keyed on the run in the Model: when the
 * Model no longer asks for the run, the runtime interrupts the stream and the
 * client stops following it. The server execution itself keeps running.
 */
export const followWorkflow = (
  workflowRef: string,
  input: Record<string, unknown> = {},
): Stream.Stream<WorkflowEvent, BifrostError, Bifrost> =>
  Stream.unwrap(
    Bifrost.use((client) =>
      Effect.succeed(
        Stream.callback<WorkflowEvent, BifrostError>((queue) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              const controller = new AbortController();
              client
                .runWorkflow(workflowRef, input, {
                  signal: controller.signal,
                  onExecutionId: (executionId) =>
                    Queue.offerUnsafe(queue, { _tag: "Started", executionId }),
                  onStatus: (status) => Queue.offerUnsafe(queue, { _tag: "Status", status }),
                  onLog: (log) => Queue.offerUnsafe(queue, { _tag: "Log", log }),
                })
                .then(
                  (result) => {
                    Queue.offerUnsafe(queue, { _tag: "Finished", result });
                    Queue.endUnsafe(queue);
                  },
                  (cause: unknown) => {
                    if (!controller.signal.aborted) {
                      Queue.failCauseUnsafe(queue, Cause.fail(toBifrostError(cause)));
                    }
                  },
                );
              return controller;
            }),
            (controller) => Effect.sync(() => controller.abort()),
          ),
        ),
      ),
    ),
  );
"""

_APP_TS = """\
import { Effect, Option, Schema } from "effect";
import { Command, Runtime, type Update } from "foldkit";
import type { Html, HtmlBuilder } from "foldkit/html";
import { defineMessageUnion } from "foldkit/message";
import { modifyFields } from "foldkit/struct";
import type { BifrostConnection } from "bifrost/core";

import { Bifrost, currentUser } from "./bifrost";

// MODEL

const User = Schema.Struct({ email: Schema.String, name: Schema.String });

const Model = Schema.Struct({
  maybeUser: Schema.Option(User),
  maybeError: Schema.Option(Schema.String),
});
type Model = typeof Model.Type;

// MESSAGE

const Message = defineMessageUnion({
  SucceededFetchUser: { user: User },
  FailedFetchUser: { error: Schema.String },
});
type Message = typeof Message.Type;

// COMMAND

const FetchUser = Command.define("FetchUser", {
  messages: [Message.SucceededFetchUser, Message.FailedFetchUser],
  execute: currentUser.pipe(
    Effect.match({
      onSuccess: (user) =>
        Message.SucceededFetchUser({ user: { email: user.email, name: user.name } }),
      onFailure: (error) => Message.FailedFetchUser({ error: error.message }),
    }),
  ),
});

// UPDATE

const update = (model: Model, message: Message): Update.Return<Model, Message, Bifrost> =>
  Message.match<Update.Return<Model, Message, Bifrost>>(message, {
    SucceededFetchUser: ({ user }) => ({
      model: modifyFields(model, { maybeUser: () => Option.some(user) }),
    }),
    FailedFetchUser: ({ error }) => ({
      model: modifyFields(model, { maybeError: () => Option.some(error) }),
    }),
  });

// VIEW

const view = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.main(
    [h.Class("mx-auto grid max-w-2xl gap-4 p-6 text-foreground")],
    [
      h.h1([h.Class("text-xl font-semibold")], ["My App"]),
      h.p(
        [h.Class("text-sm text-muted-foreground")],
        [
          Option.match(model.maybeUser, {
            onNone: () => "Loading…",
            onSome: (user) => `Signed in as ${user.name || user.email}`,
          }),
        ],
      ),
      ...Option.match(model.maybeError, {
        onNone: () => [],
        onSome: (error) => [h.p([h.Role("alert"), h.Class("text-sm text-destructive")], [error])],
      }),
    ],
  );

// PROGRAM

export const makeApp = (container: HTMLElement, connection: BifrostConnection) =>
  Runtime.makeElement({
    Model,
    init: () => ({
      model: { maybeUser: Option.none(), maybeError: Option.none() },
      commands: [FetchUser()],
    }),
    update,
    view,
    container,
    resources: Bifrost.layer(connection),
    devTools: import.meta.env.DEV ? { Message } : false,
  });
"""


def _readme(slug: str) -> str:
    return f"""\
# {slug}: a Bifrost V2 App in Foldkit (experimental)

This App uses [Foldkit](https://foldkit.dev) (The Elm Architecture on
[Effect](https://effect.website)) instead of React. Foldkit support is
experimental: the platform contract is the same as for React Apps, but the
`bifrost/core` API may change.

- `src/main.ts`: the platform `mount(el, bootstrap)` lifecycle.
- `src/bifrost.ts`: Bifrost as an Effect service, plus `currentUser`,
  `runWorkflow`, and `followWorkflow`. `tables` and `files` from
  `bifrost/core` work once the service is connected.
- `src/app.ts`: the program (Model, Message, update, view).

## Local dev

    bifrost app start

The command installs the `bifrost` SDK transiently from the selected instance.
After the first start, you can also run `npm run dev` directly.

## Deploy

    bifrost app deploy

Commit `package-lock.json` for reproducible builds. The `bifrost` package is
not listed in `package.json`; the platform supplies it at build time.
"""


def foldkit_scaffold_files(slug: str) -> dict[str, str]:
    """The files for a Foldkit V2 App skeleton (experimental)."""
    from bifrost.commands.solution import _v2_scaffold_files

    react = _v2_scaffold_files(slug)
    vite_config = react["vite.config.ts"]
    if _REACT_PLUGIN_IMPORT not in vite_config or _REACT_PLUGINS not in vite_config:
        raise RuntimeError("The React scaffold's vite.config.ts changed; update the Foldkit scaffold.")
    vite_config = (
        vite_config.replace(_REACT_PLUGIN_IMPORT, _FOLDKIT_PLUGIN_IMPORT)
        .replace(_REACT_PLUGINS, _FOLDKIT_PLUGINS)
        .replace("main.tsx", "main.ts")
    )
    index_html = react["index.html"].replace('src="/src/main.tsx"', 'src="/src/main.ts"')
    if 'src="/src/main.ts"' not in index_html:
        raise RuntimeError("The React scaffold's index.html changed; update the Foldkit scaffold.")

    pkg = {
        "name": slug,
        "private": True,
        "type": "module",
        "scripts": {
            "dev": "vite",
            "build": "vite build",
            "preview": "vite preview",
            "typecheck": "tsc --noEmit",
        },
        # The instance-specific `bifrost` SDK is deliberately absent here, as in
        # the React scaffold: `bifrost app start` installs it transiently and
        # deployed builds inject the serving instance's SDK tarball.
        "dependencies": {
            "foldkit": FOLDKIT_VERSION,
            "effect": EFFECT_VERSION,
            "@effect/platform-browser": EFFECT_VERSION,
        },
        "devDependencies": {
            "@foldkit/vite-plugin": FOLDKIT_VITE_PLUGIN_VERSION,
            "@foldkit/devtools": FOLDKIT_VERSION,
            "@foldkit/ui": FOLDKIT_VERSION,
            "@tailwindcss/vite": "^4.0.0",
            "tailwindcss": "^4.0.0",
            "tw-animate-css": "^1.2.0",
            "typescript": "^5.9.3",
            "vite": "^8.0.0",
        },
    }
    tsconfig = {
        "compilerOptions": {
            "target": "ES2022",
            "lib": ["ES2022", "DOM", "DOM.Iterable"],
            "module": "ESNext",
            "skipLibCheck": True,
            "moduleResolution": "bundler",
            "allowImportingTsExtensions": True,
            "isolatedModules": True,
            "noEmit": True,
            "strict": True,
            "types": ["vite/client"],
            "paths": {"@/*": ["./src/*"]},
        },
        "include": ["src"],
    }
    return {
        "package.json": json.dumps(pkg, indent=2) + "\n",
        "vite.config.ts": vite_config,
        "tsconfig.json": json.dumps(tsconfig, indent=2) + "\n",
        "index.html": index_html,
        "src/main.ts": _MAIN_TS,
        "src/bifrost.ts": _BIFROST_TS,
        "src/app.ts": _APP_TS,
        "src/index.css": react["src/index.css"],
        ".env.example": react[".env.example"],
        "README.md": _readme(slug),
    }
