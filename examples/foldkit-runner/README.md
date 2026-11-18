# Foldkit Runner

This is an independent V2 Bifrost App written in [Foldkit](https://foldkit.dev) instead of React. It is a fork-only exploration; see [docs/spikes/foldkit-apps.md](../../docs/spikes/foldkit-apps.md).

What it does:

- Reads the signed-in user.
- Runs the `workflows/spike_log_stream.py::spike_log_stream` workflow (from the Foldkit spike) with a line count and an optional failure.
- Polls the execution until it settles, then shows the result or error.
- Keeps a list of runs from this visit.

How it fits the platform:

- `index.html` declares `mount-v1`.
- `src/main.ts` exports `mount(mountEl, bootstrap)`, embeds the Foldkit program, and returns its teardown.
- `src/bifrost.ts` is a small Effect client for the same HTTP contract the React SDK uses: bearer token (preferring the host's refresh bridge), `X-Bifrost-Org`, and `X-Bifrost-App`.

## Deploy

```bash
bifrost app create foldkit-runner --name "Foldkit Runner" --global   # creates the App record and .env binding
# replace the generated React sources with this directory's files
npm install
bifrost app deploy .
```

**Do not commit a `package-lock.json` yet.** Bifrost injects its `bifrost` SDK package into every App build. That package declares `react` and `react-dom` as required peer dependencies. A locked build grafts only the SDK's own lock entry into the App's lockfile, so `npm ci` fails with `Missing: react from lock file`. Unlocked builds succeed, and npm installs React unused. The fix belongs in the SDK package; see the exploration doc.
