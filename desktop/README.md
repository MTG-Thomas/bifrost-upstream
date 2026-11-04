# Bifrost Desktop (hosted-only shell)

Thin Electron wrapper around a **hosted** Bifrost instance. It bundles no
backend, Postgres, Redis, or client dist — it `loadURL`s the configured server,
so same-origin `/api`, `/ws`, cookies, Web Locks, and WebSocket host logic work
with zero client changes.

## Run

```bash
cd desktop
npm install
npm run dev                    # uses saved server URL, or shows the picker
BIFROST_URL=https://bifrost.example.com npm run dev   # one-shot override (not saved)
BIFROST_URL=http://localhost:3000 npm run dev         # local dev only
```

Server URL rules: `https://` only, except `http://localhost` for local dev.
The saved URL lives in Electron `userData/settings.json` (OS-appropriate app
data dir — no home-dir dotfiles). One session partition per server origin, so
switching servers does not leak cookies between origins.

## Package

```bash
cd desktop
npm run dist
```

Outputs (electron-builder):

- Linux: `dist/Bifrost-*.AppImage`, `dist/*.deb`
- macOS: `dist/Bifrost-*.dmg`, `dist/*.zip`
- Windows: `dist/Bifrost-*.exe` (NSIS)

`npm run build` typechecks and emits `dist/main.js` + `dist/preload.js`
(`npx tsc --noEmit` for a check-only pass).

## Behavior notes

- Single-instance lock; a second launch focuses the window.
- Popups to another origin (e.g. an external IdP) open in the system browser.
  Top-level navigation stays in-window so hosted IdP redirect flows complete.
- Deep link `bifrost://auth/callback` is registered and forwarded to the window
  as a `bifrost:auth-callback` IPC message.
- Preload exposes only `window.bifrostSettings.getServerUrl/setServerUrl`.
  Auth stays in the session — no token handling in the shell.
- Auto-update (`electron-updater`) runs on launch in packaged builds once a
  publish feed is configured; launch never fails if it is not.
