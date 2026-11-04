import path from "node:path";
import fs from "node:fs";
import { app, BrowserWindow, ipcMain, shell, session } from "electron";
import { autoUpdater } from "electron-updater";

import { partitionFor, validateServerUrl } from "./server-url.js";

const DEEP_LINK_SCHEME = "bifrost";
const SETTINGS_FILE = "settings.json";

let mainWindow: BrowserWindow | null = null;
let pendingDeepLink: string | null = null;

function settingsPath(): string {
  return path.join(app.getPath("userData"), SETTINGS_FILE);
}

function loadServerUrl(): string | null {
  try {
    const data = JSON.parse(fs.readFileSync(settingsPath(), "utf8")) as {
      serverUrl?: unknown;
    };
    return typeof data.serverUrl === "string" ? (validateServerUrl(data.serverUrl) ?? null) : null;
  } catch {
    return null;
  }
}

function saveServerUrl(origin: string): void {
  fs.mkdirSync(app.getPath("userData"), { recursive: true });
  fs.writeFileSync(settingsPath(), JSON.stringify({ serverUrl: origin }, null, 2));
}

function resolveServerUrl(): string | null {
  const override = process.env.BIFROST_URL;
  if (override) return validateServerUrl(override);
  return loadServerUrl();
}

function pickerHtml(error = ""): string {
  // Mirrors the hosted Login card (client/src/pages/Login.tsx) using the dark
  // Bifrost tokens (client/src/styles/bifrost-tokens.css). Self-contained: the
  // picker renders before any server is known, so no hosted assets are used.
  return `data:text/html;charset=utf-8,${encodeURIComponent(`<!doctype html>
<html><head><meta charset="utf-8"><title>Bifrost — Connect to Your Server</title>
<style>
:root{color-scheme:dark}
body{font-family:"Inter",system-ui,-apple-system,"Segoe UI",sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background:#08090b;color:#f7f9fb;padding:16px;box-sizing:border-box}
.card{width:min(448px,100%);background:#0a0c0f;border:1px solid #252c34;border-radius:8px;overflow:hidden}
.bridge{height:4px;background:linear-gradient(90deg,#ff5a5a,#ffb24d,#ffe24d,#3ad17a,#2fd4d4,#4d6bff,#7a6bff,#d96bff)}
.body{padding:32px}
.mark{width:64px;height:64px;border-radius:8px;margin:0 auto 16px;background:linear-gradient(135deg,#2fd4d4,#4d6bff);display:flex;align-items:center;justify-content:center;font-family:"Prompt","Inter",system-ui,sans-serif;font-size:32px;font-weight:600;color:#06222a}
h1{font-family:"Prompt","Inter",system-ui,sans-serif;font-size:24px;font-weight:600;letter-spacing:-0.01em;text-align:center;margin:0 0 4px}
p.desc{text-align:center;color:#9caab5;font-size:16px;margin:0 0 24px}
label{display:block;font-size:14px;font-weight:500;margin-bottom:8px}
input{width:100%;min-height:44px;box-sizing:border-box;padding:0 12px;border-radius:6px;border:1px solid #3d4853;background:#11151a;color:#f7f9fb;font-size:14px}
input:focus{outline:2px solid #2fd4d4;outline-offset:1px;border-color:#2fd4d4}
button{width:100%;min-height:44px;margin-top:16px;border-radius:6px;border:0;background:#2fd4d4;color:#06222a;font-size:14px;font-weight:600;cursor:pointer}
button:hover{background:#63e1e1}
.error{color:#ff727a;min-height:1.4em;font-size:14px;margin-top:8px}
.hint{color:#9caab5;font-size:12px;margin-top:16px;text-align:center}
</style>
</head><body><div class="card"><div class="bridge"></div><div class="body">
<div class="mark">B</div>
<h1>Connect to Bifrost</h1>
<p class="desc">Sign in through your hosted server</p>
<form id="f"><label for="url">Server URL</label>
<input id="url" type="url" placeholder="https://bifrost.example.com" autocomplete="url" autofocus required>
<div class="error">${error}</div><button type="submit">Save &amp; Connect</button></form>
<p class="hint">Use https:// — http://localhost works for local dev.</p>
</div></div>
<script>document.getElementById('f').onsubmit = async (e) => {
e.preventDefault();
const r = await window.bifrostSettings.setServerUrl(document.getElementById('url').value);
if (!r.ok) document.querySelector('.error').textContent = r.error; };</script></body></html>`)}`;
}

function showPicker(error = ""): void {
  if (!mainWindow) return;
  void mainWindow.loadURL(pickerHtml(error));
}

function loadServer(origin: string): void {
  if (!mainWindow) return;
  void mainWindow.loadURL(origin);
}

function forwardDeepLink(url: string): void {
  if (url.startsWith(`${DEEP_LINK_SCHEME}://`)) {
    if (mainWindow) mainWindow.webContents.send("bifrost:auth-callback", url);
    else pendingDeepLink = url;
  }
}

function createMainWindow(): void {
  const serverUrl = resolveServerUrl();
  const mainSession = serverUrl
    ? session.fromPartition(partitionFor(serverUrl))
    : session.defaultSession;

  mainWindow = new BrowserWindow({
    width: 1280,
    height: 860,
    autoHideMenuBar: true,
    webPreferences: {
      session: mainSession,
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  // Popups to another origin (e.g. external IdP) open in the system browser;
  // same-origin popups stay in the shell. Top-level navigation always stays
  // in-window so hosted IdP redirect flows can complete and return.
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (!serverUrl) return { action: "deny" };
    let target: URL;
    try {
      target = new URL(url);
    } catch {
      return { action: "deny" };
    }
    if (target.origin === serverUrl) return { action: "allow" };
    if (target.protocol === "https:" || target.protocol === "http:") {
      void shell.openExternal(url);
    }
    return { action: "deny" };
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
  });

  mainWindow.webContents.on("did-finish-load", () => {
    if (pendingDeepLink && mainWindow) {
      mainWindow.webContents.send("bifrost:auth-callback", pendingDeepLink);
      pendingDeepLink = null;
    }
  });

  if (serverUrl) loadServer(serverUrl);
  else showPicker();
}

ipcMain.handle("bifrost-settings:get-server-url", () => resolveServerUrl());

ipcMain.handle(
  "bifrost-settings:set-server-url",
  (event, raw: unknown): { ok: true } | { ok: false; error: string } => {
    if (typeof raw !== "string") return { ok: false, error: "URL must be a string." };
    const origin = validateServerUrl(raw);
    if (!origin) {
      return { ok: false, error: "Use https://, or http://localhost for local dev." };
    }
    saveServerUrl(origin);
    // Recreate the window so the new origin gets its own session partition.
    const win = BrowserWindow.fromWebContents(event.sender);
    win?.close();
    createMainWindow();
    return { ok: true };
  },
);

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on("second-instance", (_event, argv) => {
    const deepLink = argv.find((a) => a.startsWith(`${DEEP_LINK_SCHEME}://`));
    if (deepLink) forwardDeepLink(deepLink);
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });

  app.on("open-url", (event, url) => {
    event.preventDefault();
    forwardDeepLink(url);
  });

  app.whenReady().then(() => {
    if (process.platform !== "darwin") {
      const link = process.argv.find((a) => a.startsWith(`${DEEP_LINK_SCHEME}://`));
      if (link) pendingDeepLink = link;
    }
    app.setAsDefaultProtocolClient(DEEP_LINK_SCHEME);
    createMainWindow();
    if (app.isPackaged) {
      try {
        void autoUpdater.checkForUpdatesAndNotify();
      } catch {
        // No update feed configured yet; launches must not fail because of it.
      }
    }
    app.on("activate", () => {
      if (BrowserWindow.getAllWindows().length === 0) createMainWindow();
    });
  });

  app.on("window-all-closed", () => {
    if (process.platform !== "darwin") app.quit();
  });
}
