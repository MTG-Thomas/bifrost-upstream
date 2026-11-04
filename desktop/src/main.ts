import path from "node:path";
import fs from "node:fs";
import { app, BrowserWindow, ipcMain, shell, session } from "electron";
import { autoUpdater } from "electron-updater";

const DEEP_LINK_SCHEME = "bifrost";
const SETTINGS_FILE = "settings.json";

let mainWindow: BrowserWindow | null = null;
let pendingDeepLink: string | null = null;

// https only, except http://localhost for local dev.
function validateServerUrl(raw: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(raw.trim());
  } catch {
    return null;
  }
  if (parsed.protocol === "https:") return parsed.origin;
  if (parsed.protocol === "http:" && parsed.hostname === "localhost") return parsed.origin;
  return null;
}

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

function partitionFor(origin: string): string {
  return `persist:bifrost-${origin.replace(/[^a-zA-Z0-9]/g, "-")}`;
}

function pickerHtml(error = ""): string {
  return `data:text/html;charset=utf-8,${encodeURIComponent(`<!doctype html>
<html><head><meta charset="utf-8"><title>Bifrost — Server Setup</title>
<style>body{font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#111827;color:#f9fafb}
form{display:flex;flex-direction:column;gap:12px;width:min(420px,90vw)}input{padding:10px;border-radius:6px;border:1px solid #374151;background:#1f2937;color:#fff}
button{padding:10px;border-radius:6px;border:0;background:#2563eb;color:#fff;cursor:pointer}.error{color:#f87171;min-height:1.2em;font-size:14px}</style>
</head><body><form id="f"><h2>Connect to Bifrost</h2>
<input id="url" type="url" placeholder="https://bifrost.example.com" required>
<div class="error">${error}</div><button type="submit">Save &amp; Connect</button></form>
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
