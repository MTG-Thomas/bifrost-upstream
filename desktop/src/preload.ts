import { contextBridge, ipcRenderer } from "electron";

declare global {
  interface Window {
    bifrostSettings: {
      getServerUrl: () => Promise<string | null>;
      setServerUrl: (url: string) => Promise<{ ok: true } | { ok: false; error: string }>;
    };
  }
}

// Minimal settings bridge only. Auth stays in the session (cookies/localStorage);
// no token ever crosses this bridge.
contextBridge.exposeInMainWorld("bifrostSettings", {
  getServerUrl: (): Promise<string | null> =>
    ipcRenderer.invoke("bifrost-settings:get-server-url"),
  setServerUrl: (url: string): Promise<{ ok: true } | { ok: false; error: string }> =>
    ipcRenderer.invoke("bifrost-settings:set-server-url", url),
});
