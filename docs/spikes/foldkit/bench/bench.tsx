import "./styles.css";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { ExecutionLogsPanel, type LogEntry } from "@/components/execution/ExecutionLogsPanel";
import { FoldkitExecutionLogsPanel } from "@/foldkit/executionLogs/FoldkitExecutionLogsPanel";

const line = (i: number): LogEntry => ({ id: i, sequence: i, level: i % 25 === 0 ? "ERROR" : "INFO", message: `step ${i}: processed ticket ${i * 7} for tenant contoso`, timestamp: new Date(1763460000000 + i * 1000).toISOString(), ...(i % 40 === 0 ? { data: { ticket: i, ok: true } } : {}) } as LogEntry);
const waitFor = (pred: () => boolean) => new Promise<void>((resolve) => { const tick = () => (pred() ? resolve() : requestAnimationFrame(tick)); tick(); });
const frame = () => new Promise((r) => requestAnimationFrame(() => r(null)));

declare global { interface Window { runBench: (renderer: string, base: number, appends: number) => Promise<unknown>; } }
window.runBench = async (renderer, base, appends) => {
  const host = document.getElementById("root")!;
  const root = createRoot(host);
  const Panel = renderer === "foldkit" ? FoldkitExecutionLogsPanel : ExecutionLogsPanel;
  let logs = Array.from({ length: base }, (_, i) => line(i + 1));
  const props = { status: "Running" as const, isConnected: true, canReadDiagnostics: true, maxHeight: "600px" };
  const t0 = performance.now();
  flushSync(() => root.render(<Panel logs={logs} {...props} />));
  await waitFor(() => host.textContent!.includes(`step ${base}:`));
  const initialMs = performance.now() - t0;
  const latencies: number[] = [];
  const t1 = performance.now();
  for (let i = 1; i <= appends; i++) {
    const n = base + i;
    logs = [...logs, line(n)];
    const s = performance.now();
    flushSync(() => root.render(<Panel logs={logs} {...props} />));
    await waitFor(() => host.textContent!.includes(`step ${n}:`));
    latencies.push(performance.now() - s);
    await frame();
  }
  const streamMs = performance.now() - t1;
  // filter keystroke latency
  const input = host.querySelector<HTMLInputElement>("input")!;
  const s2 = performance.now();
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!;
  setter.call(input, "ticket 70");
  input.dispatchEvent(new Event("input", { bubbles: true }));
  await waitFor(() => !host.textContent!.includes("step 2:"));
  const filterMs = performance.now() - s2;
  root.unmount();
  latencies.sort((a, b) => a - b);
  return { renderer, base, appends, initialMs: +initialMs.toFixed(1), streamMs: +streamMs.toFixed(1), p50: +latencies[Math.floor(appends * 0.5)].toFixed(2), p95: +latencies[Math.floor(appends * 0.95)].toFixed(2), filterMs: +filterMs.toFixed(1), domNodes: 0 };
};
