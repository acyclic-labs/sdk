/** One headless OpenCode worker in a directory, on a model, with a time cap.
 *  stdin is closed on purpose: OpenCode reads piped input when stdin is not a TTY and would wait forever. */
import { spawn, spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** OpenCode keeps one SQLite store per data dir; two workers on one store fail with "database is locked".
 *  Each worker gets its own XDG_DATA_HOME, with the user's OpenCode auth copied in so logged-in providers keep working. */
function isolatedDataHome(): string {
  const home = mkdtempSync(join(tmpdir(), "arena-oc-"));
  const real = process.env.XDG_DATA_HOME ?? join(process.env.HOME ?? "", ".local", "share");
  const auth = join(real, "opencode", "auth.json");
  if (existsSync(auth)) { mkdirSync(join(home, "opencode"), { recursive: true }); copyFileSync(auth, join(home, "opencode", "auth.json")); }
  return home;
}

export interface WorkerResult {
  model: string;
  dir: string;
  ok: boolean;
  exitCode: number | null;
  timedOut: boolean;
  ms: number;
  cost: number;
  tokens: { input: number; output: number; reasoning: number; cacheWrite: number; cacheRead: number };
  steps: number;
  toolCalls: number;
  text: string;
  error?: string;
}

export interface WorkerOptions {
  timeoutMs?: number;
  binary?: string;
  onEvent?: (e: { type: string; part?: unknown }) => void;
  env?: NodeJS.ProcessEnv;
}

export function runWorker(dir: string, model: string, task: string, opts: WorkerOptions = {}): Promise<WorkerResult> {
  const binary = opts.binary ?? process.env.OPENCODE_BIN ?? "opencode";
  const timeoutMs = opts.timeoutMs ?? 10 * 60_000;
  const t0 = Date.now();
  const res: WorkerResult = { model, dir, ok: false, exitCode: null, timedOut: false, ms: 0, cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cacheWrite: 0, cacheRead: 0 }, steps: 0, toolCalls: 0, text: "" };
  const dataHome = isolatedDataHome();
  const cleanup = () => { try { rmSync(dataHome, { recursive: true, force: true }); } catch { /* best effort */ } };
  return new Promise((resolve) => {
    const p = spawn(binary, ["run", "--format", "json", "-m", `openrouter/${model}`, "--dir", dir, task],
      { stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, XDG_DATA_HOME: dataHome, ...opts.env } });
    let buf = "", err = "";
    const timer = setTimeout(() => { res.timedOut = true; p.kill("SIGKILL"); }, timeoutMs);
    const handle = (line: string) => {
      if (!line.trim()) return;
      let e: { type?: string; part?: Record<string, unknown> };
      try { e = JSON.parse(line); } catch { return; }
      const part = e.part ?? {};
      if (e.type === "step_finish") {
        res.steps += 1;
        res.cost += Number(part.cost ?? 0);
        const t = (part.tokens ?? {}) as Record<string, unknown>;
        const cache = (t.cache ?? {}) as Record<string, unknown>;
        res.tokens.input += Number(t.input ?? 0); res.tokens.output += Number(t.output ?? 0);
        res.tokens.reasoning += Number(t.reasoning ?? 0);
        res.tokens.cacheWrite += Number(cache.write ?? 0); res.tokens.cacheRead += Number(cache.read ?? 0);
      } else if (e.type === "tool_use") res.toolCalls += 1;
      else if (e.type === "text") res.text += String(part.text ?? "");
      else if (e.type === "error") res.error = JSON.stringify(part).slice(0, 400);
      opts.onEvent?.({ type: e.type ?? "?", part });
    };
    p.stdout.on("data", (d) => { buf += d.toString(); const lines = buf.split("\n"); buf = lines.pop() ?? ""; lines.forEach(handle); });
    p.stderr.on("data", (d) => { err += d.toString(); });
    p.on("close", (code) => {
      clearTimeout(timer); handle(buf);
      res.exitCode = code; res.ms = Date.now() - t0;
      res.ok = code === 0 && !res.timedOut && !res.error;
      if (!res.ok && !res.error) res.error = res.timedOut ? `timed out after ${timeoutMs} ms` : err.trim().split("\n").slice(-3).join(" | ").slice(0, 400);
      cleanup(); resolve(res);
    });
    p.on("error", (e) => { clearTimeout(timer); res.error = String(e); res.ms = Date.now() - t0; cleanup(); resolve(res); });
  });
}

export function opencodeAvailable(binary = process.env.OPENCODE_BIN ?? "opencode"): string | null {
  const p = spawnSync(binary, ["--version"], { encoding: "utf8" });
  return !p.error && p.status === 0 ? p.stdout.trim() : null;
}
