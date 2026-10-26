// Acyclic native workspace shim. Pi's CLI owns sessions, providers and tools.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { Type } from "typebox";

const executable = "__ACYCLIC_EXECUTABLE__";
type Action = Record<string, unknown>;
type Location = { task: string; cwd: string };
type Response = { isError?: boolean; content: {type: string; text: string}[] };

function workspace<T>(cwd: string, action: Action): Promise<T> {
  return new Promise((yes, no) => {
    const child = execFile(executable, ["__workspace"], { cwd, windowsHide: true, maxBuffer: 65536 }, (error, stdout) => {
      if (error) { no(error); return; }
      try { yes(JSON.parse(stdout)); } catch (failure) { no(failure); }
    });
    child.stdin?.end(JSON.stringify(action));
  });
}

export default function acyclic(pi: ExtensionAPI) {
  let selected: Location | undefined;
  let bridge: ChildProcessWithoutNullStreams | undefined;
  let ended: Promise<void> | undefined;
  let sequence = 0;
  const pending = new Map<number, {yes: (value: Response) => void; no: (error: Error) => void}>();
  const task = () => { if (!selected) throw new Error("Acyclic workspace owner is unavailable"); return selected; };

  function call(name: string, args: Action, signal?: AbortSignal): Promise<Response> {
    if (!bridge) {
      const location = task();
      bridge = spawn(executable, ["__mcp", "pi"], {cwd: location.cwd, windowsHide: true,
        env: {...process.env, ACYCLIC_WORKSPACE: location.cwd, ACYCLIC_TASK: location.task}});
      bridge.stderr.resume();
      const stop = (error: Error) => { for (const entry of pending.values()) entry.no(error); pending.clear(); };
      createInterface({input: bridge.stdout}).on("line", line => {
        try {
          const message = JSON.parse(line);
          const entry = pending.get(message.id);
          if (!entry) return;
          pending.delete(message.id);
          if (message.error) entry.no(new Error(message.error.message)); else entry.yes(message.result);
        } catch { stop(new Error("Invalid Acyclic coordination response")); }
      });
      bridge.on("error", stop);
      bridge.stdin.on("error", stop);
      ended = new Promise(resolve => bridge!.once("close", () => {stop(new Error("Acyclic coordination session ended")); bridge = undefined; resolve();}));
      bridge.stdin.write(JSON.stringify({jsonrpc: "2.0", id: 0, method: "initialize", params: {protocolVersion: "2025-06-18", capabilities: {}, clientInfo: {name: "acyclic-pi", version: "0.2.0"}}}) + "\n");
      bridge.stdin.write(JSON.stringify({jsonrpc: "2.0", method: "notifications/initialized"}) + "\n");
    }
    const id = ++sequence;
    return new Promise<Response>((yes, no) => {
      const cancel = () => {
        bridge?.stdin.write(JSON.stringify({jsonrpc: "2.0", method: "notifications/cancelled", params: {requestId: id}}) + "\n");
        pending.delete(id); no(new Error("Coordination request cancelled"));
      };
      if (signal?.aborted) { cancel(); return; }
      signal?.addEventListener("abort", cancel, {once: true});
      pending.set(id, {yes: value => {signal?.removeEventListener("abort", cancel); yes(value);},
        no: error => {signal?.removeEventListener("abort", cancel); no(error);}});
      bridge!.stdin.write(JSON.stringify({jsonrpc: "2.0", id, method: "tools/call", params: {name, arguments: args}}) + "\n");
    });
  }

  pi.on("session_start", async (_event, ctx) => {
    selected = process.env.ACYCLIC_TASK ? {task: process.env.ACYCLIC_TASK, cwd: ctx.cwd}
      : await workspace<Location>(ctx.cwd, {kind: "open", host: "pi", session: ctx.sessionManager.getSessionId()});
    await workspace(ctx.cwd, {kind: "hook", host: "pi", event: "NativeSession",
      input: {task: task().task, native_session: {id: ctx.sessionManager.getSessionId(), file: ctx.sessionManager.getSessionFile() ?? null}}});
  });
  pi.on("agent_end", async (_event, ctx) => {
    if (selected) await workspace(ctx.cwd, {kind: "hook", host: "pi", event: "Stop",
      input: {task: selected.task}});
  });
  pi.on("session_shutdown", async (_event, ctx) => {
    bridge?.stdin.end(); await ended;
    if (selected) await workspace(ctx.cwd, {kind: "hook", host: "pi", event: "SessionEnd",
      input: {task: selected.task}});
  });

  for (const name of ["spawn", "send", "wait", "archive", "cancel", "resume"]) {
    const prompt = ["spawn", "send", "resume"].includes(name);
    pi.registerTool({name, label: `Acyclic ${name}`,
      description: `${name} a separate native Pi process in an admitted workspace view. Native subagents are unmanaged. Merge with acyclic fs merge agents/<reference>.`,
      parameters: Type.Object({reference: Type.String(), ...(prompt ? {prompt: Type.String()} : {})}, {additionalProperties: false}),
      async execute(_call, params, signal) {
        const response = await call(name, params, signal);
        if (response.isError) throw new Error(response.content.map(item => item.text).join("\n"));
        return {content: response.content.map(item => ({type: "text" as const, text: item.text})), details: response};
      },
    });
  }
}
