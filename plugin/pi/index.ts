// Acyclic native workspace shim. Pi's CLI owns sessions, providers and tools.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { McpClient } from "../shared/mcp-client.mjs";
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
  let client: McpClient | undefined;
  const task = () => { if (!selected) throw new Error("Acyclic workspace owner is unavailable"); return selected; };

  function call(name: string, args: Action, signal?: AbortSignal): Promise<Response> {
    if (!client) {
      if (signal?.aborted) return Promise.reject(new Error("step=dispatch Pi coordination: cancelled; recovery=pi"));
      const location = task();
      bridge = spawn(executable, ["__mcp", "pi"], {cwd: location.cwd, windowsHide: true,
        env: {...process.env, ACYCLIC_WORKSPACE: location.cwd, ACYCLIC_TASK: location.task}});
      bridge.stderr.resume();
      client = new McpClient(bridge, {name: "acyclic-pi", version: "0.2.0", recovery: "pi"});
      ended = client.closed;
    }
    return client.request(name, args, signal) as Promise<Response>;
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
