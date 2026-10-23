// Acyclic native workspace shim. Pi's SDK owns sessions, providers and tools.
import { createAgentSession, DefaultResourceLoader, getAgentDir, SessionManager,
  type AgentSession, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { Type } from "typebox";

const executable = "__ACYCLIC_EXECUTABLE__";
type Location = { task: string; cwd: string };
type Action = Record<string, unknown>;
const thisExtension = fileURLToPath(import.meta.url);

function workspace<T>(cwd: string, action: Action): Promise<T> {
  return new Promise((yes, no) => {
    const child = execFile(executable, ["__workspace"], { cwd, windowsHide: true, maxBuffer: 65536 }, (error, stdout) => {
      if (error) { no(error); return; }
      try { yes(JSON.parse(stdout)); } catch (failure) { no(failure); }
    });
    child.stdin?.end(JSON.stringify(action));
  });
}

function result(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value) }], details: value };
}

export async function drainNativeSession(native: AgentSession, finished: Promise<void>) {
  await native.abort();
  await native.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
  await finished.catch(() => {});
}

export default function acyclic(pi: ExtensionAPI) { install(pi); }

function install(pi: ExtensionAPI, binding?: Location, inheritedRoot?: string) {
  let selected = binding;
  let rootSession = inheritedRoot;
  const children = new Map<string, { native: AgentSession; finished: Promise<void>; location: Location }>();
  const task = () => { if (!selected) throw new Error("Acyclic workspace owner is unavailable"); return selected; };
  const nativeHook = (ctx: ExtensionContext, event: string, extra: Action = {}) => workspace(ctx.cwd, {
    kind: "hook", host: "pi", event,
    input: { session_id: rootSession, cwd: ctx.cwd, task: task().task, ...extra },
  });

  pi.on("session_start", async (_event, ctx) => {
    if (!binding) {
      rootSession = ctx.sessionManager.getSessionId();
      selected = await workspace<Location>(ctx.cwd, { kind: "open", host: "pi", session: rootSession });
    }
  });
  pi.on("tool_call", async (event, ctx) => {
    if (["spawn_agent", "wait_agent", "cancel_agent", "reopen_agent"].includes(event.toolName)) return;
    try {
      const answer = await nativeHook(ctx, "PreToolUse", { tool_name: event.toolName, tool_use_id: event.toolCallId, tool_input: event.input }) as {hookSpecificOutput?: {updatedInput?: Record<string, unknown>}};
      if (answer.hookSpecificOutput?.updatedInput) Object.assign(event.input, answer.hookSpecificOutput.updatedInput);
    } catch (error) { return { block: true, reason: String(error) }; }
  });
  pi.on("tool_result", async (event, ctx) => {
    if (["spawn_agent", "wait_agent", "cancel_agent", "reopen_agent"].includes(event.toolName)) return;
    await nativeHook(ctx, "PostToolUse", { tool_name: event.toolName, tool_use_id: event.toolCallId, tool_input: event.input, tool_response: event.content });
  });
  pi.on("session_shutdown", async (_event, ctx) => {
    for (const child of children.values()) await drainNativeSession(child.native, child.finished);
    if (selected) await workspace(ctx.cwd, { kind: "capture", task: selected.task });
    for (const child of children.values()) child.native.dispose();
  });

  async function start(location: Location, manager: SessionManager, prompt: string, ctx: ExtensionContext, restore = false) {
    const loader = new DefaultResourceLoader({
      cwd: location.cwd, agentDir: getAgentDir(),
      extensionFactories: [api => install(api, location, rootSession)],
      extensionsOverride: base => ({ ...base, extensions: base.extensions.filter(extension => resolve(extension.resolvedPath) !== resolve(thisExtension)) }),
    });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: location.cwd, model: restore ? undefined : ctx.model,
      scopedModels: [...ctx.scopedModels], thinkingLevel: restore ? undefined : pi.getThinkingLevel(),
      tools: pi.getActiveTools(), sessionManager: manager, resourceLoader: loader });
    try {
      await session.bindExtensions({ uiContext: ctx.ui, mode: ctx.mode });
      await nativeHook(ctx, "NativeSession", {task: location.task,
        native_session: {id: session.sessionId, file: session.sessionFile ?? null}});
    } catch (error) {
      try { await drainNativeSession(session, Promise.resolve()); } finally { session.dispose(); }
      throw error;
    }
    const finished = session.prompt(prompt).finally(async () => {
      await workspace(location.cwd, { kind: "capture", task: location.task });
    });
    finished.catch(() => {}); // The wait tool reports the native SDK outcome.
    const child = {native: session, finished, location};
    children.set(location.task, child);
    return child;
  }

  pi.registerTool({
    name: "spawn_agent", label: "Native subagent", description: "Fork this task's workspace and start a native Pi SDK session. Merge with acyclic fs merge agents/<task>.",
    parameters: Type.Object({ prompt: Type.String(), fresh: Type.Optional(Type.Boolean()) }),
    async execute(call, params, _signal, _update, ctx) {
      const parent = task();
      const entries = ctx.sessionManager.getBranch();
      if (entries.length > 4096) throw new Error("Native branch exceeds the context entry bound");
      const boundary = entries.find(entry => entry.type === "message" && entry.message.role === "assistant" &&
        entry.message.content.some(part => part.type === "toolCall" && part.id === call));
      if (!params.fresh && !boundary) throw new Error("Spawn invocation is absent from Pi's current native branch");
      const location = await workspace<Location>(ctx.cwd, { kind: "fork", parent: parent.task, reference: call });
      const source = ctx.sessionManager.getSessionFile();
      const manager = params.fresh ? (source ? SessionManager.create(location.cwd) : SessionManager.inMemory(location.cwd)) : source
        ? SessionManager.forkFrom(source, location.cwd)
        : SessionManager.inMemory(location.cwd, undefined, entries.slice(0, entries.indexOf(boundary!)));
      if (!params.fresh && source) {
        if (boundary!.parentId) manager.branch(boundary!.parentId); else manager.resetLeaf();
      }
      await start(location, manager, params.prompt, ctx);
      return result(location);
    },
  });
  pi.registerTool({
    name: "wait_agent", label: "Wait for native subagent", description: "Wait for a native Pi session and capture its completed writes.",
    parameters: Type.Object({ task: Type.String() }),
    async execute(_call, params) {
      const child = children.get(params.task);
      if (!child) throw new Error("Native child session is not open");
      await child.finished;
      return result({ ...child.location, completed: true });
    },
  });
  pi.registerTool({
    name: "cancel_agent", label: "Cancel native subagent", description: "Abort through Pi, then cancel shared task ownership.",
    parameters: Type.Object({ task: Type.String() }),
    async execute(_call, params, _signal, _update, ctx) {
      const child = children.get(params.task);
      if (!child) throw new Error("Native child session is not open");
      await drainNativeSession(child.native, child.finished);
      const value = await workspace(ctx.cwd, { kind: "cancel", task: params.task });
      child.native.dispose(); children.delete(params.task);
      return result(value);
    },
  });
  pi.registerTool({
    name: "reopen_agent", label: "Reopen native subagent", description: "Resume a stopped native Pi session in its retained task view.",
    parameters: Type.Object({ task: Type.String(), prompt: Type.String() }),
    async execute(_call, params, _signal, _update, ctx) {
      const child = children.get(params.task);
      if (!child) {
        const location = await workspace<Location>(ctx.cwd, { kind: "reopen", task: params.task });
        const saved = await nativeHook(ctx, "NativeSession", {task: params.task}) as {id: string; file: string | null};
        if (!saved.file) throw new Error("Pi's in-memory session has no persisted transcript to reopen");
        await start(location, SessionManager.forkFrom(saved.file, location.cwd), params.prompt, ctx, true);
        return result(location);
      }
      await child.finished;
      await workspace(ctx.cwd, { kind: "reopen", task: params.task });
      child.finished = child.native.prompt(params.prompt).finally(async () => {
        await workspace(child.location.cwd, { kind: "capture", task: params.task });
      });
      child.finished.catch(() => {});
      return result(child.location);
    },
  });
}
