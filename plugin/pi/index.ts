import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

// Installation substitutes one JSON string; no shell interprets it.
const executable = "__ACYCLIC_EXECUTABLE__";
const maximum = 64 * 1024;
// Canonical value once, its escaped text (at most twice), plus native fields.
const maximumAnswer = 3 * maximum + 1024;
type Answer = Record<string, unknown>;
export type Invoke = (event: string, input: Answer, signal?: AbortSignal) => Promise<Answer>;

/** The common Rust client owns normalization, authentication, append and follow. */
const invokeProcess: Invoke = (event, input) => new Promise((resolve, reject) => {
  const child = spawn(executable, event === "catalog" ? ["__catalog"] : ["__hook", "pi", event], { windowsHide: true, stdio: "pipe" });
  const output: Buffer[] = [];
  let bytes = 0;
  child.on("error", reject);
  child.stdin.on("error", reject);
  child.stdout.on("data", (chunk: Buffer) => {
    bytes += chunk.length;
    if (bytes > maximumAnswer) { child.kill(); reject(new Error("Harness hook answer exceeds its bound")); }
    else output.push(chunk);
  });
  // Diagnostics go to the native host; task output comes only from followed files.
  child.stderr.on("data", (chunk: Buffer) => process.stderr.write(chunk));
  child.on("close", code => {
    try {
      const answer: unknown = JSON.parse(Buffer.concat(output).toString("utf8"));
      if (!answer || typeof answer !== "object" || Array.isArray(answer)) throw new Error("Invalid harness hook answer");
      if (code !== 0 && !("block" in answer && answer.block === true)) throw new Error("Harness hook failed");
      resolve(answer as Answer);
    } catch (error) { reject(error); }
  });
  if (event === "catalog") { child.stdin.end(); return; }
  const payload = JSON.stringify(input);
  if (Buffer.byteLength(payload) > maximum) { child.kill(); reject(new Error("Native hook exceeds its bound")); }
  else child.stdin.end(payload);
});

/** Abort writes a cancellation file; the original process stays until settlement. */
export const invoke: Invoke = async (event, input, signal) => {
  if (signal?.aborted) throw new DOMException("Harness tool aborted before execution", "AbortError");
  type Reply = { answer: Answer } | { error: unknown };
  const observe = (pending: Promise<Answer>): Promise<Reply> => pending.then(
    answer => ({ answer }), error => ({ error }),
  );
  let cancellation: Promise<Reply> | undefined;
  const abort = () => {
    const { sessionId, cwd, toolCallId, agentId, threadId, turnId } = input;
    cancellation ??= observe(invokeProcess("tool_cancel", { sessionId, cwd, toolCallId, agentId, threadId, turnId }));
  };
  const original = observe(invokeProcess(event, input));
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  const reply = await original;
  signal?.removeEventListener("abort", abort);
  const cancelled = await cancellation;
  if (cancelled && "error" in cancelled) throw cancelled.error;
  if ("error" in reply) throw reply.error;
  if (signal?.aborted) throw new DOMException("Harness tool aborted after settlement", "AbortError");
  return reply.answer;
};

function identity(ctx: ExtensionContext) {
  return { sessionId: ctx.sessionManager.getSessionId(), cwd: ctx.cwd };
}

type Publish = (task: string, path: string, files: Iterable<{key:{Input:string},channel:null,bytes:number[]}>) => Promise<void>;
const publish: Publish = async (task,path,files) => {
  const child=spawn(executable,["__files",task,path],{windowsHide:true,stdio:["pipe","ignore","pipe"]});
  const closed=new Promise<void>((resolve,reject)=> {
    child.on("error",reject);
    child.on("close",code=>code===0 ? resolve() : reject(new Error("Native source publication failed")));
  });
  child.stderr.on("data",(chunk:Buffer)=>process.stderr.write(chunk));
  const sent=pipeline(Readable.from((function*(){for(const file of files) yield JSON.stringify(file)+"\n";})()),child.stdin);
  try {
    await Promise.all([sent,closed]);
  } catch (error) {child.kill();await Promise.allSettled([sent,closed]);throw error;}
};

async function nativeSpawn(input: unknown, ctx: ExtensionContext, toolCallId: string, task: unknown, write: Publish) {
  if (!input || typeof input !== "object" || !("id" in input) || "executor" in input) return input;
  if (!ctx.model || !process.argv[1]) throw new Error("Native spawn needs the current model and executable");
  const owner = ctx.sessionManager.getBranch().findLast(entry => entry.type === "message" &&
    entry.message.role === "assistant" && entry.message.content.some(part => part.type === "toolCall" && part.id === toolCallId));
  if (!owner) throw new Error("Native spawn has no owning assistant invocation");
  // Retrying the same native invocation selects the same SDK session and seed;
  // no process-local operation cache can lose this identity after a restart.
  const bytes = createHash("sha256").update("acyclic.pi-native-session/v1\0")
    .update(ctx.sessionManager.getSessionId()).update("\0").update(toolCallId).digest().subarray(0,16);
  bytes[6] = (bytes[6]! & 15) | 128;
  bytes[8] = (bytes[8]! & 63) | 128;
  const hex = bytes.toString("hex");
  const id = `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
  const session = SessionManager.inMemory(ctx.cwd,{id});
  const header = session.getHeader();
  if (!header) throw new Error("Native SDK omitted the child session header");
  header.timestamp = owner.timestamp;
  const text = (value: string) => ({ kind: "text", value });
  const args: Array<Record<string, string>> = [process.argv[1], "--print", "--mode", "json", "--session"].map(text);
  args.push({ kind: "session_file" });
  args.push(...["--provider", ctx.model.provider, "--model", ctx.model.id,
    "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", fileURLToPath(import.meta.url)].map(text));
  if (process.env.ACYCLIC_PI_PROVIDER_EXTENSION) args.push(...["-e", process.env.ACYCLIC_PI_PROVIDER_EXTENSION].map(text));
  args.push(text("--"), { kind: "prompt" });
  let selected: Record<string,unknown> | undefined;
  if ("context" in input && input.context === "native_cut") {
    if (typeof task !== "string") throw new Error("Native source has no admitted task");
    const snapshot=()=>[ctx.sessionManager.getHeader(),...ctx.sessionManager.getEntries()].map(row=>JSON.stringify(row)).join("\n")+"\n";
    const original=snapshot();
    const head=ctx.sessionManager.getLeafId();
    const source=Buffer.from(original);
    const hash=createHash("sha256").update(source).digest();
    const prefix=`workspace/native/${ctx.sessionManager.getSessionId()}/${hash.toString("hex")}`;
    const path=`${prefix}/manifest.json`;
    function* files() {
      for(let offset=0;offset<source.length;offset+=8192) yield {
        key:{Input:`${prefix}/chunk-${offset/8192}`},channel:null as null,bytes:Array.from(source.subarray(offset,offset+8192)),
      };
      yield {key:{Input:path},channel:null as null,bytes:Array.from(Buffer.from(JSON.stringify({chunks:Math.ceil(source.length/8192),length:source.length})))};
    }
    await write(task,path,files());
    if(snapshot()!==original || ctx.sessionManager.getLeafId()!==head) throw new Error("Native source changed before admission");
    selected={manifest:path,head,sha256:Array.from(hash)};
  }
  return { ...input, executor: {
    session: { host: "pi", session_id: session.getSessionId(), caller: null },
    executable: process.execPath, arguments: args, session_bytes: JSON.stringify(header), timeout_ms: 300_000,
    ...(selected ? {context:selected} : {}),
  } };
}

/** Tool definitions and execution both come from the harness registry. */
export default function extension(pi: ExtensionAPI, send: Invoke = invoke, write: Publish = publish) {
  const registered = new Set<string>();
  let task: unknown;
  pi.on("session_start", async (_event, ctx) => {
    task=(await send("session_start", identity(ctx))).task;
    const catalog = await send("catalog", {});
    if (!Array.isArray(catalog.tools)) throw new Error("Invalid harness tool catalog");
    for (const tool of catalog.tools) {
      if (!tool || typeof tool.name !== "string" || typeof tool.description !== "string" ||
          !tool.input_schema || typeof tool.input_schema !== "object") throw new Error("Invalid harness tool definition");
      pi.registerTool({
        name: tool.name, label: tool.name, description: tool.description,
        parameters: tool.input_schema as ToolDefinition["parameters"],
        renderCall: args => new Text(tool.name + " " + JSON.stringify(args), 0, 0),
        renderResult: result => new Text(result.content.map(block => block.type === "text" ? block.text : "").join("\n"), 0, 0),
        async execute(toolCallId, input, signal, _onUpdate, context) {
          if (tool.name === "acyclic_spawn") input = await nativeSpawn(input, context, toolCallId, task, write);
          const result = await send("tool_call", { toolCallId, toolName: tool.name, input, ...identity(context) }, signal);
          if (!Array.isArray(result.content) || result.isError !== false) throw new Error("Invalid harness tool result");
          return { content: result.content, details: result.details };
        },
      });
      registered.add(tool.name);
    }
    pi.setActiveTools([...new Set([...pi.getActiveTools(), ...registered])]);
  });
  pi.on("tool_call", async (event, ctx) => {
    if (!registered.has(event.toolName)) {
      const answer = await send("tool_call", { ...event, ...identity(ctx), hostExecuted: true });
      if (!answer || typeof answer !== "object" || Array.isArray(answer) ||
          typeof answer.block !== "boolean" ||
          (answer.reason !== undefined && typeof answer.reason !== "string")) {
        throw new Error("Invalid harness native-tool decision");
      }
      if (answer.block) return { block: true, reason: answer.reason as string | undefined };
    }
    return undefined;
  });
  pi.on("tool_result", async (event, ctx) => { await send("tool_result", { ...event, ...identity(ctx) }); });
  pi.on("session_shutdown", async (_event, ctx) => { await send("session_shutdown", identity(ctx)); });
}
