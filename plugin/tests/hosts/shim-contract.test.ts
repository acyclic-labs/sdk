import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { test } from "bun:test";
import { createAgentSession, DefaultResourceLoader, SessionManager,
  type AgentSession, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

test("Pi registration delegates execution and exposes only native coordination", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-contract-"));
  try {
  const path = join(directory, "index.ts");
  writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
  const { default: acyclic, drainNativeSession } = await import(pathToFileURL(path).href);
  const hooks: string[] = [];
  const tools: string[] = [];
  const forbidden = () => { throw new Error("registration executed host work"); };
  const api = { on: (event: string) => { hooks.push(event); },
    registerTool: (tool: {name: string}) => { tools.push(tool.name); },
    registerProvider: forbidden, exec: forbidden } as unknown as ExtensionAPI;
  acyclic(api);
  assert.deepEqual(tools, ["spawn_agent", "wait_agent", "cancel_agent", "reopen_agent"]);
  assert.deepEqual(hooks, ["session_start", "tool_call", "tool_result", "session_shutdown"]);
  assert(!tools.some(name => ["bash", "read", "write", "edit"].includes(name)));
  const live = new Set(["child", "grandchild"]);
  const order: string[] = [];
  const native = (name: string, descendants: AgentSession[] = []) => ({
    abort: async () => { live.delete(name); order.push(`abort ${name}`); },
    extensionRunner: { emit: async (event: {type: string; reason: string}) => {
      assert.deepEqual(event, {type: "session_shutdown", reason: "quit"});
      for (const child of descendants) await drainNativeSession(child, Promise.resolve());
      order.push(`captured ${name}`);
    } },
  }) as unknown as AgentSession;
  const child = native("child", [native("grandchild")]);
  await child.abort();
  assert(live.has("grandchild"), "negative control: native abort alone leaves descendants running");
  order.length = 0;
  await drainNativeSession(child, Promise.resolve());
  assert.equal(live.size, 0);
  assert.deepEqual(order, ["abort child", "abort grandchild", "captured grandchild", "captured child"]);
  const broken = { abort: async () => {}, extensionRunner: {emit: async () => {throw new Error("drain failed");}} } as unknown as AgentSession;
  await assert.rejects(drainNativeSession(broken, Promise.resolve()), /drain failed/);
  const cwd = join(directory, "workspace", ".volumes", "agents", "native-child");
  const agentDir = join(directory, "fixture-agent-dir");
  mkdirSync(cwd, {recursive: true}); mkdirSync(agentDir);
  const events: string[] = [];
  const loader = new DefaultResourceLoader({ cwd, agentDir, noExtensions: true,
    noSkills: true, noPromptTemplates: true, noThemes: true,
    extensionFactories: [api => {
      api.on("session_start", async (_event, ctx) => { events.push(`start ${ctx.cwd}`); });
      api.on("session_shutdown", async () => { events.push("shutdown"); });
    }],
  });
  await loader.reload();
  const {session} = await createAgentSession({cwd, agentDir, tools: [],
    sessionManager: SessionManager.inMemory(cwd), resourceLoader: loader});
  try {
    assert.deepEqual(events, [], "negative control: SDK construction does not start native extensions");
    await session.bindExtensions({mode: "print"});
    assert.deepEqual(events, [`start ${cwd}`]);
    await drainNativeSession(session, Promise.resolve());
    assert.deepEqual(events, [`start ${cwd}`, "shutdown"]);
  } finally { session.dispose(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("Pi restores persisted native context into the retained task view", () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-reopen-"));
  try {
    const cwd = join(directory, "workspace");
    const retained = join(cwd, ".volumes", "agents", "retained-child");
    mkdirSync(retained, {recursive: true});
    const sessions = join(directory, "fixture-sessions");
    const original = SessionManager.create(cwd, sessions);
    original.appendModelChange("fixture-provider", "fixture-model");
    original.appendThinkingLevelChange("high");
    original.appendMessage({role: "user", content: "native retained context", timestamp: Date.now()});
    const file = original.getSessionFile()!;
    const before = readFileSync(file, "utf8");
    assert.equal(SessionManager.inMemory(retained).getSessionFile(), undefined,
      "negative control: an in-memory session has no transcript to recover");
    assert.equal(SessionManager.inMemory(retained).buildSessionContext().messages.length, 0);
    const reopened = SessionManager.forkFrom(file, retained, sessions);
    assert.equal(reopened.getCwd(), retained);
    assert.notEqual(reopened.getSessionId(), original.getSessionId());
    assert.deepEqual(reopened.buildSessionContext(), original.buildSessionContext());
    reopened.appendMessage({role: "user", content: "reopened work", timestamp: Date.now()});
    assert.equal(readFileSync(file, "utf8"), before, "reopen must not mutate the source transcript");
    assert.equal(reopened.buildSessionContext().messages.length, 2);
  } finally { rmSync(directory, {recursive: true, force: true}); }
});

test("host manifests and shims fit the native hook budget", () => {
  const source = (relative: string) => readFileSync(new URL(relative, import.meta.url), "utf8");
  const lines = (text: string) => text.trimEnd().split(/\r?\n/).length;
  const codex = source("../../hooks/hooks.json");
  const bridge = source("../../src/codex.rs");
  const codexManifests = ["../../plugin.json", "../../.codex-plugin/plugin.json",
    "../../.mcp.json", "../../.agents/plugins/marketplace.json"].map(source);
  assert(lines(codex) + lines(bridge) + codexManifests.reduce((sum, manifest) => sum + lines(manifest), 0) <= 300);
  assert(lines(source("../../pi/index.ts")) <= 300);
  const claude = JSON.parse(source("../../claude/.claude-plugin/plugin.json"));
  assert(lines(source("../../claude/.claude-plugin/plugin.json")) <= 300);
  assert(claude.hooks.WorktreeCreate);
  assert(claude.hooks.WorktreeRemove);
  const hooks = JSON.parse(codex).hooks;
  assert.equal(hooks.PostToolUse[0].hooks[0].input.hook.tool_response, "${tool_response}");
  assert(!source("../../.mcp.json").includes("XDG_STATE_HOME"));
});

test("release packaging validates without state access or install authentication", () => {
  const directory = mkdtempSync(join(tmpdir(), "shim-package-"));
  const script = (name: string) => fileURLToPath(new URL(`../../scripts/${name}`, import.meta.url));
  const run = (name: string, args: string[]) => spawnSync(process.execPath, [script(name), ...args], {
    encoding: "utf8", timeout: 2000,
  });
  try {
    const binary = join(directory, "fixture-binary");
    writeFileSync(binary, "fixture executable bytes; packaging never executes the input binary");
    const output = join(directory, "release");
    const built = run("package.mjs", ["--binary", `linux-x64-gnu=${binary}`, "--out", output]);
    assert.equal(built.status, 0, built.stderr);
    const marketplace = JSON.parse(readFileSync(join(output, "marketplace.json"), "utf8"));
    assert.equal(marketplace.plugins[0].policy.authentication, undefined);
    const validated = run("validate-package.mjs", [output]);
    assert.equal(validated.status, 0, validated.stderr);
    const path = join(output, "plugin", ".mcp.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.mcpServers["acyclic-hooks"].env_vars = ["XDG_STATE_HOME"];
    writeFileSync(path, JSON.stringify(manifest));
    const rejected = run("validate-package.mjs", [output]);
    assert.notEqual(rejected.status, 0, "negative control: state-dependent hook manifests must be rejected");
    assert.match(rejected.stderr, /MCP declaration/);
  } finally { rmSync(directory, {recursive: true, force: true}); }
});

test("Codex root-capable MCP hooks require no optional subagent fields", () => {
  const hooks = JSON.parse(readFileSync(new URL("../../hooks/hooks.json", import.meta.url), "utf8")).hooks;
  // Codex 0.164 omits agent_id/agent_type from root events. MCP templates
  // reject missing fields; the bridge obtains the caller through _meta.threadId.
  const root: Record<string, unknown> = {session_id: "root", turn_id: "turn", cwd: "/workspace",
    tool_name: "exec_command", tool_input: {command: "true"}, tool_use_id: "call", tool_response: {exit_code: 0}};
  const select = (field: string) => field.split(".").reduce<unknown>((value, key) =>
    value && typeof value === "object" ? (value as Record<string, unknown>)[key] : undefined, root);
  assert.equal(select("agent_id"), undefined, "negative control: root inputs omit subagent identity");
  for (const event of ["UserPromptSubmit", "PreToolUse", "PostToolUse"]) {
    for (const match of JSON.stringify(hooks[event]).matchAll(/\$\{([^}]+)\}/g)) {
      assert.notEqual(select(match[1]), undefined, `${event} requires absent root field ${match[1]}`);
    }
  }
});
