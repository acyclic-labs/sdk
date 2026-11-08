import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { test } from "bun:test";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { stagePiExtension } from "./package.mjs";

test("Anthropic qualification commands explicitly select the approved model", () => {
  // Inspect the launch array without executing a host or accessing a login.
  const approved = /'--model',\s*'claude-haiku-5-5'/;
  for (const name of ["shared-mount-scale.py"]) {
    const source = readFileSync(new URL(name, import.meta.url), "utf8");
    assert.match(source, approved, name);
    assert.doesNotMatch(source.replace("'--model',", ""), approved,
      "negative control: relying on a host default must be rejected");
    assert.doesNotMatch(source.replaceAll("claude-haiku-5-5", "other-model"), approved,
      "negative control: a different model must be rejected");
  }
  assert.doesNotMatch(readFileSync(new URL("sandbox-commands.py", import.meta.url), "utf8"),
    /def scripted_model|http\.server|ANTHROPIC_API_KEY/);
});

test("Pi registration delegates execution and exposes only native coordination", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".shim-contract-"));
  try {
    const path = await stagePiExtension(directory);
    const { default: acyclic } = await import(pathToFileURL(path).href);
    const hooks: string[] = [], tools: string[] = [];
    const forbidden = () => {throw new Error("registration executed host work");};
    const api = {on: (event: string) => {hooks.push(event);},
      registerTool: (tool: {name: string}) => {tools.push(tool.name);},
      registerProvider: forbidden, exec: forbidden} as unknown as ExtensionAPI;
    acyclic(api);
    assert.deepEqual(tools, ["spawn", "send", "wait", "archive", "cancel", "resume"]);
    assert.deepEqual(hooks, ["session_start", "agent_end", "session_shutdown"]);
    assert(!tools.some(name => ["bash", "read", "write", "edit"].includes(name)));
  } finally {rmSync(directory, {recursive: true, force: true});}
});

test("qualification calls shared native lifecycle instead of the deleted Pi operator", () => {
  const commands = ["bun run --cwd plugin/tests/hosts test:shim",
    "cargo test --locked -p acyclic-plugin --all-features --bin acyclic -- --test-threads=4",
    "cargo test --locked -p acyclic-harness --all-features --lib local::host_owner:: -- --test-threads=4"];
  const verify = (source: string) => {
    for (const command of commands) assert(source.includes(command), command);
    assert.doesNotMatch(source, /test:pi-(context|bootstrap|session|service|native-recursive|builtin-paths)/);
  };
  for (const name of ["qualify-ci.sh", "qualify-ci.ps1"]) {
    const source = readFileSync(new URL(`../../../scripts/${name}`, import.meta.url), "utf8");
    verify(source);
    for (const command of commands) assert.throws(() => verify(source.replace(command, "true")));
  }
});

test("release packaging validates without state access or authentication", () => {
  const directory = mkdtempSync(join(tmpdir(), "shim-package-"));
  const script = (name: string) => fileURLToPath(new URL(`../../scripts/${name}`, import.meta.url));
  const run = (name: string, args: string[]) => {
    const result = spawnSync(process.execPath, [script(name), ...args], {encoding: "utf8"});
    if (result.error) throw result.error;
    assert.equal(result.signal, null, "packaging child terminated by signal");
    return result;
  };
  try {
    const binary = join(directory, "fixture-binary");
    writeFileSync(binary, "fixture executable bytes; packaging never executes the input binary");
    const output = join(directory, "release");
    const built = run("package.mjs", ["--binary", `linux-x64-gnu=${binary}`, "--out", output]);
    assert.equal(built.status, 0, built.stderr);
    const marketplace = JSON.parse(readFileSync(join(output, "marketplace.json"), "utf8"));
    assert.equal(marketplace.plugins[0].policy.authentication, undefined);
    assert.equal(run("validate-package.mjs", [output]).status, 0);
    for (const relative of ["hooks/hooks.json", "claude/.claude-plugin/plugin.json"]) {
      const path = join(output, "plugin", relative);
      const original = readFileSync(path, "utf8");
      const hooks = JSON.parse(original);
      hooks.hooks.PermissionRequest[0].matcher = ".*";
      writeFileSync(path, JSON.stringify(hooks));
      const rejected = run("validate-package.mjs", [output]);
      assert.notEqual(rejected.status, 0, "a broad permission hook must fail package admission");
      assert.match(rejected.stderr, /exact spawn permission prompts/);
      writeFileSync(path, original);
    }
    const path = join(output, "plugin", ".mcp.json");
    const original = readFileSync(path, "utf8");
    const manifest = JSON.parse(original);
    manifest.mcpServers["acyclic-sessions"].env_vars = ["XDG_STATE_HOME"];
    writeFileSync(path, JSON.stringify(manifest));
    const rejected = run("validate-package.mjs", [output]);
    assert.notEqual(rejected.status, 0, "negative control: state-dependent declarations must be rejected");
    assert.match(rejected.stderr, /MCP declaration/);
    const legacy = JSON.parse(original);
    legacy.mcpServers = {"acyclic-hooks": legacy.mcpServers["acyclic-sessions"]};
    writeFileSync(path, JSON.stringify(legacy));
    const oldServer = run("validate-package.mjs", [output]);
    assert.notEqual(oldServer.status, 0, "negative control: the deleted hook MCP server must be rejected");
    assert.match(oldServer.stderr, /MCP declaration/);
  } finally {rmSync(directory, {recursive: true, force: true});}
});
