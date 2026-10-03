#!/usr/bin/env node

// Run the installed GraphCoder package against a host-owned JSON-lines Harness
// bridge. The package root, bridge executable, arguments, and environment are
// explicit inputs so qualification cannot silently fall back to a fixture or
// inherit credentials from the invoking shell.

import { existsSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { join, resolve } from "node:path";

function fail(message) {
  throw new Error(`graphcoder-production-entrypoint: ${message}`);
}

function requiredEnvironment(name) {
  const value = process.env[name];
  if (value === undefined || value.trim() === "") fail(`${name} is required`);
  return value;
}

function jsonObject(name, fallback = "{}") {
  let value;
  try { value = JSON.parse(process.env[name] ?? fallback); }
  catch (error) { fail(`${name} is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (value === null || typeof value !== "object" || Array.isArray(value)) fail(`${name} must be a JSON object`);
  if (Object.entries(value).some(([key, item]) => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key) || typeof item !== "string")) {
    fail(`${name} must contain only string environment values with valid names`);
  }
  return value;
}

function jsonArray(name, fallback = "[]") {
  let value;
  try { value = JSON.parse(process.env[name] ?? fallback); }
  catch (error) { fail(`${name} is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!Array.isArray(value) || value.some(item => typeof item !== "string")) fail(`${name} must be a JSON string array`);
  return value;
}

const packageRoot = resolve(requiredEnvironment("GRAPHCODER_PACKAGE_ROOT"));
const bridgeExecutable = requiredEnvironment("GRAPHCODER_BRIDGE_EXECUTABLE");
const bridgeArgs = jsonArray("GRAPHCODER_BRIDGE_ARGS_JSON");
const bridgeEnvironment = jsonObject("GRAPHCODER_BRIDGE_ENV_JSON");
const bridgeCwd = resolve(process.env.GRAPHCODER_BRIDGE_CWD ?? process.cwd());
const terminalPath = join(packageRoot, "dist", "terminal.js");
const bridgePath = join(packageRoot, "dist", "bridge.js");
const processBridgePath = join(packageRoot, "dist", "process.js");
for (const path of [terminalPath, bridgePath, processBridgePath]) if (!existsSync(path)) fail(`installed GraphCoder module is missing: ${path}`);

const [{ GraphCoderTerminal }, { HarnessGraphCoderTransport }, { JsonLineGraphCoderBridge }] = await Promise.all([
  import(pathToFileURL(terminalPath).href),
  import(pathToFileURL(bridgePath).href),
  import(pathToFileURL(processBridgePath).href),
]);

const bridge = new JsonLineGraphCoderBridge({
  executable: bridgeExecutable,
  args: bridgeArgs,
  cwd: bridgeCwd,
  env: bridgeEnvironment,
});
try {
  const transport = new HarnessGraphCoderTransport(bridge);
  const commands = process.argv.slice(2);
  if (commands.length === 0) {
    await new GraphCoderTerminal(transport).interactive();
    process.exitCode = 0;
  } else {
    const context = new Map();
    const output = {
      write(chunk) {
        const text = String(chunk);
        process.stdout.write(text);
        for (const line of text.split("\n")) {
          if (line.trim() === "") continue;
          try {
            const value = JSON.parse(line).value;
            const selected = value?.selectedSession?.summary?.id ?? value?.selectedSession?.id;
            if (typeof selected === "string") context.set("session_id", selected);
            const approvals = Array.isArray(value) ? value : Array.isArray(value?.approvals) ? value.approvals : [];
            const pending = approvals.find(item => item?.state === "pending");
            if (typeof pending?.id === "string") context.set("approval_id", pending.id);
            if (typeof pending?.operationId === "string") context.set("writeback_operation_id", pending.operationId);
            if (typeof value?.changesGeneration === "string" || typeof value?.changesGeneration === "number") context.set("workspace_generation", String(value.changesGeneration));
            if (typeof value?.generation === "string" || typeof value?.generation === "number") context.set("workspace_generation", String(value.generation));
          } catch {
            // Diagnostic and prompt output is retained verbatim; only JSON
            // command responses contribute dynamic substitutions.
          }
        }
      },
    };
    const terminal = new GraphCoderTerminal(transport, { output });
    let failed = false;
    for (const command of commands) {
      const expanded = command.replace(/\{\{([a-z_]+)\}\}/g, (_match, name) => {
        const value = context.get(name);
        if (value === undefined) throw new Error(`unresolved command substitution: {{${name}}}`);
        return value;
      });
      try { await terminal.command(expanded); }
      catch (error) {
        failed = true;
        output.write(`${JSON.stringify({ ok: false, error: error instanceof Error ? error.message : String(error) })}\n`);
      }
    }
    process.exitCode = failed ? 1 : 0;
  }
} finally {
  bridge.close("GraphCoder qualification entrypoint finished");
}
