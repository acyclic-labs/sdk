#!/usr/bin/env node

// Drive the installed GraphCoder entrypoint through the native Windows
// ConPTY adapter. The package and bridge are selected through the explicit
// environment consumed by graphcoder-production-entrypoint.mjs.

import { existsSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const PROMPT = "graphcoder>";
const PROMPT_TIMEOUT_MS = 10_000;
const REQUIRED_MARKERS = [
  '"selectedSession"',
  '"activity"',
  '"messages"',
  '"approvals"',
  '"generation"',
  '"unifiedDiff"',
  '"mediaType"',
  '"applied":true',
  '"state":"cancelled"',
  '"exited":true',
];

function fail(message) {
  throw new Error(`graphcoder-production-pty: ${message}`);
}

function nativeConhost() {
  const candidates = [
    process.env.GRAPHCODER_CONHOST,
    join(process.env.WINDIR ?? "C:\\Windows", "System32", "conhost.exe"),
  ].filter(value => typeof value === "string" && value.trim() !== "");
  const path = candidates.find(candidate => existsSync(candidate));
  if (path === undefined) fail("native conhost.exe was not found; set GRAPHCODER_CONHOST");
  return path;
}

function bridgeEnvironment() {
  return Object.fromEntries(
    Object.entries(process.env).filter(([key]) =>
      ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT"].includes(key) || key.startsWith("GRAPHCODER_"),
    ),
  );
}

function stripAnsi(value) {
  return value.replace(/\u001b\[[0-9;?]*[ -/]*[@-~]/g, "");
}

function commandContext(transcript) {
  const context = {};
  for (const line of stripAnsi(transcript).split(/\r?\n/u)) {
    const start = line.indexOf("{");
    if (start < 0) continue;
    try {
      const value = JSON.parse(line.slice(start));
      const payload = value && typeof value === "object" ? value.value : undefined;
      if (!payload || typeof payload !== "object") continue;
      const selected = payload.selectedSession;
      if (selected?.summary?.id && typeof selected.summary.id === "string") context.session_id = selected.summary.id;
      if (Array.isArray(payload.approvals)) {
        const pending = payload.approvals.find(item => item?.state === "pending");
        if (pending?.id && typeof pending.id === "string") context.approval_id = pending.id;
        if (pending?.operationId && typeof pending.operationId === "string") context.writeback_operation_id = pending.operationId;
      }
      for (const field of ["generation", "changesGeneration"]) {
        if (typeof payload[field] === "string" || typeof payload[field] === "number") context.workspace_generation = String(payload[field]);
      }
      if (typeof payload.writeback?.operationId === "string") context.writeback_operation_id = payload.writeback.operationId;
    } catch {
      // Terminal redraws can prefix or split JSON; the next complete line is used.
    }
  }
  return context;
}

function expandCommand(command, transcript) {
  return command.replace(/\{\{([a-z_]+)\}\}/g, (_, name) => {
    const value = commandContext(transcript)[name];
    if (value === undefined) fail(`unresolved command substitution: {{${name}}}`);
    return value;
  });
}

function waitForPrompt(child, state, previousCount) {
  return new Promise((resolvePromise, reject) => {
    const deadline = Date.now() + PROMPT_TIMEOUT_MS;
    const check = () => {
      if (state.promptCount > previousCount) {
        clearInterval(timer);
        resolvePromise();
      } else if (Date.now() >= deadline) {
        clearInterval(timer);
        reject(new Error("CLI did not present the next prompt"));
      }
    };
    const timer = setInterval(check, 50);
    child.once("error", error => {
      clearInterval(timer);
      reject(error);
    });
    check();
  });
}

async function run(commands) {
  if (commands.length === 0) fail("at least one terminal command is required");
  const sdkRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
  const entrypoint = join(sdkRoot, "scripts", "graphcoder-production-entrypoint.mjs");
  if (!existsSync(entrypoint)) fail(`missing entrypoint: ${entrypoint}`);
  // conhost --headless creates a real Windows pseudoconsole while exposing
  // byte streams to this driver. winpty requires the caller itself to own a
  // console, which makes it unusable for the qualification runner's captured
  // headless process boundary.
  const child = spawn(nativeConhost(), ["--headless", process.env.GRAPHCODER_NODE ?? process.execPath, entrypoint], {
    cwd: sdkRoot,
    env: bridgeEnvironment(),
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
    shell: false,
  });
  const state = { output: "", promptCount: 0 };
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", chunk => {
    state.output += chunk;
    state.promptCount = state.output.split(PROMPT).length - 1;
  });
  child.stderr.on("data", chunk => { state.output += `\n[stderr]\n${chunk}`; });
  try {
    await waitForPrompt(child, state, 0);
    for (const rawCommand of commands) {
      const command = expandCommand(rawCommand, state.output);
      const previousCount = state.promptCount;
      child.stdin.write(`${command}\r`);
      await waitForPrompt(child, state, previousCount);
    }
    child.stdin.write("quit\r");
    const exitCode = await new Promise((resolvePromise, reject) => {
      child.once("error", reject);
      child.once("close", resolvePromise);
    });
    if (exitCode !== 0) fail(`PTY process exited with code ${exitCode}`);
    const transcript = state.output;
    const missing = [PROMPT, ...REQUIRED_MARKERS].filter(marker => !transcript.includes(marker));
    if (missing.length > 0) fail(`missing transcript markers: ${missing.join(", ")}`);
    process.stdout.write(transcript);
  } catch (error) {
    child.kill();
    throw error;
  }
}

run(process.argv.slice(2)).catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
