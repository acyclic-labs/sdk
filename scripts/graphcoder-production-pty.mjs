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

export function bridgeEnvironment(environment = process.env) {
  return Object.fromEntries(
    Object.entries(environment).filter(([key]) =>
      ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT"].includes(key) || key.startsWith("GRAPHCODER_"),
    ),
  );
}

function stripAnsi(value) {
  return value.replace(/\u001b\[[0-9;?]*[ -/]*[@-~]/g, "");
}

export function framedPromptCount(value) {
  return [...stripAnsi(value).matchAll(/(?:^|\r?\n)graphcoder>[ ]?/gu)].length;
}

export function commandContext(transcript) {
  const context = {};
  for (const line of stripAnsi(transcript).split(/\r?\n/u)) {
    const start = line.indexOf("{");
    if (start < 0) continue;
    try {
      const value = JSON.parse(line.slice(start));
      const payload = value && typeof value === "object" ? value.value : undefined;
      if (!payload || typeof payload !== "object") continue;
      const selected = payload.selectedSession;
      const selectedId = selected?.summary?.id ?? selected?.id;
      if (typeof selectedId === "string") context.session_id = selectedId;
      const approvals = Array.isArray(payload) ? payload : payload.approvals;
      if (Array.isArray(approvals)) {
        const pending = approvals.find(item => item?.state === "pending");
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

function waitForPrompt(state, previousCount, processClosed, processError) {
  return new Promise((resolvePromise, reject) => {
    const deadline = Date.now() + PROMPT_TIMEOUT_MS;
    let timer;
    const cleanup = () => {
      clearTimeout(timer);
      state.listeners.delete(check);
    };
    const check = () => {
      if (state.promptCount > previousCount) {
        cleanup();
        resolvePromise();
      } else if (Date.now() >= deadline) {
        cleanup();
        reject(new Error("CLI did not present the next framed prompt"));
      } else {
        timer = setTimeout(check, 50);
      }
    };
    const onClose = value => {
      cleanup();
      reject(new Error(`CLI closed before the next prompt (code ${value.code}, signal ${value.signal ?? "none"})`));
    };
    const onError = error => {
      cleanup();
      reject(error);
    };
    state.listeners.add(check);
    processClosed.then(onClose, () => undefined);
    processError.then(onError, () => undefined);
    check();
  });
}

async function waitForClose(processClosed, processError, child) {
  const result = await Promise.race([
    processClosed,
    processError.then(error => { throw error; }),
    new Promise(resolvePromise => setTimeout(() => resolvePromise({ timeout: true }), PROMPT_TIMEOUT_MS)),
  ]);
  if (!result.timeout) return result;
  child.kill();
  const killed = await Promise.race([
    processClosed,
    new Promise(resolvePromise => setTimeout(() => resolvePromise({ timeout: true }), PROMPT_TIMEOUT_MS)),
  ]);
  if (killed.timeout) fail("PTY process did not close after termination");
  return killed;
}

export async function run(commands) {
  if (commands.length === 0) fail("at least one terminal command is required");
  if (process.platform !== "win32") fail("Windows ConPTY qualification requires a Windows host");
  if (process.env.GRAPHCODER_MOCK_FIXTURE !== undefined) fail("mock fixture environment cannot be used by the production PTY lane");
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
  if (child.stdin === null || child.stdout === null || child.stderr === null) fail("ConPTY process did not expose all standard streams");
  const state = { output: "", promptCount: 0, listeners: new Set() };
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let closeResolve;
  const processClosed = new Promise(resolvePromise => { closeResolve = resolvePromise; });
  child.once("close", (code, signal) => closeResolve({ code, signal }));
  let errorReject;
  const processError = new Promise((_resolve, reject) => { errorReject = reject; });
  child.once("error", error => errorReject(error));
  child.stdout.on("data", chunk => {
    state.output += chunk;
    state.promptCount = framedPromptCount(state.output);
    for (const listener of state.listeners) listener();
  });
  child.stderr.on("data", chunk => { state.output += `\n[stderr]\n${chunk}`; });
  try {
    await waitForPrompt(state, 0, processClosed, processError);
    for (const rawCommand of commands) {
      const command = expandCommand(rawCommand, state.output);
      const previousCount = state.promptCount;
      child.stdin.write(`${command}\r`);
      await waitForPrompt(state, previousCount, processClosed, processError);
    }
    child.stdin.write("quit\r");
    const exit = await waitForClose(processClosed, processError, child);
    if (exit.code !== 0) fail(`PTY process exited with code ${exit.code}`);
    const transcript = state.output;
    const missing = [PROMPT, ...REQUIRED_MARKERS].filter(marker => !transcript.includes(marker));
    if (missing.length > 0) fail(`missing transcript markers: ${missing.join(", ")}`);
    process.stdout.write(transcript);
  } catch (error) {
    try {
      child.kill();
      await Promise.race([
        processClosed,
        processError.then(() => undefined, () => undefined),
        new Promise(resolvePromise => setTimeout(resolvePromise, PROMPT_TIMEOUT_MS)),
      ]);
    } catch {
      // Preserve the command failure while still bounding process cleanup.
    }
    throw error;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  run(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
