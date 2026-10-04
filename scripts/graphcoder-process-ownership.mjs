// Shared child-process ownership for qualification drivers. Every process
// started by a suite is registered here so timeout, signal, and parent-exit
// paths terminate the complete owned tree instead of only the direct child.

import { spawn } from "node:child_process";

const ownedChildren = new Set();
let cleanupInstalled = false;

function hostEnvironment() {
  const result = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value !== "string") continue;
    const normalized = key.toUpperCase();
    if (["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "CI"].includes(normalized)) {
      result[key] = value;
    }
  }
  const pathEntry = Object.entries(process.env).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) result[pathEntry[0]] = pathEntry[1];
  return result;
}

export async function terminateOwnedProcessTree(child, signal) {
  if (!child || !Number.isInteger(child.pid) || child.pid <= 0) {
    if (typeof child?.kill === "function") {
      try { child.kill(signal); } catch { /* preserve the typed uncertainty */ }
      return { state: "uncertain", verified: false, reason: "process failed before exposing an owned PID" };
    }
    return { state: "unknown", verified: false, reason: "child has no pid or kill operation" };
  }
  if (process.platform === "win32") {
    if (child.exitCode !== null || child.signalCode !== null) {
      return { state: "uncertain", verified: false, reason: "process root exited before Windows tree cleanup" };
    }
    const systemRoot = Object.entries(process.env).find(([key]) => key.toUpperCase() === "SYSTEMROOT")?.[1];
    const taskkill = typeof systemRoot === "string" && systemRoot.trim() !== ""
      ? `${systemRoot}\\System32\\taskkill.exe`
      : "taskkill.exe";
    const result = await runTaskkill(taskkill, child.pid, systemRoot);
    if (result.state !== "terminated") return result;
    const closed = await waitForChildClose(child, 5_000);
    return closed
      ? { state: "terminated", verified: true }
      : { state: "uncertain", verified: false, reason: "process root did not close after Windows tree cleanup" };
  }
  try {
    process.kill(-child.pid, "SIGKILL");
    const closed = await waitForChildClose(child, 5_000);
    return closed ? { state: "terminated", verified: true } : { state: "uncertain", verified: false, reason: "process group did not close after termination" };
  } catch (error) {
    if (error?.code === "ESRCH") return { state: "not-running", verified: true };
    return { state: "uncertain", verified: false, reason: error instanceof Error ? error.message : String(error) };
  }
}

function waitForChildClose(child, timeoutMs) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true);
  return new Promise(resolvePromise => {
    let timer = setTimeout(() => resolvePromise(false), timeoutMs);
    child.once("close", () => { clearTimeout(timer); resolvePromise(true); });
    child.once("error", () => { clearTimeout(timer); resolvePromise(false); });
  });
}

function runTaskkill(executable, pid, systemRoot) {
  return new Promise(resolvePromise => {
    let settled = false;
    const killer = spawn(executable, ["/PID", String(pid), "/T", "/F"], {
      windowsHide: true,
      stdio: "ignore",
      shell: false,
      env: systemRoot === undefined ? {} : { SystemRoot: systemRoot },
    });
    const finish = result => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolvePromise(result);
    };
    const timer = setTimeout(() => {
      try { killer.kill(); } catch { /* timeout is authoritative */ }
      finish({ state: "uncertain", verified: false, reason: "taskkill timed out" });
    }, 5_000);
    killer.once("error", error => finish({ state: "uncertain", verified: false, reason: error.message }));
    killer.once("close", code => finish(code === 0
      ? { state: "terminated", verified: true }
      : { state: "uncertain", verified: false, reason: `taskkill exited ${code}` }));
  });
}

function installOwnedCleanup() {
  if (cleanupInstalled) return;
  cleanupInstalled = true;
  process.once("beforeExit", async () => {
    // `exit` cannot await the host-owned termination boundary. beforeExit lets
    // the bounded cleanup finish while the parent still has an event loop.
    await Promise.all([...ownedChildren].map(async child => {
      try { await terminateOwnedProcessTree(child); } catch { /* preserve process exit */ }
    }));
  });
  for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]]) {
    process.once(signal, () => {
      for (const child of ownedChildren) {
        try { void terminateOwnedProcessTree(child, signal); } catch { /* preserve signal exit */ }
      }
      process.exitCode = code;
    });
  }
}

export function trackOwnedProcess(child, { installCleanup: shouldInstallCleanup = true } = {}) {
  if (shouldInstallCleanup) installOwnedCleanup();
  ownedChildren.add(child);
  return () => ownedChildren.delete(child);
}

export function spawnOwnedProcess(executable, args, options = {}) {
  return spawn(executable, args, {
    ...options,
    env: options.env ?? hostEnvironment(),
    ...(process.platform === "win32" ? {} : { detached: true }),
  });
}

export async function executeOwnedProcess(executable, args, { timeoutMs = 120_000, input, ...options } = {}) {
  const stdio = options.stdio ?? [input === undefined ? "ignore" : "pipe", "pipe", "pipe"];
  if (input !== undefined && stdio[0] !== "pipe") throw new Error("process-ownership: input requires a piped stdin");
  const child = spawnOwnedProcess(executable, args, {
    ...options,
    stdio,
  });
  const release = trackOwnedProcess(child);
  let stdout = "";
  let stderr = "";
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", chunk => { stdout += String(chunk); });
  child.stderr?.on("data", chunk => { stderr += String(chunk); });
  if (input !== undefined) child.stdin.end(input);
  const closed = new Promise(resolvePromise => child.once("close", (code, signal) => resolvePromise({ code, signal })));
  const errored = new Promise(resolvePromise => child.once("error", error => resolvePromise({ error })));
  let timer;
  let outcome;
  let termination;
  let timedOut = false;
  try {
    outcome = await Promise.race([closed, errored, new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timed_out: true }), timeoutMs); })]);
    if (outcome.timed_out) {
      timedOut = true;
      termination = await terminateOwnedProcessTree(child);
      outcome = await Promise.race([closed, new Promise(resolvePromise => setTimeout(() => resolvePromise({ timed_out: true }), 5_000))]);
    } else if (outcome.error) {
      termination = await terminateOwnedProcessTree(child);
      await Promise.race([closed, new Promise(resolvePromise => setTimeout(resolvePromise, 5_000))]);
    }
  } finally {
    clearTimeout(timer);
    if (outcome?.code !== undefined || outcome?.error) release();
  }
  return {
    stdout,
    stderr,
    status: timedOut ? null : outcome?.code ?? null,
    signal: outcome?.signal ?? null,
    error: outcome?.error ?? (timedOut ? new Error(`process timed out after ${timeoutMs}ms`) : null),
    cleanup: termination ?? (outcome?.code !== undefined
      ? { state: "exited", verified: true }
      : { state: "uncertain", verified: false, reason: "process did not close cleanly" }),
  };
}

export function installChildSignalCleanup(child) {
  const release = trackOwnedProcess(child, { installCleanup: Number.isInteger(child?.pid) && child.pid > 0 });
  const handlers = ["SIGINT", "SIGTERM"].map(signal => {
    const handler = () => {
      try { void terminateOwnedProcessTree(child, signal); } catch { /* close/error observers retain the outcome */ }
    };
    process.once(signal, handler);
    return [signal, handler];
  });
  const onClose = () => release();
  const observesClose = typeof child?.once === "function";
  if (observesClose) child.once("close", onClose);
  return () => {
    for (const [signal, handler] of handlers) process.off(signal, handler);
    // A caller may stop listening for signals while the child is still
    // running. Keep the ownership registration and close observer until the
    // process has actually exited so parent cleanup cannot lose a live child.
    if (!observesClose) release();
  };
}
