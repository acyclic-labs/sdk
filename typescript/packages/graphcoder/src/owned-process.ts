import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { join } from "node:path";

export type OwnedProcessTermination =
  | { readonly kind: "terminated"; readonly pid: number }
  | { readonly kind: "timeout"; readonly pid: number; readonly phase: "command" | "pipes" }
  | { readonly kind: "unknown"; readonly pid: number; readonly reason: string; readonly exitCode?: number | null };

type OwnedProcessState = { closed: boolean; errored: boolean };
type ProcessWait = "closed" | "timeout" | "error";
type GroupWait = "gone" | "timeout" | "error";
type TaskkillOutcome =
  | { readonly kind: "closed"; readonly code: number | null }
  | { readonly kind: "error"; readonly message: string }
  | { readonly kind: "timeout" };

const states = new WeakMap<ChildProcess, OwnedProcessState>();
interface TerminationRecord {
  readonly promise: Promise<OwnedProcessTermination>;
  outcome?: OwnedProcessTermination;
}
const terminations = new WeakMap<ChildProcess, TerminationRecord>();

/** Start a host-owned process in its own process group. This is lifecycle ownership, not a sandbox. */
export function spawnOwnedProcess(
  executable: string,
  args: readonly string[],
  options: SpawnOptions,
): ChildProcess {
  const child = spawn(executable, [...args], {
    ...options,
    detached: true,
    windowsHide: true,
  });
  track(child);
  return child;
}

/**
 * Terminate an owned process and its descendants. Calls for one child share a
 * single bounded operation, so repeated signal or close paths cannot dispatch
 * competing cleanup commands.
 */
export function terminateOwnedProcess(child: ChildProcess, graceMs = 250): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    return Promise.reject(new RangeError("process cleanup grace must be a nonnegative safe integer"));
  }
  const existing = terminations.get(child);
  if (existing !== undefined) return existing.promise;
  const pending = terminateOwnedProcessOnce(child, graceMs);
  const record: TerminationRecord = { promise: pending };
  terminations.set(child, record);
  void pending.then(outcome => { record.outcome = outcome; });
  return pending;
}

/**
 * Retry an incomplete cleanup after its host has resolved the ownership
 * condition. A pending operation is still shared; a confirmed termination is
 * never replayed. On Windows a root-exit uncertainty remains safe and typed
 * until a native boundary supplies stronger ownership evidence.
 */
export function retryOwnedProcessTermination(child: ChildProcess, graceMs = 250): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    return Promise.reject(new RangeError("process cleanup grace must be a nonnegative safe integer"));
  }
  const existing = terminations.get(child);
  if (existing === undefined || existing.outcome?.kind === "terminated") {
    return terminateOwnedProcess(child, graceMs);
  }
  terminations.delete(child);
  return terminateOwnedProcess(child, graceMs);
}

async function terminateOwnedProcessOnce(child: ChildProcess, graceMs: number): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    throw new RangeError("process cleanup grace must be a nonnegative safe integer");
  }
  const pid = child.pid;
  if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
    return { kind: "unknown", pid: -1, reason: "owned process did not expose a valid PID" };
  }
  track(child);

  if (process.platform === "win32") {
    // Node exposes only the PID, not the process handle needed to prove that
    // a descendant still belongs to this owner after the root exits. Never
    // recycle a PID through taskkill in that state; surface uncertainty until
    // the host's native process boundary can resolve ownership safely.
    if (!isAlive(child)) {
      return { kind: "unknown", pid, reason: "owned process root exited before Windows tree cleanup" };
    }
    const command = await terminateWindowsProcessTree(pid, child, Math.max(graceMs, 1_000));
    if (command.kind !== "terminated") {
      try { if (isAlive(child)) child.kill(); } catch { /* retain the typed unknown outcome */ }
      return command;
    }
    if (isAlive(child)) {
      try { child.kill(); } catch { /* close remains authoritative */ }
    }
    const pipeWait = await waitForClose(child, graceMs);
    if (pipeWait === "closed") return { kind: "terminated", pid };
    if (pipeWait === "error") return { kind: "unknown", pid, reason: "owned process emitted an error during termination" };
    return { kind: "timeout", pid, phase: "pipes" };
  }

  const softSignal = signalProcessGroup(pid, "SIGTERM", child);
  const softWait = await waitForProcessGroupGone(pid, graceMs);
  if (softWait === "gone") return { kind: "terminated", pid };
  if (softWait === "error") return { kind: "unknown", pid, reason: "owned process group state became unavailable" };
  const hardSignal = signalProcessGroup(pid, "SIGKILL", child);
  const hardWait = await waitForProcessGroupGone(pid, graceMs);
  if (hardWait === "gone") return { kind: "terminated", pid };
  if (hardWait === "error") return { kind: "unknown", pid, reason: "owned process group state became unavailable" };
  if (softSignal === "error" || hardSignal === "error") {
    return { kind: "unknown", pid, reason: "owned process group rejected termination" };
  }
  return { kind: "timeout", pid, phase: "pipes" };
}

function isAlive(child: ChildProcess): boolean {
  return child.exitCode === null && child.signalCode === null;
}

function signalProcessGroup(pid: number, signal: NodeJS.Signals, child: ChildProcess): "sent" | "absent" | "error" {
  try {
    // The negative PID is the detached group, even when the direct child has
    // already exited and only an inherited descendant still owns a pipe.
    process.kill(-pid, signal);
    return "sent";
  } catch {
    if (!isAlive(child)) return "absent";
    try {
      child.kill(signal);
      return "sent";
    } catch { return "error"; }
  }
}

function track(child: ChildProcess): OwnedProcessState {
  const existing = states.get(child);
  if (existing !== undefined) return existing;
  const state: OwnedProcessState = { closed: false, errored: false };
  states.set(child, state);
  child.once("close", () => { state.closed = true; });
  child.once("error", () => { state.errored = true; });
  return state;
}

function waitForClose(child: ChildProcess, timeoutMs: number): Promise<ProcessWait> {
  const state = track(child);
  if (state.errored) return Promise.resolve("error");
  if (state.closed) return Promise.resolve("closed");
  if (timeoutMs === 0) return Promise.resolve("timeout");
  return new Promise(resolve => {
    let settled = false;
    const finish = (result: ProcessWait): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(result);
    };
    const timer = setTimeout(() => finish("timeout"), timeoutMs);
    child.once("close", () => finish("closed"));
    child.once("error", () => finish("error"));
  });
}

function waitForProcessGroupGone(pid: number, timeoutMs: number): Promise<GroupWait> {
  const deadline = Date.now() + timeoutMs;
  return new Promise(resolve => {
    const check = (): void => {
      try {
        process.kill(-pid, 0);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ESRCH") {
          resolve("gone");
          return;
        }
        if ((error as NodeJS.ErrnoException).code !== "EPERM") {
          resolve("error");
          return;
        }
      }
      if (Date.now() >= deadline) {
        resolve("timeout");
        return;
      }
      setTimeout(check, Math.min(20, Math.max(1, deadline - Date.now())));
    };
    check();
  });
}

async function terminateWindowsProcessTree(pid: number, child: ChildProcess, timeoutMs: number): Promise<OwnedProcessTermination> {
  const systemRoot = process.env.SystemRoot;
  const taskkill = systemRoot === undefined || systemRoot.trim() === ""
    ? "taskkill.exe"
    : join(systemRoot, "System32", "taskkill.exe");
  // Cleanup tooling receives only the system root; never forward runtime
  // model/provider variables or credentials.
  const env = systemRoot === undefined ? {} : { SystemRoot: systemRoot };
  const outcome = await new Promise<TaskkillOutcome>(resolve => {
    let settled = false;
    const killer = spawn(taskkill, ["/PID", String(pid), "/T", "/F"], {
      env,
      shell: false,
      stdio: "ignore",
      windowsHide: true,
    });
    const complete = (value: TaskkillOutcome): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(value);
    };
    const timer = setTimeout(() => {
      try { killer.kill(); } catch { /* timeout remains authoritative */ }
      void Promise.all([waitForClose(killer, timeoutMs), waitForClose(child, timeoutMs)]).then(([killerResult, childResult]) => {
        if (killerResult !== "closed") {
          complete({ kind: "timeout" });
          return;
        }
        complete(childResult === "closed" ? { kind: "closed", code: null } : { kind: "timeout" });
      });
    }, timeoutMs);
    killer.once("error", error => complete({ kind: "error", message: error.message }));
    killer.once("close", code => complete({ kind: "closed", code }));
  });
  if (outcome.kind === "timeout") return { kind: "timeout", pid, phase: "command" };
  if (outcome.kind === "error") return { kind: "unknown", pid, reason: `taskkill could not start: ${outcome.message}` };
  if (outcome.code !== 0) return { kind: "unknown", pid, reason: "taskkill returned a nonzero exit code", exitCode: outcome.code };
  return { kind: "terminated", pid };
}
