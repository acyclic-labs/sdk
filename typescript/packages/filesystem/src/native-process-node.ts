import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import type { NativeProcessOwner, NativeProcessTermination } from "./native-process.js";

/** Compatibility names used by host adapters that predate this SDK export. */
export type OwnedProcessTermination = NativeProcessTermination;

/** Host-provided process ownership boundary. A native adapter may supply a
 * real process handle/Job owner; GraphCoder does not duplicate that engine. */
export type OwnedProcessOwner = NativeProcessOwner;

type OwnedProcessState = { closed: boolean; errored: boolean };
type ProcessWait = "closed" | "timeout" | "error";

const states = new WeakMap<ChildProcess, OwnedProcessState>();
interface TerminationRecord {
  readonly promise: Promise<OwnedProcessTermination>;
  outcome?: OwnedProcessTermination;
}
const terminations = new WeakMap<ChildProcess, TerminationRecord>();

/** Start a host-owned process in its own process group. This is lifecycle ownership, not a sandbox. */
export function spawnNativeProcess(
  executable: string,
  args: readonly string[],
  options: SpawnOptions,
): ChildProcess {
  const child = spawn(executable, [...args], {
    ...options,
    env: options.env ?? {},
    detached: true,
    windowsHide: true,
  });
  track(child);
  return child;
}

/**
 * Request direct-child cleanup. Node exposes no stable descendant ownership
 * proof, so the fallback never claims that a process tree was terminated.
 * Native hosts with a real process handle should inject NativeProcessOwner.
 */
export function terminateNativeProcess(child: ChildProcess, graceMs = 250): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    return Promise.reject(new RangeError("process cleanup grace must be a nonnegative safe integer"));
  }
  const existing = terminations.get(child);
  if (existing !== undefined) return existing.promise;
  const pending = terminateNativeProcessOnce(child, graceMs);
  const record: TerminationRecord = { promise: pending };
  terminations.set(child, record);
  void pending.then(outcome => { record.outcome = outcome; });
  return pending;
}

/**
 * Retry an incomplete cleanup after its host has resolved the ownership
 * condition. A pending operation is still shared; a confirmed native
 * termination is never replayed. The Node fallback remains uncertain until a
 * native boundary supplies stronger ownership evidence.
 */
export function retryNativeProcessTermination(child: ChildProcess, graceMs = 250): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    return Promise.reject(new RangeError("process cleanup grace must be a nonnegative safe integer"));
  }
  const existing = terminations.get(child);
  if (existing !== undefined && existing.outcome === undefined) return existing.promise;
  if (existing === undefined || existing.outcome?.kind === "terminated") {
    return terminateNativeProcess(child, graceMs);
  }
  terminations.delete(child);
  return terminateNativeProcess(child, graceMs);
}

/** Default Node ownership boundary used when a host does not inject a native owner. */
export const defaultNativeProcessOwner: OwnedProcessOwner = Object.freeze({
  spawn: spawnNativeProcess,
  terminate: terminateNativeProcess,
});

/** Deprecated GraphCoder names retained for source compatibility. */
export const spawnOwnedProcess = spawnNativeProcess;
export const terminateOwnedProcess = terminateNativeProcess;
export const retryOwnedProcessTermination = retryNativeProcessTermination;
export const defaultOwnedProcessOwner = defaultNativeProcessOwner;

async function terminateNativeProcessOnce(child: ChildProcess, graceMs: number): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    throw new RangeError("process cleanup grace must be a nonnegative safe integer");
  }
  const pid = child.pid;
  if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
    return { kind: "unknown", pid: -1, reason: "owned process did not expose a valid PID" };
  }
  const state = track(child);

  // Once the direct child has closed, Node no longer gives us a historical
  // process handle. The fallback never converts a PID or process-group lookup
  // into an ownership proof.
  if (state.closed || state.errored) {
    return { kind: "unknown", pid, reason: "owned process root exited before direct cleanup could start" };
  }
  try { child.kill(); }
  catch (error) {
    return { kind: "unknown", pid, reason: `owned process root cleanup failed: ${error instanceof Error ? error.message : String(error)}` };
  }
  const pipeWait = await waitForClose(child, graceMs);
  if (pipeWait === "timeout") return { kind: "timeout", pid, phase: "pipes" };
  if (pipeWait === "error") return { kind: "unknown", pid, reason: "owned process emitted an error during direct cleanup" };
  return { kind: "unknown", pid, reason: "direct process closed but descendant ownership remains unproven" };
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
    const onClose = (): void => finish("closed");
    const onError = (): void => finish("error");
    const finish = (result: ProcessWait): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeListener("close", onClose);
      child.removeListener("error", onError);
      resolve(result);
    };
    const timer = setTimeout(() => finish("timeout"), timeoutMs);
    child.once("close", onClose);
    child.once("error", onError);
  });
}
