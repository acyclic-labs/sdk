import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { join } from "node:path";
import { EventEmitter } from "node:events";
import { PassThrough, Writable } from "node:stream";

export type OwnedProcessTermination =
  | { readonly kind: "terminated"; readonly pid: number }
  | { readonly kind: "timeout"; readonly pid: number; readonly phase: "command" | "pipes" }
  | { readonly kind: "unknown"; readonly pid: number; readonly reason: string; readonly exitCode?: number | null };

/** Host-provided process ownership boundary. A native adapter may supply a
 * real process handle/Job owner; GraphCoder does not duplicate that engine. */
export interface OwnedProcessOwner {
  readonly spawn: (executable: string, args: readonly string[], options: SpawnOptions) => ChildProcess;
  readonly terminate: (child: ChildProcess, graceMs?: number) => Promise<OwnedProcessTermination>;
}

/** Token-scoped native process transport supplied by a host adapter. */
export interface NativeOwnedProcessIo {
  readonly launch: (executable: string, args: readonly string[], options: SpawnOptions) => {
    readonly token: string;
    readonly pid: number;
  };
  readonly write: (token: string, bytes: Uint8Array) => void;
  readonly closeStdin: (token: string) => void;
  readonly pollOutput: (token: string, stream: "stdout" | "stderr") => {
    readonly kind: "idle" | "data" | "eof" | "error";
    readonly bytes?: Uint8Array;
    readonly reason?: string;
  };
  readonly pollExit: (token: string) => { readonly kind: "running" | "exited"; readonly code?: number | null };
  readonly terminate: (token: string) => { readonly kind: "terminated" | "timeout" | "unknown"; readonly reason?: string };
}

type NativeOwnedChild = ChildProcess & {
  readonly __nativeOwnerToken: string;
};

const NATIVE_STREAM_HIGH_WATER_MARK = 64 * 1024;

/**
 * Adapts the native token protocol to the bridge's ChildProcess-shaped
 * lifecycle. The native owner remains the only authority for process effects;
 * this adapter only moves bounded bytes between queues and streams.
 */
export function createNativeOwnedProcessOwner(io: NativeOwnedProcessIo): OwnedProcessOwner {
  const tokens = new WeakMap<ChildProcess, string>();
  const states = new WeakMap<ChildProcess, {
    timer: ReturnType<typeof setInterval> | undefined;
    closed: boolean;
    stopping: boolean;
    resume: () => void;
    finish: () => void;
  }>();
  const owner = {
    spawn(executable: string, args: readonly string[], options: SpawnOptions): ChildProcess {
      const launch = io.launch(executable, args, options);
      const child = new EventEmitter() as NativeOwnedChild;
      Object.defineProperties(child, {
        pid: { value: launch.pid, enumerable: true },
        exitCode: { writable: true, value: null, enumerable: true },
        signalCode: { writable: true, value: null, enumerable: true },
        __nativeOwnerToken: { value: launch.token },
      });
      const stdin = new Writable({
        write(chunk, _encoding, callback) {
          try {
            io.write(launch.token, new Uint8Array(chunk));
            callback();
          } catch (error) {
            callback(error instanceof Error ? error : new Error(String(error)));
          }
        },
        final(callback) {
          try { io.closeStdin(launch.token); callback(); }
          catch (error) { callback(error instanceof Error ? error : new Error(String(error))); }
        },
      });
      const stdout = new PassThrough({ highWaterMark: NATIVE_STREAM_HIGH_WATER_MARK });
      const stderr = new PassThrough({ highWaterMark: NATIVE_STREAM_HIGH_WATER_MARK });
      Object.defineProperties(child, {
        stdin: { value: stdin },
        stdout: { value: stdout },
        stderr: { value: stderr },
      });
      const state = {
        timer: undefined as ReturnType<typeof setInterval> | undefined,
        closed: false,
        stopping: false,
        stdoutBlocked: false,
        stderrBlocked: false,
        resume: (): void => undefined,
        finish: (): void => undefined,
      };
      let stdoutDone = false;
      let stderrDone = false;
      const finish = (): void => {
        if (state.closed) return;
        state.closed = true;
        if (state.timer !== undefined) clearInterval(state.timer);
        if (!stdoutDone) stdout.end();
        if (!stderrDone) stderr.end();
        child.emit("exit", child.exitCode, child.signalCode);
        child.emit("close", child.exitCode, child.signalCode);
      };
      state.finish = finish;
      const poll = (): void => {
        if (state.closed || state.stopping) return;
        try {
          for (const stream of ["stdout", "stderr"] as const) {
            const target = stream === "stdout" ? stdout : stderr;
            const blocked = stream === "stdout" ? state.stdoutBlocked : state.stderrBlocked;
            if (blocked) continue;
            const value = io.pollOutput(launch.token, stream);
            if (value.kind === "data" && value.bytes !== undefined) {
              if (value.bytes.byteLength > NATIVE_STREAM_HIGH_WATER_MARK) {
                throw new Error(`${stream} output chunk exceeds the bounded stream limit`);
              }
              const accepted = target.write(Buffer.from(value.bytes));
              if (!accepted) {
                if (stream === "stdout") state.stdoutBlocked = true;
                else state.stderrBlocked = true;
                target.once("drain", () => {
                  if (stream === "stdout") state.stdoutBlocked = false;
                  else state.stderrBlocked = false;
                });
              }
            }
            if (value.kind === "error") child.emit("error", new Error(value.reason ?? `${stream} read failed`));
            if (value.kind === "eof") {
              target.end();
              if (stream === "stdout") stdoutDone = true;
              else stderrDone = true;
            }
          }
          const exit = io.pollExit(launch.token);
          if (exit.kind === "exited") {
            (child as ChildProcess & { exitCode: number | null }).exitCode = exit.code ?? null;
            if (stdoutDone && stderrDone) finish();
          }
        } catch (error) {
          child.emit("error", error instanceof Error ? error : new Error(String(error)));
        }
      };
      state.resume = (): void => {
        if (state.closed || state.timer !== undefined) return;
        state.timer = setInterval(poll, 10);
      };
      state.resume();
      tokens.set(child, launch.token);
      states.set(child, state);
      return child;
    },
    async terminate(child: ChildProcess, graceMs = 250): Promise<OwnedProcessTermination> {
      const token = tokens.get(child);
      if (token === undefined) return { kind: "unknown", pid: child.pid ?? -1, reason: "native owner token is unavailable" };
      const state = states.get(child);
      if (state !== undefined) {
        state.stopping = true;
        if (state.timer !== undefined) {
          clearInterval(state.timer);
          state.timer = undefined;
        }
      }
      const deadline = Date.now() + Math.max(0, graceMs);
      let result: ReturnType<NativeOwnedProcessIo["terminate"]>;
      try {
        result = io.terminate(token);
        while (result.kind !== "terminated" && Date.now() < deadline) {
          await new Promise<void>(resolve => setTimeout(resolve, 10));
          result = io.terminate(token);
        }
      } catch (error) {
        if (state !== undefined) {
          state.stopping = false;
          state.resume();
        }
        return { kind: "unknown", pid: child.pid ?? -1, reason: error instanceof Error ? error.message : String(error) };
      }
      if (result.kind !== "terminated") {
        if (state !== undefined) {
          state.stopping = false;
          state.resume();
        }
        if (result.kind === "timeout") {
          return { kind: "timeout", pid: child.pid ?? -1, phase: "command" };
        }
        return { kind: "unknown", pid: child.pid ?? -1, reason: result.reason ?? "native cleanup is uncertain" };
      }
      state?.finish();
      tokens.delete(child);
      return { kind: "terminated", pid: child.pid ?? -1 };
    },
  } satisfies OwnedProcessOwner;
  return owner;
}

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
    // A host must opt into every inherited variable. In particular, a model
    // adapter or tool must never receive credentials from the launcher.
    env: options.env ?? {},
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
  if (existing !== undefined && existing.outcome === undefined) return existing.promise;
  if (existing === undefined || existing.outcome?.kind === "terminated") {
    return terminateOwnedProcess(child, graceMs);
  }
  terminations.delete(child);
  return terminateOwnedProcess(child, graceMs);
}

/** Default Node ownership boundary used when a host does not inject a native owner. */
export const defaultOwnedProcessOwner: OwnedProcessOwner = Object.freeze({
  spawn: spawnOwnedProcess,
  terminate: terminateOwnedProcess,
});

async function terminateOwnedProcessOnce(child: ChildProcess, graceMs: number): Promise<OwnedProcessTermination> {
  if (!Number.isSafeInteger(graceMs) || graceMs < 0) {
    throw new RangeError("process cleanup grace must be a nonnegative safe integer");
  }
  const pid = child.pid;
  if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
    return { kind: "unknown", pid: -1, reason: "owned process did not expose a valid PID" };
  }
  const state = track(child);

  // Once the direct child has closed, Node no longer gives us a historical
  // process handle. A PID or process-group lookup can now describe a reused
  // owner, so the fallback must retain uncertainty until a native owner
  // supplies a real handle.
  if (state.closed || state.errored) {
    return { kind: "unknown", pid, reason: "owned process root exited before cleanup could prove ownership" };
  }

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
      // A live ChildProcess still owns the direct root handle. Closing that
      // handle is safe even when taskkill could not prove the descendant
      // result; it never authorizes a PID-only fallback after root exit.
      if (isAlive(child)) {
        try { child.kill(); } catch { /* retain the typed uncertainty */ }
        await waitForClose(child, graceMs);
      }
      return command;
    }
    const pipeWait = await waitForClose(child, graceMs);
    if (pipeWait === "closed") return { kind: "terminated", pid };
    if (pipeWait === "error") return { kind: "unknown", pid, reason: "owned process emitted an error during termination" };
    return { kind: "timeout", pid, phase: "pipes" };
  }

  const softSignal = signalProcessGroup(pid, "SIGTERM", child);
  if (softSignal === "absent") {
    return { kind: "unknown", pid, reason: "owned process group disappeared before cleanup could prove ownership" };
  }
  const softWait = await waitForProcessGroupGone(pid, graceMs);
  if (softWait === "gone") return { kind: "terminated", pid };
  if (softWait === "error") return { kind: "unknown", pid, reason: "owned process group state became unavailable" };
  const hardSignal = signalProcessGroup(pid, "SIGKILL", child);
  if (hardSignal === "absent") {
    return { kind: "unknown", pid, reason: "owned process group disappeared during cleanup" };
  }
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
    return "error";
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
      // Do not add another wait after the bounded command deadline. The
      // target's state is still uncertain and is reported as timeout; a
      // later owner reconciliation may retry with stronger evidence.
      complete({ kind: "timeout" });
    }, timeoutMs);
    killer.once("error", error => complete({ kind: "error", message: error.message }));
    killer.once("close", code => complete({ kind: "closed", code }));
  });
  if (outcome.kind === "timeout") return { kind: "timeout", pid, phase: "command" };
  if (outcome.kind === "error") return { kind: "unknown", pid, reason: `taskkill could not start: ${outcome.message}` };
  if (outcome.code !== 0) return { kind: "unknown", pid, reason: "taskkill returned a nonzero exit code", exitCode: outcome.code };
  return { kind: "terminated", pid };
}
