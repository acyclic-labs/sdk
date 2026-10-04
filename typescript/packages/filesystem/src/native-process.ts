import { EventEmitter } from "node:events";
import type { ChildProcess, SpawnOptions } from "node:child_process";
import { PassThrough, Writable } from "node:stream";

/** Versioned capability name exchanged by the native companion. */
export const NATIVE_PROCESS_OWNER_CAPABILITY = "acyclic.native-process-owner.v1";
export const NATIVE_PROCESS_OWNER_VERSION = "0.2.0";

/** The only cleanup outcomes a host may expose to a caller. */
export type NativeProcessTermination =
  | { readonly kind: "terminated"; readonly pid: number }
  | { readonly kind: "timeout"; readonly pid: number; readonly phase: "command" | "pipes" }
  | { readonly kind: "unknown"; readonly pid: number; readonly reason: string; readonly exitCode?: number | null };

/** Identity returned by the native suspended-spawn path. */
export interface NativeProcessLaunch {
  readonly token: string;
  readonly pid: number;
  /** Native owner retained this token while launch initialization failed. */
  readonly recovery?: NativeProcessLaunchRecovery;
}

/** Structured native authority that must be reconciled after an uncertain launch. */
export interface NativeProcessLaunchRecovery {
  readonly source: string;
  readonly token: string;
  readonly pid?: number;
}

/** Launch failure carrying the retained native owner instead of hiding it in text. */
export class NativeProcessLaunchError extends Error {
  readonly recovery: NativeProcessLaunchRecovery;

  constructor(recovery: NativeProcessLaunchRecovery) {
    super(recovery.source);
    this.name = "NativeProcessLaunchError";
    this.recovery = recovery;
  }
}

/** Native token-scoped stdio operations for a suspended native launch. */
export interface NativeProcessIo {
  readonly launch: (executable: string, args: readonly string[], options: SpawnOptions) => NativeProcessLaunch;
  readonly write: (token: string, bytes: Uint8Array) => void | Promise<void>;
  readonly closeStdin: (token: string) => void;
  readonly pollOutput: (token: string, stream: "stdout" | "stderr") => {
    readonly kind: "idle" | "data" | "eof" | "error";
    readonly bytes?: Uint8Array;
    readonly reason?: string;
  };
  readonly pollExit: (token: string) => { readonly kind: "running" | "exited"; readonly code?: number | null };
  readonly terminate: (token: string) => NativeProcessTermination;
}

/** Structural owner returned to a Node host without importing GraphCoder. */
export interface NativeProcessOwnerAdapter {
  readonly spawn: (executable: string, args: readonly string[], options: SpawnOptions) => ChildProcess;
  readonly terminate: (child: ChildProcess, graceMs?: number) => Promise<NativeProcessTermination>;
}

const NATIVE_STREAM_HIGH_WATER_MARK = 64 * 1024;

/**
 * Bridges token-scoped native I/O to a ChildProcess-shaped host boundary.
 * Filesystem owns this platform adapter; GraphCoder only consumes its
 * structural spawn/terminate surface.
 */
export function createNativeProcessOwnerAdapter(io: NativeProcessIo): NativeProcessOwnerAdapter {
  const tokens = new WeakMap<ChildProcess, string>();
  const states = new WeakMap<ChildProcess, {
    timer: ReturnType<typeof setInterval> | undefined;
    closed: boolean;
    stopping: boolean;
    failed: boolean;
    termination: Promise<NativeProcessTermination> | undefined;
    terminal: NativeProcessTermination | undefined;
    stdoutBlocked: boolean;
    stderrBlocked: boolean;
    resume: () => void;
    finish: () => void;
  }>();
  const owner: NativeProcessOwnerAdapter = {
    spawn(executable, args, options) {
      const launch = io.launch(executable, args, options);
      const child = new EventEmitter() as ChildProcess;
      Object.defineProperties(child, {
        pid: { value: launch.pid, enumerable: true },
        exitCode: { writable: true, value: null, enumerable: true },
        signalCode: { writable: true, value: null, enumerable: true },
      });
      const stdin = new Writable({
        write(chunk, _encoding, callback) {
          try {
            Promise.resolve(io.write(launch.token, new Uint8Array(chunk))).then(
              () => callback(),
              error => callback(error instanceof Error ? error : new Error(String(error))),
            );
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
      Object.defineProperties(child, { stdin: { value: stdin }, stdout: { value: stdout }, stderr: { value: stderr } });
      const state = {
        timer: undefined as ReturnType<typeof setInterval> | undefined,
        closed: false,
        stopping: false,
        failed: false,
        termination: undefined as Promise<NativeProcessTermination> | undefined,
        terminal: undefined as NativeProcessTermination | undefined,
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
      const reconcileNaturalExit = (): void => {
        // Root exit is only an observation. Start reconciliation before
        // waiting for both pipes: a descendant can keep a captured pipe open.
        // Close is emitted after proven termination or explicit uncertainty,
        // so a caller never waits forever while the token remains registered.
        if (state.termination !== undefined) return;
        const operation = Promise.resolve().then(() => {
          const result = io.terminate(launch.token);
          return result.kind === "terminated"
            ? { kind: "terminated", pid: child.pid ?? -1 } satisfies NativeProcessTermination
            : result;
        }).catch(error => ({
          kind: "unknown",
          pid: child.pid ?? -1,
          reason: error instanceof Error ? error.message : String(error),
        } satisfies NativeProcessTermination));
        state.termination = operation;
        void operation.then(result => {
          if (result.kind === "terminated") {
            tokens.delete(child);
            state.terminal = result;
          }
          if (state.termination === operation) state.termination = undefined;
          state.finish();
        });
      };
      const failAndTerminate = (message: string): void => {
        state.failed = true;
        state.stopping = true;
        if (state.timer !== undefined) {
          clearInterval(state.timer);
          state.timer = undefined;
        }
        if (state.termination !== undefined) return;
        const operation = Promise.resolve().then(() => {
          const result = io.terminate(launch.token);
          return result.kind === "terminated"
            ? { kind: "terminated", pid: child.pid ?? -1 } satisfies NativeProcessTermination
            : result;
        }).catch(error => ({
          kind: "unknown",
          pid: child.pid ?? -1,
          reason: error instanceof Error ? error.message : String(error),
        } satisfies NativeProcessTermination));
        state.termination = operation;
        // Claim the one cleanup operation before notifying listeners. An
        // error listener may synchronously request termination again.
        child.emit("error", new Error(message));
        void operation.then(result => {
          if (result.kind === "terminated") {
            tokens.delete(child);
            state.terminal = result;
            state.finish();
          } else if (state.termination === operation) {
            state.termination = undefined;
            // The caller must observe explicit uncertainty and retain the
            // token for a later retry; do not leave the ChildProcess hanging
            // forever after a reader failure.
            state.finish();
          }
        });
      };
      const poll = (): void => {
        if (state.closed || state.stopping) return;
        try {
          for (const stream of ["stdout", "stderr"] as const) {
            const target = stream === "stdout" ? stdout : stderr;
            if (stream === "stdout" ? state.stdoutBlocked : state.stderrBlocked) continue;
            const value = io.pollOutput(launch.token, stream);
            if (value.kind === "data" && value.bytes !== undefined) {
              if (value.bytes.byteLength > NATIVE_STREAM_HIGH_WATER_MARK) {
                failAndTerminate(`${stream} output chunk exceeds the bounded stream limit`);
                return;
              }
              if (!target.write(Buffer.from(value.bytes))) {
                if (stream === "stdout") state.stdoutBlocked = true;
                else state.stderrBlocked = true;
                target.once("drain", () => {
                  if (stream === "stdout") state.stdoutBlocked = false;
                  else state.stderrBlocked = false;
                });
              }
            }
            if (value.kind === "error") {
              failAndTerminate(value.reason ?? `${stream} read failed`);
              return;
            }
            if (value.kind === "eof") {
              target.end();
              if (stream === "stdout") stdoutDone = true;
              else stderrDone = true;
            }
          }
          const exit = io.pollExit(launch.token);
          if (exit.kind === "exited") {
            (child as ChildProcess & { exitCode: number | null }).exitCode = exit.code ?? null;
            reconcileNaturalExit();
          }
        } catch (error) {
          failAndTerminate(error instanceof Error ? error.message : String(error));
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
    terminate(child, graceMs = 250) {
      const state = states.get(child);
      if (state?.terminal !== undefined) return Promise.resolve(state.terminal);
      const token = tokens.get(child);
      if (token === undefined) {
        if (state?.termination !== undefined) return state.termination;
        return Promise.resolve({ kind: "unknown", pid: child.pid ?? -1, reason: "native owner token is unavailable" });
      }
      if (state?.termination !== undefined) return state.termination;
      let operation: Promise<NativeProcessTermination>;
      operation = Promise.resolve().then(async () => {
        if (state !== undefined) {
          state.stopping = true;
          if (state.timer !== undefined) {
            clearInterval(state.timer);
            state.timer = undefined;
          }
        }
        const deadline = Date.now() + Math.max(0, graceMs);
        let result: NativeProcessTermination;
        try {
          result = io.terminate(token);
          while (result.kind !== "terminated" && Date.now() < deadline) {
            await new Promise<void>(resolve => setTimeout(resolve, 10));
            result = io.terminate(token);
          }
        } catch (error) {
          if (state !== undefined && !state.failed) { state.stopping = false; state.resume(); }
          return { kind: "unknown", pid: child.pid ?? -1, reason: error instanceof Error ? error.message : String(error) };
        }
        if (result.kind !== "terminated") {
          if (state !== undefined && !state.failed) { state.stopping = false; state.resume(); }
          return result.kind === "timeout"
            ? { kind: "timeout", pid: child.pid ?? -1, phase: "command" }
            : { kind: "unknown", pid: child.pid ?? -1, reason: result.reason ?? "native cleanup is uncertain" };
        }
        // Retire the token before emitting close/exit so a re-entrant close
        // listener cannot dispatch a second native termination request.
        tokens.delete(child);
        if (state !== undefined) state.terminal = { kind: "terminated", pid: child.pid ?? -1 };
        state?.finish();
        return { kind: "terminated", pid: child.pid ?? -1 };
      });
      if (state !== undefined) {
        state.termination = operation;
        void operation.then(result => {
          if (result.kind !== "terminated" && state.termination === operation) state.termination = undefined;
        });
      }
      return operation;
    },
  };
  return owner;
}

/**
 * Node-facing owner supplied by the native companion.
 *
 * The adapter deliberately keeps the process handle as Node's ChildProcess:
 * GraphCoder can use one owner contract for JSON-lines, terminal, and native
 * hosts while the companion retains the platform-specific Job/process-group
 * handle behind this surface.
 */
export interface NativeProcessOwnerBinding {
  readonly capability: typeof NATIVE_PROCESS_OWNER_CAPABILITY;
  readonly version: string;
  readonly spawn: (executable: string, args: readonly string[], options: SpawnOptions) => ChildProcess;
  readonly terminate: (child: ChildProcess, graceMs?: number) => Promise<NativeProcessTermination>;
  /** Launches without inheriting environment or host stdio. */
  readonly launch?: (
    executable: string,
    args: readonly string[],
    options: SpawnOptions,
  ) => NativeProcessLaunch;
  readonly io?: NativeProcessIo;
}

/** Structural owner contract accepted by GraphCoder's process bridge. */
export interface NativeProcessOwner {
  readonly spawn: NativeProcessOwnerBinding["spawn"];
  readonly terminate: NativeProcessOwnerBinding["terminate"];
  readonly launch?: NativeProcessOwnerBinding["launch"];
  readonly io?: NativeProcessIo;
}

/**
 * Validates and freezes the native process owner exported by a companion.
 * There is no Node fallback here: a caller asking for native ownership must
 * receive that capability or an explicit unsupported error.
 */
export function createNativeProcessOwner(binding: unknown): NativeProcessOwner {
  if (typeof binding !== "object" || binding === null) {
    throw new Error("native companion did not export a process owner");
  }
  const candidate = binding as Partial<NativeProcessOwnerBinding>;
  if (candidate.capability !== NATIVE_PROCESS_OWNER_CAPABILITY) {
    throw new Error("native companion does not provide owned process capability");
  }
  if (candidate.version !== NATIVE_PROCESS_OWNER_VERSION) {
    throw new Error("native process owner version does not match @acyclic-labs/fs");
  }
  if (typeof candidate.spawn !== "function" || typeof candidate.terminate !== "function") {
    throw new Error("native companion process owner is incomplete");
  }
  return Object.freeze({
    spawn: candidate.spawn,
    terminate: candidate.terminate,
    ...(typeof candidate.launch === "function" ? { launch: candidate.launch } : {}),
    ...(candidate.io !== undefined ? { io: candidate.io } : {}),
  });
}
