import type { ChildProcess, SpawnOptions } from "node:child_process";

/** Versioned capability name exchanged by the native companion. */
export const NATIVE_PROCESS_OWNER_CAPABILITY = "acyclic.native-process-owner.v1";
export const NATIVE_PROCESS_OWNER_VERSION = "0.2.0";

/** The only cleanup outcomes a host may expose to a caller. */
export type NativeProcessTermination =
  | { readonly kind: "terminated"; readonly pid: number }
  | { readonly kind: "timeout"; readonly pid: number; readonly phase: "command" | "pipes" }
  | { readonly kind: "unknown"; readonly pid: number; readonly reason: string; readonly exitCode?: number | null };

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
}

/** Structural owner contract accepted by GraphCoder's process bridge. */
export interface NativeProcessOwner {
  readonly spawn: NativeProcessOwnerBinding["spawn"];
  readonly terminate: NativeProcessOwnerBinding["terminate"];
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
  });
}
