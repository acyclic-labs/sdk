#!/usr/bin/env node

import type { ChildProcess } from "node:child_process";
import {
  defaultOwnedProcessOwner,
  getOwnedProcessRecovery,
  type OwnedProcessOwner,
  type OwnedProcessTermination,
} from "./owned-process.js";
import { openDefaultNodeProcessOwner } from "./node.js";

const executable = process.env.GRAPHCODER_RUNTIME;
const args = process.argv.slice(2);

/**
 * Installed local runs use the filesystem companion's native ownership
 * boundary. The Node owner is an explicit source-test escape hatch; it never
 * claims descendant cleanup after a natural root exit.
 */
async function openProcessOwner(): Promise<OwnedProcessOwner> {
  if (process.env.GRAPHCODER_PROCESS_OWNER === "node") return defaultOwnedProcessOwner;
  return openDefaultNodeProcessOwner();
}

async function run(): Promise<number> {
  if (executable === undefined || executable.trim() === "") {
    process.stderr.write("GRAPHCODER_RUNTIME must name the installed graphcoder-runtime executable\n");
    return 2;
  }
  if (!args.some(value => value === "--model-fixture" || value.startsWith("--model-fixture="))) {
    process.stderr.write("the native runtime requires an explicit --model-fixture\n");
    return 2;
  }

  let owner: OwnedProcessOwner;
  try {
    owner = await openProcessOwner();
  } catch (error) {
    process.stderr.write(`failed to open native process owner: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }

  // Do not inherit host credentials or ambient provider settings. The native
  // runtime receives only explicit command-line configuration.
  let child: ChildProcess;
  try {
    child = owner.spawn(executable, args, {
      env: {},
      shell: false,
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
  } catch (error) {
    const recovery = getOwnedProcessRecovery(error);
    if (recovery !== undefined && owner.recoverLaunch !== undefined) {
      try {
        const outcome = await owner.recoverLaunch(recovery);
        if (outcome.kind !== "terminated") {
          process.stderr.write(`runtime launch cleanup ${outcome.kind}\n`);
        }
      } catch (cleanupError) {
        process.stderr.write(`runtime launch cleanup unknown: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}\n`);
      }
    } else if (recovery !== undefined) {
      process.stderr.write("runtime launch cleanup unknown: owner cannot reconcile retained launch\n");
    }
    process.stderr.write(`failed to start graphcoder-runtime: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
  const input = child.stdin;
  if (input !== null) process.stdin.pipe(input);
  child.stdout?.pipe(process.stdout);
  child.stderr?.pipe(process.stderr);

  let finished = false;
  let interrupted = false;
  let cleanupPromise: Promise<OwnedProcessTermination> | undefined;
  let cleanupReported = false;
  const cleanup = (): Promise<OwnedProcessTermination> => {
    cleanupPromise ??= owner.terminate(child);
    return cleanupPromise;
  };
  const surfaceCleanup = (outcome: OwnedProcessTermination): void => {
    if (cleanupReported) return;
    cleanupReported = true;
    if (outcome.kind === "terminated") return;
    process.stderr.write(`runtime process cleanup ${outcome.kind}\n`);
  };
  const onSignal = (): void => {
    if (finished) return;
    interrupted = true;
    void cleanup();
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  const close = new Promise<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  try {
    const exit = await close;
    finished = true;
    // The native owner retains a Job/process-group token after the direct
    // close. Reconcile that token before reporting a successful natural exit;
    // its result proves descendants and bounded pipes are disposed. The
    // explicit Node fallback has no durable owner after close, so it leaves
    // descendant cleanup unclaimed instead of upgrading `unknown` to success.
    const cleanupOutcome = interrupted || process.env.GRAPHCODER_PROCESS_OWNER !== "node"
      ? await cleanup()
      : undefined;
    if (cleanupOutcome !== undefined) surfaceCleanup(cleanupOutcome);
    return cleanupOutcome !== undefined && cleanupOutcome.kind !== "terminated"
      ? 1
      : (exit.code ?? 1);
  } catch (error) {
    finished = true;
    process.stderr.write(`failed to start graphcoder-runtime: ${error instanceof Error ? error.message : String(error)}\n`);
    const cleanupOutcome = await cleanup();
    surfaceCleanup(cleanupOutcome);
    return 1;
  } finally {
    if (input !== null) process.stdin.unpipe(input);
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
  }
}

process.exitCode = await run();
