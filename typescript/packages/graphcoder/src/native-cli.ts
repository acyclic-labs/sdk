#!/usr/bin/env node

import { openNativeProcessOwner } from "@acyclic-labs/fs/native";
import type { NativeProcessOwner, NativeProcessTermination } from "@acyclic-labs/fs/native-process";

const executable = process.env.GRAPHCODER_RUNTIME;
const args = process.argv.slice(2);

async function run(): Promise<void> {
  if (executable === undefined || executable.trim() === "") {
    process.stderr.write("GRAPHCODER_RUNTIME must name the installed graphcoder-runtime executable\n");
    process.exitCode = 2;
    return;
  }
  if (!args.some(value => value === "--model-fixture" || value.startsWith("--model-fixture="))) {
    process.stderr.write("the native runtime requires an explicit --model-fixture\n");
    process.exitCode = 2;
    return;
  }

  // Production terminal execution requires the native owner before any
  // runtime process is started. A bounded Node fallback cannot prove that
  // descendants were cleaned up, so an unavailable companion is fatal here.
  let owner: NativeProcessOwner;
  try {
    owner = await openNativeProcessOwner();
  } catch (error) {
    process.stderr.write(`native process owner unavailable: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
    return;
  }

  // Do not inherit host credentials or ambient provider settings. The native
  // runtime receives only explicit command-line configuration.
  const child = owner.spawn(executable, args, {
    env: {},
    shell: false,
    stdio: "inherit",
    windowsHide: true,
  });
  let finished = false;
  let cleanupPromise: Promise<NativeProcessTermination> | undefined;
  let cleanupReported = false;
  const cleanup = (): Promise<NativeProcessTermination> => {
    cleanupPromise ??= owner.terminate(child);
    return cleanupPromise;
  };
  const surfaceCleanup = (outcome: NativeProcessTermination): void => {
    if (cleanupReported) return;
    cleanupReported = true;
    if (outcome.kind === "terminated") return;
    process.stderr.write(`runtime process cleanup ${outcome.kind}\n`);
    if (process.exitCode === undefined || process.exitCode === 0) process.exitCode = 1;
  };
  const onSignal = (): void => {
    if (finished) return;
    void cleanup().then(surfaceCleanup);
  };
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  child.once("error", error => {
    finished = true;
    process.stderr.write(`failed to start graphcoder-runtime: ${error.message}\n`);
    process.exitCode = 1;
  });
  child.once("close", async (code, signal) => {
    finished = true;
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
    process.exitCode = code ?? (signal === null ? 1 : 1);
    surfaceCleanup(await cleanup());
  });
}

await run();
