#!/usr/bin/env node

import { spawnOwnedProcess, terminateOwnedProcess, type OwnedProcessTermination } from "./owned-process.js";

const executable = process.env.GRAPHCODER_RUNTIME;
const args = process.argv.slice(2);

if (executable === undefined || executable.trim() === "") {
  process.stderr.write("GRAPHCODER_RUNTIME must name the installed graphcoder-runtime executable\n");
  process.exitCode = 2;
} else if (!args.some(value => value === "--model-fixture" || value.startsWith("--model-fixture="))) {
  process.stderr.write("the native runtime requires an explicit --model-fixture\n");
  process.exitCode = 2;
} else {
  // Do not inherit host credentials or ambient provider settings. The native
  // runtime receives only explicit command-line configuration.
  const child = spawnOwnedProcess(executable, args, {
    env: {},
    shell: false,
    stdio: "inherit",
  });
  let finished = false;
  let cleanupPromise: Promise<OwnedProcessTermination> | undefined;
  let cleanupReported = false;
  const cleanup = (): Promise<OwnedProcessTermination> => {
    cleanupPromise ??= terminateOwnedProcess(child);
    return cleanupPromise;
  };
  const surfaceCleanup = (outcome: OwnedProcessTermination): void => {
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
    // A naturally exited root no longer has a recoverable Node handle. The
    // generic owner cannot safely infer descendant ownership from its PID;
    // native hosts must inject the platform owner when descendant cleanup is
    // required. Signal paths above still await the shared cleanup operation.
  });
}
