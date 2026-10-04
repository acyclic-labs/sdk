#!/usr/bin/env node

import { spawnOwnedProcess, terminateOwnedProcess } from "./owned-process.js";

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
  const cleanup = (): void => {
    if (finished) return;
    void terminateOwnedProcess(child);
  };
  process.once("SIGINT", cleanup);
  process.once("SIGTERM", cleanup);
  child.once("error", error => {
    finished = true;
    process.stderr.write(`failed to start graphcoder-runtime: ${error.message}\n`);
    process.exitCode = 1;
  });
  child.once("close", (code, signal) => {
    finished = true;
    process.removeListener("SIGINT", cleanup);
    process.removeListener("SIGTERM", cleanup);
    process.exitCode = code ?? (signal === null ? 1 : 1);
  });
}
