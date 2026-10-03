#!/usr/bin/env node

import { spawn } from "node:child_process";

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
  const child = spawn(executable, args, {
    env: {},
    shell: false,
    stdio: "inherit",
    windowsHide: true,
  });
  child.once("error", error => {
    process.stderr.write(`failed to start graphcoder-runtime: ${error.message}\n`);
    process.exitCode = 1;
  });
  child.once("exit", (code, signal) => {
    process.exitCode = code ?? (signal === null ? 1 : 1);
  });
}
