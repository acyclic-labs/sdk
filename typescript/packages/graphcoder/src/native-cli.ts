#!/usr/bin/env node

import { spawn } from "node:child_process";
import { OwnedChild, installOwnedChildSignalCleanup } from "./owned-child.js";

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
  try {
    const child = new OwnedChild(spawn(executable, args, {
      env: {},
      shell: false,
      stdio: "inherit",
      windowsHide: true,
    }));
    const uninstallSignalCleanup = installOwnedChildSignalCleanup(child);
    try {
      const outcome = await child.waitForClose();
      if (outcome.kind === "error") {
        process.stderr.write(`failed to start graphcoder-runtime: ${outcome.error.message}\n`);
        process.exitCode = 1;
      } else {
        process.exitCode = outcome.code ?? 1;
      }
    } finally {
      uninstallSignalCleanup();
    }
  } catch (error) {
    process.stderr.write(`failed to start graphcoder-runtime: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
