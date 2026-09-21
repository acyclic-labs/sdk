#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");

let executable;
try {
  // Installation is the only phase allowed to mutate the package directory.
  // Runtime verification is deliberately read-only because Codex and other
  // hosts may expose installed plugins through immutable package stores.
  executable = require("./install.js").installedExecutable();
} catch (error) {
  console.error(`Unable to verify the bundled Acyclic binary: ${error.message}`);
  process.exit(1);
}
const result = spawnSync(executable, process.argv.slice(2), { stdio: "inherit" });
if (result.error) {
  console.error(`Unable to launch the bundled Acyclic binary: ${result.error.message}`);
  process.exit(1);
}
process.exit(result.status === null ? 1 : result.status);
