#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const { join } = require("node:path");

const executable = join(__dirname, process.platform === "win32" ? "acyclic.exe" : "acyclic");
try {
  require("./install.js").ensureInstalled();
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
