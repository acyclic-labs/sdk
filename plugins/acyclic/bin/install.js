#!/usr/bin/env node
"use strict";

const { chmodSync, copyFileSync } = require("node:fs");
const { join } = require("node:path");
const { verifyTarget } = require("./verify.js");

const { source } = verifyTarget(__dirname);
const installed = join(__dirname, process.platform === "win32" ? "acyclic.exe" : "acyclic");
copyFileSync(source, installed);
if (process.platform !== "win32") chmodSync(installed, 0o755);
