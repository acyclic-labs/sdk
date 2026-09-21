#!/usr/bin/env node
"use strict";

const { createHash } = require("node:crypto");
const { existsSync, readFileSync } = require("node:fs");
const { join } = require("node:path");

function linuxLibc(report = process.report?.getReport?.()) {
  return typeof report?.header?.glibcVersionRuntime === "string"
    && report.header.glibcVersionRuntime.length > 0 ? "gnu" : "musl";
}

function hostTarget() {
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  const os = { win32: "win32", darwin: "darwin", linux: "linux" }[process.platform];
  if (!arch || !os) throw new Error(`unsupported Acyclic platform: ${process.platform}/${process.arch}`);
  return os === "linux" ? `${os}-${arch}-${linuxLibc()}` : `${os}-${arch}`;
}

function targetExecutables(directory) {
  const schema = JSON.parse(readFileSync(join(directory, "targets.json"), "utf8"));
  if (schema.version !== 1 || !schema.targets || Array.isArray(schema.targets)) {
    throw new Error("unsupported Acyclic target schema");
  }
  return schema.targets;
}

function verifyTarget(directory, target = hostTarget()) {
  const manifestPath = join(directory, "platform-binaries.json");
  if (!existsSync(manifestPath)) throw new Error("Acyclic platform manifest is missing");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (manifest.version !== 1 || typeof manifest.targets !== "object") {
    throw new Error("unsupported Acyclic platform manifest");
  }
  const targets = targetExecutables(directory);
  const entry = manifest.targets[target];
  if (!entry) throw new Error(`@acyclic-labs/plugin does not contain the ${target} binary`);
  const executable = targets[target];
  if (!executable) throw new Error(`unsupported Acyclic target: ${target}`);
  const expected = `${target}/${executable}`;
  if (entry.path !== expected) throw new Error("invalid Acyclic binary path in platform manifest");
  const source = join(directory, ...entry.path.split("/"));
  if (!existsSync(source)) throw new Error(`Acyclic binary is missing: ${entry.path}`);
  const contents = readFileSync(source);
  const actual = createHash("sha256").update(contents).digest("hex");
  if (actual !== entry.sha256) throw new Error("Acyclic binary checksum verification failed");
  return { source, manifest };
}

module.exports = { hostTarget, linuxLibc, verifyTarget };

if (require.main === module) {
  verifyTarget(__dirname, process.argv[2]);
}
