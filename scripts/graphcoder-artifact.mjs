#!/usr/bin/env node

// Emit the exact artifact record consumed by the qualification receipt. This
// records bytes and source provenance at the packaging boundary; it does not
// build or publish an artifact.

import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HEX40 = /^[0-9a-f]{40}$/;

function fail(message) {
  throw new Error(`graphcoder-artifact: ${message}`);
}

function git(cwd, ...args) {
  return execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8" }).trim();
}

function regularFile(path) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata || !metadata.isFile() || metadata.isSymbolicLink()) fail(`artifact must be an existing regular file: ${path}`);
}

function timestamp(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) fail("built_at must be an ISO timestamp");
  return parsed.toISOString();
}

export function describeArtifact({ path, sourceCwd = process.cwd(), buildId, builtAt, fresh = true, gitOps = { git } }) {
  if (typeof path !== "string" || path.trim() === "") fail("path must be nonempty");
  const artifactPath = resolve(path);
  if (!existsSync(artifactPath)) fail(`artifact does not exist: ${artifactPath}`);
  regularFile(artifactPath);
  if (typeof buildId !== "string" || buildId.trim() === "") fail("build_id must be nonempty");
  if (typeof fresh !== "boolean") fail("fresh must be boolean");
  const cwd = resolve(sourceCwd);
  const sourceCommit = gitOps.git(cwd, "rev-parse", "HEAD");
  const sourceTree = gitOps.git(cwd, "rev-parse", "HEAD^{tree}");
  if (!/^[0-9a-f]{40}$/.test(sourceCommit) || !HEX40.test(sourceTree)) fail("source repository returned invalid commit/tree");
  const built_at = timestamp(builtAt ?? new Date().toISOString());
  return {
    path: artifactPath,
    sha256: createHash("sha256").update(readFileSync(artifactPath)).digest("hex"),
    source_commit: sourceCommit,
    source_tree: sourceTree,
    built_at,
    build_id: buildId.trim(),
    fresh,
  };
}

function usage() {
  console.error("usage: graphcoder-artifact.mjs describe PATH BUILD_ID [BUILT_AT] [SOURCE_CWD]");
  process.exitCode = 2;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [, , command, path, buildId, builtAt, sourceCwd] = process.argv;
    if (command !== "describe" || path === undefined || buildId === undefined || process.argv.length > 7) usage();
    else process.stdout.write(`${JSON.stringify(describeArtifact({ path, buildId, builtAt, sourceCwd }), null, 2)}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
