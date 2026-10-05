#!/usr/bin/env node

// Thin compatibility launcher. Package archive membership, checksums, source
// revisions, and installed-consumer qualification are owned by the Rust
// sdk-generation entrypoint; this wrapper only preserves the release lane's
// existing command shape.
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const artifactRoot = process.argv[2];
const sourceRoot = resolve(process.argv[3] ?? dirname(dirname(fileURLToPath(import.meta.url))));
if (!artifactRoot) {
  console.error("usage: write-package-qualification-manifest.mjs ABSOLUTE_PACKAGE_ROOT [ABSOLUTE_SOURCE_ROOT]");
  process.exit(2);
}

const result = spawnSync(
  process.env.CARGO ?? "cargo",
  [
    "run",
    "--manifest-path",
    resolve(sourceRoot, "rust/crates/sdk-generation/Cargo.toml"),
    "--locked",
    "--bin",
    "sdk-generation",
    "--",
    "package-manifest",
    "--source-root",
    sourceRoot,
    "--output",
    resolve(artifactRoot),
  ],
  { cwd: sourceRoot, stdio: "inherit" },
);
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);