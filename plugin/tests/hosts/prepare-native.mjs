#!/usr/bin/env node
// Prepares what the Pi host tests import from this checkout's SDK packages:
// the root workspace's locked dependencies (the contract generators use the
// locked @napi-rs/cli dev dependency, never a global install), the generated
// filesystem and actors contracts, the compiled actors package, and the native
// filesystem adapter installed as its platform package.
//
//   node plugin/tests/hosts/prepare-native.mjs
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit", shell: process.platform === "win32" && command !== "node" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} exited ${result.status}`);
}

run("bun", ["install", "--frozen-lockfile", "--ignore-scripts"]);
// The filesystem package's generation closure: protobuf and Rust-owned
// contracts for it and the actors package it builds on.
run("node", ["scripts/generate.mjs", "filesystem"]);
run("node", ["node_modules/typescript/bin/tsc", "-b", "typescript/packages/actors", "--pretty", "false"]);
run("cargo", ["build", "-p", "acyclic-fs-napi", "--locked"]);

const { version } = JSON.parse(readFileSync(join(root, "typescript/packages/filesystem/package.json"), "utf8"));
const name = `@acyclic-labs/fs-${process.platform}-${process.arch}`;
const directory = join(root, "node_modules", name);
const library = { win32: "acyclic_fs_napi.dll", linux: "libacyclic_fs_napi.so", darwin: "libacyclic_fs_napi.dylib" }[process.platform];
mkdirSync(directory, { recursive: true });
copyFileSync(resolve(root, process.env.CARGO_TARGET_DIR ?? "target", "debug", library), join(directory, "index.node"));
writeFileSync(join(directory, "package.json"), JSON.stringify({ name, version, main: "index.node" }));
