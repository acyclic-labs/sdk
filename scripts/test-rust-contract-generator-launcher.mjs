import assert from "node:assert/strict";
import test from "node:test";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runRustContractGenerator } from "./run-rust-contract-generator.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const buildRoot = resolve(repositoryRoot, "..", ".test-rust-contract-launcher");

function fakeCargoPath() {
  const path = join(buildRoot, "fake-cargo.mjs");
  writeFileSync(path, "process.exitCode = 0;\n");
  return path;
}

function withLauncherEnvironment(callback) {
  const previous = {
    SDK_BUILD_ROOT: process.env.SDK_BUILD_ROOT,
    ACYCLIC_SDK_WORK_ROOT: process.env.ACYCLIC_SDK_WORK_ROOT,
    CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR,
    ACYCLIC_CARGO_BIN: process.env.ACYCLIC_CARGO_BIN,
  };
  process.env.SDK_BUILD_ROOT = buildRoot;
  delete process.env.ACYCLIC_SDK_WORK_ROOT;
  delete process.env.CARGO_TARGET_DIR;
    delete process.env.ACYCLIC_CARGO_BIN;
  try {
    return callback();
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("contract generation retains one shared Cargo target and cleans owned output", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  withLauncherEnvironment(() => {
    const sharedTarget = join(buildRoot, "acyclic-sdk-contracts", "cargo-target");
    mkdirSync(sharedTarget, { recursive: true });
    const sentinel = join(sharedTarget, "must-survive");
    writeFileSync(sentinel, "shared target");

    runRustContractGenerator(repositoryRoot, "all-write", undefined, {
      cargoProgram: process.execPath,
      cargoArgs: [fakeCargoPath()],
    });

    assert.equal(existsSync(sentinel), true, "the shared target must be reusable");
    const children = requireChildren(join(buildRoot, "acyclic-sdk-contracts"));
    assert.deepEqual(children, ["cargo-target"], "owned output staging must be cleaned");
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

test("contract generation rejects output nested inside the Rust source", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  withLauncherEnvironment(() => {
    assert.throws(
      () => runRustContractGenerator(repositoryRoot, "all-write", join(repositoryRoot, "target", "contract-test")),
      /Contract output must be outside the Rust source root/,
    );
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

function requireChildren(path) {
  // Avoid depending on directory enumeration order in the assertion.
  return [...new Set(readdirSync(path))].sort();
}
